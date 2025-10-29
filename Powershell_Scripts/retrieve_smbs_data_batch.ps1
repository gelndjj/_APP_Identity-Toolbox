# --- CONFIGURATION ---
Connect-ExchangeOnline -ShowProgress $true
$TargetUserUPN = (Get-ConnectionInformation).UserPrincipalName
Write-Host "Detected signed-in admin: $TargetUserUPN" -ForegroundColor Cyan

if (-not $TargetUserUPN) {
    Write-Host "No UPN provided. Exiting." -ForegroundColor Red
    exit
}

# Resolve script directory cross-platform (Windows/macOS/Linux)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# --- OUTPUT PATHS ---
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"

# ✅ Folder for Exchange reports
$ReportFolder = Join-Path (Split-Path -Parent $ScriptDir) "Database_Exchange"
if (!(Test-Path $ReportFolder)) {
    New-Item -ItemType Directory -Path $ReportFolder | Out-Null
}

$ReportPath = Join-Path $ReportFolder ("{0}_ExchangeReport.csv" -f $timestamp)
$ExportListPath = Join-Path $ReportFolder ("{0}_SharedMailboxes_List.csv" -f $timestamp)

Write-Host "Reports will be saved under: $ReportFolder" -ForegroundColor DarkCyan

# --- CONNECT ---
try {
    Connect-ExchangeOnline -ShowProgress $true -ErrorAction Stop
    Write-Host "Connected to Exchange Online." -ForegroundColor Green
} catch {
    Write-Host "Failed to connect to Exchange Online: $_" -ForegroundColor Red
    exit
}

try {
    Connect-MgGraph -Scopes "Mail.Read.Shared" -ErrorAction Stop
    Write-Host "Connected to Microsoft Graph." -ForegroundColor Green
} catch {
    Write-Host "Failed to connect to Graph API: $_" -ForegroundColor Red
    Disconnect-ExchangeOnline -Confirm:$false
    exit
}

$startTime = Get-Date

# --- EXPORT SHARED MAILBOX LIST ---
$SharedMailboxes = Get-Mailbox -RecipientTypeDetails SharedMailbox | Select-Object DisplayName, PrimarySmtpAddress
$SharedMailboxes | Export-Csv -Path $ExportListPath -NoTypeInformation -Encoding UTF8
Write-Host "Exported mailbox list to: $ExportListPath" -ForegroundColor Cyan

# --- GRANT FULL ACCESS ---
foreach ($Mailbox in $SharedMailboxes) {
    try {
        Add-MailboxPermission -Identity $Mailbox.PrimarySmtpAddress -User $TargetUserUPN -AccessRights FullAccess -InheritanceType All -AutoMapping:$false
        Write-Host "Granted Full Access to $($Mailbox.PrimarySmtpAddress)" -ForegroundColor Green
    } catch {
        Write-Warning "Failed to grant access to $($Mailbox.PrimarySmtpAddress): $_"
    }
}

Write-Host "Waiting 60 seconds to ensure permission replication..." -ForegroundColor DarkCyan
Start-Sleep -Seconds 60

# --- GENERATE REPORT ---
$Results = @()
foreach ($Mailbox in $SharedMailboxes) {
    $Email = $Mailbox.PrimarySmtpAddress
    Write-Host "Processing mailbox: $Email" -ForegroundColor Cyan

    $SubjectSent = "No sent emails found"
    $SentDate = "N/A"
    $SentBy = "N/A"
    $Recipients = "N/A"
    $SubjectReceived = "No emails"
    $ReceivedDate = "N/A"
    $IsRead = "Unknown"

    try {
        $FullAccessUsers = (Get-MailboxPermission -Identity $Email | Where-Object { $_.AccessRights -contains "FullAccess" -and -not $_.IsInherited }).User -join "; "
        $SendAsUsers = (Get-RecipientPermission -Identity $Email | Where-Object { $_.AccessRights -contains "SendAs" }).Trustee -join "; "
    } catch {
        $FullAccessUsers = "Error"
        $SendAsUsers = "Error"
    }

    try {
        $EncodedEmail = [System.Web.HttpUtility]::UrlEncode($Email)
        $UriSent = "https://graph.microsoft.com/v1.0/users/$EncodedEmail/mailFolders/SentItems/messages?`$orderby=sentDateTime desc&`$top=1"
        $SentResult = Invoke-MgGraphRequest -Method GET -Uri $UriSent
        if ($SentResult.value.Count -gt 0) {
            $SentEmail = $SentResult.value[0]
            $SubjectSent = $SentEmail.subject
            $SentDate = $SentEmail.sentDateTime
            $SentBy = $SentEmail.sender.emailAddress.address
            $Recipients = ($SentEmail.toRecipients | ForEach-Object { $_.emailAddress.address }) -join ", "
        }
    } catch {
        Write-Warning "Graph API error for sent email of $Email $_"
    }

    try {
        $UriReceived = "https://graph.microsoft.com/v1.0/users/$EncodedEmail/mailFolders/Inbox/messages?`$orderby=receivedDateTime desc&`$top=1"
        $InboxEmail = Invoke-MgGraphRequest -Method GET -Uri $UriReceived
        if ($InboxEmail.value.Count -gt 0) {
            $SubjectReceived = $InboxEmail.value[0].subject
            $ReceivedDate = $InboxEmail.value[0].receivedDateTime
            $IsRead = $InboxEmail.value[0].isRead
        }
    } catch {
        Write-Warning "Graph API error for received email of $Email $_"
    }

    $Results += [PSCustomObject]@{
        "Shared Mailbox"           = $Mailbox.DisplayName
        "Email Address"            = $Email
        "Subject of Last Sent"     = $SubjectSent
        "Sent Date"                = $SentDate
        "Sent By"                  = $SentBy
        "Recipient"                = $Recipients
        "Subject of Last Received" = $SubjectReceived
        "Last Received Date"       = $ReceivedDate
        "Is Last Received Read?"   = $IsRead
        "Full Access Users"        = $FullAccessUsers
        "SendAs Users"             = $SendAsUsers
    }
}

# ✅ Export to the right place with timestamp
$Results | Export-Csv -Path $ReportPath -NoTypeInformation -Encoding UTF8
Write-Host "[✓] Final report saved to: $ReportPath" -ForegroundColor Green

# --- REMOVE FULL ACCESS ---
foreach ($Mailbox in $SharedMailboxes) {
    try {
        $permission = Get-MailboxPermission -Identity $Mailbox.PrimarySmtpAddress | Where-Object {
            $_.User.ToString() -eq $TargetUserUPN -and $_.AccessRights -contains "FullAccess" -and -not $_.IsInherited
        }

        if ($permission) {
            Remove-MailboxPermission -Identity $Mailbox.PrimarySmtpAddress -User $TargetUserUPN -AccessRights FullAccess -InheritanceType All -Confirm:$false
            Write-Host "Removed Full Access from $($Mailbox.PrimarySmtpAddress)" -ForegroundColor Yellow
        } else {
            Write-Host "No Full Access to remove from $($Mailbox.PrimarySmtpAddress)" -ForegroundColor DarkGray
        }
    } catch {
        Write-Warning "Failed to remove access from $($Mailbox.PrimarySmtpAddress): $_"
    }
}

$endTime = Get-Date
$duration = $endTime - $startTime

# --- CLEANUP ---
Disconnect-MgGraph
Disconnect-ExchangeOnline -Confirm:$false
Write-Host "All done. Exiting script." -ForegroundColor Cyan
Write-Host "Total Execution Time: $($duration.ToString())" -ForegroundColor Cyan
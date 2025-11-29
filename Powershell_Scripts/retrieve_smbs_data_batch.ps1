# =========================================================
# Shared Mailboxes Activity Report (EXO + Graph)
# =========================================================

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

# --- Resolve paths ---
$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir    = Split-Path -Parent $ScriptDir

# Output folder for Exchange reports
$ReportFolder = Join-Path $RootDir "Database_Exchange"
if (-not (Test-Path $ReportFolder)) {
    New-Item -ItemType Directory -Path $ReportFolder | Out-Null
}

$timestamp       = Get-Date -Format "yyyyMMdd-HHmmss"
$ReportPath      = Join-Path $ReportFolder ("{0}_ExchangeReport.csv"        -f $timestamp)
$ExportListPath  = Join-Path $ReportFolder ("{0}_SharedMailboxes_List.csv"  -f $timestamp)

Write-Host "Reports will be saved under: $ReportFolder" -ForegroundColor DarkCyan

# --- 1) Connect to Microsoft Graph first (to avoid module conflicts) ---
try {
    Connect-MgGraph -Scopes "Mail.Read.Shared" -NoWelcome -ErrorAction Stop
    Write-Host "Connected to Microsoft Graph." -ForegroundColor Green
}
catch {
    Write-Host "Failed to connect to Graph API: $_" -ForegroundColor Red
    return
}

# --- 2) Connect to Exchange Online ---
try {
    Connect-ExchangeOnline -ShowProgress $true -ErrorAction Stop
    Write-Host "Connected to Exchange Online." -ForegroundColor Green
}
catch {
    Write-Host "Failed to connect to Exchange Online: $_" -ForegroundColor Red
    Disconnect-MgGraph
    return
}

# Who is running this?
$TargetUserUPN = (Get-ConnectionInformation).UserPrincipalName
Write-Host "Detected signed-in admin: $TargetUserUPN" -ForegroundColor Cyan

if (-not $TargetUserUPN) {
    Write-Host "No UPN detected, exiting." -ForegroundColor Red
    Disconnect-MgGraph
    Disconnect-ExchangeOnline -Confirm:$false
    return
}

$startTime = Get-Date

# --- 3) Export list of shared mailboxes ---
$SharedMailboxes = Get-Mailbox -RecipientTypeDetails SharedMailbox |
    Select-Object DisplayName, PrimarySmtpAddress

$SharedMailboxes | Export-Csv -Path $ExportListPath -NoTypeInformation -Encoding UTF8
Write-Host "Exported mailbox list to: $ExportListPath" -ForegroundColor Cyan

# --- 4) Grant temporary Full Access to the running admin ---
foreach ($Mailbox in $SharedMailboxes) {
    try {
        Add-MailboxPermission -Identity $Mailbox.PrimarySmtpAddress `
            -User $TargetUserUPN -AccessRights FullAccess `
            -InheritanceType All -AutoMapping:$false
        Write-Host "Granted Full Access to $($Mailbox.PrimarySmtpAddress)" -ForegroundColor Green
    }
    catch {
        Write-Warning "Failed to grant access to $($Mailbox.PrimarySmtpAddress): $_"
    }
}

Write-Host "Waiting 60 seconds to ensure permission replication..." -ForegroundColor DarkCyan
Start-Sleep -Seconds 60

# --- 5) Build activity report using Graph for last sent/received messages ---
$Results = @()

foreach ($Mailbox in $SharedMailboxes) {

    $Email = $Mailbox.PrimarySmtpAddress
    Write-Host "Processing mailbox: $Email" -ForegroundColor Cyan

    $SubjectSent     = "No sent emails found"
    $SentDate        = "N/A"
    $SentBy          = "N/A"
    $Recipients      = "N/A"
    $SubjectReceived = "No emails"
    $ReceivedDate    = "N/A"
    $IsRead          = "Unknown"

    try {
        $FullAccessUsers = (Get-MailboxPermission -Identity $Email |
            Where-Object { $_.AccessRights -contains "FullAccess" -and -not $_.IsInherited }).User -join "; "
        $SendAsUsers = (Get-RecipientPermission -Identity $Email |
            Where-Object { $_.AccessRights -contains "SendAs" }).Trustee -join "; "
    }
    catch {
        $FullAccessUsers = "Error"
        $SendAsUsers     = "Error"
    }

    try {
        $EncodedEmail = [System.Web.HttpUtility]::UrlEncode($Email)

        # Last sent
        $uriSent = "https://graph.microsoft.com/v1.0/users/$EncodedEmail/mailFolders/SentItems/messages?`$orderby=sentDateTime desc&`$top=1"
        $sentRes = Invoke-MgGraphRequest -Method GET -Uri $uriSent
        if ($sentRes.value.Count -gt 0) {
            $sentMail      = $sentRes.value[0]
            $SubjectSent   = $sentMail.subject
            $SentDate      = $sentMail.sentDateTime
            $SentBy        = $sentMail.sender.emailAddress.address
            $Recipients    = ($sentMail.toRecipients | ForEach-Object { $_.emailAddress.address }) -join ", "
        }
    }
    catch {
        Write-Warning "Graph API error for sent email of $Email $_"
    }

    try {
        # Last received
        $uriReceived = "https://graph.microsoft.com/v1.0/users/$EncodedEmail/mailFolders/Inbox/messages?`$orderby=receivedDateTime desc&`$top=1"
        $recvRes = Invoke-MgGraphRequest -Method GET -Uri $uriReceived
        if ($recvRes.value.Count -gt 0) {
            $lastInbox      = $recvRes.value[0]
            $SubjectReceived = $lastInbox.subject
            $ReceivedDate    = $lastInbox.receivedDateTime
            $IsRead          = $lastInbox.isRead
        }
    }
    catch {
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

$Results | Export-Csv -Path $ReportPath -NoTypeInformation -Encoding UTF8
Write-Host "[✓] Final report saved to: $ReportPath" -ForegroundColor Green

# --- 6) Remove temporary Full Access ---
foreach ($Mailbox in $SharedMailboxes) {
    try {
        $perm = Get-MailboxPermission -Identity $Mailbox.PrimarySmtpAddress |
            Where-Object {
                $_.User.ToString() -eq $TargetUserUPN -and
                $_.AccessRights -contains "FullAccess" -and
                -not $_.IsInherited
            }

        if ($perm) {
            Remove-MailboxPermission -Identity $Mailbox.PrimarySmtpAddress `
                -User $TargetUserUPN -AccessRights FullAccess `
                -InheritanceType All -Confirm:$false
            Write-Host "Removed Full Access from $($Mailbox.PrimarySmtpAddress)" -ForegroundColor Yellow
        }
        else {
            Write-Host "No Full Access to remove from $($Mailbox.PrimarySmtpAddress)" -ForegroundColor DarkGray
        }
    }
    catch {
        Write-Warning "Failed to remove access from $($Mailbox.PrimarySmtpAddress): $_"
    }
}

$endTime  = Get-Date
$duration = $endTime - $startTime
Write-Host "Total Execution Time: $($duration.ToString())" -ForegroundColor Cyan

# --- Cleanup ---
Disconnect-MgGraph
Disconnect-ExchangeOnline -Confirm:$false
Write-Host "All done. Exiting script." -ForegroundColor Cyan
param(
    [string]$UserUPNs,
    [string]$SMBEmails
)

$UserList = $UserUPNs -split ","
$MailboxList = $SMBEmails -split ","

Write-Host "Connecting to Exchange Online..." -ForegroundColor Cyan
Connect-ExchangeOnline -ShowProgress $false

$results = @()

foreach ($mbx in $MailboxList) {
    foreach ($usr in $UserList) {
        $status = "✅ Success"
        try {
            Add-MailboxPermission -Identity $mbx -User $usr `
                -AccessRights FullAccess -InheritanceType All -AutoMapping:$false -ErrorAction Stop

            $status = "✅ Access Granted"
        }
        catch {
            if ($_ -like "*already has permissions*") {
                $status = "⚠ Already Granted"
            } else {
                $status = "❌ Failed: $($_.Exception.Message)"
            }
        }

        $results += [PSCustomObject]@{
            UserUPN    = $usr
            Mailbox    = $mbx
            Status     = $status
        }
    }
}

$results | ConvertTo-Json -Depth 5 | Write-Output
Disconnect-ExchangeOnline -Confirm:$false
exit 0
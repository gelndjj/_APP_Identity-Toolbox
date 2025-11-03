param(
    [string]$Mailboxes,
    [string]$Users,
    [string]$LogPath
)

Write-Output "DEBUG: Users = $Users"
Write-Output "DEBUG: Mailboxes = $Mailboxes"
Write-Output "DEBUG: LogPath = $LogPath"

$MailboxList = $Mailboxes -split ","
$UserList = $Users -split ","

Start-Transcript -Path $LogPath -Append | Out-Null

Write-Output "###JSON_START###"

$results = @()

Connect-ExchangeOnline -ShowBanner:$false -ShowProgress:$false

foreach ($mbx in $MailboxList) {
    foreach ($usr in $UserList) {
        try {
            Add-MailboxPermission -Identity $mbx -User $usr `
                -AccessRights FullAccess -InheritanceType All -AutoMapping:$false

            $results += [PSCustomObject]@{
                UserUPN = $usr
                Mailbox = $mbx
                Status  = "✅ Access Granted"
            }
        }
        catch {
            $results += [PSCustomObject]@{
                UserUPN = $usr
                Mailbox = $mbx
                Status  = "❌ Failed: $($_.Exception.Message)"
            }
        }
    }
}

Disconnect-ExchangeOnline -Confirm:$false

$results | ConvertTo-Json -Depth 5

Write-Output "###JSON_END###"

Stop-Transcript | Out-Null
exit 0
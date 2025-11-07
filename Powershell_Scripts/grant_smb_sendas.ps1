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

foreach ($usr in $UserList) {
    foreach ($mbx in $MailboxList) {
        try {
            Add-RecipientPermission -Identity $mbx -Trustee $usr -AccessRights SendAs -Confirm:$false

            $results += [PSCustomObject]@{
                UserUPN = $usr
                Mailbox = $mbx
                Status  = "✅ Send-As Granted"
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
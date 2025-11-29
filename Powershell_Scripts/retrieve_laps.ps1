<#
.SYNOPSIS
  Retrieve LAPS (device local credentials) silently from Microsoft Graph
#>

param(
    [string]$DeviceId,
    [string]$DeviceName
)

# ✅ Force minimal clean stdout
$host.Runspace.ThreadOptions = "ReuseThread"

# ✅ Disable ALL noisy output
$VerbosePreference = "SilentlyContinue"
$WarningPreference = "SilentlyContinue"
$InformationPreference = "SilentlyContinue"
$ProgressPreference = "SilentlyContinue"
$ErrorActionPreference = "Stop"
$env:GRAPH_NO_WELCOME = "1"

function Emit-Result {
    param($obj)
    $json = $obj | ConvertTo-Json -Depth 8
    Write-Host "###LAPS_JSON_START###"
    Write-Host $json
    Write-Host "###LAPS_JSON_END###"
}

try {
    if (-not (Get-MgContext)) {
        Connect-MgGraph -Scopes "Device.Read.All,Directory.Read.All,DeviceLocalCredential.Read.All" -NoWelcome
    }
} catch {
    Emit-Result @{
        Device = $DeviceName
        Password = ""
        BackupTime = ""
        Status = "❌ Failed — Graph authentication failed"
    }
    exit 1
}

# ✅ Resolve DeviceId from name if needed
if (-not $DeviceId -and $DeviceName) {
    $lookupUri = 'https://graph.microsoft.com/v1.0/deviceManagement/managedDevices?$filter=deviceName eq ''' + $DeviceName + '''&$select=azureADDeviceId'
    $lookup = Invoke-MgGraphRequest -Uri $lookupUri -Method GET
    if ($lookup.value.Count -gt 0) {
        $DeviceId = $lookup.value[0].azureADDeviceId
    }
}

if (-not $DeviceId) {
    Emit-Result @{
        Device = $DeviceName
        Password = ""
        BackupTime = ""
        Status = "❌ Failed — Missing DeviceId"
    }
    exit 1
}

# ✅ Correct request format
$uri = 'https://graph.microsoft.com/v1.0/directory/deviceLocalCredentials/' + $DeviceId + '?$select=credentials,deviceName,lastBackupDateTime'

try {
    $resp = Invoke-MgGraphRequest -Uri $uri -Method GET
} catch {
    Emit-Result @{
        Device = $DeviceId
        Password = ""
        BackupTime = ""
        Status = "❌ Failed — Graph query error"
    }
    exit 1
}

if (-not $resp.credentials) {
    Emit-Result @{
        Device = $resp.deviceName
        Password = ""
        BackupTime = $resp.lastBackupDateTime
        Status = "❌ No LAPS credentials found"
    }
    exit 0
}

$cred = $resp.credentials[0]
$pwdPlain = ""

if ($cred.passwordBase64) {
    $pwdPlain = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($cred.passwordBase64))
}

Emit-Result @{
    Device = $resp.deviceName
    Password = $pwdPlain
    BackupTime = $cred.backupDateTime
    Status = "✅ Success"
}
exit 0
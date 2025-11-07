<#  Retrieves BitLocker keys by Device **DisplayName** (delegated auth)
    Requires: BitLockerKey.Read.All + Device.Read.All
    Emits: ###JSON_START### ... ###JSON_END###  #>

param(
    [string]$DeviceIds    = "",      # unused for Option A, keep for future
    [string]$DeviceNames  = ""       # comma-separated from Python
)

# Connect (delegated)
Connect-MgGraph -Scopes "BitlockerKey.Read.All","Device.Read.All" | Out-Null

# Normalize incoming names to an array, trimming empties
$names = @()
if ($DeviceNames) {
    $names = $DeviceNames.Split(',', [System.StringSplitOptions]::RemoveEmptyEntries) |
        ForEach-Object { $_.Trim() } |
        Where-Object { $_ -ne "" }
}

$results = @()

foreach ($name in $names) {
    try {
        # escape single quotes for OData filter: O'Brien -> O''Brien
        $safeName = $name -replace "'", "''"

        # 1) resolve device by displayName
        $dev = Get-MgDevice -Filter "displayName eq '$safeName'" -Select id,deviceId,displayName -ConsistencyLevel eventual -CountVariable cnt -ErrorAction Stop

        if (-not $dev) {
            $results += [pscustomobject]@{
                KeyId           = "N/A"
                DeviceId        = "N/A"
                DeviceName      = $name
                CreatedDateTime = ""
                CreatedBy       = ""
                RecoveryKey     = "Device not found"
            }
            continue
        }

        foreach ($d in $dev) {
            # 2) list recovery key objects for this deviceId to obtain their IDs
            $rkList = Get-MgInformationProtectionBitlockerRecoveryKey `
                        -Filter "deviceId eq '$($d.deviceId)'" `
                        -Select id,deviceId,createdDateTime `
                        -ErrorAction Stop

            if (-not $rkList) {
                $results += [pscustomobject]@{
                    KeyId           = "N/A"
                    DeviceId        = $d.deviceId
                    DeviceName      = $d.displayName
                    CreatedDateTime = ""
                    CreatedBy       = ""
                    RecoveryKey     = "No key found"
                }
                continue
            }

            foreach ($rk in $rkList) {
                try {
                    $rkFull = Invoke-MgGraphRequest -Method GET `
                        -Uri "https://graph.microsoft.com/v1.0/informationProtection/bitlocker/recoveryKeys/$($rk.Id)?`$select=key" `
                        -ErrorAction Stop

                    # Extract key value — Graph returns it in AdditionalProperties
                    if ($rkFull.PSObject.Properties.Name -contains "key") {
                        $keyValue = $rkFull.key
                    } elseif ($rkFull.AdditionalProperties.ContainsKey("key")) {
                        $keyValue = $rkFull.AdditionalProperties["key"]
                    } else {
                        $keyValue = "Key not returned"
                    }
                }
                catch {
                    $keyValue = "Error retrieving key"
                }

                $results += [pscustomobject]@{
                    KeyId           = $rk.Id
                    DeviceId        = $d.deviceId
                    DeviceName      = $d.displayName
                    CreatedDateTime = $rk.createdDateTime
                    CreatedBy       = ""
                    RecoveryKey     = $keyValue
                }
            }
        }
    }
    catch {
        $results += [pscustomobject]@{
            KeyId           = "N/A"
            DeviceId        = "N/A"
            DeviceName      = $name
            CreatedDateTime = ""
            CreatedBy       = ""
            RecoveryKey     = "Error: $($_.Exception.Message)"
        }
    }
}

# Fallback if nothing at all
if (-not $results) {
    $results = @([pscustomobject]@{
        KeyId           = "N/A"; DeviceId = "N/A"; DeviceName = ""; CreatedDateTime = ""; CreatedBy = ""; RecoveryKey = "No input"
    })
}

Write-Output "###JSON_START###"
$results | ConvertTo-Json -Depth 6
Write-Output "###JSON_END###"
# ==============================================
# Intune Detected Apps - Aggregated Summary Report
# ==============================================

# --- CONNECT ---
Connect-MgGraph -Scopes "DeviceManagementManagedDevices.Read.All"
Write-Host "[+] Connected to Microsoft Graph." -ForegroundColor Green

# --- BATCH FUNCTION ---
function Send-MgGraphBatchRequests {
    param (
        [Parameter(Mandatory)] $requests,
        [Parameter()] [ValidateSet('beta','v1.0')] $Apiversion = 'v1.0',
        [int] $batchSize = 20
    )

    $batches = [System.Collections.Generic.List[pscustomobject]]::new()
    $responses = [System.Collections.Concurrent.ConcurrentBag[System.Object]]::new()

    for ($i = 0; $i -lt $requests.Count; $i += $batchSize) {
        $end = [math]::Min($i + $batchSize - 1, $requests.Count - 1)
        $batches.Add(@{
            'Method'      = 'Post'
            'Uri'         = "https://graph.microsoft.com/$Apiversion/`$batch"
            'ContentType' = 'application/json'
            'Body'        = @{ 'requests' = @($requests[$i..$end]) } | ConvertTo-Json -Depth 5
        })
    }

    $batches | ForEach-Object {
        $result = Invoke-MgGraphRequest @_
        foreach ($r in $result.responses) {
            $responses.Add([pscustomobject]@{
                requestid = $r.id
                body      = $r.body
                error     = $r.error
            })
        }
    }

    return $responses
}

# --- PREPARE OUTPUT FOLDER ---
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"

# ✅ Folder for App reports
$ReportFolder = Join-Path (Split-Path -Parent $ScriptDir) "Database_Apps"
if (!(Test-Path $ReportFolder)) {
    New-Item -ItemType Directory -Path $ReportFolder | Out-Null
}

$outputPath = Join-Path $ReportFolder ("{0}_IntuneDetectedApps.csv" -f $timestamp)
Write-Host "Reports will be saved under: $ReportFolder" -ForegroundColor DarkCyan

# --- RETRIEVE DEVICES ---
$scriptStart = Get-Date
$timer = [System.Diagnostics.Stopwatch]::StartNew()
Write-Host "[+] Retrieving all devices..."

$devices = Invoke-MgGraphRequest -Method GET -Uri "https://graph.microsoft.com/beta/deviceManagement/managedDevices?`$select=id,deviceName" -OutputType PSObject
$allDevices = @()
do {
    $allDevices += $devices.value
    $next = $devices.'@odata.nextLink'
    if ($next) {
        $devices = Invoke-MgGraphRequest -Uri $next -Method GET -OutputType PSObject
    }
} while ($next)
$devices = $allDevices
Write-Host ("    → Retrieved {0} devices." -f $devices.Count)

# --- BUILD BATCH REQUESTS ---
Write-Host "[+] Building batch requests..."
$requests = [System.Collections.Generic.List[object]]::new()
foreach ($device in $devices) {
    $requests.Add(@{
        id     = "$($device.Id)_apps"
        method = "GET"
        url    = "/deviceManagement/managedDevices/$($device.Id)/detectedApps"
    })
    $requests.Add(@{
        id     = "$($device.Id)_user"
        method = "GET"
        url    = "/deviceManagement/managedDevices/$($device.Id)?`$select=userDisplayName"
    })
}

# --- EXECUTE BATCHES ---
Write-Host "[+] Sending batched requests..."
$responsesList = Send-MgGraphBatchRequests -requests $requests -Apiversion 'beta'
Write-Host ("    → Received {0} responses." -f $responsesList.Count)

# --- PROCESS RAW DATA ---
Write-Host "[+] Processing responses..."
$appData = [System.Collections.Concurrent.ConcurrentBag[PSCustomObject]]::new()

foreach ($device in $devices) {
    $deviceId   = $device.Id
    $deviceName = $device.deviceName

    $appsResponse = $responsesList | Where-Object { $_.requestid -eq "$deviceId`_apps" }
    $userResponse = $responsesList | Where-Object { $_.requestid -eq "$deviceId`_user" }

    $userDisplayName = if ($userResponse -and $userResponse.body.userDisplayName) {
        $userResponse.body.userDisplayName
    } else {
        "N/A"
    }

    if ($appsResponse.error) {
        Write-Warning "Error for $deviceName $($appsResponse.error.message)"
        continue
    }

    foreach ($app in $appsResponse.body.value) {
        $appData.Add([PSCustomObject]@{
            AppDisplayName  = $app.displayName
            Version         = $app.version
            Publisher       = $app.publisher
            Platform        = $app.platform
            DeviceName      = $deviceName
            UserDisplayName = $userDisplayName
        })
    }
}

Write-Host ("    → Total app-device pairs collected: {0}" -f $appData.Count)

# --- AGGREGATE BY APP ---
Write-Host "[+] Aggregating results by AppDisplayName, Version, Publisher..."
$appSummary = $appData |
    Group-Object AppDisplayName, Version, Publisher | ForEach-Object {
        $devices = $_.Group | Select-Object -ExpandProperty DeviceName -Unique
        $users   = $_.Group | Select-Object -ExpandProperty UserDisplayName -Unique

        [PSCustomObject]@{
            AppDisplayName = $_.Group[0].AppDisplayName
            Version        = $_.Group[0].Version
            Publisher      = $_.Group[0].Publisher
            Platform       = ($_.Group | Select-Object -ExpandProperty Platform -Unique) -join '; '
            DeviceCount    = $devices.Count
            UserCount      = $users.Count
            Devices        = $devices -join '; '
            Users          = $users -join '; '
        }
    }

# --- EXPORT TO CSV (✅ standardized) ---
$appSummary |
    Sort-Object -Property DeviceCount -Descending |
    Export-Csv -Path $outputPath -NoTypeInformation -Encoding UTF8

# --- SUMMARY ---
$timer.Stop()
Write-Host "✔ Done. Aggregated report saved to: $outputPath" -ForegroundColor Green
Write-Host ("🕒 Total execution time: {0}" -f $timer.Elapsed.ToString())
# Connect to Graph
Connect-MgGraph -Scopes "DeviceManagementServiceConfig.Read.All,Device.Read.All"
Write-Host "[+] Connected. Getting Autopilot devices..."

# Folder where the CSV will be stored
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$folder = Join-Path $scriptDir "..\Database_Autopilot_Devices"
New-Item -ItemType Directory -Path $folder -Force | Out-Null

# Get Autopilot devices
$ap = Invoke-MgGraphRequest -Method GET `
    -Uri "https://graph.microsoft.com/beta/deviceManagement/windowsAutopilotDeviceIdentities" `
    -OutputType PSObject

$rows = foreach ($d in $ap.value) {
    [pscustomobject]@{
        SerialNumber                = $d.serialNumber
        Manufacturer                = $d.manufacturer
        Model                       = $d.model
        GroupTag                    = $d.groupTag
        EnrollmentState             = $d.enrollmentState
        DeploymentProfileStatus     = $d.deploymentProfileAssignmentStatus
        LastContact                 = $d.lastContactedDateTime
        AssignedUser                = $d.userPrincipalName
        AADDeviceId                 = $d.azureActiveDirectoryDeviceId
        ManagedDeviceId             = $d.managedDeviceId
        UserlessEnrollmentStatus    = $d.userlessEnrollmentStatus
    }
}

# Output filename (Timestamped)
$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$out = Join-Path $folder "${timestamp}_AutopilotDevices.csv"

# Export CSV
$rows | Export-Csv $out -NoTypeInformation -Encoding UTF8

Write-Host "✅ Autopilot Devices report exported to $out" -ForegroundColor Green
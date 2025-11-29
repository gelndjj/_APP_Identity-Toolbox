param([string]$PwshRoot)

# Isolated cache directories
$env:GRAPH_TOKEN_CACHE_LOCATION = "$PwshRoot/.MgGraph"
$env:MSAL_CACHE_DIR             = "$PwshRoot/.Msal"
$env:MSAL_LOG_DIR               = "$PwshRoot/.Logs"

# Ensure Graph does NOT use system caches
$env:POWERPSS_USE_SYSTEM_TOKEN_CACHE = "false"

# Create folders
foreach ($d in @($env:GRAPH_TOKEN_CACHE_LOCATION, $env:MSAL_CACHE_DIR, $env:MSAL_LOG_DIR)) {
    if (!(Test-Path $d)) { New-Item -ItemType Directory -Path $d | Out-Null }
}

# Force portable modules only
$env:PSModulePath = "$PwshRoot/Modules"

$scopes = @(
    "User.Read",
    "User.ReadWrite.All",
    "Directory.Read.All",
    "Directory.ReadWrite.All",
    "Group.ReadWrite.All",
    "GroupMember.ReadWrite.All",
    "UserAuthenticationMethod.ReadWrite.All",
    "Device.ReadWrite.All",
    "DeviceManagementManagedDevices.Read.All",
    "DeviceLocalCredential.Read.All",
    "BitlockerKey.Read.All",
    "EntitlementManagement.ReadWrite.All",
    "RoleManagement.Read.Directory",
    "Policy.Read.All",
    "Application.Read.All"
)


try {
    Connect-MgGraph -Scopes $scopes | Out-Null
    $ctx = Get-MgContext

    @{
        status  = "success"
        account = $ctx.Account
    } | ConvertTo-Json
}
catch {
    @{
        status = "error"
        message = $_.Exception.Message
    } | ConvertTo-Json
}

exit 0
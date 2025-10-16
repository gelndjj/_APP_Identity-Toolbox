param(
    [string]$SourceUserUPN,
    [string]$TargetUserUPN,
    [string]$GroupsJson  # 👈 JSON string input
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

try {
    $GroupsToAssign = $GroupsJson | ConvertFrom-Json
    if (-not $GroupsToAssign) { throw "No groups found in JSON input." }
}
catch {
    Write-Host "❌ Failed to parse GroupsJson: $GroupsJson"
    exit 1
}

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$result = @()

try {
    # Ensure Graph session
    $ctx = Get-MgContext -ErrorAction SilentlyContinue
    if (-not $ctx) {
        Connect-MgGraph -Scopes "Group.ReadWrite.All","User.Read.All","Directory.Read.All" -NoWelcome | Out-Null
    }

    # Make sure we have the necessary modules
    Import-Module Microsoft.Graph.Users -ErrorAction SilentlyContinue
    Import-Module Microsoft.Graph.Groups -ErrorAction SilentlyContinue

    # Get users
    $source = Get-MgUser -Filter "userPrincipalName eq '$($SourceUserUPN.Replace("'", "''"))'" -ErrorAction Stop
    $target = Get-MgUser -Filter "userPrincipalName eq '$($TargetUserUPN.Replace("'", "''"))'" -ErrorAction Stop

    foreach ($gName in $GroupsToAssign) {
        try {
            $group = Get-MgGroup -Filter "displayName eq '$($gName.Replace("'", "''"))'" -ErrorAction Stop | Select-Object -First 1

            if (-not $group) {
                $result += [PSCustomObject]@{ GroupName = $gName; Status = "⚠️ Group not found" }
                continue
            }

            # Skip dynamic groups
            if ($group.GroupTypes -contains "DynamicMembership") {
                $result += [PSCustomObject]@{ GroupName = $gName; Status = "⏭️ Skipped (Dynamic group)" }
                continue
            }

            # Build REST request manually
            $uri = "https://graph.microsoft.com/v1.0/groups/$($group.Id)/members/`$ref"
            $body = @{ "@odata.id" = "https://graph.microsoft.com/v1.0/directoryObjects/$($target.Id)" } | ConvertTo-Json

            try {
                Invoke-MgGraphRequest -Method POST -Uri $uri -Body $body -ErrorAction Stop
                $result += [PSCustomObject]@{ GroupName = $group.DisplayName; Status = "✅ Added successfully" }
            }
            catch {
                $result += [PSCustomObject]@{ GroupName = $group.DisplayName; Status = "❌ Add failed: $($_.Exception.Message)" }
            }
        }
        catch {
            $result += [PSCustomObject]@{ GroupName = $gName; Status = "❌ Query error: $($_.Exception.Message)" }
        }
    }
}
catch {
    $result += [PSCustomObject]@{ GroupName = "N/A"; Status = "❌ Script failed: $($_.Exception.Message)" }
}

if (-not $result -or $result.Count -eq 0) {
    $result = @([PSCustomObject]@{
        GroupName = "None"
        Status    = "No groups processed"
    })
}

# Output proper JSON
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$result | ConvertTo-Json -Depth 5 -Compress
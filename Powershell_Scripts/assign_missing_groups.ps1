param(
    [string]$SourceUserUPN,
    [string]$TargetUserUPN,
    [string]$GroupsJson
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

$result = @()

try {
    $ctx = Get-MgContext -ErrorAction SilentlyContinue
    if (-not $ctx) {
        Connect-MgGraph -Scopes "Group.ReadWrite.All","User.Read.All","Directory.Read.All" -NoWelcome | Out-Null
    }

    Import-Module Microsoft.Graph.Users -ErrorAction SilentlyContinue
    Import-Module Microsoft.Graph.Groups -ErrorAction SilentlyContinue

    $source = Get-MgUser -Filter "userPrincipalName eq '$($SourceUserUPN.Replace("'", "''"))'" -ErrorAction Stop
    $target = Get-MgUser -Filter "userPrincipalName eq '$($TargetUserUPN.Replace("'", "''"))'" -ErrorAction Stop

    foreach ($gName in $GroupsToAssign) {
        try {
            $group = Get-MgGroup -Filter "displayName eq '$($gName.Replace("'", "''"))'" -ErrorAction Stop | Select-Object -First 1

            if (-not $group) {
                $result += [PSCustomObject]@{ GroupName = $gName; Status = "⚠️ Group not found" }
                continue
            }

            if ($group.GroupTypes -contains "DynamicMembership") {
                $result += [PSCustomObject]@{ GroupName = $gName; Status = "⏭️ Skipped (Dynamic group)" }
                continue
            }

            try {
                if (-not $target.Id -or -not $group.Id) {
                    throw "Missing user or group ID. TargetId=$($target.Id), GroupId=$($group.Id)"
                }

                New-MgGroupMemberByRef -GroupId $group.Id -BodyParameter @{
                    "@odata.id" = "https://graph.microsoft.com/v1.0/directoryObjects/$($target.Id)"
                } -ErrorAction Stop

                $result += [PSCustomObject]@{
                    GroupName = $group.DisplayName
                    Status    = "✅ Added successfully"
                }
            }
            catch {
                $msg = $_.Exception.Message
                if ($msg -match "One or more added object references already exist") {
                    $status = "ℹ️ Already a member"
                }
                elseif ($msg -match "BadRequest" -and $msg -match "dynamic") {
                    $status = "⏭️ Dynamic group (cannot add manually)"
                }
                elseif ($msg -match "does not indicate success") {
                    $status = "❌ Add failed: BadRequest (check if user already member or group is role-assignable)"
                }
                elseif ($msg -match "Insufficient privileges") {
                    $status = "⚠️ Permission denied"
                }
                else {
                    $status = "❌ Add failed: $msg"
                }

                if ($status.Length -gt 90) { $status = $status.Substring(0, 90) + "…" }

                $result += [PSCustomObject]@{
                    GroupName = $group.DisplayName
                    Status    = $status
                }
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

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$result | ConvertTo-Json -Depth 5 -Compress
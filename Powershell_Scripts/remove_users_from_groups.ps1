param(
    [string[]]$UserUPNs,
    [string[]]$GroupIDs
)

# --- Normalize GroupIDs array ---
$normalizedGroups = @()
if ($GroupIDs -is [string]) {
    $normalizedGroups = ($GroupIDs -split "[, ]+") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
} else {
    foreach ($g in $GroupIDs) {
        if ($g -match "[, ]") {
            $normalizedGroups += ($g -split "[, ]+") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
        } else {
            $normalizedGroups += $g.Trim()
        }
    }
}
$GroupIDs = $normalizedGroups | Select-Object -Unique

# --- Normalize UserUPNs array ---
$normalizedUsers = @()
if ($UserUPNs -is [string]) {
    $normalizedUsers = ($UserUPNs -split "[,; ]+") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
} else {
    foreach ($u in $UserUPNs) {
        if ($u -match "[,; ]") {
            $normalizedUsers += ($u -split "[,; ]+") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
        } else {
            $normalizedUsers += $u.Trim()
        }
    }
}
$UserUPNs = $normalizedUsers | Select-Object -Unique

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
$result = @()

# --- Connect to Graph ---
try {
    $ctx = Get-MgContext -ErrorAction SilentlyContinue
    if (-not $ctx) {
        Connect-MgGraph -Scopes "User.Read.All","Group.ReadWrite.All","Directory.ReadWrite.All" -NoWelcome | Out-Null
    }
} catch {
    Connect-MgGraph -Scopes "User.Read.All","Group.ReadWrite.All","Directory.ReadWrite.All" -NoWelcome | Out-Null
}

Import-Module Microsoft.Graph.Users -ErrorAction SilentlyContinue
Import-Module Microsoft.Graph.Groups -ErrorAction SilentlyContinue

foreach ($upn in $UserUPNs) {
    try {
        $escaped = $upn.Replace("'", "''")
        $user = Get-MgUser -Filter "startswith(userPrincipalName,'$escaped')" -All |
                Where-Object { $_.UserPrincipalName -ieq $upn }

        if (-not $user) {
            $result += [PSCustomObject]@{
                Phase     = "Remove"
                UserUPN   = $upn
                GroupName = "N/A"
                Status    = "❌ User not found"
            }
            continue
        }

        foreach ($gid in $GroupIDs) {
            try {
                $group = Get-MgGroup -GroupId $gid -ErrorAction Stop

                # Check membership first
                $isMember = $false
                try {
                    $members = Get-MgGroupMember -GroupId $gid -All -ErrorAction Stop
                    $isMember = $members | Where-Object { $_.Id -eq $user.Id } | ForEach-Object { $true } | Select-Object -First 1
                } catch { }

                if (-not $isMember) {
                    $result += [PSCustomObject]@{
                        Phase     = "Remove"
                        UserUPN   = $upn
                        GroupName = $group.DisplayName
                        Status    = "ℹ️ Not a member"
                    }
                    continue
                }

                # Remove via REST: DELETE /groups/{id}/members/{userId}/$ref
                $uri = "https://graph.microsoft.com/v1.0/groups/$($gid)/members/$($user.Id)/`$ref"
                Invoke-MgGraphRequest -Method DELETE -Uri $uri -ErrorAction Stop

                $result += [PSCustomObject]@{
                    Phase     = "Remove"
                    UserUPN   = $upn
                    GroupName = $group.DisplayName
                    Status    = "✅ Removed successfully"
                }
            }
            catch {
                $msg = $_.Exception.Message
                $status = if ($msg -match "Insufficient privileges") { "⚠️ Permission denied" }
                          elseif ($msg -match "BadRequest") { "❌ Invalid request" }
                          else { "❌ Group error: $msg" }

                $result += [PSCustomObject]@{
                    Phase     = "Remove"
                    UserUPN   = $upn
                    GroupName = $group.DisplayName
                    Status    = $status
                }
            }
        }
    }
    catch {
        $result += [PSCustomObject]@{
            Phase     = "Remove"
            UserUPN   = $upn
            GroupName = "N/A"
            Status    = "❌ General error: $($_.Exception.Message)"
        }
    }
}

if (-not $result) {
    $result = @([PSCustomObject]@{
        Phase     = "Remove"
        UserUPN   = "None"
        GroupName = "None"
        Status    = "No operations performed"
    })
}

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$result | ConvertTo-Json -Depth 6 -Compress | Write-Output
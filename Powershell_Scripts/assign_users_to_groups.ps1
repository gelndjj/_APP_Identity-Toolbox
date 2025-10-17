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

# --- Setup Environment ---
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

# --- Main logic ---
foreach ($upn in $UserUPNs) {
    try {
        $escaped = $upn.Replace("'", "''")
        $user = Get-MgUser -Filter "startswith(userPrincipalName,'$escaped')" -All |
                Where-Object { $_.UserPrincipalName -ieq $upn }

        if (-not $user) {
            $result += [PSCustomObject]@{
                UserUPN   = $upn
                GroupName = "N/A"
                Status    = "❌ User not found"
            }
            continue
        }

        foreach ($gid in $GroupIDs) {
            try {
                $group = Get-MgGroup -GroupId $gid -ErrorAction Stop

                # Skip dynamic groups
                if ($group.GroupTypes -contains "DynamicMembership") {
                    $result += [PSCustomObject]@{
                        UserUPN   = $upn
                        GroupName = $group.DisplayName
                        Status    = "⏭️ Skipped (Dynamic group)"
                    }
                    continue
                }

                # Check if already member
                $existing = Get-MgGroupMember -GroupId $gid -All -ErrorAction SilentlyContinue |
                            Where-Object { $_.Id -eq $user.Id }

                if ($existing) {
                    $result += [PSCustomObject]@{
                        UserUPN   = $upn
                        GroupName = $group.DisplayName
                        Status    = "ℹ️ Already a member"
                    }
                    continue
                }

                # Add user to group via REST
                $uri = "https://graph.microsoft.com/v1.0/groups/$($gid)/members/`$ref"
                $body = @{
                    "@odata.id" = "https://graph.microsoft.com/v1.0/directoryObjects/$($user.Id)"
                } | ConvertTo-Json -Compress

                Invoke-MgGraphRequest -Method POST -Uri $uri -Body $body -ContentType "application/json" -ErrorAction Stop

                $result += [PSCustomObject]@{
                    UserUPN   = $upn
                    GroupName = $group.DisplayName
                    Status    = "✅ Added successfully"
                }
            }
            catch {
                $msg = $_.Exception.Message
                if ($msg -match "Insufficient privileges") {
                    $status = "⚠️ Permission denied"
                } elseif ($msg -match "BadRequest") {
                    $status = "❌ Invalid request (check group type or membership)"
                } else {
                    $status = "❌ Group error: $msg"
                }

                $result += [PSCustomObject]@{
                    UserUPN   = $upn
                    GroupName = $group.DisplayName
                    Status    = $status
                }
            }
        }
    }
    catch {
        $result += [PSCustomObject]@{
            UserUPN   = $upn
            GroupName = "N/A"
            Status    = "❌ General error: $($_.Exception.Message)"
        }
    }
}

# --- Always output clean JSON for the app ---
if (-not $result) {
    $result = @([PSCustomObject]@{
        UserUPN   = "None"
        GroupName = "None"
        Status    = "No operations performed"
    })
}

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$result | ConvertTo-Json -Depth 6 -Compress | Write-Output
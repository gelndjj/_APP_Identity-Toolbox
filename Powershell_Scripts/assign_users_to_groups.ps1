param(
    [string[]]$UserUPNs,
    [string[]]$GroupIDs
)

# --- Normalize GroupIDs array ---
# Handle multiple input styles (space, comma, or separate args)
$normalizedGroups = @()

if ($GroupIDs -is [string]) {
    # If PowerShell sees it as one long string
    $normalizedGroups = ($GroupIDs -split "[, ]+") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
}
else {
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
    # Handle comma, space, semicolon separated list
    $normalizedUsers = ($UserUPNs -split "[,; ]+") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
}
else {
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

try {
    # Reuse session if possible
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
        # Lookup user (case-insensitive)
        $escaped = $upn.Replace("'", "''")
        $user = Get-MgUser -Filter "startswith(userPrincipalName,'$escaped')" -All |
                Where-Object { $_.UserPrincipalName -ieq $upn }

        if (-not $user) {
            $result += [PSCustomObject]@{
                UserPrincipalName = $upn
                GroupName         = "N/A"
                Status            = "❌ User not found"
            }
            continue
        }

        foreach ($gid in $GroupIDs) {
            try {
                $group = Get-MgGroup -GroupId $gid -ErrorAction Stop

                # Skip dynamic groups
                if ($group.GroupTypes -contains "DynamicMembership") {
                    $result += [PSCustomObject]@{
                        UserPrincipalName = $upn
                        GroupName         = $group.DisplayName
                        Status            = "⏭️ Skipped (Dynamic group)"
                    }
                    continue
                }

                # Check if already member
                $existing = Get-MgGroupMember -GroupId $gid -All -ErrorAction SilentlyContinue |
                            Where-Object { $_.Id -eq $user.Id }

                if ($existing) {
                    $result += [PSCustomObject]@{
                        UserPrincipalName = $upn
                        GroupName         = $group.DisplayName
                        Status            = "ℹ️ Already a member"
                    }
                    continue
                }

                # Add user to group via REST
                $uri = "https://graph.microsoft.com/v1.0/groups/$($gid)/members/`$ref"
                $body = @{
                    "@odata.id" = "https://graph.microsoft.com/v1.0/directoryObjects/$($user.Id)"
                } | ConvertTo-Json -Compress

                try {
                    Invoke-MgGraphRequest -Method POST -Uri $uri -Body $body -ContentType "application/json" -ErrorAction Stop
                    $result += [PSCustomObject]@{
                        UserPrincipalName = $upn
                        GroupName         = $group.DisplayName
                        Status            = "✅ Added successfully"
                    }
                }
                catch {
                    $message = $_.Exception.Message
                    if ($message -match "Insufficient privileges") {
                        $status = "⚠️ Permission denied"
                    } elseif ($message -match "BadRequest") {
                        $status = "❌ Invalid request (check group type or membership)"
                    } else {
                        $status = "❌ Add failed: $message"
                    }

                    $result += [PSCustomObject]@{
                        UserPrincipalName = $upn
                        GroupName         = $group.DisplayName
                        Status            = $status
                    }
                }
            }
            catch {
                $result += [PSCustomObject]@{
                    UserPrincipalName = $upn
                    GroupName         = $gid
                    Status            = "❌ Group lookup failed: $($_.Exception.Message)"
                }
            }
        }
    }
    catch {
        $result += [PSCustomObject]@{
            UserPrincipalName = $upn
            GroupName         = "N/A"
            Status            = "❌ Script error: $($_.Exception.Message)"
        }
    }
}

# --- Always output clean JSON for the app ---
if (-not $result) {
    $result = @([PSCustomObject]@{
        UserPrincipalName = "None"
        GroupName         = "None"
        Status            = "No operations performed"
    })
}

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$result | ConvertTo-Json -Depth 6 -Compress | Write-Output
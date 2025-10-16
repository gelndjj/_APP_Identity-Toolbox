param(
    [string[]]$UserUPNs,
    [string]$AccessPackagesJson
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

try {
    $AccessPackages = $AccessPackagesJson | ConvertFrom-Json
    if (-not $AccessPackages) { throw "No access packages found in JSON input." }
}
catch {
    Write-Host "❌ Failed to parse AccessPackagesJson: $AccessPackagesJson"
    exit 1
}

$result = @()

try {
    # Ensure Graph connection
    $ctx = Get-MgContext -ErrorAction SilentlyContinue
    if (-not $ctx) {
        Connect-MgGraph -Scopes "EntitlementManagement.ReadWrite.All","User.Read.All" -NoWelcome | Out-Null
    }

    Import-Module Microsoft.Graph.Identity.Governance -ErrorAction SilentlyContinue
    Import-Module Microsoft.Graph.Users -ErrorAction SilentlyContinue

    foreach ($user in $UserUPNs) {
        try {
            $target = Get-MgUser -Filter "userPrincipalName eq '$($user.Replace("'", "''"))'" -ErrorAction Stop

            foreach ($pkg in $AccessPackages) {
                try {
                    $pkgId = $pkg.AccessPackageId
                    $pkgName = $pkg.AccessPackageName
                    $policyId = $pkg.Policies[0].PolicyId

                    if (-not $pkgId -or $pkgId -notmatch '^[0-9a-fA-F-]{36}$') {
                        $result += [PSCustomObject]@{
                            UserUPN = $user
                            AccessPackageName = $pkgName
                            Status = "⚠️ Invalid AccessPackageId"
                        }
                        continue
                    }

                    # Build the same request body your create_user.ps1 uses
                    $params = @{
                        requestType = "AdminAdd"
                        assignment = @{
                            targetId = $target.Id
                            assignmentPolicyId = $policyId
                            accessPackageId = $pkgId
                        }
                    }

                    # Create the assignment request
                    $response = New-MgEntitlementManagementAssignmentRequest -BodyParameter $params -ErrorAction Stop

                    if ($response.Id) {
                        $result += [PSCustomObject]@{
                            UserUPN = $user
                            AccessPackageName = $pkgName
                            Status = "✅ Request created (ID: $($response.Id))"
                        }
                    }
                    else {
                        $result += [PSCustomObject]@{
                            UserUPN = $user
                            AccessPackageName = $pkgName
                            Status = "⚠️ Created, but no request ID returned"
                        }
                    }
                }
                catch {
                    $result += [PSCustomObject]@{
                        UserUPN = $user
                        AccessPackageName = $pkg.AccessPackageName
                        Status = "❌ Failed: $($_.Exception.Message)"
                    }
                }
            }
        }
        catch {
            $result += [PSCustomObject]@{
                UserUPN = $user
                AccessPackageName = "N/A"
                Status = "❌ Failed to get user: $($_.Exception.Message)"
            }
        }
    }
}
catch {
    $result += [PSCustomObject]@{
        UserUPN = "N/A"
        AccessPackageName = "N/A"
        Status = "❌ Script failed: $($_.Exception.Message)"
    }
}

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$result | ConvertTo-Json -Depth 6 -Compress
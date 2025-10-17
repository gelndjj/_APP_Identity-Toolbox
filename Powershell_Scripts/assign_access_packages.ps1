param(
    [string]$UserUPNs,
    [string]$AccessPackageName
)

$ErrorActionPreference   = 'Stop'
$ProgressPreference      = 'SilentlyContinue'
$VerbosePreference       = 'SilentlyContinue'   # 👈 debug is off unless -Verbose
$InformationPreference   = 'SilentlyContinue'   # (if you prefer Write-Information)


# --- Normalize ---
$upnList = $UserUPNs -split '[,; \r\n]+' | ForEach-Object { $_.Trim() } | Where-Object { $_ }
Write-Verbose "🧩 Parsed UPNs as array ($($upnList.Count)):`n - $($upnList -join "`n - ")"

# --- Graph connection ---
$ctx = Get-MgContext -ErrorAction SilentlyContinue
if (-not $ctx) {
    Connect-MgGraph -Scopes "EntitlementManagement.ReadWrite.All","User.Read.All","Directory.Read.All" -NoWelcome | Out-Null
}
Import-Module Microsoft.Graph.Identity.Governance -ErrorAction SilentlyContinue | Out-Null
Import-Module Microsoft.Graph.Users -ErrorAction SilentlyContinue | Out-Null

# --- Find Access Package ---
$escapedName = $AccessPackageName.Replace("'", "''")
$apResp = Invoke-MgGraphRequest -Method GET -OutputType PSObject `
    -Uri "https://graph.microsoft.com/beta/identityGovernance/entitlementManagement/accessPackages?`$filter=displayName eq '$escapedName'"
$ap = $apResp.value | Select-Object -First 1
if (-not $ap) {
    Write-Output (@([pscustomobject]@{
        UserUPN = "N/A"
        AccessPackageName = $AccessPackageName
        Status = "❌ Access package not found"
    }) | ConvertTo-Json -Compress)
    exit
}

Write-Verbose "🎁 Found Access Package: $($ap.displayName) [$($ap.id)]"

# --- Get a valid policy ---
$polResp = Invoke-MgGraphRequest -Method GET -OutputType PSObject `
    -Uri "https://graph.microsoft.com/beta/identityGovernance/entitlementManagement/accessPackageAssignmentPolicies?`$filter=accessPackageId eq '$($ap.id)'"
$policy = ($polResp.value | Where-Object { $_.status -eq 'enabled' }) | Select-Object -First 1
if (-not $policy) { $policy = $polResp.value | Select-Object -First 1 }
Write-Verbose "📦 Using policy: $($policy.displayName) [$($policy.id)]"

# --- Assign ---
$results = @()
foreach ($upn in $upnList) {
    Write-Host "👤 Processing user: $upn"
    try {
        $user = Get-MgUser -Filter "userPrincipalName eq '$($upn.Replace("'", "''"))'" -ErrorAction Stop
        $body = @{
            requestType = "AdminAdd"
            assignment  = @{
                targetId           = $user.Id
                assignmentPolicyId = $policy.id
                accessPackageId    = $ap.id
            }
        }
        $resp = New-MgEntitlementManagementAssignmentRequest -BodyParameter $body -ErrorAction Stop
        if ($resp.id) {
            $results += [pscustomobject]@{
                UserUPN           = $upn
                AccessPackageName = $ap.displayName
                Status            = "✅ Request created (ID: $($resp.id))"
            }
        } else {
            $results += [pscustomobject]@{
                UserUPN           = $upn
                AccessPackageName = $ap.displayName
                Status            = "⚠️ No ID returned"
            }
        }
    } catch {
        $results += [pscustomobject]@{
            UserUPN           = $upn
            AccessPackageName = $ap.displayName
            Status            = "❌ Failed: $($_.Exception.Message)"
        }
    }
}

# --- Output JSON ---
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$results | ConvertTo-Json -Depth 5 -Compress
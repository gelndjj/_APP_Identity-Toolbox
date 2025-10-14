<#
.SYNOPSIS
Exports all Entra ID Access Packages and their assignment policies to JSON (using /beta endpoints).

#>

param(
    [string]$OutputPath = "$(Join-Path $PWD 'AccessPackages.json')"
)

$ErrorActionPreference = "Stop"

Write-Host "🔗 Connecting to Microsoft Graph (beta endpoint)..." -ForegroundColor Cyan

try {
    $ctx = Get-MgContext -ErrorAction Stop
    if (-not $ctx) {
        Connect-MgGraph -Scopes "EntitlementManagement.Read.All","EntitlementManagement.ReadWrite.All","Directory.Read.All","User.Read.All" -NoWelcome
    }
} catch {
    Connect-MgGraph -Scopes "EntitlementManagement.Read.All","EntitlementManagement.ReadWrite.All","Directory.Read.All","User.Read.All" -NoWelcome
}

Write-Host "📦 Retrieving Access Packages..." -ForegroundColor Cyan
$packages = Invoke-MgGraphRequest -Uri "https://graph.microsoft.com/beta/identityGovernance/entitlementManagement/accessPackages" -Method GET -OutputType PSObject

if (-not $packages.value) {
    Write-Host "⚠️  No access packages found in tenant." -ForegroundColor Yellow
    exit
}

$result = @()

foreach ($pkg in $packages.value) {
    Write-Host "→ Access Package: $($pkg.displayName)" -ForegroundColor Cyan

    $uri = "https://graph.microsoft.com/beta/identityGovernance/entitlementManagement/accessPackageAssignmentPolicies`?$filter=accessPackageId eq '$($pkg.id)'"

    try {
        $response = Invoke-MgGraphRequest -Uri $uri -Method GET -OutputType PSObject -ErrorAction Stop
        $policies = $response.value
    } catch {
        Write-Host "⚠️  Failed to retrieve policies for $($pkg.displayName): $($_.Exception.Message)" -ForegroundColor Yellow
        $policies = @()
    }

    $policyList = @()
    foreach ($policy in $policies) {
        $policyList += [PSCustomObject]@{
            PolicyName  = $policy.displayName
            PolicyId    = $policy.id
            Description = $policy.description
            Status      = if ($policy.isEnabled) { "Enabled" } else { "Disabled" }
        }
    }

    $result += [PSCustomObject]@{
        AccessPackageName = $pkg.displayName
        AccessPackageId   = $pkg.id
        Description       = $pkg.description
        CatalogId         = $pkg.catalogId
        CreatedDateTime   = $pkg.createdDateTime
        ModifiedDateTime  = $pkg.modifiedDateTime
        Policies          = $policyList
    }
}

Write-Host "💾 Saving results to: $OutputPath" -ForegroundColor Cyan
$result | ConvertTo-Json -Depth 6 | Out-File -Encoding UTF8 -FilePath $OutputPath

Write-Host "✅ Done! Exported $($result.Count) Access Packages with their policies." -ForegroundColor Green
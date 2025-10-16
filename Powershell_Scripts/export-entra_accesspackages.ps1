<#
.SYNOPSIS
Exports all Entra ID Access Packages and their assignment policies to JSON (using /beta endpoints).

.EXAMPLE
.\Export-EntraAccessPackages.ps1 -OutputPath ".\JSONs\AccessPackages.json"
#>

param(
    [string]$OutputPath = "$(Join-Path $PWD 'AccessPackages.json')",
    [switch]$SkipDisabledPolicies  # Optional: exclude disabled policies from export
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

# Retrieve all access packages
$packages = Invoke-MgGraphRequest -Uri "https://graph.microsoft.com/beta/identityGovernance/entitlementManagement/accessPackages" -Method GET -OutputType PSObject

if (-not $packages.value) {
    Write-Host "⚠️  No access packages found in tenant." -ForegroundColor Yellow
    exit
}

$result = @()

foreach ($pkg in $packages.value) {
    Write-Host "→ Access Package: $($pkg.displayName)" -ForegroundColor Cyan

    # Retrieve all policies for this package with extended fields
    $uri = "https://graph.microsoft.com/beta/identityGovernance/entitlementManagement/accessPackageAssignmentPolicies?`$filter=accessPackageId eq '$($pkg.id)'&`$expand=customExtensionHandlers"

    try {
        $response = Invoke-MgGraphRequest -Uri $uri -Method GET -OutputType PSObject -ErrorAction Stop
        $policies = $response.value
    } catch {
        Write-Host "⚠️  Failed to retrieve policies for $($pkg.displayName): $($_.Exception.Message)" -ForegroundColor Yellow
        $policies = @()
    }

    $policyList = @()
    foreach ($policy in $policies) {
        # --- Robust Enabled detection ---
        $isEnabled = $false

        if ($null -ne $policy.isEnabled) {
            $isEnabled = [bool]$policy.isEnabled
        }
        elseif ($null -ne $policy.accessPackageAssignmentPolicyStatus) {
            # New Graph field (enum: enabled / disabled)
            $isEnabled = ($policy.accessPackageAssignmentPolicyStatus -eq "enabled")
        }
        elseif ($policy.state -eq "enabled" -or $policy.status -eq "enabled" -or $policy.status -eq "Enabled") {
            $isEnabled = $true
        }
        else {
            # As a safeguard: if Graph doesn’t provide any status, assume Enabled
            $isEnabled = $true
        }

        if ($SkipDisabledPolicies -and -not $isEnabled) {
            continue
        }

        $policyList += [PSCustomObject]@{
            PolicyName  = $policy.displayName
            PolicyId    = $policy.id
            Description = $policy.description
            Status      = if ($isEnabled) { "Enabled" } else { "Disabled" }
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

# Ensure output folder exists
$dir = Split-Path $OutputPath
if (-not (Test-Path $dir)) {
    New-Item -Path $dir -ItemType Directory -Force | Out-Null
}

Write-Host "💾 Preparing JSON export..." -ForegroundColor Cyan

# --- Add a blank placeholder entry at the top ---
$placeholder = [PSCustomObject]@{
    AccessPackageName = ""
    AccessPackageId   = ""
    Description       = ""
    CatalogId         = ""
    CreatedDateTime   = ""
    ModifiedDateTime  = ""
    Policies          = @()
}

# --- Combine placeholder + actual data ---
$finalList = @($placeholder) + $result

# --- Ensure output folder exists ---
$dir = Split-Path $OutputPath
if (-not (Test-Path $dir)) {
    New-Item -Path $dir -ItemType Directory -Force | Out-Null
}

# --- Save JSON ---
Write-Host "💾 Saving results to: $OutputPath" -ForegroundColor Cyan
$finalList | ConvertTo-Json -Depth 6 | Out-File -Encoding UTF8 -FilePath $OutputPath

Write-Host "✅ Done! Exported $($result.Count) Access Packages (+ placeholder) with their policies." -ForegroundColor Green

<#
.SYNOPSIS
    Generate Temporary Access Pass(es) for one or more users.
#>

param(
    [Parameter(Mandatory = $true)]
    [string[]]$UserPrincipalName,

    [int]$LifetimeInMinutes = 60,
    [switch]$OneTimeUse
)

# --- Handle comma-separated list ---
if ($UserPrincipalName.Count -eq 1 -and $UserPrincipalName[0] -like "*,*") {
    $UserPrincipalName = $UserPrincipalName[0] -split ","
}

Write-Host "`n🪪 Generating Temporary Access Pass(es)..." -ForegroundColor Cyan
Write-Host "Connecting to Microsoft Graph..." -ForegroundColor Yellow

# --- Ensure modules ---
$required = @('Microsoft.Graph.Authentication', 'Microsoft.Graph.Users')
foreach ($m in $required) {
    if (-not (Get-Module -ListAvailable -Name $m)) {
        Write-Host "Installing missing module: $m" -ForegroundColor Yellow
        Install-Module $m -Scope CurrentUser -Force
    }
}

# --- Connect ---
try {
    Connect-MgGraph -Scopes "UserAuthenticationMethod.ReadWrite.All,User.ReadWrite.All,Directory.ReadWrite.All" -NoWelcome
    $ctx = Get-MgContext
    Write-Host "✅ Connected as: $($ctx.Account)" -ForegroundColor Green
}
catch {
    Write-Host "❌ Failed to connect to Microsoft Graph: $($_.Exception.Message)" -ForegroundColor Red
    [System.Environment]::Exit(1)
}

$results = @()

foreach ($upn in $UserPrincipalName) {
    Write-Host "`n→ Processing user: $upn" -ForegroundColor Cyan
    try {
        $user = Get-MgUser -UserId $upn -ErrorAction Stop

        $startUtc  = (Get-Date).ToUniversalTime()
        $expiryUtc = $startUtc.AddMinutes($LifetimeInMinutes)

        $body = @{
            startDateTime      = $startUtc.ToString("yyyy-MM-ddTHH:mm:ssZ")
            lifetimeInMinutes  = $LifetimeInMinutes
            isUsableOnce       = [bool]$OneTimeUse
        }

        # Create TAP
        $tap = New-MgUserAuthenticationTemporaryAccessPassMethod -UserId $user.Id -BodyParameter $body

        # Try to fetch full TAP (may be empty)
        $tapFull = Get-MgUserAuthenticationTemporaryAccessPassMethod -UserId $user.Id |
            Sort-Object -Property startDateTime -Descending |
            Select-Object -First 1

        # Fallback expiration if null
        $expiry = if ($tapFull.ExpirationDateTime) {
            $tapFull.ExpirationDateTime
        } else {
            $expiryUtc.ToString("yyyy-MM-ddTHH:mm:ssZ")
        }

        $result = [PSCustomObject]@{
            UserUPN             = $upn
            TemporaryAccessPass = $tap.TemporaryAccessPass
            ExpirationDateTime  = $expiry
            OneTimeUse          = [bool]$tap.IsUsableOnce
            Status              = "✅ Success"
        }
        $results += $result

        Write-Host "   🔑 TAP: $($tap.TemporaryAccessPass)" -ForegroundColor Yellow
        Write-Host "   ⏰ Expires: $($expiry)" -ForegroundColor DarkGray
    }
    catch {
        $results += [PSCustomObject]@{
            UserUPN = $upn
            TemporaryAccessPass = ""
            ExpirationDateTime  = ""
            OneTimeUse = $false
            Status = "❌ Failed — $($_.Exception.Message)"
        }
        Write-Host "❌ Failed to create TAP for $upn — $($_.Exception.Message)" -ForegroundColor Red
    }
}

# Print single summary JSON (no duplicates)
Write-Host "`n────────────── Summary ──────────────" -ForegroundColor Cyan
Write-Host ($results | ConvertTo-Json -Depth 3)
Write-Host "─────────────────────────────────────" -ForegroundColor Cyan

Disconnect-MgGraph | Out-Null
[System.Environment]::Exit(0)
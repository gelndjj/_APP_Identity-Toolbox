<#
.SYNOPSIS
    Revoke all active sessions for one or more Entra ID users (sign-out everywhere).
#>

param(
    [Parameter(Mandatory = $true)]
    [string[]]$UserPrincipalName
)

# Handle comma-separated input
if ($UserPrincipalName.Count -eq 1 -and $UserPrincipalName[0] -like "*,*") {
    $UserPrincipalName = $UserPrincipalName[0] -split ","
}

Write-Host "`n🚪 Revoking user sessions..." -ForegroundColor Cyan
Write-Host "Connecting to Microsoft Graph..." -ForegroundColor Yellow

# Ensure modules
$required = @('Microsoft.Graph.Authentication', 'Microsoft.Graph.Users.Actions')
foreach ($m in $required) {
    if (-not (Get-Module -ListAvailable -Name $m)) {
        Write-Host "Installing missing module: $m" -ForegroundColor Yellow
        Install-Module $m -Scope CurrentUser -Force
    }
}

# Connect
try {
    Connect-MgGraph -Scopes "User.ReadWrite.All,Directory.ReadWrite.All" -NoWelcome
    $ctx = Get-MgContext
    Write-Host "✅ Connected as: $($ctx.Account)" -ForegroundColor Green
}
catch {
    Write-Host "❌ Connection failed: $($_.Exception.Message)" -ForegroundColor Red
    [System.Environment]::Exit(1)
}

$results = @()

foreach ($upn in $UserPrincipalName) {
    Write-Host "`n→ Processing user: $upn" -ForegroundColor Cyan
    try {
        $user = Get-MgUser -UserId $upn -ErrorAction Stop
        Revoke-MgUserSignInSession -UserId $user.Id -ErrorAction Stop

        $results += [PSCustomObject]@{
            UserUPN = $upn
            Status  = "✅ Sessions revoked successfully"
        }

        Write-Host "   🔒 Active sessions revoked" -ForegroundColor Yellow
    }
    catch {
        $results += [PSCustomObject]@{
            UserUPN = $upn
            Status  = "❌ Failed — $($_.Exception.Message)"
        }
        Write-Host "❌ Failed for $upn — $($_.Exception.Message)" -ForegroundColor Red
    }
}

Write-Host "`n────────────── Summary ──────────────" -ForegroundColor Cyan
Write-Host ($results | ConvertTo-Json -Depth 3)
Write-Host "─────────────────────────────────────" -ForegroundColor Cyan

Disconnect-MgGraph | Out-Null
[System.Environment]::Exit(0)
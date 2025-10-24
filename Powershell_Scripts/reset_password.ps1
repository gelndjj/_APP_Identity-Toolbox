<#
.SYNOPSIS
    Reset password for one or more Entra ID users.
.DESCRIPTION
    Supports Base64-encoded passwords and optional enforcement of password change at next login.
#>

param(
    [Parameter(Mandatory = $true)]
    [string[]]$UserPrincipalName,

    [Parameter(Mandatory = $false)]
    [string]$NewPassword,

    [Parameter(Mandatory = $false)]
    [string]$NewPasswordBase64,

    [switch]$ForceChangeAtNextLogin  # ← NEW: controls whether user must change password
)

# --- Handle comma-separated list ---
if ($UserPrincipalName.Count -eq 1 -and $UserPrincipalName[0] -like "*,*") {
    $UserPrincipalName = $UserPrincipalName[0] -split ","
}

Write-Host "`n🔐 Resetting password(s)..." -ForegroundColor Cyan
Write-Host "Connecting to Microsoft Graph..." -ForegroundColor Yellow

# --- Ensure required modules ---
$required = @('Microsoft.Graph.Authentication', 'Microsoft.Graph.Users')
foreach ($m in $required) {
    if (-not (Get-Module -ListAvailable -Name $m)) {
        Write-Host "Installing missing module: $m" -ForegroundColor Yellow
        Install-Module $m -Scope CurrentUser -Force
    }
}

# --- Decode Base64 password if provided ---
if ($PSBoundParameters.ContainsKey('NewPasswordBase64')) {
    try {
        $decodedBytes = [System.Convert]::FromBase64String($NewPasswordBase64)
        $NewPassword = [System.Text.Encoding]::UTF8.GetString($decodedBytes)
    }
    catch {
        Write-Host "❌ Invalid Base64 input for NewPassword: $($_.Exception.Message)" -ForegroundColor Red
        [Environment]::Exit(1)
    }
}

# --- Validate password existence ---
if (-not $NewPassword) {
    Write-Host "❌ No NewPassword provided. Use -NewPassword or -NewPasswordBase64." -ForegroundColor Red
    [Environment]::Exit(1)
}

# --- Connect to Graph ---
try {
    Connect-MgGraph -Scopes "User.ReadWrite.All,Directory.ReadWrite.All" -NoWelcome
    $ctx = Get-MgContext
    Write-Host "✅ Connected as: $($ctx.Account)" -ForegroundColor Green
}
catch {
    Write-Host "❌ Failed to connect: $($_.Exception.Message)" -ForegroundColor Red
    [Environment]::Exit(1)
}

$results = @()

foreach ($upn in $UserPrincipalName) {
    Write-Host "`n→ Processing user: $upn" -ForegroundColor Cyan
    try {
        $user = Get-MgUser -UserId $upn -ErrorAction Stop

        # --- Build password payload ---
        $body = @{
            passwordProfile = @{
                forceChangePasswordNextSignIn = [bool]$ForceChangeAtNextLogin
                password                      = $NewPassword
            }
        }

        # --- Apply password reset ---
        Update-MgUser -UserId $user.Id -BodyParameter $body -ErrorAction Stop

        $results += [PSCustomObject]@{
            UserUPN     = $upn
            NewPassword = $NewPassword
            ForceChange = [bool]$ForceChangeAtNextLogin
            Status      = "✅ Password reset successfully"
        }

        Write-Host "   🔑 New Password: $NewPassword" -ForegroundColor Yellow
        if ($ForceChangeAtNextLogin) {
            Write-Host "   🔁 User must change password at next login" -ForegroundColor DarkGray
        }
        else {
            Write-Host "   🟢 Password remains active (no change required at next login)" -ForegroundColor DarkGray
        }
    }
    catch {
        $results += [PSCustomObject]@{
            UserUPN     = $upn
            NewPassword = ""
            ForceChange = $null
            Status      = "❌ Failed — $($_.Exception.Message)"
        }
        Write-Host "❌ Failed to reset password for $upn — $($_.Exception.Message)" -ForegroundColor Red
    }
}

# --- Print single summary JSON ---
Write-Host "`n────────────── Summary ──────────────" -ForegroundColor Cyan
Write-Host ($results | ConvertTo-Json -Depth 3)
Write-Host "─────────────────────────────────────" -ForegroundColor Cyan

Disconnect-MgGraph | Out-Null
[Environment]::Exit(0)
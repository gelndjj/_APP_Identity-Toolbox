param(
    [Parameter(Mandatory = $true)]
    [string[]]$UserPrincipalName,

    [Parameter(Mandatory = $true)]
    [string]$NewPasswordBase64,

    [switch]$NoForceChange
)

# Determine if the user must change password
$ForceChangeAtNextLogin = -not $NoForceChange

Write-Host "🔐 Resetting password(s)..."

# Decode password
try {
    $NewPassword = [System.Text.Encoding]::UTF8.GetString(
        [System.Convert]::FromBase64String($NewPasswordBase64)
    )
}
catch {
    Write-Host "❌ Failed to decode Base64 password: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

if (-not $NewPassword) {
    Write-Host "❌ Password is empty after decoding." -ForegroundColor Red
    exit 1
}

# Connect with proper scopes
Connect-MgGraph -Scopes "User.ReadWrite.All","Directory.ReadWrite.All", "User-PasswordProfile.ReadWrite.All" -NoWelcome
$ctx = Get-MgContext
Write-Host "✅ Connected as: $($ctx.Account)" -ForegroundColor Green

$results = @()

foreach ($upn in $UserPrincipalName) {
    Write-Host "`n→ Processing user: $upn" -ForegroundColor Cyan
    try {
        $user = Get-MgUser -UserId $upn -ErrorAction Stop

        $body = @{
            passwordProfile = @{
                forceChangePasswordNextSignIn = $ForceChangeAtNextLogin
                password                      = $NewPassword
            }
        }

        Update-MgUser -UserId $user.Id -BodyParameter $body -ErrorAction Stop

        $results += [PSCustomObject]@{
            UserUPN     = $upn
            NewPassword = $NewPassword
            ForceChange = $ForceChangeAtNextLogin
            Status      = "✅ Password reset successfully"
        }

        Write-Host "   🔑 New Password: $NewPassword" -ForegroundColor Yellow

    }
    catch {
        $results += [PSCustomObject]@{
            UserUPN     = $upn
            NewPassword = ""
            ForceChange = $ForceChangeAtNextLogin
            Status      = "❌ Failed — $($_.Exception.Message)"
        }
        Write-Host "❌ Failed to reset password for $upn — $($_.Exception.Message)" -ForegroundColor Red
    }
}

Write-Host "`n────────────── Summary ──────────────" -ForegroundColor Cyan
Write-Host ($results | ConvertTo-Json -Depth 3)
Write-Host "─────────────────────────────────────" -ForegroundColor Cyan

Disconnect-MgGraph | Out-Null
exit 0
# =====================================================================
# Disable Entra ID Users & Remove Memberships (Graph + Exchange)
# Requires: PowerShell 7.2+, Microsoft.Graph, ExchangeOnlineManagement
# =====================================================================

param(
    [Parameter(Mandatory = $true)]
    [string[]]$upn   # Accepts one OR many UPNs
)

# If Python sends "user1,user2", split it into an array
if ($upn.Count -eq 1 -and $upn[0] -like "*,*") {
    $upn = $upn[0] -split ","
}

$upns = $upn

$PSStyle.OutputRendering = 'PlainText'

# --- Ensure required Graph modules are available ---
$graphModules = @(
    'Microsoft.Graph.Authentication',
    'Microsoft.Graph.Users',
    'Microsoft.Graph.Groups'
)
foreach ($m in $graphModules) {
    if (-not (Get-Module -ListAvailable -Name $m)) {
        Install-Module $m -Scope CurrentUser -Force
    }
}

# --- Connect to Graph ---
Connect-MgGraph -Scopes "User.ReadWrite.All,Group.ReadWrite.All,GroupMember.ReadWrite.All,Directory.ReadWrite.All" -NoWelcome
$ctx = Get-MgContext
Write-Host "Connected to Microsoft Graph as: $($ctx.Account)" -ForegroundColor Green

# Try to connect to EXO once (optional)
$exoAvailable = $false
try {
    Import-Module ExchangeOnlineManagement -ErrorAction Stop
    Connect-ExchangeOnline -ShowBanner:$false
    $exoAvailable = $true
} catch {
    Write-Host "⚠️ Exchange Online not available or insufficient permissions. DL cleanup will be skipped." -ForegroundColor Cyan
}

foreach ($u in $upns) {
    Write-Host "`n============================================================"
    Write-Host "Processing: $u"
    Write-Host "============================================================`n"

    # --- Validate user ---
    $user = Get-MgUser -UserId $u -ErrorAction SilentlyContinue
    if (-not $user) {
        Write-Host "❌ User '$u' not found." -ForegroundColor Red
        continue
    }

    # --- Disable the user ---
    Write-Host "Disabling account: $($user.DisplayName) <$($user.UserPrincipalName)>"
    Update-MgUser -UserId $user.Id -AccountEnabled:$false
    Write-Host "✅ User has been disabled successfully." -ForegroundColor Green

    # =================================================================
    # GRAPH PART: Remove from Entra ID direct groups (per user)
    # =================================================================
    $dynamic = @()
    $failed  = @()

    $groups = Get-MgUserMemberOfAsGroup -UserId $user.Id -All
    if (-not $groups) {
        Write-Host "User is not a direct member of any groups." -ForegroundColor Yellow
    } else {
        Write-Host "User is direct member of $($groups.Count) groups. Attempting removal..."
        $removed = 0

        foreach ($g in $groups) {
            $details = Get-MgGroup -GroupId $g.Id -Property "id,displayName,membershipRule" -ErrorAction SilentlyContinue
            if ($details -and $details.membershipRule) {
                $dynamic += $details
                Write-Host "⚠️ Dynamic group (cannot remove manually): $($details.DisplayName)" -ForegroundColor Cyan
                continue
            }

            try {
                Remove-MgGroupMemberDirectoryObjectByRef -GroupId $g.Id -DirectoryObjectId $user.Id -ErrorAction Stop
                $removed++
                Write-Host "Removed from group: $($g.DisplayName)" -ForegroundColor Yellow
            } catch {
                $failed += [PSCustomObject]@{ DisplayName = $g.DisplayName; Id = $g.Id; Error = $_.Exception.Message }
                Write-Host "❌ Could not remove from group: $($g.DisplayName) — $($_.Exception.Message)" -ForegroundColor Red
            }
        }
        Write-Host "✅ Removed from $removed Graph-managed groups."
    }

    # =================================================================
    # MANAGER: Remove manager relationship
    # =================================================================
    try {
        $currentManager = Get-MgUserManager -UserId $user.Id -ErrorAction SilentlyContinue
        if ($currentManager) {
            Remove-MgUserManagerByRef -UserId $user.Id -ErrorAction Stop
            Write-Host "Manager removed." -ForegroundColor Yellow
        } else {
            Write-Host "No manager assigned to user." -ForegroundColor Green
        }
    } catch {
        Write-Host "❌ Could not remove manager — $($_.Exception.Message)" -ForegroundColor Red
    }

    # =================================================================
    # EXCHANGE: Remove from all DLs (if EXO available)
    # =================================================================
    if ($exoAvailable) {
        Write-Host "Checking for mail-enabled security groups / DL memberships..."
        $allDLs  = Get-DistributionGroup -ResultSize Unlimited -ErrorAction SilentlyContinue
        $userDLs = @()

        foreach ($dl in $allDLs) {
            try {
                $members  = Get-DistributionGroupMember -Identity $dl.Identity -ResultSize Unlimited -ErrorAction Stop
                $isMember = $members | Where-Object { $_.PrimarySmtpAddress -ieq $user.UserPrincipalName }
                if ($isMember) { $userDLs += $dl }
            } catch { }
        }

        if ($userDLs.Count -gt 0) {
            Write-Host "User is member of $($userDLs.Count) DL(s). Removing..."
            foreach ($dl in $userDLs) {
                try {
                    Remove-DistributionGroupMember -Identity $dl.Identity -Member $user.UserPrincipalName -Confirm:$false -ErrorAction Stop
                    Write-Host "Removed from DL: $($dl.DisplayName)" -ForegroundColor Yellow
                } catch {
                    Write-Host "❌ Could not remove from DL: $($dl.DisplayName) — $($_.Exception.Message)" -ForegroundColor Red
                }
            }
        } else {
            Write-Host "No DL memberships found." -ForegroundColor Green
        }
    }

    # =================================================================
    # ROLES: Active + Eligible
    # =================================================================
    $activeRoles = Get-MgRoleManagementDirectoryRoleAssignment -Filter "principalId eq '$($user.Id)'" -All
    if ($activeRoles) {
        Write-Host "User has $($activeRoles.Count) active role assignment(s). Removing..."
        foreach ($ra in $activeRoles) {
            try {
                Remove-MgRoleManagementDirectoryRoleAssignment -UnifiedRoleAssignmentId $ra.Id -ErrorAction Stop
                Write-Host "Removed active role assignment: RoleDefinitionId=$($ra.RoleDefinitionId)" -ForegroundColor Yellow
            } catch {
                Write-Host "❌ Could not remove active role assignment — $($_.Exception.Message)" -ForegroundColor Red
            }
        }
    } else {
        Write-Host "No active role assignments found." -ForegroundColor Green
    }

    $eligibleRoles = Get-MgRoleManagementDirectoryRoleEligibilitySchedule -Filter "principalId eq '$($user.Id)'" -All
    if ($eligibleRoles) {
        Write-Host "User has $($eligibleRoles.Count) eligible role assignment(s)." -ForegroundColor Yellow
        foreach ($er in $eligibleRoles) {
            Write-Host "⚠️ Eligible role (cannot remove via Graph API): RoleDefinitionId=$($er.RoleDefinitionId)" -ForegroundColor Cyan
        }
        Write-Host "Note: Eligible role assignments must be removed via Entra PIM (portal) or beta Graph API." -ForegroundColor DarkGray
    } else {
        Write-Host "No eligible role assignments found." -ForegroundColor Green
    }

    # =================================================================
    # LICENSES
    # =================================================================
    $licenses = Get-MgUserLicenseDetail -UserId $user.Id -All
    if ($licenses) {
        Write-Host "User has $($licenses.Count) assigned license(s). Removing..."
        $skuIds = $licenses.SkuId
        try {
            Set-MgUserLicense -UserId $user.Id -AddLicenses @() -RemoveLicenses $skuIds
            Write-Host "✅ Removed all licenses from user." -ForegroundColor Yellow
        } catch {
            Write-Host "❌ Could not remove licenses — $($_.Exception.Message)" -ForegroundColor Red
        }
    } else {
        Write-Host "No licenses assigned." -ForegroundColor Green
    }

    # =================================================================
    # ACCESS PACKAGES
    # =================================================================
    try {
        $accessPackages = Get-MgEntitlementManagementAssignment `
            -Filter "target/objectId eq '$($user.Id)' and state eq 'Delivered'" `
            -ExpandProperty target -All -ErrorAction Stop

        if ($accessPackages) {
            Write-Host "User has $($accessPackages.Count) access package assignment(s). Removing..."
            foreach ($package in $accessPackages) {
                try {
                    $params = @{ requestType = "adminRemove"; assignment = @{ id = $package.Id } }
                    New-MgEntitlementManagementAssignmentRequest -BodyParameter $params -ErrorAction Stop
                    Write-Host "Removed access package: $($package.Id)" -ForegroundColor Yellow
                } catch {
                    Write-Host "❌ Could not remove access package $($package.Id) — $($_.Exception.Message)" -ForegroundColor Red
                }
            }
        } else {
            Write-Host "No access package assignments found." -ForegroundColor Green
        }
    } catch {
        Write-Host "⚠️ Failed to query/remove access packages — $($_.Exception.Message)" -ForegroundColor Cyan
    }

    # =================================================================
    # Verification (per user)
    # =================================================================
    Start-Sleep -Seconds 5
    $remaining = Get-MgUserMemberOfAsGroup -UserId $user.Id -All
    if (-not $remaining) {
        Write-Host "🎯 Verification: user now has 0 direct group memberships (Graph)." -ForegroundColor Green
    } else {
        Write-Host "🔎 Verification: user still has $($remaining.Count) group(s):" -ForegroundColor Yellow
        $remaining | ForEach-Object { Write-Host " - $($_.DisplayName) ($($_.Id))" }
    }

    if ($dynamic.Count -gt 0) {
        Write-Host "`nℹ️ Dynamic groups (rule-based, cannot be removed manually):"
        $dynamic | ForEach-Object { Write-Host " - $($_.DisplayName)" }
    }
    if ($failed.Count -gt 0) {
        Write-Host "`n❗ Groups that failed removal (check errors):"
        $failed | ForEach-Object { Write-Host " - $($_.DisplayName): $($_.Error)" }
    }
}

# Optional clean-up
try { Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue } catch {}
Disconnect-MgGraph | Out-Null
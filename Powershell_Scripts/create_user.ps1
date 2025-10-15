param (
    [string]$CsvPath  # Path to the CSV file
)

$ErrorActionPreference = "Stop"

# ============================================================
# 1️⃣ Connect to Microsoft Graph
# ============================================================
try {
    $ctx = Get-MgContext -ErrorAction Stop
    if (-not $ctx) {
        Connect-MgGraph -Scopes "User.ReadWrite.All", "EntitlementManagement.ReadWrite.All", "Directory.ReadWrite.All" -NoWelcome
    }
} catch {
    Connect-MgGraph -Scopes "User.ReadWrite.All", "EntitlementManagement.ReadWrite.All", "Directory.ReadWrite.All" -NoWelcome
}

# ============================================================
# 2️⃣ Load AccessPackages.json for Access Package assignment
# ============================================================
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$jsonPath  = Join-Path $scriptDir "..\JSONs\AccessPackages.json"

$AccessPackageLookup = @{}

if (Test-Path $jsonPath) {
    try {
        $jsonContent = Get-Content -Raw -Path $jsonPath | ConvertFrom-Json
        foreach ($ap in $jsonContent) {
            $enabledPolicy = $ap.Policies | Where-Object { $_.Status -eq "Enabled" } | Select-Object -First 1
            if ($enabledPolicy) {
                $AccessPackageLookup[$ap.AccessPackageName] = @{
                    AccessPackageId = $ap.AccessPackageId
                    PolicyId        = $enabledPolicy.PolicyId
                }
            }
        }

        Write-Host "`n📦 Loaded Access Packages from JSON:"
        foreach ($item in $AccessPackageLookup.GetEnumerator()) {
            Write-Host "  - $($item.Key): PackageID=$($item.Value.AccessPackageId) | PolicyID=$($item.Value.PolicyId)"
        }
    } catch {
        Write-Host "⚠️ Failed to parse AccessPackages.json: $_" -ForegroundColor Yellow
    }
} else {
    Write-Host "⚠️ AccessPackages.json not found in $jsonPath" -ForegroundColor Yellow
}

# ============================================================
# 3️⃣ Validate CSV and import users
# ============================================================
if (-not (Test-Path -Path $CsvPath)) {
    throw "❌ CSV file not found: $CsvPath"
}

$users = Import-Csv -Path $CsvPath

# ============================================================
# 4️⃣ Process each user
# ============================================================
foreach ($user in $users) {
    $UPN = $user.'User Principal Name'

    Write-Host "`n============================================================"
    Write-Host "Processing user: $UPN"
    Write-Host "============================================================`n"

    if (-not $UPN) {
        Write-Host "⚠️ Skipping row with missing UPN"
        continue
    }

    $existingUser = Get-MgUser -Filter "userPrincipalName eq '$UPN'" -ErrorAction SilentlyContinue

    if ($null -eq $existingUser) {
        Write-Host "🔎 User does not exist, creating new one..."

        # --- Password profile ---
        $passwordProfile = @{
            Password = $user.Password
            ForceChangePasswordNextSignIn = $true
        }

        # --- Core user details ---
        $userDetails = @{
            AccountEnabled    = $true
            DisplayName       = $user.'Display Name'
            UserPrincipalName = $UPN
            MailNickname      = ($UPN.Split('@')[0] -replace '[^a-zA-Z0-9]', '')
            PasswordProfile   = $passwordProfile
        }

        # --- Optional attributes mapping ---
        $attributeMapping = @{
            'First name'                 = "GivenName"
            'Last name'                  = "Surname"
            'Job title'                  = "JobTitle"
            'Company name'               = "CompanyName"
            'Department'                 = "Department"
            'Employee ID'                = "EmployeeId"
            'Employee type'              = "EmployeeType"
            'Office location'            = "OfficeLocation"
            'Street address'             = "StreetAddress"
            'City'                       = "City"
            'State or province'          = "State"
            'ZIP or postal code'         = "PostalCode"
            'Country or region'          = "Country"
            'Usage location'             = "UsageLocation"
            'Preferred data location'    = "PreferredDataLocation"
            'Age group'                  = "AgeGroup"
            'Consent provided for minor' = "ConsentProvidedForMinor"
        }

        foreach ($key in $attributeMapping.Keys) {
            if (-not [string]::IsNullOrEmpty($user.$key)) {
                $userDetails[$attributeMapping[$key]] = $user.$key
            }
        }

        # --- Lists ---
        if ($user.'Business phone') {
            $userDetails.BusinessPhones = $user.'Business phone' -split ';'
        }
        if ($user.'Mobile phone') {
            $userDetails.MobilePhone = $user.'Mobile phone'
        }
        if ($user.'Other emails') {
            $userDetails.OtherMails = $user.'Other emails' -split ';'
        }
        if ($user.'Proxy addresses') {
            $userDetails.ProxyAddresses = $user.'Proxy addresses' -split ';'
        }
        if ($user.'IM addresses') {
            $userDetails.IMAddresses = $user.'IM addresses' -split ';'
        }
        if ($user.'Employee hire date') {
            try {
                $userDetails.EmployeeHireDate = [datetime]::ParseExact($user.'Employee hire date', 'yyyy-MM-dd', $null)
            } catch {
                Write-Host "⚠️ Invalid EmployeeHireDate format: $($user.'Employee hire date')"
            }
        }

        # --- Create user ---
        Write-Host "🚀 Creating user in Entra ID..."
        try {
            $newUser = New-MgUser -BodyParameter $userDetails
            Write-Host "✅ Created new user: $UPN (User ID: $($newUser.Id))" -ForegroundColor Green

            # --- Manager assignment ---
            if ($user.'Manager') {
                try {
                    $manager = Get-MgUser -Filter "userPrincipalName eq '$($user.'Manager')'" -ErrorAction Stop
                    if ($manager) {
                        $ref = @{ "@odata.id" = "https://graph.microsoft.com/v1.0/users/$($manager.Id)" }
                        Set-MgUserManagerByRef -UserId $newUser.Id -BodyParameter $ref
                        Write-Host "   - ✅ Manager set to: $($user.'Manager')" -ForegroundColor Green
                    }
                } catch {
                    Write-Host "❌ Could not assign manager ($($user.'Manager')) for $UPN - $_" -ForegroundColor Red
                }
            }

            # --- Sponsors ---
            if ($user.'Sponsors') {
                $sponsorUPNs = $user.'Sponsors' -split ';'
                foreach ($sponsorUPN in $sponsorUPNs) {
                    try {
                        $sponsor = Get-MgUser -Filter "userPrincipalName eq '$sponsorUPN'" -ErrorAction Stop
                        if ($sponsor) {
                            $uri = "https://graph.microsoft.com/beta/users/$($newUser.Id)/sponsors/`$ref"
                            $body = @{ "@odata.id" = "https://graph.microsoft.com/beta/users/$($sponsor.Id)" } | ConvertTo-Json -Compress
                            Invoke-MgGraphRequest -Method POST -Uri $uri -Body $body -ContentType "application/json"
                            Write-Host "   - ✅ Sponsor added: $sponsorUPN" -ForegroundColor Green
                        }
                    } catch {
                        Write-Host "❌ Sponsor assignment failed ($sponsorUPN): $_" -ForegroundColor Red
                    }
                }
            }

            # --- Access Package assignment ---
            if ($user.'Access Package') {
                Write-Host "🧩 CSV Access Package column value detected: '$($user.'Access Package')'" -ForegroundColor Cyan
                $packageName = $user.'Access Package'

                if ($AccessPackageLookup.ContainsKey($packageName)) {
                    $pkg = $AccessPackageLookup[$packageName]
                    $accessPackageId = $pkg.AccessPackageId
                    $policyId = $pkg.PolicyId
                    $userId = $newUser.Id

                    Write-Host "🎁 Assigning Access Package '$packageName'..." -ForegroundColor Cyan
                    Write-Host "   ↳ AccessPackageID: $accessPackageId" -ForegroundColor DarkGray
                    Write-Host "   ↳ PolicyID: $policyId" -ForegroundColor DarkGray
                    Write-Host "   ↳ Target UserID: $userId" -ForegroundColor DarkGray

                    $params = @{
                        requestType = "AdminAdd"
                        assignment = @{
                            targetId = $userId
                            assignmentPolicyId = $policyId
                            accessPackageId = $accessPackageId
                        }
                    }

                    try {
                        $response = New-MgEntitlementManagementAssignmentRequest -BodyParameter $params -ErrorAction Stop
                        if ($response.id) {
                            Write-Host "   - ✅ Access Package assignment request created successfully (Request ID: $($response.id))" -ForegroundColor Green
                        } else {
                            Write-Host "   - ⚠️ Assignment request created but no ID returned. Check Entra portal." -ForegroundColor Yellow
                        }
                    } catch {
                        Write-Host "❌ Failed to assign Access Package '$packageName' - $($_.Exception.Message)" -ForegroundColor Red
                        if ($_.ErrorDetails.Message) {
                            Write-Host "🪣 Graph Response: $($_.ErrorDetails.Message)" -ForegroundColor DarkYellow
                        }
                    }
                }
                else {
                    Write-Host "⚠️ Access Package '$packageName' not found in AccessPackages.json lookup." -ForegroundColor Yellow
                }
            }
        } catch {
            Write-Host "❌ Failed to create user: $UPN - $_" -ForegroundColor Red
            continue
        }

    } else {
        Write-Host "⚠️ User already exists: $UPN (User ID: $($existingUser.Id))" -ForegroundColor Yellow
    }
}
param (
    [string]$CsvPath  # Path to the CSV file
)

$ErrorActionPreference = "Stop"

# Ensure Microsoft Graph connection
try {
    $ctx = Get-MgContext -ErrorAction Stop
    if (-not $ctx) {
        Connect-MgGraph -Scopes "User.ReadWrite.All", "EntitlementManagement.ReadWrite.All", "Directory.ReadWrite.All" -NoWelcome
    }
} catch {
    Connect-MgGraph -Scopes "User.ReadWrite.All", "EntitlementManagement.ReadWrite.All", "Directory.ReadWrite.All" -NoWelcome
}

# Validate CSV file
if (-not (Test-Path -Path $CsvPath)) {
    throw "❌ CSV file not found: $CsvPath"
}

# Import the CSV
$users = Import-Csv -Path $CsvPath

foreach ($user in $users) {
    $UPN = $user.'User Principal Name'

    Write-Host "`n============================================================"
    Write-Host "Processing user: $UPN"
    Write-Host "============================================================`n"

    if (-not $UPN) {
        Write-Host "⚠️ Skipping row with missing UPN"
        continue
    }

    # Check if user exists
    $existingUser = Get-MgUser -Filter "userPrincipalName eq '$UPN'" -ErrorAction SilentlyContinue

    if ($null -eq $existingUser) {
        Write-Host "🔎 User does not exist, creating new one..."

        # Define password profile
        $passwordProfile = @{
            Password = $user.Password
            ForceChangePasswordNextSignIn = $true
        }

        # Define mandatory properties
        $userDetails = @{
            AccountEnabled    = $true
            DisplayName       = $user.'Display Name'
            UserPrincipalName = $UPN
            MailNickname      = ($UPN.Split('@')[0] -replace '[^a-zA-Z0-9]', '')
            PasswordProfile   = $passwordProfile
        }

        Write-Host "📝 Setting mandatory attributes:"
        Write-Host "   - DisplayName       : $($user.'Display Name')"
        Write-Host "   - UserPrincipalName : $UPN"
        Write-Host "   - MailNickname      : $($UPN.Split('@')[0] -replace '[^a-zA-Z0-9]', '')"

        # Map optional attributes
        $attributeMapping = @{
            'First name'                     = "GivenName"
            'Last name'                      = "Surname"
            'Job title'                      = "JobTitle"
            'Company name'                   = "CompanyName"
            'Department'                     = "Department"
            'Employee ID'                    = "EmployeeId"
            'Employee type'                  = "EmployeeType"
            'Office location'                = "OfficeLocation"
            'Street address'                 = "StreetAddress"
            'City'                           = "City"
            'State or province'              = "State"
            'ZIP or postal code'             = "PostalCode"
            'Country or region'              = "Country"
            'Usage location'                 = "UsageLocation"
            'Preferred data location'        = "PreferredDataLocation"
            'Age group'                      = "AgeGroup"
            'Legal age group classification' = "LegalAgeGroupClassification"
            'Consent provided for minor'     = "ConsentProvidedForMinor"
            'Employee hire date'             = "EmployeeHireDate"
        }

        foreach ($key in $attributeMapping.Keys) {
            if (-not [string]::IsNullOrEmpty($user.$key)) {
                $userDetails[$attributeMapping[$key]] = $user.$key
                Write-Host "   - $($attributeMapping[$key]) : $($user.$key)"
            }
        }

        # Handle lists
        if ($user.'Business phone') {
            $userDetails.BusinessPhones = $user.'Business phone' -split ';'
            Write-Host "   - BusinessPhones : $($user.'Business phone')"
        }
        if ($user.'Mobile phone') {
            $userDetails.MobilePhone = $user.'Mobile phone'
            Write-Host "   - MobilePhone : $($user.'Mobile phone')"
        }
        if ($user.'Other emails') {
            $userDetails.OtherMails = $user.'Other emails' -split ';'
            Write-Host "   - OtherMails : $($user.'Other emails')"
        }
        if ($user.'Proxy addresses') {
            $userDetails.ProxyAddresses = $user.'Proxy addresses' -split ';'
            Write-Host "   - ProxyAddresses : $($user.'Proxy addresses')"
        }
        if ($user.'IM addresses') {
            $userDetails.IMAddresses = $user.'IM addresses' -split ';'
            Write-Host "   - IMAddresses : $($user.'IM addresses')"
        }

        Write-Host "🚀 Creating user in Entra ID..."
        try {
            $newUser = New-MgUser -BodyParameter $userDetails
            Write-Host "✅ Created new user: $UPN (User ID: $($newUser.Id))" -ForegroundColor Green
        } catch {
            Write-Host "❌ Failed to create user: $UPN - $_" -ForegroundColor Red
            continue
        }
    } else {
        Write-Host "⚠️ User already exists: $UPN (User ID: $($existingUser.Id))" -ForegroundColor Yellow
    }
}
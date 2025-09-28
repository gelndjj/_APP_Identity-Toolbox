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

    if (-not $UPN) {
        Write-Host "⚠️ Skipping row with missing UPN"
        continue
    }

    # Check if user exists
    $existingUser = Get-MgUser -Filter "userPrincipalName eq '$UPN'" -ErrorAction SilentlyContinue

    if ($null -eq $existingUser) {
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
            }
        }

        # Handle lists
        if ($user.'Business phone') { $userDetails.BusinessPhones = $user.'Business phone' -split ';' }
        if ($user.'Mobile phone')   { $userDetails.MobilePhone    = $user.'Mobile phone' } # single value
        if ($user.'Other emails')   { $userDetails.OtherMails     = $user.'Other emails'   -split ';' }
        if ($user.'Proxy addresses'){ $userDetails.ProxyAddresses = $user.'Proxy addresses' -split ';' }
        if ($user.'IM addresses')   { $userDetails.IMAddresses    = $user.'IM addresses'   -split ';' }
        if ($user.'Fax number')     { $userDetails.FaxNumber      = $user.'Fax number' } # NOTE: not in Graph user schema – needs testing

        try {
            $newUser = New-MgUser -BodyParameter $userDetails
            Write-Host "✅ Created new user: $UPN (User ID: $($newUser.Id))"
        } catch {
            Write-Host "❌ Failed to create user: $UPN - $_"
            continue
        }
    } else {
        Write-Host "⚠️ User already exists: $UPN (User ID: $($existingUser.Id))"
    }
}
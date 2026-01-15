$properties = @(
    "Id",
    "DisplayName",
    "Surname",
    "GivenName",
    "UserPrincipalName",
    "UserType",
    "CreatedDateTime",
    "LastPasswordChangeDateTime",
    "PasswordPolicies",
    "PreferredLanguage",
    "SignInSessionsValidFromDateTime",
    "JobTitle",
    "CompanyName",
    "Department",
    "EmployeeId",
    "EmployeeType",
    "EmployeeHireDate",
    "EmployeeLeaveDateTime",
    "Manager",
    "StreetAddress",
    "City",
    "State",
    "PostalCode",
    "Country",
    "BusinessPhones",
    "MobilePhone",
    "Mail",
    "OtherMails",
    "ProxyAddresses",
    "ImAddresses",
    "Mailnickname",
    "AgeGroup",
    "ConsentProvidedForMinor",
    "LegalAgeGroupClassification",
    "AccountEnabled",
    "UsageLocation",
    "PreferredDataLocation",
    "OnPremisesSyncEnabled",
    "OnPremisesLastSyncDateTime",
    "OnPremisesDistinguishedName",
    "OnPremisesImmutableId",
    "OnPremisesSamAccountName",
    "OnPremisesUserPrincipalName",
    "OnPremisesDomainName",
    "SignInActivity",
    "onPremisesImmutableId")

$CSVproperties = @(
    # Identity section
    "Id",
    "DisplayName",
    @{Name="First name"; Expression={$_.GivenName}},
    @{Name="Last name"; Expression={$_.Surname}},
    "UserPrincipalName",
    @{Name="Domain name"; Expression = { $_.UserPrincipalName.Split('@')[1] }},
    "UserType",
    "CreatedDateTime",
    "LastPasswordChangeDateTime",
    @{Name="LicensesSkuType";Expression={[string]::join(";", ($_.LicensesSkuType))}},
    "PasswordPolicies",
    "PreferredLanguage",
    "SignInSessionsValidFromDateTime",
    # Job Information section
    "JobTitle",
    "CompanyName",
    "Department",
    "EmployeeId",
    "EmployeeType",
    "EmployeeHireDate",
    "EmployeeLeaveDateTime",
    "ManagerDisplayName",
    "ManagerUPN",
    "SponsorDisplayName",
    "SponsorUPN",
    # Contact Information
    "StreetAddress",
    "City",
    "State",
    "PostalCode",
    "Country",
    @{Name="BusinessPhones"; Expression = { ($_.BusinessPhones -join " ; ") }},
    "MobilePhone",
    "Mail",
    @{Name="OtherMails";Expression={[string]::join(" ; ", ($_.OtherMails))}},
    @{Name="ProxyAddresses";Expression={[string]::join(" ; ", ($_.ProxyAddresses))}},
    @{Name="ImAddresses";Expression={[string]::join(" ; ", ($_.ImAddresses))}},
    "Mailnickname",
    # Parental controls
    "AgeGroup",
    "ConsentProvidedForMinor",
    "LegalAgeGroupClassification",
    # Settings
    "AccountEnabled",
    "UsageLocation",
    "PreferredDataLocation",
    # On-premises
    "OnPremisesSyncEnabled",
    "OnPremisesLastSyncDateTime",
    "OnPremisesDistinguishedName",
    "OnPremisesImmutableId",
    "OnPremisesSamAccountName",
    "OnPremisesUserPrincipalName",
    "OnPremisesDomainName",
    # Authentication methods
    "DefaultAuthentication",
    "PreferredAuthentication",
    "MicrosoftAuthenticatorDisplayName",
    "EmailAuthAddress",
    "SMSPhoneNumber",
    "FIDO2DisplayName",
    "WindowsHelloEnabled",
    "SoftwareOATHEnabled",
    @{Name="AuthenticationMethod";Expression={[string]::join(" ; ", ($_.AuthenticationMethod))}},
    @{Name="LastSignInDateTime";Expression={$_.SignInActivity.LastSuccessfulSignInDateTime}}
    # Registered Devices
    @{Name="Devices";Expression={[string]::join(" ; ", ($_.Devices))}}
    )

#Requires -Version 7
Connect-MgGraph -Scopes `
"User.Read.All", `
"Directory.Read.All", `
"Device.Read.All", `
"AuditLog.Read.All", `
"UserAuthenticationMethod.Read.All", `
"EntitlementManagement.Read.All" `
-ContextScope CurrentUser -NoWelcome

# Start timing
$startTime = Get-Date

# --- Retrieve tenant info ---
$org = Get-MgOrganization | Select-Object DisplayName, Id, VerifiedDomains
$tenantInfo = [PSCustomObject]@{
    TenantName = $org.DisplayName
    TenantId   = $org.Id
    Domain     = $org.VerifiedDomains[0].Name
}

# Timestamp for the CSV file
$LogDate = Get-Date -f yyyyMMddhhmm
$Csvfile = Join-Path -Path $PSScriptRoot -ChildPath "EntraIDUsers_$LogDate.csv"

Write-Output "Retrieving all users..."
$users = Get-MgUser -All -Property $properties

# Debug - Run only on first 50 users (comment above line if used)
# $users = Get-MgUser -All -Property $properties | Select-Object -First 50

$usersDetails = [System.Collections.Concurrent.ConcurrentBag[System.Object]]::new()
$length = $users.length
$i = 0
$batchSize = 3

Write-Output "Batch Creation..."

$batches = [System.Collections.Generic.List[pscustomobject]]::new()
for ($i = 0; $i -lt $users.Length; $i += $batchSize) {
    $end = $i + $batchSize - 1
    if ($end -ge $users.Length) { $end = $users.Length }
    $index = $i * 3


    $requests = $users[$i..($end)] | ForEach-Object {
        @{
            'Id'     = "$($PSItem.Id):manager"
            'Method' = 'GET'
            'Url'    = "users/{0}/manager" -f $PSItem.Id
        },
        @{
            'Id'     = "$($PSItem.Id):sponsor"
            'Method' = 'GET'
            'Url'    = "users/{0}/sponsors" -f $PSItem.Id
        },
        @{
            'Id'     = "$($PSItem.Id):registeredDevices"
            'Method' = 'GET'
            'Url'    = "users/{0}/registeredDevices" -f $PSItem.Id
        },
        @{
            'Id'     = "$($PSItem.Id):license"
            'Method' = 'GET'
            'Url'    = "users/{0}/licenseDetails" -f $PSItem.Id
        },
        @{
            'Id'     = "$($PSItem.Id):authenticationMethods"
            'Method' = 'GET'
            'Url'    = "users/{0}/authentication/methods" -f $PSItem.Id
        },
        @{
            'Id'     = "$($PSItem.Id):authenticationPreference"
            'Method' = 'GET'
            'Url'    = "users/{0}/authentication/SignInPreferences" -f $PSItem.Id
        }
    }

    $batches.Add(@{
        'Method'      = 'Post'
        'Uri'         = 'https://graph.microsoft.com/beta/$batch'
        'ContentType' = 'application/json'
        'Body'        = @{
            'requests' = @($requests)
        } | ConvertTo-Json
    })
}

Write-Output "Sending requests"

$batches | ForEach-Object -Parallel {
    $responses = $using:usersDetails
    $request = Invoke-MgGraphRequest @PSItem
    $request.responses | ForEach-Object {$responses.Add([pscustomobject]@{
            'UserId' = $PSItem.Id.Split(":")[0]
            'requesttype' = $PSItem.Id.Split(":")[1]
            'body' = $PSItem.body
        })}
}

$usersDetails = $usersDetails | Group-Object -Property UserId -AsHashTable

Write-Output "Processing requests"

foreach ($user in $users) {
    $DaySinceLastCo = ($user.SignInActivity.LastSuccessfulSignInDateTime - (Get-Date)).Days

    if ($usersDetails.ContainsKey($user.Id)) {

        # Initialize authentication-related arrays and variables
        $authenticationType = @()
        $microsoftAuthenticatorDisplayName = $null
        $emailAuthAddress = $null
        $smsPhoneNumber = $null
        $fido2DisplayName = $null
        $windowsHelloEnabled = $false
        $softwareOathEnabled = $false

        # Loop through authentication methods and gather details
        foreach ($method in ($usersDetails[$user.Id] | Where { $_.requesttype -eq "authenticationMethods" }).body.value) {
            $odataType = $method.'@odata.type'
            switch ($odataType) {
                "#microsoft.graph.microsoftAuthenticatorAuthenticationMethod" {
                    $authenticationType += "MicrosoftAuthenticator"
                    $microsoftAuthenticatorDisplayName = $method.displayName
                }
                "#microsoft.graph.softwareOathAuthenticationMethod" {
                    $authenticationType += "SoftwareOath"
                    $softwareOathEnabled = $true
                }
                "#microsoft.graph.phoneAuthenticationMethod" {
                    $authenticationType += "SMS"
                    $smsPhoneNumber = $method.phoneNumber
                }
                "#microsoft.graph.emailAuthenticationMethod" {
                    $authenticationType += "Email"
                    $emailAuthAddress = $method.emailAddress
                }
                "#microsoft.graph.fido2AuthenticationMethod" {
                    $authenticationType += "Fido2"
                    $fido2DisplayName = $method.displayName
                }
                "#microsoft.graph.windowsHelloForBusinessAuthenticationMethod" {
                    $authenticationType += "Windows Hello"
                    $windowsHelloEnabled = $true
                }
            }
        }

        # Add all properties in one grouped block
        $user | Add-Member -MemberType NoteProperty -Name Devices `
            -Value ($usersDetails[$user.Id] | Where { $_.requesttype -eq "registeredDevices" }).body.value.displayName -Force

        $user | Add-Member -MemberType NoteProperty -Name ManagerUPN `
            -Value ($usersDetails[$user.Id] | Where { $_.requesttype -eq "manager" }).body.userPrincipalName -Force

        $user | Add-Member -MemberType NoteProperty -Name SponsorUPN `
            -Value (($usersDetails[$user.Id] | Where { $_.requesttype -eq "sponsor" }).body.value)[0].userPrincipalName -Force

        $user | Add-Member -MemberType NoteProperty -Name ManagerDisplayName `
            -Value ($usersDetails[$user.Id] | Where { $_.requesttype -eq "manager" }).body.displayName -Force

        $user | Add-Member -MemberType NoteProperty -Name SponsorDisplayName `
            -Value ($usersDetails[$user.Id] | Where { $_.requesttype -eq "sponsor" }).body.value[0].displayName -Force

        $user | Add-Member -MemberType NoteProperty -Name LicensesSkuType `
            -Value (($usersDetails[$user.Id] | Where { $_.requesttype -eq "license" }).body.value | Select-Object -ExpandProperty SkuPartNumber) -Force

        $user | Add-Member -MemberType NoteProperty -Name DefaultAuthentication `
            -Value (($usersDetails[$user.Id] | Where { $_.requesttype -eq "authenticationPreference" }).body.systemPreferredAuthenticationMethod ?? "Not set") -Force

        $user | Add-Member -MemberType NoteProperty -Name PreferredAuthentication `
            -Value $($($usersDetails[$user.Id] | where {$_.requesttype -eq "authenticationPreference"}).body.userPreferredMethodForSecondaryAuthentication ?? "Not set") -Force

        $user | Add-Member -MemberType NoteProperty -Name AuthenticationMethod -Value $authenticationType -Force

        # Add dedicated columns for detailed authentication methods
        $user | Add-Member NoteProperty MicrosoftAuthenticatorDisplayName $microsoftAuthenticatorDisplayName -Force
        $user | Add-Member NoteProperty EmailAuthAddress $emailAuthAddress -Force
        $user | Add-Member NoteProperty SMSPhoneNumber $smsPhoneNumber -Force
        $user | Add-Member NoteProperty FIDO2DisplayName $fido2DisplayName -Force
        $user | Add-Member NoteProperty WindowsHelloEnabled $windowsHelloEnabled -Force
        $user | Add-Member NoteProperty SoftwareOATHEnabled $softwareOathEnabled -Force
    }

    if ($user.OnPremisesSyncEnabled -eq $true) {
        $user.EmployeeLeaveDateTime = $user.OnPremisesExtensionAttributes.ExtensionAttribute1
    }
}

Disconnect-MgGraph

# ==== CSV with timestamp ====
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$fileName  = "${timestamp}_EntraIdentities.csv"

# Go one level up from script folder (Powershell_Scripts → _APP_Toolbox)
$rootDir = Split-Path -Parent $PSScriptRoot

# Database_Identity folder at root level
$identityFolder = Join-Path $rootDir "Database_Identity"

# Ensure the folder exists
if (-not (Test-Path $identityFolder)) {
    New-Item -ItemType Directory -Path $identityFolder | Out-Null
}

# Final CSV path
$csvPath = Join-Path $identityFolder $fileName

# Write CSV
Write-Output "Writing CSV to: $csvPath"
$users | Select-Object -Property $CSVproperties | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8 -Delimiter ';'

# Save tenant info JSON in the same Database_Identity folder as the CSV
$tenantJsonPath = Join-Path $identityFolder "TenantInfo.json"
$tenantInfo | ConvertTo-Json | Set-Content -Path $tenantJsonPath -Encoding UTF8

# End timing
$endTime = Get-Date
$elapsed = $endTime - $startTime

Write-Host "Entra ID user export completed. File saved at: $csvPath" -ForegroundColor Green
Write-Host "Total time: $($elapsed.Minutes) minutes and $($elapsed.Seconds) seconds." -ForegroundColor Cyan

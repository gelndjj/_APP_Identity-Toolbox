param (
    [string]$CsvPath
)

$ErrorActionPreference = 'Stop'

# -------------------------------
# Connect to Microsoft Graph
# -------------------------------
try {
    $ctx = Get-MgContext -ErrorAction Stop
    if (-not $ctx) {
        Connect-MgGraph -Scopes 'User.ReadWrite.All','Directory.ReadWrite.All' -NoWelcome
    }
} catch {
    Connect-MgGraph -Scopes 'User.ReadWrite.All','Directory.ReadWrite.All' -NoWelcome
}

if (-not (Test-Path -LiteralPath $CsvPath)) {
    throw "❌ CSV file not found: $CsvPath"
}

# -------------------------------
# Helper: strong password when blank (optional)
# -------------------------------
function New-StrongPassword([int]$Length = 16) {
    $upper = 'ABCDEFGHJKLMNPQRSTUVWXYZ'.ToCharArray()
    $lower = 'abcdefghijkmnpqrstuvwxyz'.ToCharArray()
    $digit = '23456789'.ToCharArray()
    $sym   = '@#$%&*+-_!?'.ToCharArray()
    $all   = $upper + $lower + $digit + $sym

    # Guarantee at least one of each type
    $base = @(
        ($upper | Get-Random -Count 1)
        ($lower | Get-Random -Count 1)
        ($digit | Get-Random -Count 1)
        ($sym   | Get-Random -Count 1)
    )

    # Fill remaining characters randomly from all allowed
    $remaining = $Length - $base.Count
    if ($remaining -gt 0) {
        $base += (1..$remaining | ForEach-Object { $all | Get-Random -Count 1 })
    }

    # Shuffle and return as a string
    -join ($base | Sort-Object { Get-Random })
}

# -------------------------------
# CSV parsing by column index
# -------------------------------
Add-Type -AssemblyName Microsoft.VisualBasic
$parser = New-Object Microsoft.VisualBasic.FileIO.TextFieldParser($CsvPath)
$parser.TextFieldType = 'Delimited'
$parser.SetDelimiters(';')   # <<<<<< FIXED HERE

# Read header row (we won’t use names, only order)
$headers = $parser.ReadFields()
Write-Host "📋 Detected columns ($($headers.Count)):" -ForegroundColor Cyan
Write-Host "  " ($headers -join ' | ')

$lineNo = 1
while (-not $parser.EndOfData) {
    $lineNo++
    $cells = $parser.ReadFields()

    # Guard against short rows
    if (-not $cells -or $cells.Count -lt 23) {
        Write-Host "⚠️  Skipping row $lineNo — expected 23 columns, got $($cells.Count)." -ForegroundColor Yellow
        continue
    }

    # ------------------------------------------------------
    # Column mapping (0-based index, exact order you gave)
    # ------------------------------------------------------
    $DisplayName             = $cells[0].Trim()
    $GivenName               = $cells[1].Trim()
    $Surname                 = $cells[2].Trim()
    $UPN                     = $cells[3].Trim()
    $Password                = $cells[4]
    $JobTitle                = $cells[5].Trim()
    $CompanyName             = $cells[6].Trim()
    $Department              = $cells[7].Trim()
    $EmployeeId              = $cells[8].Trim()
    $City                    = $cells[9].Trim()
    $Country                 = $cells[10].Trim()
    $State                   = $cells[11].Trim()
    $OfficeLocation          = $cells[12].Trim()
    $StreetAddress           = $cells[13].Trim()
    $ManagerUPN              = $cells[14].Trim()
    $SponsorsRaw             = $cells[15].Trim()
    $UsageLocation           = $cells[16].Trim()
    $PostalCode              = $cells[17].Trim()
    $BusinessPhoneMobileRaw  = $cells[18].Trim()   # NOTE: header is combined
    $OtherEmailsRaw          = $cells[19].Trim()
    $AgeGroup                = $cells[20].Trim()
    $ConsentForMinor         = $cells[21].Trim()
    $AccessPackage           = $cells[22].Trim()   # not used at creation time

    if (-not $UPN) {
        Write-Host "⚠️  Skipping row $lineNo — missing UPN."
        continue
    }

    # Already exists?
    $existing = Get-MgUser -Filter "userPrincipalName eq '$($UPN.Replace("'", "''"))'" -ErrorAction SilentlyContinue
    if ($existing) {
        Write-Host "⚠️  User already exists: $UPN"
        continue
    }

    Write-Host "`n============================================================"
    Write-Host "Processing user: $UPN"
    Write-Host "============================================================`n"

    # Normalize usage location (must be 2 letters)
    if ($UsageLocation) {
        $UsageLocation = $UsageLocation.Substring(0, [Math]::Min(2, $UsageLocation.Length)).ToUpperInvariant()
    }

    # Password: auto-generate if blank or too weak (less than 8 chars or missing complexity)
    $generated = $false

    if ([string]::IsNullOrWhiteSpace($Password) -or
        $Password.Length -lt 8 -or
        -not ($Password -match '[A-Z]' -and $Password -match '[a-z]' -and $Password -match '\d' -and $Password -match '[@#$%&*+\-_!?]')) {

        $Password = New-StrongPassword 16
        $generated = $true
        Write-Host "🔐 Generated new strong password for user: $UPN" -ForegroundColor Cyan
    } else {
        Write-Host "🔎 Using password from CSV for user: $UPN"
    }

    Write-Host "   → Final password used: $Password" -ForegroundColor DarkGray

    $passwordProfile = @{
        Password = $Password
        ForceChangePasswordNextSignIn = $true
    }

    $userDetails = @{
        AccountEnabled    = $true
        DisplayName       = $DisplayName
        UserPrincipalName = $UPN
        MailNickname      = ($UPN.Split('@')[0] -replace '[^a-zA-Z0-9]', '')
        PasswordProfile   = $passwordProfile
    }

    # Optional scalar attributes
    if ($GivenName)       { $userDetails.GivenName       = $GivenName }
    if ($Surname)         { $userDetails.Surname         = $Surname }
    if ($JobTitle)        { $userDetails.JobTitle        = $JobTitle }
    if ($CompanyName)     { $userDetails.CompanyName     = $CompanyName }
    if ($Department)      { $userDetails.Department      = $Department }
    if ($EmployeeId)      { $userDetails.EmployeeId      = $EmployeeId }
    if ($City)            { $userDetails.City            = $City }
    if ($State)           { $userDetails.State           = $State }
    if ($Country)         { $userDetails.Country         = $Country }
    if ($OfficeLocation)  { $userDetails.OfficeLocation  = $OfficeLocation }
    if ($StreetAddress)   { $userDetails.StreetAddress   = $StreetAddress }
    if ($PostalCode)      { $userDetails.PostalCode      = $PostalCode }
    if ($UsageLocation)   { $userDetails.UsageLocation   = $UsageLocation }
    if ($AgeGroup)        { $userDetails.AgeGroup        = $AgeGroup } # Minor|NotAdult|Adult
    if ($ConsentForMinor) { $userDetails.ConsentProvidedForMinor = $ConsentForMinor } # Granted|Denied|notRequired

    # Lists
    if ($BusinessPhoneMobileRaw) { $userDetails.BusinessPhones = @($BusinessPhoneMobileRaw) } # single combined column
    if ($OtherEmailsRaw)         { $userDetails.OtherMails     = $OtherEmailsRaw -split ';' }

    # Create user
    try {
        $newUser = New-MgUser -BodyParameter $userDetails
        Write-Host "✅ Created: $UPN (User ID: $($newUser.Id))" -ForegroundColor Green
    }
    catch {
        Write-Host "❌ Failed to create: $UPN - $($_.Exception.Message)" -ForegroundColor Red
        continue
    }

    # Manager (by UPN)
    if ($ManagerUPN) {
        try {
            $manager = Get-MgUser -Filter "userPrincipalName eq '$($ManagerUPN.Replace("'", "''"))'" -ErrorAction Stop
            if ($manager) {
                $ref = @{ '@odata.id' = "https://graph.microsoft.com/v1.0/users/$($manager.Id)" }
                Set-MgUserManagerByRef -UserId $newUser.Id -BodyParameter $ref
                Write-Host "   - ✅ Manager set: $ManagerUPN"
            }
        } catch {
            Write-Host "❌ Manager assignment failed ($ManagerUPN): $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }

    # Sponsors (beta, comma/semicolon separated list)
    if ($SponsorsRaw) {
        $sponsorUPNs = $SponsorsRaw -split '[,;]'
        foreach ($s in $sponsorUPNs) {
            $s = $s.Trim()
            if (-not $s) { continue }
            try {
                $sObj = Get-MgUser -Filter "userPrincipalName eq '$($s.Replace("'", "''"))'" -ErrorAction Stop
                if ($sObj) {
                    $body = @{ '@odata.id' = "https://graph.microsoft.com/v1.0/users/$($sObj.Id)" }
                    Invoke-MgGraphRequest -Method POST -Uri "https://graph.microsoft.com/beta/users/$($newUser.Id)/sponsors/`$ref" -Body $body
                    Write-Host "   - ✅ Sponsor added: $s"
                }
            } catch {
                Write-Host "❌ Sponsor add failed ($s): $($_.Exception.Message)" -ForegroundColor Yellow
            }
        }
    }

    # ------------------------------------------------------
    # Access Package assignment
    # ------------------------------------------------------
    if ($AccessPackage) {
        Write-Host "🧩 CSV Access Package detected: '$AccessPackage'" -ForegroundColor Cyan

        # Locate the AccessPackages.json file (same folder structure)
        $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
        $jsonPath  = Join-Path $scriptDir "..\JSONs\AccessPackages.json"

        if (Test-Path $jsonPath) {
            try {
                $jsonContent = Get-Content -Raw -Path $jsonPath | ConvertFrom-Json
                $ap = $jsonContent | Where-Object { $_.AccessPackageName -eq $AccessPackage }

                if ($ap) {
                    $policy = $ap.Policies | Where-Object { $_.Status -eq "Enabled" } | Select-Object -First 1
                    if (-not $policy) { $policy = $ap.Policies[0] }

                    $accessPackageId = $ap.AccessPackageId
                    $policyId = $policy.PolicyId

                    Write-Host "🎁 Assigning Access Package '$AccessPackage' (AccessPackageID: $accessPackageId | PolicyID: $policyId)..." -ForegroundColor Cyan

                    # Build the request body
                    $params = @{
                        requestType = "AdminAdd"
                        assignment  = @{
                            targetId           = $newUser.Id
                            assignmentPolicyId = $policyId
                            accessPackageId    = $accessPackageId
                        }
                    }

                    try {
                        $response = New-MgEntitlementManagementAssignmentRequest -BodyParameter $params -ErrorAction Stop
                        if ($response.id) {
                            Write-Host "   - ✅ Access Package assigned successfully (Request ID: $($response.id))" -ForegroundColor Green
                        } else {
                            Write-Host "   - ⚠️ Access Package request succeeded but no ID returned. Verify in Entra portal." -ForegroundColor Yellow
                        }
                    } catch {
                        Write-Host "❌ Failed to assign Access Package '$AccessPackage' - $($_.Exception.Message)" -ForegroundColor Red
                        if ($_.ErrorDetails.Message) {
                            Write-Host "🪣 Graph Response: $($_.ErrorDetails.Message)" -ForegroundColor DarkYellow
                        }
                    }
                } else {
                    Write-Host "⚠️ Access Package '$AccessPackage' not found in JSON." -ForegroundColor Yellow
                }
            } catch {
                Write-Host "⚠️ Failed to read or parse AccessPackages.json: $_" -ForegroundColor Yellow
            }
        } else {
            Write-Host "⚠️ AccessPackages.json not found in $jsonPath" -ForegroundColor Yellow
        }
    }
}

# Clean up parser
$parser.Close()
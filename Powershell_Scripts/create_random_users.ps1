# =========================================================
# Create Entra ID Users from CSV (Parallel, Logged)
# =========================================================

[CmdletBinding()]
param(
  [string]$CsvPath,   # always passed by Python
  [switch]$Parallel,
  [int]$ThrottleLimit = 8,
  [string]$LogPath
)

# --- Ensure log folder and set default log path if not provided ---
$baseDir   = Split-Path -Parent $PSScriptRoot
$logFolder = Join-Path $baseDir "Powershell_Logs"
if (-not (Test-Path $logFolder)) {
    New-Item -ItemType Directory -Path $logFolder | Out-Null
}

if (-not $LogPath) {
    $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $LogPath   = Join-Path $logFolder "$timestamp_create_random_users.log"
}

# --- Connect to Graph ---
$ErrorActionPreference = 'Stop'

try {
    $ctx = Get-MgContext -ErrorAction Stop
    if (-not $ctx) {
        Connect-MgGraph -Scopes "User.ReadWrite.All,Directory.ReadWrite.All"
    }
} catch {
    Connect-MgGraph -Scopes "User.ReadWrite.All,Directory.ReadWrite.All"
}

if (-not (Test-Path -LiteralPath $CsvPath)) {
  Write-Error "CSV not found: $CsvPath"
  exit 1
}

$rows = Import-Csv -LiteralPath $CsvPath

function NullIfEmpty([string]$v) {
  if ([string]::IsNullOrWhiteSpace($v)) { return $null }
  return $v.Trim()
}

# Ensure UsageLocation is valid (must be 2 letters)
$usageLocation = $u.'Usage location'
if ($usageLocation -and $usageLocation.Length -gt 2) {
    try {
        # Take only the first 2 letters (uppercased)
        $usageLocation = $usageLocation.Substring(0,2).ToUpper()
    } catch {
        $usageLocation = "US"  # fallback
    }
}
if (-not $usageLocation) { $usageLocation = "US" }  # fallback default


# --- worker block that returns a result object ---
$worker = {
  param($u)

  try { $null = Get-MgContext -ErrorAction Stop } catch { }

  $upn = $u.'User principal name'
  try {
    # Skip if already exists
    $existing = Get-MgUser -Filter "userPrincipalName eq '$($upn.Replace("'","''"))'" -ConsistencyLevel eventual -ErrorAction SilentlyContinue
    if ($existing) {
      return [pscustomobject]@{ UPN = $upn; Id = $existing.Id; Status = 'Exists'; Error = '' }
    }

    $pp = @{
      ForceChangePasswordNextSignIn = $true
      Password = $u.Password
    }

    $params = @{
      AccountEnabled     = $true
      DisplayName        = NullIfEmpty $u.'Display name'
      GivenName          = NullIfEmpty $u.'First name'
      Surname            = NullIfEmpty $u.'Last name'
      MailNickname       = NullIfEmpty $u.'Mail nickname'
      UserPrincipalName  = $upn
      PasswordProfile    = $pp
      JobTitle           = NullIfEmpty $u.'Job title'
      Department         = NullIfEmpty $u.Department
      CompanyName        = NullIfEmpty $u.'Company name'
      City               = NullIfEmpty $u.City
      Country            = NullIfEmpty $u.'Country or region'
      State              = NullIfEmpty $u.'State or province'
      StreetAddress      = NullIfEmpty $u.'Street address'
      PostalCode         = NullIfEmpty $u.'ZIP or postal code'
      UsageLocation      = NullIfEmpty $u.'Usage location'
      OfficeLocation     = NullIfEmpty $u.'Office location'
    }

    $newUser = New-MgUser @params

    return [pscustomobject]@{
      UPN    = $upn
      Id     = $newUser.Id
      Status = 'Created'
      Error  = ''
    }
  }
  catch {
    return [pscustomobject]@{
      UPN    = $upn
      Id     = ''
      Status = 'Failed'
      Error  = $_.Exception.Message
    }
  }
}

# --- run serial or parallel ---
if ($Parallel) {
  $results = $rows | ForEach-Object -Parallel $worker -ThrottleLimit $ThrottleLimit
} else {
  $results = foreach ($r in $rows) { & $worker $r }
}

# --- Save results ---
$results | Export-Csv -Path $LogPath -NoTypeInformation -Encoding UTF8

# --- Console summary ---
foreach ($r in $results) {
  switch ($r.Status) {
    'Created' { Write-Host ("Created: {0} (User ID: {1})" -f $r.UPN, $r.Id) -ForegroundColor Green }
    'Exists'  { Write-Host ("Exists:  {0} (User ID: {1})" -f $r.UPN, $r.Id) -ForegroundColor Yellow }
    default   { Write-Host ("Failed:  {0} -> {1}" -f $r.UPN, $r.Error) -ForegroundColor Red }
  }
}

exit 0
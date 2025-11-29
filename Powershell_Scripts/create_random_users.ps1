# =========================================================
# Create Entra ID Users from CSV (Parallel, Logged)
# =========================================================

[CmdletBinding()]
param(
  [string]$CsvPath,
  [switch]$Parallel,
  [int]$ThrottleLimit = 8,
  [string]$LogPath
)

# --- Ensure log folder ---
$baseDir   = Split-Path -Parent $PSScriptRoot
$logFolder = Join-Path $baseDir "Powershell_Logs"
if (-not (Test-Path $logFolder)) { New-Item -ItemType Directory -Path $logFolder | Out-Null }

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
}
catch {
    Connect-MgGraph -Scopes "User.ReadWrite.All,Directory.ReadWrite.All"
}

# --- CSV exists? ---
if (-not (Test-Path -LiteralPath $CsvPath)) {
  Write-Error "CSV not found: $CsvPath"
  exit 1
}

$rows = Import-Csv -LiteralPath $CsvPath

function NullIfEmpty([string]$v) {
  if ([string]::IsNullOrWhiteSpace($v)) { return $null }
  return $v.Trim()
}

# --- Worker block ---
$worker = {
  param($u)

  try { $null = Get-MgContext -ErrorAction Stop } catch { }

  # --- UsageLocation fix ---
  $usageLocation = $u.'Usage location'
  if ($usageLocation) {
      $usageLocation = $usageLocation.Trim().Substring(0, [Math]::Min(2, $usageLocation.Length)).ToUpper()
  } else {
      $usageLocation = "US"
  }

  $upn = $u.'User principal name'

  try {

    # Skip existing user
    $existing = Get-MgUser -Filter "userPrincipalName eq '$($upn.Replace("'","''"))'" `
                -ConsistencyLevel eventual -ErrorAction SilentlyContinue
    if ($existing) {
      return [pscustomobject]@{ UPN = $upn; Id = $existing.Id; Status = 'Exists'; Error = '' }
    }

    $pp = @{
      ForceChangePasswordNextSignIn = $true
      Password = $u.Password
    }

    $params = @{
      AccountEnabled     = $true
      DisplayName        = $u.'Display name'
      GivenName          = $u.'First name'
      Surname            = $u.'Last name'
      MailNickname       = ($upn.Split("@")[0])          # FIXED
      UserPrincipalName  = $upn
      PasswordProfile    = $pp
      JobTitle           = $u.'Job title'
      Department         = $u.Department
      CompanyName        = $u.'Company name'
      City               = $u.City
      Country            = $u.'Country or region'
      State              = $u.'State or province'
      StreetAddress      = $u.'Street address'
      PostalCode         = $u.'ZIP or postal code'
      UsageLocation      = $usageLocation
      OfficeLocation     = $u.'Office location'
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

# --- Execute ---
if ($Parallel) {
  $results = $rows | ForEach-Object -Parallel $worker -ThrottleLimit $ThrottleLimit
} else {
  $results = foreach ($r in $rows) { & $worker $r }
}

# --- Log CSV ---
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
param(
    [Parameter(Mandatory = $true)]
    [string]$User1,
    [Parameter(Mandatory = $true)]
    [string]$User2
)

$ErrorActionPreference = "Stop"

# 1️⃣ Connect to Microsoft Graph
try {
    $ctx = Get-MgContext -ErrorAction Stop
    if (-not $ctx) {
        Connect-MgGraph -Scopes "User.Read.All","Group.Read.All","Directory.Read.All" -NoWelcome
    }
} catch {
    Connect-MgGraph -Scopes "User.Read.All","Group.Read.All","Directory.Read.All" -NoWelcome
}

# 2️⃣ Helper: get all group DisplayNames for a UPN (transitive safe)
function Get-EntraUserGroups {
    param([string]$UPN)

    try {
        $user = Get-MgUser -Filter "userPrincipalName eq '$UPN'" -ErrorAction Stop
        if (-not $user) {
            Write-Warning "⚠️ User not found: $UPN"
            return @()
        }

        $userId = $user.Id

        # Use transitiveMemberOf for full membership visibility
        $groups = Get-MgUserTransitiveMemberOf -UserId $userId -All -ErrorAction SilentlyContinue |
            Where-Object { $_.AdditionalProperties['displayName'] } |
            ForEach-Object { $_.AdditionalProperties['displayName'] }

        if (-not $groups) {
            # Fallback to standard MemberOf if transitive empty
            $groups = Get-MgUserMemberOf -UserId $userId -All -ErrorAction SilentlyContinue |
                Where-Object { $_.AdditionalProperties['displayName'] } |
                ForEach-Object { $_.AdditionalProperties['displayName'] }
        }

        return $groups | Sort-Object -Unique
    }
    catch {
        Write-Warning "❌ Failed to get groups for $UPN $_"
        return @()
    }
}

# 3️⃣ Compare memberships
$user1Groups = Get-EntraUserGroups -UPN $User1
$user2Groups = Get-EntraUserGroups -UPN $User2

# 4️⃣ Build result
$result = [ordered]@{
    User1          = $User1
    User2          = $User2
    User1Groups    = $user1Groups
    User2Groups    = $user2Groups
    MissingInUser1 = @($user2Groups | Where-Object { $_ -notin $user1Groups })
    MissingInUser2 = @($user1Groups | Where-Object { $_ -notin $user2Groups })
}

# 5️⃣ Output JSON (for app parsing)
$result | ConvertTo-Json -Depth 5 | Out-String
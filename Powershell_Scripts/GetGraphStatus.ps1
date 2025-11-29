try {
    $ctx = Get-MgContext -ErrorAction Stop

    @{
        connected = $true
        account   = $ctx.Account
        scopes    = $ctx.Scopes
    } | ConvertTo-Json
}
catch {
    @{
        connected = $false
    } | ConvertTo-Json
}

exit 0
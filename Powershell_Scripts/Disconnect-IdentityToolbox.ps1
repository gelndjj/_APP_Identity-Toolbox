param([string]$PwshRoot)

# Cached folders inside portable pwsh
$portableCaches = @(
    "$PwshRoot/.MgGraph",
    "$PwshRoot/.Msal",
    "$PwshRoot/.Logs"
)

# macOS system caches
$systemCaches = @(
    "$HOME/Library/Caches/msal",
    "$HOME/Library/Application Support/Microsoft/TokenBroker"
)

try { Disconnect-MgGraph | Out-Null } catch {}

foreach ($c in $portableCaches + $systemCaches) {
    if (Test-Path $c) {
        try { Remove-Item -Recurse -Force $c -ErrorAction SilentlyContinue } catch {}
    }
}

@{
    status  = "success"
    removed = $portableCaches + $systemCaches
} | ConvertTo-Json

exit 0
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot

# ── Step 1: Build Go core ────────────────────────────────────────────────────
Write-Host "▶ Building Go core..." -ForegroundColor Cyan
& "$PSScriptRoot\build-core.ps1"
Write-Host "✓ Core built." -ForegroundColor Green

# ── Step 2: Check Node / npm ─────────────────────────────────────────────────
$npmCommand = Get-Command npm -ErrorAction SilentlyContinue
if (-not $npmCommand) {
  throw "npm not found. Install Node.js and add it to PATH."
}

# ── Step 3: Install npm deps if needed ───────────────────────────────────────
if (-not (Test-Path (Join-Path $root "node_modules"))) {
  Write-Host "▶ Installing npm dependencies..." -ForegroundColor Cyan
  Push-Location $root
  try { & npm install } finally { Pop-Location }
}

# ── Step 4: Launch dev mode (Vite + Electron) ────────────────────────────────
Write-Host "▶ Starting Phantasm (dev mode)..." -ForegroundColor Cyan
Push-Location $root
try {
  & npm run dev
} finally {
  Pop-Location
}

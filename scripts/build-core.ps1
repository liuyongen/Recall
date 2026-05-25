$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$outDir = Join-Path $root "core\bin"
$isWindows = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
  [System.Runtime.InteropServices.OSPlatform]::Windows
)
$exe = if ($isWindows) { "phantasm-core.exe" } else { "phantasm-core" }
$goCommand = Get-Command go -ErrorAction SilentlyContinue
$goPath = if ($goCommand) { $goCommand.Source } else { $null }
if (-not $goPath) {
  $fallbackGo = "C:\Software\Go\bin\go.exe"
  if (Test-Path $fallbackGo) {
    $goPath = $fallbackGo
  }
}
if (-not $goPath) {
  throw "Go executable not found. Add Go to PATH or install it at C:\Software\Go\bin."
}

# Pure-Go SQLite (modernc.org/sqlite) — no CGO, no C compiler, no DLL dependencies.
$env:CGO_ENABLED = "0"

New-Item -ItemType Directory -Force -Path $outDir | Out-Null
Push-Location $root
try {
  $target = Join-Path $outDir $exe
  if (Test-Path $target) {
    Remove-Item -LiteralPath $target -Force
  }
  & $goPath build -trimpath -ldflags "-s -w" -o $target ./core/cmd/phantasm-core
}
finally {
  Pop-Location
}

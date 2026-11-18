param([switch]$Daemon)
$ErrorActionPreference = 'Stop'
Push-Location -LiteralPath $PSScriptRoot
try {
    $env:Path = "$PSScriptRoot\.local;C:\Program Files\Git\mingw64\bin;$env:Path"
    $env:PYTHONUTF8 = '1'
    if ($Daemon) {
        & .\.venv\Scripts\python.exe -m HGXZ.cli --config config.json --daemon
    } else {
        & .\.venv\Scripts\python.exe -c "import runpy; runpy.run_path('.local/webui.py', run_name='__main__')"
    }
    exit $LASTEXITCODE
} finally {
    Pop-Location
}

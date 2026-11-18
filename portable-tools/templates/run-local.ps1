param([switch]$Daemon)
$ErrorActionPreference='Stop'
Push-Location -LiteralPath $PSScriptRoot
try {
 $env:Path = "$PSScriptRoot\.local;$env:Path"
 $env:PYTHONUTF8='1'
 & .\runtime\python.exe prepare.py
 if($LASTEXITCODE -ne 0){exit $LASTEXITCODE}
 if($Daemon){ & .\runtime\python.exe -m HGXZ.cli --config config.json --daemon }
 else { & .\runtime\python.exe -c "import runpy; runpy.run_path('.local/webui.py',run_name='__main__')" }
 exit $LASTEXITCODE
} finally { Pop-Location }

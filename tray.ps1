param([switch]$CheckOnly)
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
$root = $PSScriptRoot
$state = @{ Process = $null; Stopping = $false; ExitAfter = $false; StopSince = $null; StopSent = $false }
function Get-HgxzStatus {
    try {
        $value = Invoke-RestMethod 'http://127.0.0.1:8099/api/sync-status' -TimeoutSec 1
        if ($null -eq $value.instance_id) { return $null }
        return $value
    } catch { return $null }
}
function Attach-Hgxz {
    $status = Get-HgxzStatus
    if (!$status -or !$status.service_pid -or !$status.service_root) { return $false }
    if ([IO.Path]::GetFullPath($status.service_root).TrimEnd('') -ne [IO.Path]::GetFullPath($root).TrimEnd('')) { return $false }
    try {
        $process = Get-Process -Id ([int]$status.service_pid) -ErrorAction Stop
        if ($process.ProcessName -notin @('python','pythonw')) { return $false }
        $state.Process = $process
        $state.AttachedInstance = $status.instance_id
        return $true
    } catch { return $false }
}
function Start-Hgxz {
    if ($state.Stopping -or ($state.Process -and !$state.Process.HasExited)) { return }
    if (Get-HgxzStatus) { Attach-Hgxz | Out-Null; return }
    $exe = (Get-Process -Id $PID).Path
    $state.Process = Start-Process -FilePath $exe -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-File',('"' + $root + '\run-local.ps1"')) -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput "$root\logs\tray.stdout.log" -RedirectStandardError "$root\logs\tray.stderr.log" -PassThru
}
function Request-HgxzStop {
    if (!$state.Process -or $state.Process.HasExited) { Attach-Hgxz | Out-Null }
    if (!$state.Process -or $state.Process.HasExited) {
        if (Get-HgxzStatus) {
            [System.Windows.Forms.MessageBox]::Show('无法确认此服务属于当前 HGXZ 目录，请检查启动路径。','HGXZ') | Out-Null
            $state.ExitAfter = $false
        } elseif ($state.ExitAfter) { $context.ExitThread() }
        return
    }
    $state.Stopping = $true
    $state.StopSince = [datetime]::UtcNow
    $state.StopSent = $false
}
if ($CheckOnly) {
    if (!(Test-Path "$root\run-local.ps1")) { throw 'Missing launcher' }
    'Tray configuration valid'
    exit
}
$mutex = New-Object System.Threading.Mutex($false, 'Local\HGXZ.Tray.Fwork')
if (!$mutex.WaitOne(0)) { $mutex.Dispose(); exit }
$context = New-Object System.Windows.Forms.ApplicationContext
$menu = New-Object System.Windows.Forms.ContextMenuStrip
$open = $menu.Items.Add('打开管理页面')
$start = $menu.Items.Add('启动服务')
$stop = $menu.Items.Add('停止服务')
$folder = $menu.Items.Add('打开下载目录')
$null = $menu.Items.Add('-')
$quit = $menu.Items.Add('停止并退出')
$notify = New-Object System.Windows.Forms.NotifyIcon
$trayIcon = New-Object System.Drawing.Icon((Join-Path $root 'hgxz.ico'))
$notify.Icon = $trayIcon
$notify.ContextMenuStrip = $menu
$notify.Text = 'HGXZ'
$notify.Visible = $true
$open.Add_Click({ Start-Process 'http://127.0.0.1:8099/' })
$notify.Add_DoubleClick({ Start-Process 'http://127.0.0.1:8099/' })
$start.Add_Click({ try { Start-Hgxz } catch { [System.Windows.Forms.MessageBox]::Show($_.Exception.Message,'HGXZ') | Out-Null } })
$stop.Add_Click({ Request-HgxzStop })
$folder.Add_Click({ Start-Process explorer.exe -ArgumentList ('"' + $root + '\media"') })
$quit.Add_Click({ $state.ExitAfter = $true; Request-HgxzStop })
$timer = New-Object System.Windows.Forms.Timer
$timer.Interval = 2000
$timer.Add_Tick({
    $status = Get-HgxzStatus
    if ($status -and (!$state.Process -or $state.Process.HasExited)) { Attach-Hgxz | Out-Null }
    $owned = $state.Process -and !$state.Process.HasExited
    $start.Enabled = !$state.Stopping -and !$owned -and !$status
    $stop.Enabled = $owned -and !$state.Stopping
    if ($state.Stopping) {
        $notify.Text = 'HGXZ - 正在停止'
        if ($status -and $status.running) {
            if (!$state.StopSent -and $status.cancellable) {
                try {
                    Invoke-RestMethod ("http://127.0.0.1:8099/api/sync/stop?job_id=" + $status.job_id) -Method Post -TimeoutSec 2 | Out-Null
                    $state.StopSent = $true
                } catch {}
            }
        } elseif (!$status) {
            if (!$owned) {
                $state.Stopping = $false
                if ($state.ExitAfter) { $context.ExitThread() }
            } elseif (([datetime]::UtcNow - $state.StopSince).TotalSeconds -gt 12) {
                $state.Stopping = $false
                $state.ExitAfter = $false
                [System.Windows.Forms.MessageBox]::Show('暂时无法确认服务状态，已保留服务。请稍后再试。','HGXZ') | Out-Null
            }
        } elseif ($status) {
            if ($state.AttachedInstance -and $status.instance_id -ne $state.AttachedInstance) {
                $state.Stopping = $false
                $state.ExitAfter = $false
                return
            }
            if ($owned) {
                & "$env:SystemRoot\System32\taskkill.exe" /PID $state.Process.Id /T /F | Out-Null
                if ($LASTEXITCODE -ne 0) {
                    $state.Stopping = $false
                    $state.ExitAfter = $false
                    [System.Windows.Forms.MessageBox]::Show('无法停止服务，请检查托盘启动日志。','HGXZ') | Out-Null
                    return
                }
            }
            $state.Stopping = $false
            if ($state.ExitAfter) { $context.ExitThread() }
        }
    } elseif ($status) {
        $notify.Text = if ($status.running) { 'HGXZ - 任务运行中' } else { 'HGXZ - 服务运行中' }
    } else {
        $notify.Text = if ($owned) { 'HGXZ - 正在启动' } else { 'HGXZ - 已停止' }
    }
})
try {
    New-Item -ItemType Directory -Force "$root\logs" | Out-Null
    Start-Hgxz
    $timer.Start()
    [System.Windows.Forms.Application]::Run($context)
} catch {
    [System.Windows.Forms.MessageBox]::Show($_.Exception.Message, 'HGXZ') | Out-Null
} finally {
    $timer.Stop()
    $timer.Dispose()
    $notify.Visible = $false
    $notify.Dispose()
    $trayIcon.Dispose()
    $menu.Dispose()
    $context.Dispose()
    $mutex.ReleaseMutex()
    $mutex.Dispose()
}

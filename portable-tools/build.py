from pathlib import Path
import json
import shutil
import sqlite3
import sys
from urllib.request import urlretrieve

source = Path(__file__).resolve().parents[1]
target = source / "HGXZ-Portable"
target.mkdir(exist_ok=True)
# Never overwrite a used portable directory containing runtime user data.
if any((target / 'media').rglob('*')) if (target / 'media').exists() else False:
    raise RuntimeError('Portable media directory is not empty; use a fresh build directory')
for path in (source / 'portable-tools/templates').iterdir():
    shutil.copy2(path, target / path.name)
for folder in ("HGXZ",):
    shutil.copytree(source / folder, target / folder, dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__"))
for folder in ("data", "logs", "media", "runtime", ".local"):
    (target / folder).mkdir(exist_ok=True)
for name in ("tray.ps1", "hgxz.ico"):
    shutil.copy2(source / name, target / name)
for name in ("ffmpeg.exe", "ffprobe.exe", "webui.py"):
    shutil.copy2(source / ".local" / name, target / ".local" / name)

# The embedded runtime is relocatable and uses a local module search path.
import zipfile
archive = source / "portable-tools" / "python-embed.zip"
if not archive.exists() or not zipfile.is_zipfile(archive):
    urlretrieve("https://www.python.org/ftp/python/3.12.10/python-3.12.10-embed-amd64.zip", archive)
with zipfile.ZipFile(archive) as z:
    z.extractall(target / "runtime")
(target / "runtime" / "python312._pth").write_text(
    "python312.zip\n.\nLib/site-packages\n..\nimport site\n", encoding="ascii")
packages = (target / "runtime/Lib/site-packages").resolve()
assert packages == (source / "HGXZ-Portable/runtime/Lib/site-packages").resolve()
if packages.exists():
    shutil.rmtree(packages)
shutil.copytree(source / ".venv/Lib/site-packages", target / "runtime/Lib/site-packages",
                dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__", "pip", "pip-*.dist-info"))
git = Path("C:/Program Files/Git/mingw64/bin")
for path in git.glob("*.dll"):
    shutil.copy2(path, target / ".local" / path.name)
shutil.copy2(git / "openssl.exe", target / ".local/openssl.exe")
cfg = json.loads((source / "config.json").read_text(encoding="utf-8-sig"))
cfg["download"]["root"] = "./media"
cfg["state"]["database"] = "./data/state.sqlite3"
cfg["log"]["directory"] = "./logs"
(target / "config.json").write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
# A consistent SQLite snapshot includes committed WAL data.
with sqlite3.connect(source / "data/state.sqlite3") as db:
    with sqlite3.connect(target / "data/state.sqlite3") as out:
        db.backup(out)
        row = out.execute("SELECT payload FROM sync_queue WHERE id=1").fetchone()
        if row:
            queue = json.loads(row[0]); queue["paused"] = True
            out.execute("UPDATE sync_queue SET payload=? WHERE id=1", (json.dumps(queue),))
shutil.copy2(source / "portable-tools/prepare.py", target / "prepare.py")
print(target)

with sqlite3.connect(target / 'data/state.sqlite3') as db:
    albums = db.execute('SELECT COUNT(*) FROM albums').fetchone()[0]
    done = db.execute("SELECT COUNT(*) FROM episodes WHERE status='done'").fetchone()[0]
    row = db.execute('SELECT payload FROM sync_queue WHERE id=1').fetchone()
    queued = len(json.loads(row[0]).get('ids', [])) if row else 0
(target / '迁移清单.txt').write_text(
    f'专辑目录记录 {albums} 部\n下载完成记录 {done} 集\n队列 {queued} 部，已暂停\n'
    '本包不含视频和续传分段。\n缺失文件需人工补下载。\n'
    '目标电脑请检查 network.proxy 和使用代理开关，并测试连接。\n',encoding='utf-8')
# Package only the program, a consistent DB snapshot, and required runtime files.
# An explicit allowlist excludes logs, test clips, caches, and all media.
package = source / 'HGXZ-Portable-Windows-x64.zip'
temporary = package.with_suffix('.zip.tmp')
top = {'config.json', 'run-local.ps1', 'tray.ps1', 'hgxz.ico',
       'prepare.py', '启动HGXZ.vbs', '使用说明.txt', '迁移清单.txt'}
with zipfile.ZipFile(temporary, 'w', zipfile.ZIP_DEFLATED) as out:
    out.writestr('HGXZ-Portable/media/', '')
    out.writestr('HGXZ-Portable/logs/', '')
    for path in target.rglob('*'):
        if not path.is_file():
            continue
        relative = path.relative_to(target)
        if '__pycache__' in relative.parts or path.suffix == '.pyc':
            continue
        allowed = (relative.as_posix() in top or relative.parts[0] in ('HGXZ', 'runtime')
                   or relative.as_posix() == 'data/state.sqlite3'
                   or (relative.parts[0] == '.local' and (
                       path.name in ('ffmpeg.exe', 'ffprobe.exe', 'openssl.exe', 'webui.py')
                       or path.suffix == '.dll')))
        if allowed:
            out.write(path, 'HGXZ-Portable/' + relative.as_posix())
temporary.replace(package)
print(package)

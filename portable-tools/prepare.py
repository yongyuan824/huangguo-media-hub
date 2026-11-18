from pathlib import Path
import json
import sqlite3
import sys

root = Path(__file__).resolve().parent
cfg = json.loads((root / "config.json").read_text(encoding="utf-8-sig"))
db_path = root / cfg["state"]["database"]
media = (root / cfg["download"]["root"]).resolve()
if db_path.exists():
    with sqlite3.connect(db_path) as db:
        previous = db.execute("SELECT directory FROM albums WHERE directory != ''").fetchall()
        def relocate(value):
            if not value:
                return value
            normalized = value.replace("\\", "/")
            # Album paths encode category/show; find that suffix from database
            # directories rather than depending on the former root folder name.
            for (directory,) in previous:
                old = directory.replace("\\", "/").rstrip("/")
                parts = old.split("/")
                if len(parts) < 2:
                    continue
                suffix = "/".join(parts[-2:])
                if normalized == old or normalized.startswith(old + "/"):
                    tail = normalized[len(old):].lstrip("/")
                    return str(Path(cfg["download"]["root"]) / suffix / tail).replace("\\", "/").rstrip("/")
            return value
        for table, column, keys in (("albums", "directory", ["id"]), ("episodes", "path", ["album_id", "episode"])):
            rows = db.execute(f"SELECT {','.join(keys)},{column} FROM {table}").fetchall()
            for row in rows:
                original = row[-1]
                moved = relocate(original)
                if moved != original:
                    db.execute(f"UPDATE {table} SET {column}=? WHERE " + " AND ".join(k+"=?" for k in keys), [moved,*row[:-1]])
print("Portable data paths ready")

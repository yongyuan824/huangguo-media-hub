from __future__ import annotations

from pathlib import Path
import sqlite3
import json
import re
from html import unescape
from datetime import datetime, timezone
import threading


class StateDB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute('PRAGMA journal_mode=WAL')
        # Multiple threads share this connection (scheduled sync, manual sync,
        # request handlers); wait for a busy writer instead of failing.
        self.conn.execute('PRAGMA busy_timeout=10000')
        self._lock = threading.RLock()
        self.conn.executescript('''
        CREATE TABLE IF NOT EXISTS albums (
          id INTEGER PRIMARY KEY,
          category TEXT NOT NULL,
          title TEXT NOT NULL,
          episode_count INTEGER NOT NULL DEFAULT 0,
          ended INTEGER NOT NULL DEFAULT 0,
          directory TEXT NOT NULL DEFAULT '',
          last_checked_at TEXT,
          last_changed_at TEXT
        );
        CREATE TABLE IF NOT EXISTS album_preferences(
          album_id INTEGER PRIMARY KEY, favorite INTEGER NOT NULL DEFAULT 0, hidden INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS playback_progress(
          album_id INTEGER NOT NULL, episode INTEGER NOT NULL, seconds REAL NOT NULL, updated_at TEXT NOT NULL,
          PRIMARY KEY(album_id,episode));
        CREATE TABLE IF NOT EXISTS album_history(
          id INTEGER PRIMARY KEY, album_id INTEGER NOT NULL, action TEXT NOT NULL, detail TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS collections(
          id INTEGER PRIMARY KEY, kind TEXT NOT NULL, path TEXT UNIQUE NOT NULL,
          name TEXT NOT NULL, refreshed_at TEXT);
        CREATE TABLE IF NOT EXISTS collection_albums(
          collection_id INTEGER NOT NULL, album_id INTEGER NOT NULL, position INTEGER NOT NULL,
          PRIMARY KEY(collection_id,album_id));
        CREATE TABLE IF NOT EXISTS sync_queue (
          id INTEGER PRIMARY KEY CHECK (id=1), payload TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS episodes (
          album_id INTEGER NOT NULL,
          episode INTEGER NOT NULL,
          path TEXT NOT NULL,
          status TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          size_bytes INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY(album_id, episode)
        );
        ''')
        self.conn.commit()
        self._migrate()

    def _migrate(self) -> None:
        cols = {row[1] for row in self.conn.execute('PRAGMA table_info(albums)')}
        if 'directory' not in cols:
            self.conn.execute("ALTER TABLE albums ADD COLUMN directory TEXT NOT NULL DEFAULT ''")
            self.conn.commit()
        for column, kind in (('site_score', 'REAL'), ('site_badge', 'TEXT'), ('site_tags', 'TEXT')):
            if column not in cols:
                self.conn.execute(f'ALTER TABLE albums ADD COLUMN {column} {kind}')
        self.conn.commit()
        if 'update_from' not in cols:
            self.conn.execute('ALTER TABLE albums ADD COLUMN update_from INTEGER')
            self.conn.commit()
        ep_cols = {row[1] for row in self.conn.execute('PRAGMA table_info(episodes)')}
        if 'size_bytes' not in ep_cols:
            self.conn.execute('ALTER TABLE episodes ADD COLUMN size_bytes INTEGER NOT NULL DEFAULT 0')
            self.conn.commit()

        if 'last_error' not in ep_cols:
            self.conn.execute('ALTER TABLE episodes ADD COLUMN last_error TEXT')
            self.conn.commit()

    def load_queue(self) -> dict:
        with self._lock:
            row = self.conn.execute('SELECT payload FROM sync_queue WHERE id=1').fetchone()
            return json.loads(row[0]) if row else {}

    def save_queue(self, queue: dict) -> None:
        with self._lock:
            self.conn.execute('INSERT INTO sync_queue VALUES(1, ?) ON CONFLICT(id) '
                              'DO UPDATE SET payload=excluded.payload', (json.dumps(queue),))
            self.conn.commit()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def upsert_album(self, album_id: int, category: str, title: str,
                     episode_count: int, ended: bool, directory: str = '') -> None:
        now = self._now()
        with self._lock:
            self.conn.execute('''
              INSERT INTO albums(id,category,title,episode_count,ended,directory,last_checked_at,last_changed_at)
              VALUES(?,?,?,?,?,?,?,?)
              ON CONFLICT(id) DO UPDATE SET category=excluded.category,title=excluded.title,
                episode_count=excluded.episode_count,ended=excluded.ended,
                directory=CASE WHEN excluded.directory != '' THEN excluded.directory ELSE albums.directory END,
                last_checked_at=excluded.last_checked_at,
                last_changed_at=CASE WHEN albums.episode_count != excluded.episode_count OR albums.ended != excluded.ended
                  THEN excluded.last_changed_at ELSE albums.last_changed_at END
            ''', (album_id, category, title, episode_count, int(ended), directory, now, now))
            self.conn.commit()

    def update_online_total(self, album_id: int, total: int, ended: bool) -> None:
        with self._lock:
            self.conn.execute('''
                UPDATE albums SET
                    update_from=CASE WHEN episode_count>0 AND ?>episode_count
                        AND update_from IS NULL THEN episode_count+1 ELSE update_from END,
                    episode_count=?, ended=? WHERE id=?
            ''', (total, total, int(ended), album_id))
            self.conn.commit()

    def register_collection(self, kind, path, name):
        with self._lock:
            self.conn.execute(
                'INSERT INTO collections(kind,path,name) VALUES(?,?,?) ON CONFLICT(path) '
                'DO UPDATE SET name=excluded.name', (kind, path, name))
            self.conn.commit()

    def collections(self):
        with self._lock:
            return [dict(r) for r in self.conn.execute(
                'SELECT c.*,COUNT(m.album_id) AS album_count FROM collections c LEFT JOIN '
                'collection_albums m ON c.id=m.collection_id GROUP BY c.id ORDER BY c.kind,c.name')]

    def replace_collection(self, collection_id, albums):
        with self._lock, self.conn:
            self.conn.executemany(
                "INSERT INTO albums(id,category,title) VALUES(?,'待确认',?) ON CONFLICT(id) DO NOTHING",
                ((i, title or f'huangguo-{i}') for i,title in albums.items()))
            self.conn.execute('DELETE FROM collection_albums WHERE collection_id=?',(collection_id,))
            self.conn.executemany('INSERT INTO collection_albums VALUES(?,?,?)',
                                  ((collection_id,i,n) for n,i in enumerate(albums,1)))
            self.conn.execute('UPDATE collections SET refreshed_at=? WHERE id=?',
                              (self._now(),collection_id))

    def update_site_metadata(self, album_id: int, source: str) -> None:
        match = re.search(r'<body[^>]+data-history=(["\'])(.*?)\1',source,re.I|re.S)
        if not match:
            return
        try:
            history = json.loads(unescape(match.group(2)))
            score = float(history['score']) if history.get('score') else None
            if score is not None and not 0 <= score <= 10:
                score = None
            badge = str(history.get('badge') or '')[:40]
            tags = list(dict.fromkeys(t.strip() for t in str(history.get('tags') or '').split(',') if t.strip()))[:30]
        except (ValueError,TypeError,KeyError):
            return
        with self._lock:
            self.conn.execute('UPDATE albums SET site_score=?,site_badge=?,site_tags=? WHERE id=?',
                              (score,badge,json.dumps(tags,ensure_ascii=False),album_id))
            self.conn.commit()

    def update_catalog_scores(self, source: str, album_ids: list[int]) -> None:
        allowed = set(album_ids)
        cards = re.split(r'<(?:article|div)\b[^>]*class=["\']hg-drama-card(?:\s[^"\']*)?["\'][^>]*>', source)
        with self._lock:
            for card in cards[1:]:
                link = re.search(r'href=["\']/(?:video|detail)/(\d+)/',card)
                score = re.search(r'class=["\'][^"\']*hg-drama-card__score[^"\']*["\'][^>]*>\s*([0-9]+(?:\.[0-9]+)?)',card)
                if not link or not score or int(link.group(1)) not in allowed:
                    continue
                number = float(score.group(1))
                if not 0 <= number <= 10:
                    continue
                badges = re.findall(r'class=["\']hg-drama-card__badge["\'][^>]*>([^<]+)',card)
                self.conn.execute('UPDATE albums SET site_score=?,site_badge=? WHERE id=?',
                                  (number, ' '.join(unescape(b).strip() for b in badges)[:40],int(link.group(1))))
            self.conn.commit()

    def add_history(self, album_id, action, detail=''):
        with self._lock:
            self.conn.execute('INSERT INTO album_history(album_id,action,detail,created_at) VALUES(?,?,?,?)',
                              (album_id,action,detail,self._now()))
            self.conn.execute('DELETE FROM album_history WHERE album_id=? AND id NOT IN '
                              '(SELECT id FROM album_history WHERE album_id=? ORDER BY id DESC LIMIT 100)',
                              (album_id,album_id))
            self.conn.commit()

    def preferences(self):
        with self._lock:
            return {r['album_id']:dict(r) for r in self.conn.execute('SELECT * FROM album_preferences')}

    def register_local_album(self, album_id: int, category: str, title: str,
                             local_max_episode: int, directory: str) -> None:
        with self._lock:
            self.conn.execute('''
                INSERT INTO albums(id, category, title, episode_count, directory)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET directory=excluded.directory,
                    episode_count=CASE WHEN albums.last_checked_at IS NULL
                        THEN MAX(albums.episode_count, excluded.episode_count)
                        ELSE albums.episode_count END
            ''', (album_id, category, title, local_max_episode, directory))
            self.conn.commit()

    def register_catalog(self, category: str, ids: list[int], titles: dict[int, str]) -> None:
        with self._lock:
            self.conn.executemany('''
                INSERT INTO albums(id, category, title) VALUES(?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET title=excluded.title
                WHERE albums.last_checked_at IS NULL AND albums.directory=''
                    AND excluded.title != ('huangguo-' || albums.id)
            ''', ((i, category, titles.get(i, f'huangguo-{i}')) for i in ids))
            self.conn.commit()

    def mark_episode(self, album_id: int, episode: int, path: str, status: str,
                     size_bytes: int = 0, *, last_error: str | None = None) -> None:
        with self._lock:
            self.conn.execute('''
              INSERT INTO episodes(album_id,episode,path,status,updated_at,size_bytes,last_error) VALUES(?,?,?,?,?,?,?)
              ON CONFLICT(album_id,episode) DO UPDATE SET path=excluded.path,status=excluded.status,
                updated_at=excluded.updated_at,size_bytes=excluded.size_bytes,last_error=excluded.last_error
            ''', (album_id, episode, path, status, self._now(), max(0, int(size_bytes)), last_error))
            self.conn.commit()
        self.add_history(album_id, '下载完成' if status=='done' else '下载失败', f'第 {episode} 集')

    def episode_done(self, album_id: int, episode: int) -> bool:
        with self._lock:
            row = self.conn.execute(
                'SELECT status,path FROM episodes WHERE album_id=? AND episode=?',
                (album_id, episode)).fetchone()
            return bool(row and row['status'] == 'done' and Path(row['path']).exists())

    def episode_row(self, album_id: int, episode: int) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                'SELECT * FROM episodes WHERE album_id=? AND episode=?',
                (album_id, episode)).fetchone()

    def episode_failed_at(self, album_id: int, episode: int) -> str | None:
        with self._lock:
            row = self.conn.execute(
                'SELECT status,updated_at FROM episodes WHERE album_id=? AND episode=?',
                (album_id, episode)).fetchone()
            return row['updated_at'] if row and row['status'] == 'failed' else None

    def album_row(self, album_id: int) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute('SELECT * FROM albums WHERE id=?', (album_id,)).fetchone()

    def album_is_complete(self, album_id: int, episode_count: int) -> bool:
        with self._lock:
            rows = self.conn.execute(
                'SELECT episode,path,status FROM episodes WHERE album_id=?',
                (album_id,)).fetchall()
            done = {row['episode']: row['path'] for row in rows if row['status'] == 'done'}
            if episode_count < 1 or len(done) < episode_count:
                return False
            return all(number in done and Path(done[number]).exists()
                       for number in range(1, episode_count + 1))

    def unfinished_albums(self) -> list[dict]:
        with self._lock:
            return [dict(x) for x in self.conn.execute(
                'SELECT * FROM albums WHERE ended=0 ORDER BY id')]

    # ---- WebUI query methods ----

    def all_albums(self, category: str | None = None,
                   status: str | None = None,
                   search: str | None = None) -> list[dict]:
        query = '''
          SELECT a.*,
            COALESCE(e.done_count, 0) AS done_count,
            COALESCE(e.failed_count, 0) AS failed_count
          FROM albums a
          LEFT JOIN (
            SELECT album_id,
              SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS done_count,
              SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_count
            FROM episodes GROUP BY album_id
          ) e ON a.id = e.album_id
          WHERE 1=1
        '''
        params: list = []
        if category:
            query += ' AND a.category = ?'
            params.append(category)
        if search:
            query += ' AND a.title LIKE ?'
            params.append(f'%{search}%')
        if status == 'done':
            query += (' AND COALESCE(e.done_count, 0) >= a.episode_count'
                      ' AND a.episode_count > 0')
        elif status == 'pending':
            query += ' AND (a.episode_count = 0 OR COALESCE(e.done_count, 0) < a.episode_count)'
        elif status == 'failed':
            query += ' AND COALESCE(e.failed_count, 0) > 0'
        elif status == 'downloading':
            query += ' AND (a.ended = 0 OR COALESCE(e.done_count, 0) < a.episode_count)'
        query += ' ORDER BY a.last_checked_at DESC'
        with self._lock:
            rows = [dict(row) for row in self.conn.execute(query, params)]
            missing = {}
            for episode in self.conn.execute("SELECT album_id,path FROM episodes WHERE status='done'"):
                try:
                    exists = Path(episode['path']).is_file()
                except OSError:
                    exists = False
                if not exists:
                    album_id = episode['album_id']
                    missing[album_id] = missing.get(album_id, 0) + 1
            for row in rows:
                row['missing_file_count'] = missing.get(row['id'], 0)
                row['available_count'] = row['done_count'] - row['missing_file_count']
                row['new_episode_count'] = 0
                if row['update_from'] is not None:
                    completed_new = self.conn.execute(
                        "SELECT COUNT(*) FROM episodes WHERE album_id=? AND status='done' "
                        "AND episode>=? AND episode<=?",
                        (row['id'], row['update_from'], row['episode_count'])).fetchone()[0]
                    row['new_episode_count'] = max(0, row['episode_count'] - row['update_from'] + 1 - completed_new)
            return [r for r in rows if (status != 'missing_files' or r['missing_file_count']) and (status != 'updates' or r['new_episode_count'])]

    def album_with_episodes(self, album_id: int) -> dict | None:
        with self._lock:
            album = self.album_row(album_id)
            if not album:
                return None
            result = dict(album)
            episodes = self.conn.execute(
                'SELECT episode, path, status, updated_at, size_bytes, last_error '
                'FROM episodes WHERE album_id=? ORDER BY episode',
                (album_id,)).fetchall()
            result['episodes'] = [dict(e) for e in episodes]
            for episode in result['episodes']:
                episode['file_missing'] = episode['status'] == 'done' and not Path(episode['path']).is_file()
            result['missing_file_count'] = sum(e['file_missing'] for e in result['episodes'])
            done = sum(1 for e in episodes if e['status'] == 'done')
            failed = sum(1 for e in episodes if e['status'] == 'failed')
            result['done_count'] = done
            result['failed_count'] = failed
            return result

    def dashboard_stats(self) -> dict:
        with self._lock:
            albums = self.conn.execute('SELECT COUNT(*) AS c FROM albums').fetchone()['c']
            total = self.conn.execute(
                'SELECT COALESCE(SUM(episode_count), 0) AS c FROM albums').fetchone()['c']
            done = self.conn.execute(
                "SELECT COUNT(*) AS c FROM episodes WHERE status='done'").fetchone()['c']
            failed = self.conn.execute(
                "SELECT COUNT(*) AS c FROM episodes WHERE status='failed'").fetchone()['c']
            ended = self.conn.execute(
                'SELECT COUNT(*) AS c FROM albums WHERE ended=1').fetchone()['c']
            complete = self.conn.execute('''
                SELECT COUNT(*) AS c FROM albums a
                JOIN (SELECT album_id, SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS d
                      FROM episodes GROUP BY album_id) e ON e.album_id = a.id
                WHERE a.episode_count > 0 AND e.d >= a.episode_count
            ''').fetchone()['c']
            bytes_total = self.conn.execute(
                "SELECT COALESCE(SUM(size_bytes), 0) AS c FROM episodes WHERE status='done'"
            ).fetchone()['c']
            categories = [row['category'] for row in self.conn.execute(
                'SELECT DISTINCT category FROM albums ORDER BY category')]
            return {
                'albums': albums,
                'total_episodes': total,
                'done_episodes': done,
                'failed_episodes': failed,
                'ended_albums': ended,
                'complete_albums': complete,
                'downloaded_bytes': bytes_total,
                'categories': categories,
            }

    def episode_stats(self, album_id: int) -> dict:
        """Per-album counters plus the most recent episode activity."""
        with self._lock:
            row = self.conn.execute('''
                SELECT
                  SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS done,
                  SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                  MAX(CASE WHEN status='done' THEN updated_at END) AS last_done_at,
                  COALESCE(SUM(CASE WHEN status='done' THEN size_bytes ELSE 0 END), 0) AS bytes
                FROM episodes WHERE album_id=?
            ''', (album_id,)).fetchone()
            return {
                'done': row['done'] or 0,
                'failed': row['failed'] or 0,
                'last_done_at': row['last_done_at'],
                'downloaded_bytes': row['bytes'] or 0,
            }

    def reset_failed(self, album_id: int | None = None) -> int:
        with self._lock:
            if album_id is not None:
                cur = self.conn.execute(
                    "DELETE FROM episodes WHERE album_id=? AND status='failed'",
                    (album_id,))
            else:
                cur = self.conn.execute(
                    "DELETE FROM episodes WHERE status='failed'")
            self.conn.commit()
            return cur.rowcount

    def close(self) -> None:
        with self._lock:
            self.conn.close()

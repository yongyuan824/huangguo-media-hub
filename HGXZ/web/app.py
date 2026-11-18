from __future__ import annotations

import asyncio
import logging
import json
import os
import socket
import ssl
import httpx
import tempfile
import time
import math
from html.parser import HTMLParser
from xml.etree import ElementTree
from urllib.parse import urljoin
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Event, RLock, Thread
from typing import Literal
from uuid import uuid4
from urllib.parse import urlsplit
from urllib.error import HTTPError
from urllib.request import Request, getproxies, proxy_bypass

from fastapi import Body, FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from ..cancel import SyncCancelled
from ..media_lock import media_lock
from ..download import SpaceLowError, free_space
from ..network import configure_proxy, request_bytes, network_status, route_for
from ..state import StateDB
from ..collections import discover_collections, refresh_collection
from .log_handler import LogBuffer

LOG = logging.getLogger('HGXZ')

STATIC_DIR = Path(__file__).parent / 'static'

_state_db: StateDB | None = None
_log_buffer: LogBuffer | None = None
_archiver_ref: list = []
@dataclass(frozen=True)
class SyncJob:
    id: str
    stop: Event | None


_sync_status: dict = {
    'job_id': None, 'revision': 0, 'instance_id': uuid4().hex, 'result': 'idle',
    'running': False, 'cancellable': False, 'stopping': False, 'stopped': False,
    'last_sync': None, 'last_error': None, 'failed_count': 0,
}
_sync_lock = RLock()
_queue: list[int] = []
_targets: dict[int, list[int]] = {}
_queue_paused = False
_queued_job_id: str | None = None
_progress: dict = {}

_current_album: int | None = None
_active_job: SyncJob | None = None


def _save_queue() -> None:
    if _state_db:
        _state_db.save_queue({'ids': _queue, 'targets': _targets, 'paused': _queue_paused})


def _update_status(**fields) -> None:
    # Call under _sync_lock to publish each transition as one snapshot.
    _sync_status.update(fields)
    _sync_status['revision'] += 1


def try_acquire_sync(*, cancellable: bool = True, kind: str = 'scheduled-sync') -> SyncJob | None:
    global _active_job
    with _sync_lock:
        if _active_job is not None:
            return None
        _active_job = SyncJob(uuid4().hex, Event() if cancellable else None)
        _update_status(job_id=_active_job.id, kind=kind, result='running', running=True, cancellable=cancellable,
                       stopping=False, stopped=False, last_error=None, failed_count=0)
        return _active_job


def _finish_job(job: SyncJob, failures: int = 0, error: str | None = None,
                cancelled: bool = False, pause_queue: bool = False) -> None:
    global _active_job, _current_album, _queue_paused, _queued_job_id
    with _sync_lock:
        if _active_job is not job:
            return
        stopped = cancelled or (error is None and job.stop is not None and job.stop.is_set())
        if error is not None:
            result = 'error'
        elif stopped:
            result = 'stopped'
        elif failures:
            result = 'partial_failure'
        else:
            result = 'completed'
        last_sync = _sync_status['last_sync']
        if result in ('completed', 'partial_failure'):
            last_sync = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        _update_status(result=result, running=False, cancellable=False, stopping=False,
                       stopped=stopped, failed_count=failures, last_error=error, last_sync=last_sync)
        if _current_album is not None and _state_db:
            try:
                _state_db.add_history(_current_album, {'stopped':'同步停止','completed':'同步完成',
                    'partial_failure':'同步部分失败','error':'同步异常'}[result],
                    '请查看同步日志' if error else '')
            except Exception:
                LOG.exception('Could not save album history')
        try:
            if _queued_job_id == job.id:
                if stopped or pause_queue:
                    _queue_paused = True
                elif _current_album in _queue:
                    _queue.remove(_current_album)
                    _targets.pop(_current_album, None)
                _save_queue()
            elif stopped or pause_queue:
                _queue_paused = True
                _save_queue()
        except Exception as exc:
            LOG.exception('Could not save the sync queue')
            _queue_paused = True
            _update_status(result='error', last_error=str(exc))
        finally:
            _queued_job_id = None
            _progress.clear()
            _active_job = None
            _current_album = None
    if stopped:
        LOG.info('同步已停止，已完成的剧集已保留')


def run_sync_job(job: SyncJob, work: Callable[[Event | None], int], name: str) -> None:
    try:
        failures = work(job.stop)
    except SyncCancelled:
        _finish_job(job, cancelled=True)
    except SpaceLowError as exc:
        LOG.warning('%s paused: %s', name, exc)
        _finish_job(job, error=str(exc), pause_queue=True)
    except Exception as exc:
        LOG.exception('%s failed: %s', name, exc)
        _finish_job(job, error=str(exc) or type(exc).__name__)
    else:
        _finish_job(job, failures=failures)


def sync_status_snapshot() -> dict:
    with _sync_lock:
        return dict(_sync_status, service_pid=os.getpid(), service_root=str(Path.cwd().resolve()), network=dict(route_for(_archiver_ref[0].client.base_url) if _archiver_ref else {}, **network_status()),
                    queue=[i for i in _queue if i != _current_album],
                    active_album_id=_current_album, queue_paused=_queue_paused,
                    progress=dict(_progress))


def set_shared_state(state: StateDB, log_buf: LogBuffer, archiver_holder: list) -> None:
    global _state_db, _log_buffer, _archiver_ref, _queue_paused
    _state_db = state
    _log_buffer = log_buf
    _archiver_ref = archiver_holder
    saved = state.load_queue() if state else {}
    _queue[:] = saved.get('ids', [])
    _targets.clear()
    _targets.update({int(i): episodes for i, episodes in saved.get('targets', {}).items()})
    _queue_paused = saved.get('paused', False)


def album_tags(record, root: Path) -> list[str]:
    try:
        tags = json.loads(record.get('site_tags') or '[]')
        if not isinstance(tags,list):
            tags = []
        tags = [t for t in tags if isinstance(t,str)]
    except (ValueError,TypeError):
        tags = []
    if not tags and record.get('directory'):
        directory = Path(record['directory']).resolve()
        if directory.is_relative_to(root.resolve()):
            nfo = directory / 'tvshow.nfo'
            try:
                if nfo.stat().st_size < 1024 * 1024:
                    tree = ElementTree.fromstring(nfo.read_bytes())
                    tags = [node.text.strip() for node in tree.findall('genre')
                            if node.text and node.text.strip() != record.get('category')]
            except (OSError,ElementTree.ParseError,ValueError):
                pass
    return list(dict.fromkeys(tags))[:30]


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        _pump_queue()
        try:
            yield
        finally:
            global _queue_paused
            with _sync_lock:
                _queue_paused = True
                _save_queue()
                if _active_job is not None and _active_job.stop is not None:
                    _active_job.stop.set()
            # Wait for the owned worker to flush its queue and release media files.
            while True:
                with _sync_lock:
                    active = _active_job is not None
                if not active:
                    break
                await asyncio.sleep(0.1)

    app = FastAPI(title='HGXZ - 黄果下载', lifespan=lifespan)

    app.mount('/static', StaticFiles(directory=str(STATIC_DIR)), name='static')

    @app.get('/', response_class=HTMLResponse)
    async def index():
        return FileResponse(STATIC_DIR / 'index.html')

    @app.get('/api/dashboard')
    async def dashboard():
        if not _state_db:
            return JSONResponse({'error': 'not ready'}, 503)
        stats = _state_db.dashboard_stats()
        stats['sync'] = sync_status_snapshot()
        return stats

    @app.get('/api/albums')
    async def albums(
        category: str | None = Query(None),
        status: str | None = Query(None),
        search: str | None = Query(None),
        collection_id: int | None = Query(None),
    ):
        if not _state_db:
            return JSONResponse({'error': 'not ready'}, 503)
        rows = _state_db.all_albums(category=category, status=status, search=search)
        if collection_id is not None:
            with _state_db._lock:
                positions = dict(_state_db.conn.execute(
                    'SELECT album_id,position FROM collection_albums WHERE collection_id=?',(collection_id,)))
            rows = [dict(r, collection_position=positions[r['id']]) for r in rows if r['id'] in positions]
            rows.sort(key=lambda r:r['collection_position'])
        with _state_db._lock:
            links = {}
            for item in _state_db.conn.execute(
                    'SELECT m.album_id,c.name,c.kind,m.position FROM collection_albums m '
                    'JOIN collections c ON c.id=m.collection_id'):
                links.setdefault(item['album_id'],[]).append({
                    'name':item['name'],'kind':item['kind'],'position':item['position']})
        preferences = _state_db.preferences()
        for row in rows:
            row.update(preferences.get(row['id'],{'favorite':0,'hidden':0}))
            row['collections'] = links.get(row['id'],[])
            row['tags'] = list(dict.fromkeys([
                *album_tags(row,_archiver_ref[0].root),
                *(c['name'] for c in row['collections'] if c['kind']=='tag')]))
            with _state_db._lock:
                hot = _state_db.conn.execute(
                    "SELECT m.position FROM collection_albums m JOIN collections c ON c.id=m.collection_id "
                    "WHERE m.album_id=? AND c.path='/ranks/hot/'",(row['id'],)).fetchone()
            row['hot_rank'] = hot[0] if hot else None
        return rows

    @app.post('/api/albums/{album_id}/preferences')
    async def album_preferences(album_id:int,payload:dict=Body(...)):
        if not _state_db or not _state_db.album_row(album_id):
            return JSONResponse({'error':'专辑不存在'},404)
        if set(payload)-{'favorite','hidden'} or any(type(v) is not bool for v in payload.values()):
            return JSONResponse({'error':'无效的偏好设置'},400)
        with _state_db._lock:
            _state_db.conn.execute('INSERT OR IGNORE INTO album_preferences(album_id) VALUES(?)',(album_id,))
            for key,value in payload.items():
                _state_db.conn.execute(f'UPDATE album_preferences SET {key}=? WHERE album_id=?',(int(value),album_id))
            _state_db.conn.commit()
        return _state_db.preferences()[album_id]

    @app.get('/api/albums/{album_id}/history')
    async def album_history(album_id:int):
        with _state_db._lock:
            return [dict(r) for r in _state_db.conn.execute(
                'SELECT action,detail,created_at FROM album_history WHERE album_id=? ORDER BY id DESC LIMIT 30',(album_id,))]

    @app.post('/api/playback/progress')
    async def save_playback(payload:dict=Body(...)):
        try:
            album_id=int(payload['album_id']);episode=int(payload['episode']);seconds=float(payload['seconds'])
            if not math.isfinite(seconds) or seconds<0 or seconds>604800:
                raise ValueError()
        except (KeyError,ValueError,TypeError,OverflowError):
            return JSONResponse({'error':'播放进度无效'},400)
        row=_state_db.episode_row(album_id,episode)
        if not row or row['status']!='done':
            return JSONResponse({'error':'没有该集下载记录'},404)
        with _state_db._lock:
            _state_db.conn.execute(
                'INSERT INTO playback_progress VALUES(?,?,?,?) ON CONFLICT(album_id,episode) DO UPDATE SET '
                'seconds=excluded.seconds,updated_at=excluded.updated_at',
                (album_id,episode,seconds,_state_db._now()))
            _state_db.conn.commit()
        return {'saved':True}

    @app.post('/api/sync/download-missing-batch')
    async def missing_batch(album_id:int,episodes:list[int]=Body(...)):
        numbers=list(dict.fromkeys(episodes))
        if not numbers:
            return JSONResponse({'error':'请选择缺失集'},400)
        for episode in numbers:
            row=_state_db.episode_row(album_id,episode)
            if not row or row['status']!='done' or Path(row['path']).is_file():
                return JSONResponse({'error':'所选集的文件状态已变化，请刷新'},409)
        with _sync_lock:
            if album_id==_current_album or (album_id in _queue and album_id not in _targets):
                return JSONResponse({'error':'该专辑已有整部同步任务，请先移除或等待结束'},409)
            for episode in numbers:
                _add_to_queue(album_id,True,episode)
            _state_db.add_history(album_id,'批量补下载','、'.join(map(str,numbers)))
            return _queue_changed()

    @app.get('/api/albums/{album_id}')
    async def album_detail(album_id: int):
        if not _state_db:
            return JSONResponse({'error': 'not ready'}, 503)
        result = _state_db.album_with_episodes(album_id)
        if not result:
            return JSONResponse({'error': 'not found'}, 404)
        result.update(_state_db.episode_stats(album_id))
        directory = result.get('directory') or ''
        result['storage_path'] = str(Path(directory).resolve()) if directory else ''
        result['poster_available'] = bool(directory) and (Path(directory) / 'poster.jpg').is_file()
        return result

    @app.get('/api/albums/{album_id}/poster')
    async def album_poster(album_id: int):
        """Serve the scraped poster so the library list can show real covers."""
        if not _state_db:
            return JSONResponse({'error': 'not ready'}, 503)
        row = _state_db.album_row(album_id)
        if not row or not row['directory']:
            return JSONResponse({'error': 'not found'}, 404)
        poster = Path(row['directory']) / 'poster.jpg'
        try:
            if not poster.is_file() or poster.stat().st_size == 0:
                return JSONResponse({'error': 'not found'}, 404)
        except OSError:
            return JSONResponse({'error': 'not found'}, 404)
        return FileResponse(poster, media_type='image/jpeg',
                            headers={'Cache-Control': 'max-age=3600'})

    def _pump_queue() -> None:
        global _current_album, _queued_job_id
        with _sync_lock:
            if not _queue or not _archiver_ref or _queue_paused or _active_job is not None:
                return
            album_id = _queue[0]
            targets = _targets.get(album_id)
            archiver = _archiver_ref[0]

            def progress(**fields):
                with _sync_lock:
                    if _current_album == album_id:
                        _progress.update(fields)
                        _update_status()

            job = _start_background('queued-sync', lambda stop: archiver.scan(
                only_id=album_id, stop=stop, episodes=targets, progress=progress)[1])
            if job is not None:
                _current_album = album_id
                _queued_job_id = job.id
                _progress.update(phase='resolving')
                _update_status()

    def _start_background(name: str, work: Callable[[Event | None], int], *,
                          cancellable: bool = True) -> SyncJob | None:
        job = try_acquire_sync(cancellable=cancellable, kind=name)
        if job is None:
            return None

        def execute():
            run_sync_job(job, work, name)
            _pump_queue()

        try:
            Thread(target=execute, daemon=True, name=name).start()
        except RuntimeError as exc:
            _finish_job(job, error=str(exc))
            raise
        return job

    @app.get('/api/collections')
    async def list_collections():
        return _state_db.collections() if _state_db else []

    @app.post('/api/collections/discover')
    async def collection_discover():
        if not _archiver_ref:
            return JSONResponse({'error':'not ready'},503)
        def work(stop):
            discover_collections(_archiver_ref[0].client, _state_db, stop)
            return 0
        if _start_background('collection-discovery', work) is None:
            return JSONResponse({'error':'请等当前任务结束'},409)
        return sync_status_snapshot()

    @app.post('/api/collections/{collection_id}/refresh')
    async def collection_refresh(collection_id: int):
        collection = next((r for r in _state_db.collections() if r['id']==collection_id),None) if _state_db else None
        if not collection:
            return JSONResponse({'error':'列表不存在'},404)
        def work(stop):
            refresh_collection(_archiver_ref[0].client,_state_db,collection,stop)
            return 0
        if _start_background('collection-refresh', work) is None:
            return JSONResponse({'error':'请等当前任务结束'},409)
        return sync_status_snapshot()

    @app.post('/api/catalog/refresh')
    async def refresh_catalog():
        if not _archiver_ref:
            return JSONResponse({'error': 'not ready'}, 503)
        archiver = _archiver_ref[0]
        def discover(stop):
            before = {r['id'] for r in archiver.state.all_albums()}
            archiver.discover(stop=stop, catalog=True)
            failures = archiver.check_catalog_updates(stop=stop)
            after = archiver.state.all_albums()
            summary = {
                'new_ids':[r['id'] for r in after if r['id'] not in before],
                'updated_ids':[r['id'] for r in after if r['new_episode_count']],
                'failed_ids':archiver.update_check_failed,
                'checked_at':datetime.now().isoformat(timespec='seconds'),
            }
            with _sync_lock:
                _update_status(catalog_summary=summary)
            return failures
        if _start_background('catalog-refresh', discover) is None:
            return JSONResponse({'error': '请在当前任务结束后刷新目录'}, 409)
        return sync_status_snapshot()

    def _add_to_queue(album_id: int, priority: bool, episode: int | None = None) -> None:
        if album_id == _current_album:
            return
        if album_id not in _queue:
            _queue.append(album_id)
            if episode is not None:
                _targets[album_id] = [episode]
        elif episode is None:
            _targets.pop(album_id, None)
        elif album_id in _targets and episode not in _targets[album_id]:
            _targets[album_id].append(episode)
            _targets[album_id].sort()
        if priority:
            _queue.remove(album_id)
            _queue.insert(1 if _queue and _queue[0] == _current_album else 0, album_id)

    def _queue_changed() -> dict:
        _save_queue()
        _update_status()
        _pump_queue()
        return sync_status_snapshot()

    @app.get('/api/sync/queue')
    async def get_queue():
        with _sync_lock:
            status = sync_status_snapshot()
            ids = ([status['active_album_id']] if status['active_album_id'] else []) + status['queue']
            rows = [_state_db.album_row(i) for i in ids] if _state_db else []
            return dict(status, items=[{
                'id': row['id'], 'title': row['title'], 'episodes': _targets.get(row['id'])
            } for row in rows if row])

    @app.post('/api/sync/queue')
    async def enqueue_sync(album_id: int, priority: bool = False):
        if not _state_db or not _state_db.album_row(album_id):
            return JSONResponse({'error': '专辑不存在'}, 404)
        with _sync_lock:
            _add_to_queue(album_id, priority)
            _state_db.add_history(album_id,'加入队列','优先' if priority else '')
            return _queue_changed()

    @app.post('/api/sync/queue/batch')
    async def enqueue_batch(album_ids: list[int]):
        ids = list(dict.fromkeys(album_ids))
        if not _state_db or not ids or not all(_state_db.album_row(i) for i in ids):
            return JSONResponse({'error': '请选择有效专辑'}, 400)
        with _sync_lock:
            for i in ids:
                _add_to_queue(i, False)
            return _queue_changed()

    @app.post('/api/sync/queue/control')
    async def control_queue(action: Literal['pause', 'resume', 'remove', 'top'], album_id: int | None = None):
        global _queue_paused
        with _sync_lock:
            if action in ('pause', 'resume'):
                _queue_paused = action == 'pause'
            elif album_id not in _queue or album_id == _current_album:
                return JSONResponse({'error': '该专辑已离开等待队列'}, 409)
            elif action == 'remove':
                _queue.remove(album_id)
                _targets.pop(album_id, None)
            else:
                _queue.remove(album_id)
                _queue.insert(1 if _queue and _queue[0] == _current_album else 0, album_id)
            return _queue_changed()

    @app.post('/api/sync/download-missing')
    async def download_missing(album_id: int, episode: int):
        row = _state_db.episode_row(album_id, episode) if _state_db else None
        if not row or row['status'] != 'done' or Path(row['path']).is_file():
            return JSONResponse({'error': '该集没有缺失文件'}, 409)
        with _sync_lock:
            if album_id == _current_album or (album_id in _queue and album_id not in _targets):
                return JSONResponse({'error': '请先等该专辑结束或从队列移除'}, 409)
            _add_to_queue(album_id, True, episode)
            return _queue_changed()

    @app.post('/api/sync/retry-episode')
    async def retry_episode(album_id: int, episode: int):
        row = _state_db.episode_row(album_id, episode) if _state_db else None
        if not row or row['status'] != 'failed':
            return JSONResponse({'error': '该集当前没有失败记录'}, 409)
        with _sync_lock:
            if album_id == _current_album:
                return JSONResponse({'error': '请等当前专辑同步结束'}, 409)
            if album_id in _queue and album_id not in _targets:
                return JSONResponse({'error': '该专辑已加入全部剧集队列'}, 409)
            _add_to_queue(album_id, True, episode)
            return _queue_changed()

    @app.post('/api/sync')
    async def trigger_sync(album_id: int | None = None):
        if not _archiver_ref:
            return JSONResponse({'error': 'not ready'}, 503)
        archiver = _archiver_ref[0]
        global _current_album
        with _sync_lock:
            job = _start_background('manual-sync',
                                    lambda stop: archiver.scan(only_id=album_id, stop=stop)[1])
            if job is not None:
                _current_album = album_id
                _update_status()
        if job is None:
            return JSONResponse({'error': '同步正在进行中'}, 409)
        return {'status': 'started', 'album_id': album_id, 'job_id': job.id,
                'sync': sync_status_snapshot()}

    @app.post('/api/sync/stop')
    async def stop_sync(job_id: str):
        with _sync_lock:
            job = _active_job
            if job is None or job.id != job_id or job.stop is None:
                return JSONResponse({'error': '该同步任务已结束或不可停止'}, 409)
            if not job.stop.is_set():
                job.stop.set()
                _update_status(stopping=True)
            return sync_status_snapshot()

    @app.post('/api/retry-failed')
    async def retry_failed(album_id: int | None = None):
        if not _state_db:
            return JSONResponse({'error': 'not ready'}, 503)
        count = _state_db.reset_failed(album_id)
        return {'reset': count}

    @app.post('/api/scan-existing')
    async def scan_existing_api():
        if not _archiver_ref:
            return JSONResponse({'error': 'not ready'}, 503)
        archiver = _archiver_ref[0]

        def work(stop: Event | None) -> int:
            from ..scanner import scan_existing
            with media_lock(archiver.root):
                result = scan_existing(archiver.root, archiver.state,
                                       archiver.minimum_duration, client=archiver.client, stop=stop)
            LOG.info('scan existing result: %s', result)
            return result['errors'] + result['online_failed']

        job = _start_background('scan-existing', work)
        if job is None:
            return JSONResponse({'error': '同步正在进行中'}, 409)
        return {'status': 'started', 'job_id': job.id, 'sync': sync_status_snapshot()}

    @app.get('/api/logs')
    async def get_logs(n: int = Query(200, ge=1, le=2000)):
        if not _log_buffer:
            return []
        return _log_buffer.recent(n)

    @app.get('/api/playback/library')
    async def playback_library():
        if not _state_db or not _archiver_ref:
            return JSONResponse({'error': 'not ready'}, 503)
        root = _archiver_ref[0].root.resolve()
        items = []
        with _state_db._lock:
            rows = _state_db.conn.execute(
                 "SELECT a.id,a.title,a.category,a.directory,a.episode_count,a.site_score,a.site_badge,a.site_tags,a.last_changed_at,"
                "(SELECT m.position FROM collection_albums m JOIN collections c ON c.id=m.collection_id "
                "WHERE m.album_id=a.id AND c.path='/ranks/hot/') AS hot_rank,"
                "e.episode,e.path FROM albums a JOIN episodes e ON a.id=e.album_id "
                "WHERE e.status='done' ORDER BY a.title,e.episode").fetchall()
        metadata = {}
        for row in rows:
            path = Path(row['path']).resolve()
            if path.is_relative_to(root) and path.is_file() and path.suffix.lower() == '.mp4':
                if row['id'] not in metadata:
                    tags = album_tags(dict(row),root)
                    directory = Path(row['directory']).resolve()
                    poster = False
                    if directory.is_relative_to(root):
                        poster = (directory / 'poster.jpg').is_file()
                        nfo = directory / 'tvshow.nfo'
                        try:
                            if not tags and nfo.stat().st_size < 1024 * 1024:
                                tree = ElementTree.fromstring(nfo.read_bytes())
                                tags = list(dict.fromkeys(
                                    node.text.strip() for node in tree.findall('genre')
                                    if node.text and node.text.strip() != row['category']))[:12]
                        except (OSError, ElementTree.ParseError, ValueError):
                            pass
                    metadata[row['id']] = {'category': row['category'], 'tags': tags,
                                           'episode_count': row['episode_count'], 'poster_available': poster,
                                           'site_score':row['site_score'],'site_badge':row['site_badge'],
                                           'last_changed_at':row['last_changed_at'],
                                           'hot_rank':row['hot_rank']}
                items.append({'id': row['id'], 'title': row['title'], 'episode': row['episode'],
                              **metadata[row['id']]})
        with _state_db._lock:
            associations = {}
            for link in _state_db.conn.execute(
                    'SELECT m.album_id,c.name,c.kind,m.position FROM collection_albums m '
                    'JOIN collections c ON c.id=m.collection_id'):
                associations.setdefault(link['album_id'],[]).append({
                    'name':link['name'],'kind':link['kind'],'position':link['position']})
        preferences = _state_db.preferences()
        with _state_db._lock:
            progress = {(r['album_id'],r['episode']):dict(r)
                        for r in _state_db.conn.execute('SELECT * FROM playback_progress')}
        for item in items:
            item.update(preferences.get(item['id'],{'favorite':0,'hidden':0}))
            saved = progress.get((item['id'],item['episode']),{})
            item['resume_seconds'] = saved.get('seconds',0)
            item['watched_at'] = saved.get('updated_at','')
            item['collections'] = associations.get(item['id'],[])
            item['tags'] = list(dict.fromkeys([*item['tags'],
                *(c['name'] for c in item['collections'] if c['kind']=='tag')]))
        return items

    @app.get('/api/playback/local/{album_id}/{episode}')
    async def local_video(album_id: int, episode: int):
        if not _state_db or not _archiver_ref:
            return JSONResponse({'error': 'not ready'}, 503)
        row = _state_db.episode_row(album_id, episode)
        if row and row['status'] == 'done':
            path = Path(row['path']).resolve()
            if path.is_relative_to(_archiver_ref[0].root.resolve()) and path.is_file() and path.suffix.lower() == '.mp4':
                return FileResponse(path, media_type='video/mp4')
        return JSONResponse({'error': '视频文件不存在或格式不支持'}, 404)

    @app.get('/api/settings')
    async def settings():
        """Read-only runtime settings for the WebUI footer/settings panel."""
        if not _archiver_ref:
            return JSONResponse({'error': 'not ready'}, 503)
        archiver = _archiver_ref[0]
        configured = getproxies().get('https') or getproxies().get('all')
        proxy = None
        if configured:
            try:
                endpoint = urlsplit(configured if '://' in configured else '//' + configured)
                if endpoint.hostname:
                    proxy = endpoint.hostname if endpoint.port is None else f'{endpoint.hostname}:{endpoint.port}'
            except ValueError:
                pass
        return {
            'proxy_enabled': archiver.config.get('network',{}).get('enabled',True),
            'proxy': proxy,
            'network':network_status(),
            'site_route':route_for(archiver.client.base_url),
            'proxy_url': ((urlsplit(configured).scheme + '://' + proxy) if configured and proxy else ''),
            'configured_base_url': archiver.config['site']['base_url'],
            'backup_urls': archiver.config['site'].get('backup_urls', []),
            'category_config': archiver.config['site']['categories'],
            'base_url': archiver.client.base_url,
            'base_urls': archiver.client.base_urls,
            'media_root': str(archiver.root),
            'categories': [c['name'] for c in archiver.config['site']['categories']],
            'retries': archiver.retries,
            'minimum_duration': archiver.minimum_duration,
            'recheck_days': archiver.recheck_days,
            'failure_cooldown_hours': archiver.failure_cooldown_hours,
            'episode_timeout': archiver.episode_timeout,
            'segment_workers': archiver.segment_workers,
            'min_free_bytes': archiver.min_free_bytes,
            'media_free_bytes': free_space(archiver.root),
            'temp_free_bytes': free_space(Path(tempfile.gettempdir())),
        }

    @app.post('/api/settings/categories/discover')
    async def discover_categories():
        if not _archiver_ref:
            return JSONResponse({'error': 'not ready'}, 503)
        archiver = _archiver_ref[0]

        def discover():
            from ..core import parse_category_panel
            client = type(archiver.client)(
                archiver.client.base_url, 0, 10, backup_urls=archiver.client.base_urls)
            source = client.get_text('/', stop=Event())
            base = client.base_url
            candidates = {}

            class Links(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.link = None
                    self.text = []
                def handle_starttag(self, tag, attrs):
                    if tag == 'a':
                        self.link = dict(attrs).get('href')
                        self.text = []
                def handle_data(self, value):
                    if self.link:
                        self.text.append(value)
                def handle_endtag(self, tag):
                    if tag != 'a' or not self.link:
                        return
                    parsed = urlsplit(urljoin(base + '/', self.link))
                    name = ' '.join(''.join(self.text).split())
                    path = parsed.path
                    if (parsed.netloc == urlsplit(base).netloc and path not in ('', '/')
                            and not parsed.query and not parsed.fragment and name and len(name) < 40
                            and len(path.strip('/').split('/')) == 1):
                        candidates.setdefault(path.rstrip('/') + '/', name)
                    self.link = None
            Links().feed(source)
            results = []
            rejected = 0
            for path, name in list(candidates.items())[:30]:
                try:
                    page = client.get_text(path, stop=Event())
                    ids, total = parse_category_panel(page)
                    if ids or total is not None:
                        results.append({'name': name, 'path': path, 'aliases': [], 'enabled': False})
                    else:
                        rejected += 1
                except Exception:
                    rejected += 1
            return {'categories': results, 'unrecognized': rejected}

        try:
            return await asyncio.to_thread(discover)
        except Exception:
            return JSONResponse({'error': '无法读取站点导航，请检查地址和代理'}, 502)

    @app.post('/api/settings/categories')
    async def save_categories(payload: list[dict] = Body(...)):
        if not _archiver_ref:
            return JSONResponse({'error': 'not ready'}, 503)
        categories = []
        seen = set()
        for item in payload:
            name, path = item.get('name'), item.get('path')
            if (not isinstance(name, str) or not name.strip() or not isinstance(path, str)
                    or not path.startswith('/') or path.startswith('//') or
                    any(x in path for x in ('?', '#', '..', '\\')) or urlsplit(path).netloc):
                return JSONResponse({'error': '请填写栏目名和 / 开头的栏目路径'}, 400)
            path = path.rstrip('/') + '/'
            if path in seen:
                return JSONResponse({'error': '栏目路径重复'}, 400)
            seen.add(path)
            aliases = item.get('aliases', [])
            if not isinstance(aliases, list) or not all(isinstance(a, str) for a in aliases):
                return JSONResponse({'error': '栏目别名格式错误'}, 400)
            enabled = item.get('enabled', True)
            if not isinstance(enabled, bool):
                return JSONResponse({'error': '启用值格式错误'}, 400)
            categories.append({'name': name.strip(), 'path': path, 'library': name.strip(),
                               'enabled': enabled, 'aliases': aliases})
        if not categories or not any(c['enabled'] for c in categories):
            return JSONResponse({'error': '至少启用一个栏目'}, 400)
        with _sync_lock:
            if _active_job is not None:
                return JSONResponse({'error': '请先停止当前任务'}, 409)
            archiver = _archiver_ref[0]
            path = Path(archiver.config.get('_config_path', 'config.json'))
            config = json.loads(path.read_text(encoding='utf-8-sig'))
            # Preserve existing media-library folder names when editing labels.
            old = {c['path']: c for c in config['site']['categories']}
            for category in categories:
                if category['path'] in old:
                    category['library'] = old[category['path']]['library']
            config['site']['categories'] = categories
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent,
                                             prefix='.categories-', delete=False) as output:
                temporary = Path(output.name)
                json.dump(config, output, ensure_ascii=False, indent=2)
                output.flush()
                os.fsync(output.fileno())
            try:
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
            archiver.config['site']['categories'] = categories
        return {'categories': categories}

    @app.post('/api/settings/sites')
    async def save_sites(payload: dict = Body(...)):
        if not _archiver_ref:
            return JSONResponse({'error': 'not ready'}, 503)

        def normalize(value):
            if not isinstance(value, str) or not value.strip():
                raise ValueError('站点地址不能为空')
            url = urlsplit(value.strip())
            if (url.scheme not in ('https', 'http') or not url.hostname or url.username
                    or url.password or url.path not in ('', '/') or url.query or url.fragment):
                raise ValueError('请填写 http/https 站点根地址，不含剧集路径或账号')
            url.port
            return url.scheme + '://' + url.netloc.lower()

        try:
            primary = normalize(payload.get('base_url'))
            backups = payload.get('backup_urls', [])
            if not isinstance(backups, list) or len(backups) > 30:
                raise ValueError('备选地址需为列表，最多 30 个')
            urls = list(dict.fromkeys([primary, *(normalize(v) for v in backups)]))
        except ValueError as exc:
            return JSONResponse({'error': str(exc)}, 400)
        with _sync_lock:
            if _active_job is not None:
                return JSONResponse({'error': '请先停止当前任务再保存地址'}, 409)
            archiver = _archiver_ref[0]
            config_path = Path(archiver.config.get('_config_path', 'config.json'))
            try:
                config = json.loads(config_path.read_text(encoding='utf-8-sig'))
                config['site'].update(base_url=primary, backup_urls=urls[1:])
                with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=config_path.parent,
                                                 prefix='.sites-', suffix='.tmp', delete=False) as output:
                    tmp = Path(output.name)
                    json.dump(config, output, ensure_ascii=False, indent=2)
                    output.flush()
                    os.fsync(output.fileno())
                try:
                    os.replace(tmp, config_path)
                finally:
                    tmp.unlink(missing_ok=True)
            except OSError:
                return JSONResponse({'error': '配置写入失败'}, 500)
            archiver.config['site'].update(base_url=primary, backup_urls=urls[1:])
            archiver.client.base_urls = urls
            archiver.client.base_url = primary
            archiver.client._backup_requests = 0
        return {'base_url': primary, 'backup_urls': urls[1:]}

    @app.post('/api/settings/proxy')
    async def save_proxy(payload: dict = Body(...)):
        value = payload.get('proxy')
        enabled = payload.get('enabled',True)
        if not isinstance(enabled,bool):
            return JSONResponse({'error':'开关值无效'},400)
        try:
            if not isinstance(value, str):
                raise ValueError('请输入代理地址')
            value = value.strip()
            url = urlsplit(value)
            if (url.scheme not in ('http', 'https') or not url.hostname or url.port is None
                    or url.username or url.password or url.path not in ('', '/') or url.query or url.fragment):
                raise ValueError('请输入 http://主机:端口 或 https://主机:端口，不含账号和路径')
            value = url.scheme + '://' + url.netloc
        except ValueError as exc:
            return JSONResponse({'error': str(exc)}, 400)
        with _sync_lock:
            if _active_job is not None:
                return JSONResponse({'error': '请先停止当前任务，再保存代理'}, 409)
            if not _archiver_ref:
                return JSONResponse({'error':'not ready'}, 503)
            archiver = _archiver_ref[0]
            path = Path(archiver.config.get('_config_path','config.json'))
            temporary = None
            try:
                config = json.loads(path.read_text(encoding='utf-8-sig'))
                config.setdefault('network',{}).update(proxy=value,enabled=enabled)
                with tempfile.NamedTemporaryFile(mode='w',encoding='utf-8',dir=path.parent,
                                                 prefix='.proxy-',delete=False) as output:
                    temporary = Path(output.name)
                    json.dump(config,output,ensure_ascii=False,indent=2)
                    output.flush()
                    os.fsync(output.fileno())
                os.replace(temporary,path)
            except OSError:
                return JSONResponse({'error':'代理配置保存失败'},500)
            finally:
                if temporary:
                    temporary.unlink(missing_ok=True)
            archiver.config.setdefault('network',{}).update(proxy=value,enabled=enabled)
            configure_proxy(archiver.config)
        return {'proxy': value,'enabled':enabled}

    @app.post('/api/settings/test-video')
    async def test_video(album_id: int | None = None):
        if not _archiver_ref:
            return JSONResponse({'error':'not ready'},503)
        if sync_status_snapshot()['running']:
            return JSONResponse({'error':'请等当前任务结束再测试视频线路'},409)
        archiver = _archiver_ref[0]
        if album_id is None:
            with _state_db._lock:
                row = _state_db.conn.execute(
                    'SELECT id FROM albums WHERE last_checked_at IS NOT NULL ORDER BY id LIMIT 1').fetchone()
            if not row:
                return JSONResponse({'error':'请先同步或扫描至少一部专辑'},400)
            album_id = row[0]
        started = time.monotonic()
        title_row = _state_db.album_row(album_id) if _state_db else None
        title = title_row['title'] if title_row else str(album_id)
        route = route_for(archiver.client.base_url)
        stage = '获取播放地址'
        try:
            # Separate client prevents the test changing the active download client.
            from ..site import HuangguoClient
            from ..download import _resolve_media_playlist, _parse_m3u8
            client = HuangguoClient(archiver.config['site']['base_url'],0,8,backup_urls=[])
            stream = await asyncio.wait_for(asyncio.to_thread(client.play_url,album_id,1,stop=Event()),20)
            stage = '读取播放清单'
            manifest,text = await asyncio.wait_for(
                asyncio.to_thread(_resolve_media_playlist,stream,client.base_url+'/',8,Event()),25)
            segments = _parse_m3u8(text,manifest)['segments']
            if not segments:
                raise ValueError('播放清单没有分段')
            selected = segments[0]
            route = route_for(selected['url'])
            stage = '读取密钥' if selected['key_uri'] else '读取视频样本'
            async with httpx.AsyncClient(
                    timeout=8,follow_redirects=True,verify=ssl.create_default_context(),
                    trust_env=archiver.config.get('network',{}).get('enabled',True)) as transport:
                headers = {'User-Agent':'Mozilla/5.0','Referer':client.base_url+'/'}
                if selected['key_uri']:
                    async with transport.stream('GET',selected['key_uri'],headers=headers) as response:
                        response.raise_for_status()
                        key = bytearray()
                        async for chunk in response.aiter_bytes(chunk_size=32):
                            key.extend(chunk)
                            if len(key)>16:
                                break
                        if len(key)!=16:
                            raise ValueError('视频密钥长度异常')
                stage = '读取视频样本'
                async with transport.stream('GET',selected['url'],headers=dict(headers,Range='bytes=0-65535')) as response:
                    response.raise_for_status()
                    sampled = 0
                    async for chunk in response.aiter_bytes(chunk_size=4096):
                        sampled += len(chunk)
                        if sampled>=65536:
                            break
                    if not sampled:
                        raise ValueError('视频样本为空')
            return {'ok':True,'album_id':album_id,'stage':'视频线路连通',
                    'sample_bytes':sampled,'route':route['route'],'proxy':route['endpoint'],
                    'title':title,'episode':1,'seconds':round(time.monotonic()-started,2)}
        except Exception as exc:
            return {'ok':False,'album_id':album_id,'stage':stage,
                    'error':type(exc).__name__,'title':title,'episode':1,
                    'route':route['route'],'proxy':route['endpoint'],
                    'seconds':round(time.monotonic()-started,2)}

    @app.post('/api/settings/test-connection')
    async def test_connection():
        if not _archiver_ref:
            return JSONResponse({'error': 'not ready'}, 503)
        archiver = _archiver_ref[0]
        site = archiver.config['site']
        base = site['base_url'].rstrip('/')
        path = site['categories'][0]['path'] if site['categories'] else '/'
        url = base + path

        def test():
            host = urlsplit(url).hostname
            configured = ((getproxies().get(urlsplit(url).scheme) or getproxies().get('all'))
                          if archiver.config.get('network',{}).get('enabled',True) else None)
            proxy_status = 'bypassed' if configured and proxy_bypass(host or '') else 'not_configured'
            if configured and proxy_status != 'bypassed':
                try:
                    endpoint = urlsplit(configured if '://' in configured else '//' + configured)
                    with socket.create_connection((endpoint.hostname, endpoint.port or 80), timeout=4):
                        proxy_status = 'reachable'
                except (OSError, ValueError, TypeError):
                    proxy_status = 'failed'
            try:
                body = request_bytes(Request(url, headers={'User-Agent': archiver.client.user_agent}),
                                     timeout=18, stop=Event())
                site_status = ('ok' if not site['categories'] or b'data-channel-panel' in body
                               else 'unexpected_response')
            except HTTPError as exc:
                site_status = f'HTTP {exc.code}'
            except Exception:
                site_status = 'connection_failed'
            return {'proxy_status': proxy_status, 'site_status': site_status}

        return await asyncio.to_thread(test)

    @app.get('/api/sync-status')
    async def sync_status():
        return sync_status_snapshot()

    @app.websocket('/ws/logs')
    async def ws_logs(websocket: WebSocket):
        if not _log_buffer:
            await websocket.close(code=1011)
            return
        await websocket.accept()
        loop, q = _log_buffer.subscribe()
        # Push whatever is already buffered so a fresh page is not blank until
        # the next log line happens to arrive.
        for entry in _log_buffer.recent(200):
            await websocket.send_json(entry)
        try:
            while True:
                try:
                    entry = await asyncio.wait_for(q.get(), timeout=30.0)
                except asyncio.TimeoutError:
                    # Keepalive: the client reconnects if this ever fails.
                    await websocket.send_json({'level': 'PING', 'message': '',
                                               'timestamp': 0, 'keepalive': True})
                    continue
                await websocket.send_json(entry)
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        except Exception:
            LOG.debug('log websocket closed unexpectedly', exc_info=True)
        finally:
            _log_buffer.unsubscribe(loop, q)

    return app

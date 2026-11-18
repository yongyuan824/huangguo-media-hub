from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from pathlib import Path
from threading import Event
from urllib.parse import urljoin, urlsplit
from urllib.request import Request
import errno
import json
import shutil
import logging
import os
import re
import subprocess
import tempfile
import time


from .cancel import SyncCancelled, check_cancelled, run_process, wait_or_cancel
from .network import request_bytes


LOG = logging.getLogger(__name__)

_HEADERS_UA = 'Mozilla/5.0'
_SEGMENT_ATTEMPTS = 3


def probe_media(path: Path, *, stop: Event | None = None) -> dict:
    if not path.exists():
        raise FileNotFoundError(path)
    result = run_process([
        'ffprobe', '-v', 'error', '-show_entries',
        'format=duration,size:stream=codec_type', '-of', 'json', str(path)
    ], check=True, text=True, stop=stop, timeout=30)
    payload = json.loads(result.stdout)
    streams = payload.get('streams') or []
    return {
        'duration': float((payload.get('format') or {}).get('duration') or 0),
        'size': int((payload.get('format') or {}).get('size') or path.stat().st_size),
        'video': sum(1 for x in streams if x.get('codec_type') == 'video'),
        'audio': sum(1 for x in streams if x.get('codec_type') == 'audio'),
    }


def validate_media(info: dict, minimum_duration: float = 3.0) -> bool:
    return bool(info.get('duration', 0) >= minimum_duration and info.get('video', 0) >= 1)


def _fetch(url: str, referer: str, timeout: float, stop: Event | None = None) -> bytes:
    check_cancelled(stop)
    req = Request(url, headers={'User-Agent': _HEADERS_UA, 'Referer': referer})
    return request_bytes(req, timeout, stop)


def _attrs(line: str) -> dict:
    attrs = {}
    for match in re.finditer(r'([A-Z0-9-]+)=("([^"]*)"|[^,]*)', line):
        attrs[match.group(1)] = match.group(3) if match.group(3) is not None else match.group(2)
    return attrs


def _parse_m3u8(text: str, playlist_url: str) -> dict:
    """Parse a media playlist; keys may change mid-stream so each segment
    remembers the key that was active when it was listed."""
    key_uri = None
    iv = None
    sequence = 0
    segments: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith('#EXT-X-MEDIA-SEQUENCE:'):
            sequence = int(line.split(':', 1)[1])
        elif line.startswith('#EXT-X-KEY:'):
            attrs = _attrs(line)
            method = attrs.get('METHOD', 'NONE')
            if method == 'NONE':
                key_uri, iv = None, None
            elif method == 'AES-128':
                uri = attrs.get('URI', '')
                if not uri:
                    raise RuntimeError('AES-128 key without URI')
                key_uri = urljoin(playlist_url, uri)
                raw_iv = attrs.get('IV')
                iv = bytes.fromhex(raw_iv[2:]) if raw_iv else None
            else:
                raise RuntimeError(f'unsupported key method: {method}')
        elif not line.startswith('#'):
            segments.append({
                'url': urljoin(playlist_url, line),
                'key_uri': key_uri,
                'iv': iv if iv is not None else sequence.to_bytes(16, 'big'),
            })
            sequence += 1
    return {'segments': segments}


def _resolve_media_playlist(url: str, referer: str, timeout: float,
                            stop: Event | None = None) -> tuple[str, str]:
    """Follow master playlists (multiple variants) down to a media playlist."""
    playlist_url = url
    for _ in range(3):
        text = _fetch(playlist_url, referer, timeout, stop).decode('utf-8', errors='replace')
        if '#EXT-X-STREAM-INF' not in text:
            return playlist_url, text
        lines = text.splitlines()
        variant, best_bw = None, -1
        for index, line in enumerate(lines):
            if line.startswith('#EXT-X-STREAM-INF'):
                bandwidth = int(_attrs(line).get('BANDWIDTH', '0') or 0)
                if bandwidth >= best_bw and index + 1 < len(lines):
                    variant, best_bw = lines[index + 1].strip(), bandwidth
        if not variant:
            raise RuntimeError('master playlist has no variant')
        playlist_url = urljoin(playlist_url, variant)
    raise RuntimeError('too many playlist levels')


def _decrypt_segment(data: bytes, key: bytes, iv: bytes, stop: Event | None = None) -> bytes:
    if len(data) % 16:
        raise RuntimeError(f'segment size {len(data)} not AES block aligned')
    # Decrypt via openssl (like cover.py) and strip PKCS#7 padding manually:
    # the last byte tells how many padding bytes to drop.
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / 'seg.bin'
        dst = Path(td) / 'seg.ts'
        src.write_bytes(data)
        proc = run_process([
            'openssl', 'enc', '-d', '-aes-128-cbc', '-nopad',
            '-K', key.hex(), '-iv', iv.hex(),
            '-in', str(src), '-out', str(dst),
        ], stop=stop, timeout=30)
        if proc.returncode:
            raise RuntimeError('segment AES decryption failed')
        plain = dst.read_bytes()
    pad = plain[-1] if plain else 0
    if 1 <= pad <= 16 and plain.endswith(bytes([pad]) * pad):
        plain = plain[:-pad]
    return plain


class SpaceLowError(RuntimeError):
    pass


def free_space(path: Path) -> int:
    current = path.resolve()
    while not current.exists():
        current = current.parent
    return shutil.disk_usage(current).free


def check_download_space(media: Path, minimum: int, *, additional: int = 0) -> None:
    for label, path, required in (
        ('Media drive', media, minimum + additional),
        ('Temporary drive', Path(tempfile.gettempdir()), minimum),
    ):
        free = free_space(path)
        if free < required:
            raise SpaceLowError(f'{label} {path.resolve().anchor}: {free / 2**30:.2f} GiB free; '
                                f'{required / 2**30:.2f} GiB required')


_CACHE_FILE = re.compile(r'(?:[0-9]+\.ts(?:\.sha256)?|fingerprint|concat\.txt)(?:\.tmp)?')


def _clear_segments(cache: Path) -> None:
    for path in cache.iterdir():
        if _CACHE_FILE.fullmatch(path.name) and path.is_file():
            path.unlink()


def _write_atomic(path: Path, data: bytes) -> None:
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_bytes(data)
    os.replace(temporary, path)


def _cached_size(path: Path) -> int:
    try:
        digest = path.with_suffix('.ts.sha256').read_text(encoding='ascii')
        with path.open('rb') as fh:
            if sha256(fh.read()).hexdigest() != digest:
                return 0
        return path.stat().st_size
    except (OSError, UnicodeError):
        return 0


def download_hls(url: str, destination: Path, referer: str, minimum_duration: float = 3.0,
                 timeout: float | None = 1800.0, *, stop: Event | None = None,
                 progress: Callable[..., None] | None = None, workers: int = 1,
                 min_free_bytes: int = 0) -> dict:
    # Fetch every HLS segment as its own short-lived request with retries
    # instead of one long ffmpeg stream: the CDN resets long connections
    # mid-transfer, but a few megabytes per request survive that fine.
    check_download_space(destination.parent, min_free_bytes)
    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + '.part')
    partial.unlink(missing_ok=True)
    deadline = None if timeout is None else time.monotonic() + timeout
    fetch_timeout = 30.0
    try:
        playlist_url, playlist_text = _resolve_media_playlist(url, referer, fetch_timeout, stop)
        parsed = _parse_m3u8(playlist_text, playlist_url)
        segments = parsed['segments']
        if not segments:
            raise RuntimeError('playlist has no segments')
        if progress:
            progress(phase='downloading', segments=0, segments_total=len(segments), bytes_per_second=0)
        started = time.monotonic()
        downloaded = 0
        LOG.info('HLS playlist parsed: %d segments', len(segments))
        playlist_referer = f'{urlsplit(playlist_url).scheme}://{urlsplit(playlist_url).netloc}/'
        keys = {uri: _fetch(uri, playlist_referer, fetch_timeout, stop)
                for uri in dict.fromkeys(s['key_uri'] for s in segments if s['key_uri'])}
        fingerprint = sha256(json.dumps(
            [playlist_text, [(s['url'], s['iv'].hex(), sha256(keys[s['key_uri']]).hexdigest()
                              if s['key_uri'] else None) for s in segments]],
            ensure_ascii=False).encode()).hexdigest()
        cache = destination.parent / ('.hgxz-resume-' + sha256(destination.name.encode()).hexdigest()[:20])
        cache.mkdir(exist_ok=True)
        marker = cache / 'fingerprint'
        if not marker.is_file() or marker.read_text(encoding='ascii', errors='replace') != fingerprint:
            # A refreshed signed URL is not proof of unchanged content.
            # Verify cached segments against the current source before reuse.
            for index, segment in enumerate(segments):
                path = cache / f'{index:05d}.ts'
                if not _cached_size(path):
                    continue
                check_cancelled(stop)
                remote = _fetch(segment['url'], playlist_referer, fetch_timeout, stop)
                if segment['key_uri']:
                    remote = _decrypt_segment(remote, keys[segment['key_uri']], segment['iv'], stop)
                if sha256(remote).hexdigest() != path.with_suffix('.ts.sha256').read_text(encoding='ascii'):
                    path.unlink(missing_ok=True)
                    path.with_suffix('.ts.sha256').unlink(missing_ok=True)
            for path in cache.glob('*.ts'):
                if path.stem.isdigit() and int(path.stem) >= len(segments):
                    path.unlink()
                    path.with_suffix('.ts.sha256').unlink(missing_ok=True)
            _write_atomic(marker, fingerprint.encode('ascii'))

        def fetch_segment(index: int) -> int:
            segment = segments[index]
            key_uri = segment['key_uri']
            for attempt in range(1, _SEGMENT_ATTEMPTS + 1):
                check_cancelled(stop)
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError(f'HLS download exceeded {timeout}s')
                try:
                    data = _fetch(segment['url'], playlist_referer,
                                  fetch_timeout if remaining is None else min(fetch_timeout, remaining), stop)
                    if key_uri:
                        data = _decrypt_segment(data, keys[key_uri], segment['iv'], stop)
                    if not data:
                        raise RuntimeError('empty HLS segment')
                    break
                except SyncCancelled:
                    raise
                except Exception as exc:
                    check_cancelled(stop)
                    if attempt == _SEGMENT_ATTEMPTS:
                        raise RuntimeError(f'segment {index + 1}/{len(segments)} failed after {attempt} attempts: {exc}') from exc
                    LOG.warning('segment %d/%d attempt %d failed: %s', index + 1, len(segments), attempt, exc)
                    wait_or_cancel(stop, 2 * attempt)
            path = cache / f'{index:05d}.ts'
            _write_atomic(path, data)
            _write_atomic(path.with_suffix('.ts.sha256'), sha256(data).hexdigest().encode('ascii'))
            return len(data)

        sizes = []
        for index in range(len(segments)):
            check_cancelled(stop)
            sizes.append(_cached_size(cache / f'{index:05d}.ts'))
        missing = [i for i, size in enumerate(sizes) if not size]
        reused = len(segments) - len(missing)
        if reused:
            LOG.info('resumed %d/%d cached HLS segments', reused, len(segments))
        if progress:
            progress(segments=reused, resumed_segments=reused)
        done = reused

        def record(index, size):
            nonlocal done, downloaded
            sizes[index] = size
            done += 1
            downloaded += size
            if progress:
                progress(segments=done, resumed_segments=reused, bytes_per_second=round(
                    downloaded / max(time.monotonic() - started, 0.01)))
            if done % 10 == 0 or done == len(segments):
                LOG.info('downloaded segments %d/%d', done, len(segments))

        if workers == 1:
            for index in missing:
                check_download_space(destination.parent, min_free_bytes)
                record(index, fetch_segment(index))
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                for offset in range(0, len(missing), workers):
                    check_cancelled(stop)
                    check_download_space(destination.parent, min_free_bytes)
                    batch = [(i, pool.submit(fetch_segment, i)) for i in missing[offset:offset + workers]]
                    for i, future in batch:
                        record(i, future.result())
        check_download_space(destination.parent, min_free_bytes, additional=sum(sizes))
        listing = cache / 'concat.txt'
        listing.write_text(''.join(f"file '{i:05d}.ts'\n" for i in range(len(segments))), encoding='ascii')
        try:
            check_cancelled(stop)
            if progress:
                progress(phase='merging', bytes_per_second=0)
            LOG.info('merging %d segments with ffmpeg...', len(segments))
            base_cmd = [
                'ffmpeg', '-hide_banner', '-loglevel', 'warning', '-y',
                '-analyzeduration', '100M', '-probesize', '100M',
                '-fflags', '+igndts+genpts',
                '-f', 'concat', '-safe', '0', '-i', str(listing),
                '-avoid_negative_ts', 'make_zero',
                '-map', '0',
            ]
            cmd = base_cmd + ['-c', 'copy', '-movflags', '+faststart', '-f', 'mp4', str(partial)]
            try:
                result = run_process(cmd, check=True, text=True, stop=stop,
                                        timeout=None if deadline is None else max(1.0, deadline - time.monotonic()))
                stderr = result.stderr or ''
                if 'failed too many times, skipping' in stderr.lower():
                    raise RuntimeError('concat reported skipped segments')
            except subprocess.CalledProcessError as exc:
                check_cancelled(stop)
                LOG.warning('ffmpeg copy mode failed, retrying with audio transcode: %s', (exc.stderr or '').strip()[-200:])
                cmd_fallback = base_cmd + ['-c:v', 'copy', '-c:a', 'aac', '-movflags', '+faststart', '-f', 'mp4', str(partial)]
                try:
                    result = run_process(cmd_fallback, check=True, text=True, stop=stop,
                                            timeout=None if deadline is None else max(1.0, deadline - time.monotonic()))
                except subprocess.CalledProcessError as exc2:
                    stderr = (exc2.stderr or '').strip()
                    raise RuntimeError(f'ffmpeg exited with {exc2.returncode}: {stderr[-800:]}') from exc2
        finally:
            listing.unlink(missing_ok=True)
        info = probe_media(partial, stop=stop)
        if not validate_media(info, minimum_duration):
            _clear_segments(cache)
            raise RuntimeError(f'invalid media: {info}')
        check_cancelled(stop)
        os.replace(partial, destination)
        try:
            _clear_segments(cache)
            if not any(cache.iterdir()):
                cache.rmdir()
        except OSError:
            LOG.warning('completed video saved; could not clear its segment cache')
        return info
    except OSError as exc:
        partial.unlink(missing_ok=True)
        if exc.errno == errno.ENOSPC or getattr(exc, 'winerror', None) in (39, 112):
            raise SpaceLowError(f'Not enough disk space for {destination.parent.anchor}') from exc
        raise
    except Exception:
        partial.unlink(missing_ok=True)
        raise

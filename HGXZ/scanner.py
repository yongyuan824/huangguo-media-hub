from __future__ import annotations

import logging
import re
from pathlib import Path
from threading import Event

from .cancel import SyncCancelled, check_cancelled
from .core import parse_detail_html

from .core import ALBUM_DIR_RE
from .download import probe_media, validate_media
from .state import StateDB

LOG = logging.getLogger('HGXZ')

EPISODE_RE = re.compile(r'\.S01E(\d+)\.\w+$', re.I)


def scan_existing(root: Path, state: StateDB,
                  minimum_duration: float = 3.0,
                  validate: bool = True, *, client=None, stop: Event | None = None) -> dict:
    """Walk the media root, find previously downloaded videos by the
    ``[huangguo-{id}]`` directory naming convention, and register them in the
    state database so subsequent syncs treat them as already downloaded."""

    result = {
        'albums_found': 0,
        'episodes_found': 0,
        'episodes_valid': 0,
        'episodes_invalid': 0,
        'errors': 0,
        'online_checked': 0,
        'online_failed': 0,
    }

    if not root.is_dir():
        LOG.warning('media root does not exist: %s', root)
        return result

    for category_dir in sorted(root.iterdir()):
        if not category_dir.is_dir() or category_dir.name.startswith('.'):
            continue
        category_name = category_dir.name

        for album_dir in sorted(category_dir.iterdir()):
            check_cancelled(stop)
            if not album_dir.is_dir():
                continue
            match = ALBUM_DIR_RE.match(album_dir.name)
            if not match:
                continue

            title = match.group(1).strip()
            album_id = int(match.group(2))
            result['albums_found'] += 1

            season_dir = album_dir / 'Season 01'
            search_dir = season_dir if season_dir.is_dir() else album_dir

            episodes: list[tuple[int, Path]] = []
            for f in sorted(search_dir.iterdir()):
                if not f.is_file():
                    continue
                if f.suffix.lower() not in ('.mp4', '.mkv', '.ts'):
                    continue
                ep_match = EPISODE_RE.search(f.name)
                if ep_match:
                    episodes.append((int(ep_match.group(1)), f))
                    result['episodes_found'] += 1

            max_ep = max((ep_num for ep_num, _ in episodes), default=0)
            state.register_local_album(album_id, category_name, title, max_ep, str(album_dir))

            if client is not None:
                try:
                    source = client.get_text(f'/video/{album_id}/', stop=stop)
                    album = parse_detail_html(album_id, category_name, source)
                    # Missing episode markup is not sufficient evidence of a total.
                    if not re.search(r'data-ep-id=["\']\d+', source):
                        raise ValueError('No authoritative episode list on detail page')
                    check_cancelled(stop)
                    state.update_online_total(album_id, album.episode_count, album.ended)
                    state.upsert_album(album_id, category_name, album.title, album.episode_count,
                                       album.ended, str(album_dir))
                    state.update_site_metadata(album_id, source)
                    result['online_checked'] += 1
                    LOG.info('online checked album=%s total=%s', album_id, album.episode_count)
                except SyncCancelled:
                    raise
                except Exception as exc:
                    result['online_failed'] += 1
                    LOG.warning('online check failed album=%s reason=%s', album_id, type(exc).__name__)

            for ep_num, ep_path in episodes:
                check_cancelled(stop)
                if state.episode_done(album_id, ep_num):
                    result['episodes_valid'] += 1
                    # Databases written before size tracking existed report
                    # 0 bytes; backfill from the file itself, no re-probe.
                    row = state.episode_row(album_id, ep_num)
                    if row and not row['size_bytes']:
                        try:
                            state.mark_episode(album_id, ep_num, row['path'], 'done',
                                               ep_path.stat().st_size)
                        except OSError:
                            pass
                    continue
                if validate:
                    try:
                        info = probe_media(ep_path, stop=stop)
                        if validate_media(info, minimum_duration):
                            state.mark_episode(album_id, ep_num, str(ep_path), 'done',
                                               info.get('size', 0))
                            result['episodes_valid'] += 1
                        else:
                            state.mark_episode(album_id, ep_num, str(ep_path), 'failed')
                            result['episodes_invalid'] += 1
                            LOG.warning('invalid media: album=%s ep=%s info=%s',
                                        album_id, ep_num, info)
                    except SyncCancelled:
                        raise
                    except Exception as exc:
                        result['errors'] += 1
                        LOG.warning('probe error: album=%s ep=%s error=%s',
                                    album_id, ep_num, exc)
                else:
                    state.mark_episode(album_id, ep_num, str(ep_path), 'done',
                                       ep_path.stat().st_size)
                    result['episodes_valid'] += 1

            LOG.info('scanned album=%s title=%s episodes=%d', album_id, title,
                     len(episodes))

    LOG.info('scan_existing complete: %s', result)
    return result

from __future__ import annotations

from threading import Event
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request
import json
import logging
import random
import time


from .cancel import check_cancelled, wait_or_cancel
from .network import request_bytes


LOG = logging.getLogger(__name__)


class HuangguoClient:
    def __init__(
        self,
        base_url: str,
        request_interval: float = 2.0,
        timeout: int = 30,
        backup_urls: list[str] | None = None,
    ):
        urls = [base_url, *(backup_urls or [])]
        self.base_urls = list(dict.fromkeys(url.rstrip('/') for url in urls if url))
        if not self.base_urls:
            raise ValueError('at least one site base URL is required')
        self.base_url = self.base_urls[0]
        self.request_interval = request_interval
        self.timeout = timeout
        self.last_request = 0.0
        self.user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131 Safari/537.36'
        self._backup_requests = 0

    def _throttle(self, stop: Event | None) -> None:
        wait = self.request_interval - (time.monotonic() - self.last_request)
        if wait > 0:
            wait_or_cancel(stop, wait + random.uniform(0, min(0.25, self.request_interval / 4)))

    def _candidates(self, is_absolute: bool) -> list[str | None]:
        if is_absolute:
            return [None]
        candidates = [self.base_url, *[url for url in self.base_urls if url != self.base_url]]
        if self.base_url != self.base_urls[0]:
            # After a failover, probe the primary host now and then so a
            # transient outage does not pin us to a backup forever.
            self._backup_requests += 1
            if self._backup_requests >= 20:
                self._backup_requests = 0
                candidates = [self.base_urls[0], self.base_url, *[url for url in self.base_urls if url not in (self.base_urls[0], self.base_url)]]
        return candidates

    def _request(self, method: str, path: str, *, data: bytes | None = None, headers: dict | None = None,
                 stop: Event | None = None) -> bytes:
        self._throttle(stop)
        is_absolute = path.startswith('http')
        candidates = self._candidates(is_absolute)
        last_error: Exception | None = None
        try:
            for index, candidate in enumerate(candidates):
                check_cancelled(stop)
                url = path if is_absolute else candidate + path
                referer = self.base_url + '/' if is_absolute else candidate + '/'
                request_headers = {'User-Agent': self.user_agent, 'Referer': referer}
                request_headers.update(headers or {})
                req = Request(url, method=method, data=data, headers=request_headers)
                try:
                    body = request_bytes(req, self.timeout, stop)
                    if candidate and candidate != self.base_url:
                        LOG.warning('site base URL switched to: %s', candidate)
                        self.base_url = candidate
                        self._backup_requests = 0
                    return body
                except (HTTPError, URLError, TimeoutError, ConnectionError, OSError) as error:
                    check_cancelled(stop)
                    last_error = error
                    if is_absolute or index == len(candidates) - 1:
                        raise
                    LOG.warning('site request failed via %s; trying backup: %s', candidate, error)
            raise last_error or RuntimeError('site request failed')
        finally:
            self.last_request = time.monotonic()

    def get_text(self, path: str, *, stop: Event | None = None) -> str:
        return self._request('GET', path, stop=stop).decode('utf-8', errors='replace')

    def get_bytes(self, url: str, *, stop: Event | None = None) -> bytes:
        return self._request('GET', url, stop=stop)

    def play_url(self, album_id: int, episode: int, *, stop: Event | None = None) -> str:
        raw = self._request('GET', f'/api/videos/{int(album_id)}/play?' + urlencode({'ep': int(episode)}), stop=stop)
        payload = json.loads(raw)
        if payload.get('status') != 1:
            raise RuntimeError(payload.get('msg') or 'play endpoint failed')
        url = ((payload.get('data') or {}).get('video_url') or '').strip()
        if url:
            return url

        # The legacy endpoint can return episode metadata without a stream.
        # Use the same playback request as the current web player in that case.
        path = f'/api/videos/{int(album_id)}/episodes/{int(episode)}/playback'
        raw = self._request('POST', path, data=b'{}', stop=stop, headers={
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        })
        payload = json.loads(raw)
        if payload.get('status') != 1:
            raise RuntimeError(payload.get('msg') or 'play endpoint failed')
        playback = payload.get('data') or {}
        stream = ((playback.get('streams') or {}).get('main') or '').strip()
        if playback.get('manifest_method') != 'GET' or not stream:
            raise RuntimeError('empty video_url')
        return stream

    def cover_url(self, album_id: int, *, stop: Event | None = None) -> str:
        body = json.dumps({'items': [{'type': 'video', 'id': int(album_id)}]}, ensure_ascii=False).encode()
        raw = self._request('POST', '/api/media/covers', data=body, stop=stop,
                            headers={'Content-Type': 'application/json'})
        payload = json.loads(raw)
        if payload.get('status') != 1:
            raise RuntimeError(payload.get('msg') or 'cover endpoint failed')
        return (((payload.get('data') or {}).get('covers') or {}).get(f'video:{int(album_id)}') or '').strip()

from __future__ import annotations

import asyncio
from functools import lru_cache
import logging
import os
import ssl
from threading import Event, Lock
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen, getproxies, proxy_bypass, build_opener, ProxyHandler

import httpx

from .cancel import check_cancelled

# Playback URLs can contain signed parameters.
logging.getLogger('httpx').setLevel(logging.WARNING)


_route_lock = Lock()
_route_status = {'active_requests':0,'last_result':None}
_route_generation = 0
_proxy_disabled = False


def route_for(url: str) -> dict:
    target = urlsplit(url)
    proxies = {} if _proxy_disabled else getproxies()
    proxy = proxies.get(target.scheme) or proxies.get('all')
    if proxy and not proxy_bypass(target.hostname or ''):
        parsed = urlsplit(proxy if '://' in proxy else '//' + proxy)
        endpoint = parsed.hostname or ''
        if parsed.port:
            endpoint += ':' + str(parsed.port)
        return {'route':'proxy','endpoint':endpoint,'host':target.hostname}
    return {'route':'direct','endpoint':None,'host':target.hostname}


def network_status() -> dict:
    with _route_lock:
        return dict(_route_status)


def configure_proxy(config: dict) -> None:
    global _route_generation, _proxy_disabled
    network = config.get('network', {})
    _proxy_disabled = network.get('enabled', True) is False
    if _proxy_disabled:
        with _route_lock:
            _route_generation += 1
            _route_status.clear()
            _route_status.update(active_requests=0,last_result=None)
        return
    proxy = network.get('proxy')
    if not proxy:
        return
    target = urlsplit(proxy)
    if target.scheme not in ('http', 'https') or not target.hostname or target.port is None:
        raise ValueError('network.proxy must be an HTTP proxy URL with a host and port')
    bypass = os.environ.get('no_proxy') or os.environ.get('NO_PROXY', '')
    entries = list(dict.fromkeys([*(x.strip() for x in bypass.split(',') if x.strip()),
                                  '127.0.0.1', 'localhost', '::1']))
    for key, value in (('http_proxy', proxy), ('https_proxy', proxy), ('no_proxy', ','.join(entries))):
        os.environ[key] = value
        os.environ[key.upper()] = value
    with _route_lock:
        _route_generation += 1
        _route_status.clear()
        _route_status.update(active_requests=0,last_result=None)



@lru_cache(maxsize=1)
def _tls_context() -> ssl.SSLContext:
    return ssl.create_default_context()


async def _request_bytes(request: Request, timeout: float, stop: Event) -> bytes:
    headers = dict(request.header_items())
    headers['Accept-Encoding'] = 'identity'
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True,
                                trust_env=not _proxy_disabled, verify=_tls_context()) as client:
        transfer = asyncio.create_task(client.request(
            request.get_method(), request.full_url, headers=headers, content=request.data))
        try:
            while not transfer.done():
                check_cancelled(stop)
                await asyncio.wait({transfer}, timeout=0.1)
            check_cancelled(stop)
            response = transfer.result()
        except httpx.RequestError as error:
            raise URLError(type(error).__name__) from error
        finally:
            if not transfer.done():
                transfer.cancel()
            await asyncio.gather(transfer, return_exceptions=True)

        if not response.is_success:
            raise HTTPError(str(response.url), response.status_code,
                            response.reason_phrase, response.headers, None)
        return response.content


def _perform_request(request: Request, timeout: float, stop: Event | None = None) -> bytes:
    if stop is None:
        opener = build_opener(ProxyHandler({})) if _proxy_disabled else build_opener()
        with opener.open(request, timeout=timeout) as response:
            return response.read()
    check_cancelled(stop)
    return asyncio.run(_request_bytes(request, timeout, stop))


def request_bytes(request: Request, timeout: float, stop: Event | None = None) -> bytes:
    with _route_lock:
        generation = _route_generation
        _route_status.update(route_for(request.full_url))
        _route_status['active_requests'] += 1
        _route_status['last_result'] = 'requesting'
    result = 'failed'
    try:
        body = _perform_request(request, timeout, stop)
        result = 'success'
        return body
    finally:
        if stop is not None and stop.is_set():
            result = 'cancelled'
        with _route_lock:
            if generation == _route_generation:
                _route_status['active_requests'] -= 1
                _route_status.update(last_result=result,
                                     checked_at=datetime.now(timezone.utc).isoformat())

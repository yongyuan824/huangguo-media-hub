from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit
import re

from .cancel import check_cancelled


class CollectionPage(HTMLParser):
    def __init__(self):
        super().__init__()
        self.depth = 0
        self.scope = None
        self.scope_finished = False
        self.anchor = None
        self.text = []
        self.links = []
        self.albums = {}
        self.next_url = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag not in ('img', 'input', 'link', 'meta', 'br', 'hr', 'source', 'wbr', 'area', 'base', 'embed', 'param', 'track', 'col'):
            self.depth += 1
        if self.scope is None and not self.scope_finished and ('hg-card-grid' in attrs.get('class', '').split() or 'data-rank-list' in attrs):
            self.scope = self.depth
        if tag in ('a', 'link') and 'next' in attrs.get('rel', '').split():
            self.next_url = attrs.get('href')
        if tag == 'a':
            self.anchor = attrs.get('href')
            self.text = [attrs.get('title', '')]
        if tag == 'img' and self.anchor and attrs.get('alt'):
            self.text.append(attrs['alt'])

    def handle_data(self, value):
        if self.anchor:
            self.text.append(value)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in ('img', 'input', 'link', 'meta', 'br', 'hr', 'source', 'wbr', 'area', 'base', 'embed', 'param', 'track', 'col'):
            self.handle_endtag(tag)

    def handle_endtag(self, tag):
        if tag in ('img', 'input', 'link', 'meta', 'br', 'hr', 'source', 'wbr', 'area', 'base', 'embed', 'param', 'track', 'col'):
            return
        if tag == 'a' and self.anchor:
            title = ' '.join(' '.join(self.text).split())
            self.links.append((self.anchor, title))
            match = re.fullmatch(r'/(?:video|detail)/(\d+)/?', urlsplit(self.anchor).path)
            if self.scope is not None and match:
                ident = int(match.group(1))
                if ident not in self.albums or not self.albums[ident]:
                    self.albums[ident] = title
            self.anchor = None
        if self.scope == self.depth:
            self.scope = None
            self.scope_finished = True
        self.depth = max(0, self.depth - 1)


def collection_kind(path):
    parts = path.strip('/').split('/')
    return {'topics': 'topic', 'ranks': 'rank', 'tag': 'tag'}.get(parts[0]) if len(parts) == 2 else None


def discover_collections(client, state, stop=None):
    for path in ('/', '/topics/', '/ranks/hot/'):
        check_cancelled(stop)
        page = CollectionPage()
        page.feed(client.get_text(path, stop=stop))
        for url, title in page.links:
            parsed = urlsplit(urljoin(client.base_url + '/', url))
            kind = collection_kind(parsed.path)
            if kind and parsed.netloc == urlsplit(client.base_url).netloc:
                state.register_collection(kind, parsed.path.rstrip('/') + '/', title or parsed.path)


def refresh_collection(client, state, collection, stop=None):
    path = collection['path']
    albums = {}
    seen = set()
    while path:
        check_cancelled(stop)
        if path in seen or len(seen) >= 100:
            raise ValueError('Collection pagination did not finish; previous memberships retained')
        seen.add(path)
        page = CollectionPage()
        page.feed(client.get_text(path, stop=stop))
        albums.update({i: title for i, title in page.albums.items() if i not in albums})
        if not page.albums:
            raise ValueError('Collection page has no recognizable albums; previous memberships retained')
        if not page.next_url:
            break
        url = urlsplit(urljoin(client.base_url + path, page.next_url))
        if url.netloc != urlsplit(client.base_url).netloc or not url.path.startswith(collection['path']):
            raise ValueError('Unexpected collection pagination target')
        path = url.path
    check_cancelled(stop)
    state.replace_collection(collection['id'], albums)
    return len(albums)

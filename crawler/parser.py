"""HTML parsing and URL normalization for the mock site."""

from __future__ import annotations

from html.parser import HTMLParser
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse


def normalize_url(url: str) -> str:
    """Stable identity: drop fragment, sort query params."""
    parts = urlparse(url)
    query = urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True)))
    return urlunparse((parts.scheme, parts.netloc, parts.path, "", query, ""))


def kind_for(url: str) -> str:
    path = urlparse(url).path
    if path.startswith("/item/"):
        return "detail"
    if path.startswith("/cat/"):
        return "listing"
    return "home"


class _PageParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self.item = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "a" and attrs.get("href"):
            self.links.append(attrs["href"])
        elif tag == "div" and attrs.get("class") == "item":
            self.item = {
                "sku": attrs.get("data-sku"),
                "category": attrs.get("data-category"),
                "title": attrs.get("data-title"),
                "price": float(attrs.get("data-price", "0")),
                "stock": int(attrs.get("data-stock", "0")),
            }


def parse_page(base_url: str, html: str):
    """Returns (child_urls, page_data)."""
    parser = _PageParser()
    parser.feed(html)
    children = []
    seen = set()
    for href in parser.links:
        url = normalize_url(urljoin(base_url, href))
        if url not in seen:
            seen.add(url)
            children.append(url)
    data = {"links_found": len(children)}
    if parser.item is not None:
        data["item"] = parser.item
    return children, data

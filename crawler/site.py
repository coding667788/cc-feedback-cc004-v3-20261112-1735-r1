"""Deterministic local mock site: home -> category listings -> details.

Served by a stdlib ThreadingHTTPServer (runs in a background thread).
The dataset is fixed for the lifetime of the server process. Pages contain
duplicate links and cycles on purpose:
  - every listing page links to page 1 of its category (cycle) and lists
    each detail link twice (duplicates);
  - detail pages link back to home, to their category, and to a "related"
    detail (cross-links / cycles).

Test-only control endpoints (prefix /__ctrl/) provide synchronization
points: request gates, 503 injection, artificial latency, and per-path
request counters. The crawler itself never calls them.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

CATEGORIES = {
    "books": {"pages": 3, "items_per_page": 4},
    "games": {"pages": 2, "items_per_page": 3},
    "music": {"pages": 1, "items_per_page": 5},
}


def item_sku(category: str, index: str) -> str:
    return f"{category}-{index}"


def item_record(category: str, index: int) -> dict:
    """Deterministic detail payload for an item."""
    sku = item_sku(category, index)
    return {
        "sku": sku,
        "category": category,
        "title": f"{category.title()} Item {index}",
        "price": round(5.0 + (index * 7 % 23) + len(category) * 0.5, 2),
        "stock": (index * 3 + len(category)) % 17,
    }


def category_items(category: str) -> list:
    cfg = CATEGORIES[category]
    return list(range(cfg["pages"] * cfg["items_per_page"]))


def page_items(category: str, page: int) -> list:
    per = CATEGORIES[category]["items_per_page"]
    return category_items(category)[(page - 1) * per: page * per]


def _a(href: str, text: str) -> str:
    return f'<a href="{href}">{text}</a>'


def render_home() -> str:
    links = [_a(f"/cat/{c}", c) for c in CATEGORIES]
    links.append(_a("/", "home again"))  # self-cycle
    return "<html><body><h1>home</h1>" + "".join(links) + "</body></html>"


def render_listing(category: str, page: int) -> str:
    total = CATEGORIES[category]["pages"]
    parts = [f"<h1>{category} page {page}</h1>"]
    for idx in page_items(category, page):
        href = f"/item/{item_sku(category, idx)}"
        parts.append(_a(href, f"item {idx}"))
        parts.append(_a(href, f"item {idx} (dup)"))  # duplicate link
    if page < total:
        parts.append(_a(f"/cat/{category}?page={page + 1}", "next"))
    if page > 1:
        parts.append(_a(f"/cat/{category}?page={page - 1}", "prev"))
    parts.append(_a(f"/cat/{category}?page=1", "first"))  # cycle to page 1
    parts.append(_a("/", "home"))  # cycle to home
    return "<html><body>" + "".join(parts) + "</body></html>"


def render_detail(category: str, index: int) -> str:
    rec = item_record(category, index)
    others = category_items(category)
    related = item_sku(category, others[(others.index(index) + 1) % len(others)])
    div = (
        f'<div class="item" data-sku="{rec["sku"]}"'
        f' data-category="{rec["category"]}"'
        f' data-title="{rec["title"]}"'
        f' data-price="{rec["price"]}"'
        f' data-stock="{rec["stock"]}"></div>'
    )
    links = (
        _a("/", "home")
        + _a(f"/cat/{category}?page=1", "category")
        + _a(f"/item/{related}", "related")
        + _a(f"/item/{rec['sku']}", "self")  # self-cycle
    )
    return "<html><body>" + div + links + "</body></html>"


class SiteState:
    """Shared mutable state for control endpoints (test-only)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.request_counts = {}   # path (with query) -> int
        self.gates = {}            # name -> threading.Event (set = open)
        self.gated_paths = {}      # path -> gate name
        self.flaky = {}            # path -> [remaining_failures, status]
        self.delays = {}           # path -> seconds

    def count_request(self, path: str) -> None:
        with self.lock:
            self.request_counts[path] = self.request_counts.get(path, 0) + 1


def make_handler(state: SiteState):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):  # keep test output quiet
            pass

        def _send(self, status: int, body: str,
                  content_type: str = "text/html; charset=utf-8"):
            payload = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)
            full = path + (("?" + parsed.query) if parsed.query else "")

            if path.startswith("/__ctrl/"):
                return self._control(path, query)

            state.count_request(full)

            with state.lock:
                gate_name = state.gated_paths.get(full)
                delay = state.delays.get(full)
                flaky = state.flaky.get(full)
                if flaky and flaky[0] > 0:
                    flaky[0] -= 1
                    status = flaky[1]
                else:
                    status = None
            if gate_name is not None:
                state.gates[gate_name].wait()  # block until test opens gate
            if delay:
                time.sleep(delay)
            if status is not None:
                return self._send(status, "transient error")

            if path == "/":
                return self._send(200, render_home())
            if path.startswith("/cat/"):
                category = path[len("/cat/"):]
                if category not in CATEGORIES:
                    return self._send(404, "no such category")
                page = int(query.get("page", ["1"])[0])
                if not 1 <= page <= CATEGORIES[category]["pages"]:
                    return self._send(404, "no such page")
                return self._send(200, render_listing(category, page))
            if path.startswith("/item/"):
                sku = path[len("/item/"):]
                category, _, idx = sku.rpartition("-")
                if category not in CATEGORIES or not idx.isdigit() \
                        or int(idx) not in category_items(category):
                    return self._send(404, "no such item")
                return self._send(200, render_detail(category, int(idx)))
            return self._send(404, "not found")

        # ------------------------------------------------ test-only controls
        def _control(self, path, query):
            if path == "/__ctrl/gate":
                # /__ctrl/gate?name=g1&path=/item/books-0
                name = query["name"][0]
                target = query["path"][0]
                with state.lock:
                    state.gates.setdefault(name, threading.Event())
                    state.gated_paths[target] = name
                return self._send(200, "gated", "text/plain")
            if path == "/__ctrl/open":
                name = query["name"][0]
                with state.lock:
                    event = state.gates.get(name)
                if event:
                    event.set()
                return self._send(200, "opened", "text/plain")
            if path == "/__ctrl/flaky":
                # /__ctrl/flaky?path=/item/books-1&times=2&status=503
                target = query["path"][0]
                times = int(query.get("times", ["1"])[0])
                status = int(query.get("status", ["503"])[0])
                with state.lock:
                    state.flaky[target] = [times, status]
                return self._send(200, "flaky", "text/plain")
            if path == "/__ctrl/slow":
                target = query["path"][0]
                seconds = float(query.get("seconds", ["1"])[0])
                with state.lock:
                    state.delays[target] = seconds
                return self._send(200, "slow", "text/plain")
            if path == "/__ctrl/requests":
                with state.lock:
                    snapshot = dict(state.request_counts)
                return self._send(200, json.dumps(snapshot), "application/json")
            return self._send(404, "unknown control", "text/plain")

    return Handler


class MockSite:
    """Context manager: starts the site on 127.0.0.1 in a daemon thread."""

    def __init__(self, port: int = 0):
        self.state = SiteState()
        self.server = _QuietServer(
            ("127.0.0.1", port), make_handler(self.state))
        self.port = self.server.server_address[1]
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class _QuietServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        pass  # clients may abort (timeouts, injected crashes); stay quiet


def serve_forever(port: int = 8765):  # CLI helper: python -m crawler site
    with MockSite(port) as site:
        print(f"mock site on {site.base_url}")
        try:
            threading.Event().wait()
        except KeyboardInterrupt:
            pass

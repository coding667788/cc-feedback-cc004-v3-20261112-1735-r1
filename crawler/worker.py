"""Async worker: leases tasks, fetches pages, commits atomically.

Crash injection (for recovery tests), via env var:
    CRAWLER_CRASH="<point>@<url-substring>"
points:
    response    - after the HTTP response body was received
    pre_commit  - after result+children were written, before COMMIT
    post_commit - after COMMIT succeeded, before the task is acknowledged
The worker process hard-exits (os._exit) at the first matching task.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from dataclasses import dataclass

import httpx

from .db import Store
from .parser import kind_for, parse_page

CRASH_EXIT_CODE = 42


@dataclass
class WorkerConfig:
    db_path: str
    base_url: str
    worker_id: str
    concurrency: int = 4       # max simultaneous fetches
    capacity: int = 8          # max in-memory (claimed, unprocessed) tasks
    lease_seconds: float = 30.0
    poll_interval: float = 0.2
    http_timeout: float = 5.0
    max_retries: int = 3
    backoff_base: float = 0.5
    backoff_cap: float = 8.0
    crash_spec: str = ""       # "<point>@<substring>" or ""


def _crash_point(spec: str):
    if not spec:
        return None, None
    point, _, needle = spec.partition("@")
    return point, needle


class Worker:
    def __init__(self, config: WorkerConfig):
        self.cfg = config
        self.store = Store(config.db_path)
        self.crash_point, self.crash_needle = _crash_point(config.crash_spec)
        self._crashed = False

    # ------------------------------------------------------------ crash hook
    def maybe_crash(self, point: str, url: str) -> None:
        if self._crashed:
            return
        if self.crash_point == point and self.crash_needle in url:
            self._crashed = True
            print(f"[{self.cfg.worker_id}] CRASH injected at '{point}' "
                  f"for {url}", file=sys.stderr, flush=True)
            os._exit(CRASH_EXIT_CODE)

    # ------------------------------------------------------------- one task
    async def process_task(self, client: httpx.AsyncClient, task) -> None:
        cfg = self.cfg
        url = task.url
        try:
            response = await client.get(url, timeout=cfg.http_timeout)
            status = response.status_code
            body = response.text
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            self.store.commit_retry(
                task.task_id, task.claim_seq, cfg.worker_id,
                f"transport: {type(exc).__name__}",
                cfg.max_retries, cfg.backoff_base, cfg.backoff_cap)
            return

        # crash point 1: response received, nothing persisted yet
        self.maybe_crash("response", url)

        if status >= 500:
            self.store.commit_retry(
                task.task_id, task.claim_seq, cfg.worker_id,
                f"http {status}",
                cfg.max_retries, cfg.backoff_base, cfg.backoff_cap)
            return
        if status >= 400:
            # permanent error: fail without burning retries
            self.store.commit_retry(
                task.task_id, task.claim_seq, cfg.worker_id,
                f"http {status} (permanent)",
                max_retries=0, backoff_base=0, backoff_cap=0)
            return

        child_urls, data = parse_page(url, body)
        children = [(child, kind_for(child)) for child in child_urls]

        committed = self.store.commit_done(
            task.task_id, task.claim_seq, cfg.worker_id,
            task.kind, url, data, children,
            # crash point 2: rows written, transaction not yet committed
            before_commit=lambda: self.maybe_crash("pre_commit", url),
        )
        if not committed:
            # stale lease: another worker owns this task now; drop result
            return

        # crash point 3: commit durable, but worker dies before ack
        self.maybe_crash("post_commit", url)

    # -------------------------------------------------------------- mainloop
    async def run(self) -> int:
        """Returns 0 when every discovered task is done/failed."""
        cfg = self.cfg
        inflight = set()
        limits = httpx.Limits(max_connections=cfg.concurrency,
                              max_keepalive_connections=cfg.concurrency)
        async with httpx.AsyncClient(base_url=cfg.base_url,
                                     limits=limits) as client:
            semaphore = asyncio.Semaphore(cfg.concurrency)

            async def guarded(task):
                async with semaphore:
                    await self.process_task(client, task)

            try:
                while True:
                    room = cfg.capacity - len(inflight)
                    claimed = []
                    if room > 0:
                        claimed = self.store.claim(cfg.worker_id, room,
                                                   cfg.lease_seconds)
                        for task in claimed:
                            inflight.add(asyncio.ensure_future(guarded(task)))

                    if inflight:
                        done, inflight = await asyncio.wait(
                            inflight, timeout=cfg.poll_interval,
                            return_when=asyncio.FIRST_COMPLETED)
                        for fut in done:
                            exc = fut.exception()
                            if exc is not None:
                                print(f"[{cfg.worker_id}] task error: {exc}",
                                      file=sys.stderr)
                    elif not claimed:
                        # Nothing claimable right now. Do NOT exit while any
                        # task is still leased (maybe by a dead worker whose
                        # lease will expire) or waiting for backoff.
                        if self.store.all_finished():
                            return 0
                        await asyncio.sleep(cfg.poll_interval)
            finally:
                for fut in inflight:
                    fut.cancel()


def run_worker(config: WorkerConfig) -> int:
    return asyncio.run(Worker(config).run())

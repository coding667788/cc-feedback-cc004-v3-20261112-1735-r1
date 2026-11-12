"""Command line entry: python -m crawler <command> ...

Commands:
  site     run the mock site (foreground)
  init     create/reset the crawl database and seed the home page task
  worker   run one worker process (usually spawned by `run`)
  run      spawn N worker processes and crawl to completion
  status   print task/retry/result status and the completion verdict
"""

from __future__ import annotations

import argparse
import os
import sys

from .db import Store
from .parser import kind_for, normalize_url
from .scheduler import build_report, run_crawl
from .worker import WorkerConfig, run_worker


def _seed(db_path: str, base_url: str) -> None:
    store = Store(db_path)
    try:
        home = normalize_url(base_url.rstrip("/") + "/")
        added = store.add_tasks([(home, kind_for(home))])
        print(f"seeded {added} task(s); total={store.task_count()}")
    finally:
        store.close()


def _worker_config(args) -> WorkerConfig:
    return WorkerConfig(
        db_path=args.db,
        base_url=args.base_url,
        worker_id=args.worker_id,
        concurrency=args.concurrency,
        capacity=args.capacity,
        lease_seconds=args.lease_seconds,
        poll_interval=args.poll_interval,
        http_timeout=args.http_timeout,
        max_retries=args.max_retries,
        backoff_base=args.backoff_base,
        crash_spec=args.crash or os.environ.get("CRAWLER_CRASH", ""),
    )


def _add_worker_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--capacity", type=int, default=8)
    parser.add_argument("--lease-seconds", type=float, default=30.0)
    parser.add_argument("--poll-interval", type=float, default=0.2)
    parser.add_argument("--http-timeout", type=float, default=5.0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--backoff-base", type=float, default=0.5)
    parser.add_argument("--crash", default="",
                        help="crash injection '<point>@<url-substring>'")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="crawler")
    sub = parser.add_subparsers(dest="command", required=True)

    p_site = sub.add_parser("site", help="run the mock site")
    p_site.add_argument("--port", type=int, default=8765)

    p_init = sub.add_parser("init", help="create db and seed home task")
    p_init.add_argument("--db", required=True)
    p_init.add_argument("--base-url", required=True)

    p_worker = sub.add_parser("worker", help="run one worker process")
    p_worker.add_argument("--db", required=True)
    p_worker.add_argument("--base-url", required=True)
    p_worker.add_argument("--worker-id", required=True)
    _add_worker_args(p_worker)

    p_run = sub.add_parser("run", help="spawn workers and crawl")
    p_run.add_argument("--db", required=True)
    p_run.add_argument("--base-url", required=True)
    p_run.add_argument("--workers", type=int, default=3)
    _add_worker_args(p_run)

    p_status = sub.add_parser("status", help="print crawl status")
    p_status.add_argument("--db", required=True)
    p_status.add_argument("--verbose", action="store_true",
                          help="list every task with status and attempts")

    args = parser.parse_args(argv)

    if args.command == "site":
        from .site import serve_forever
        serve_forever(args.port)
        return 0

    if args.command == "init":
        _seed(args.db, args.base_url)
        return 0

    if args.command == "worker":
        return run_worker(_worker_config(args))

    if args.command == "run":
        if Store(args.db).task_count() == 0:
            _seed(args.db, args.base_url)
        worker_args = [
            "--concurrency", str(args.concurrency),
            "--capacity", str(args.capacity),
            "--lease-seconds", str(args.lease_seconds),
            "--poll-interval", str(args.poll_interval),
            "--http-timeout", str(args.http_timeout),
            "--max-retries", str(args.max_retries),
            "--backoff-base", str(args.backoff_base),
        ]
        report = run_crawl(args.db, args.base_url, args.workers, worker_args)
        print(report.render())
        return 0 if report.complete else 1

    if args.command == "status":
        report = build_report(args.db)
        print(report.render())
        if args.verbose:
            store = Store(args.db)
            for row in store.all_tasks():
                print(f"  {row['status']:8s} attempts={row['attempts']} "
                      f"{row['url']} {row['last_error'] or ''}")
            store.close()
        return 0 if report.complete else 1

    return 2


if __name__ == "__main__":
    sys.exit(main())

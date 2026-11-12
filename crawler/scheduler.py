"""Multi-process scheduler: spawns worker processes and decides completion.

Completion rule: the crawl is finished only when the task table contains
no pending and no leased rows. An empty in-memory queue never ends the
run - workers keep polling while retries are scheduled or leases are
held by (possibly dead) workers.
"""

from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import dataclass, field

from .db import Store


@dataclass
class Report:
    complete: bool
    counts: dict
    failed: list = field(default_factory=list)
    missing: list = field(default_factory=list)
    result_rows: int = 0

    def render(self) -> str:
        lines = []
        c = self.counts
        lines.append(
            f"tasks: total={sum(c.values())} done={c['done']} "
            f"pending={c['pending']} leased={c['leased']} failed={c['failed']}")
        lines.append(f"result rows: {self.result_rows}")
        if self.failed:
            lines.append("FAILED tasks:")
            for row in self.failed:
                lines.append(f"  {row['url']} attempts={row['attempts']} "
                             f"error={row['last_error']}")
        if self.missing:
            lines.append("MISSING results (done without result row):")
            for row in self.missing:
                lines.append(f"  {row['url']}")
        if self.complete:
            lines.append("VERDICT: COMPLETE - every discovered task "
                         "succeeded and has a result row.")
        else:
            lines.append("VERDICT: INCOMPLETE - see failures/missing above.")
        return "\n".join(lines)


def build_report(db_path: str) -> Report:
    store = Store(db_path)
    try:
        counts = store.counts()
        failed = store.failed_tasks()
        missing = store.missing_results()
        result_rows = len(store.all_results())
        complete = (store.all_finished() and not failed and not missing
                    and store.task_count() > 0)
        return Report(complete=complete, counts=counts,
                      failed=failed, missing=missing,
                      result_rows=result_rows)
    finally:
        store.close()


def run_crawl(db_path: str, base_url: str, workers: int,
              worker_args: list, poll: float = 0.5,
              timeout: float = 300.0) -> Report:
    """Spawn worker processes, wait for completion, return the report."""
    store = Store(db_path)
    procs = []
    try:
        for index in range(workers):
            cmd = [sys.executable, "-m", "crawler", "worker",
                   "--db", db_path, "--base-url", base_url,
                   "--worker-id", f"w{index}"] + worker_args
            procs.append(subprocess.Popen(cmd))

        deadline = time.time() + timeout
        while time.time() < deadline:
            if store.all_finished():
                break
            time.sleep(poll)

        # workers exit on their own once all_finished; give them a moment
        for proc in procs:
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.terminate()
                proc.wait(timeout=10)
    finally:
        for proc in procs:
            if proc.poll() is None:
                proc.kill()
        store.close()
    return build_report(db_path)

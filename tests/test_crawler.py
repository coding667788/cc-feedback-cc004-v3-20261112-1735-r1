"""End-to-end and scenario tests for the crash-resilient crawler.

Run from the repo root:  python -m unittest discover -s tests -v

The expected task set and detail records live in expected_data.json next
to this file; only tests read it - the crawler never does.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
import unittest
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from crawler.db import Store, task_id_for  # noqa: E402
from crawler.parser import kind_for, normalize_url  # noqa: E402
from crawler.scheduler import build_report  # noqa: E402
from crawler.site import MockSite  # noqa: E402
from crawler.worker import CRASH_EXIT_CODE, WorkerConfig, run_worker  # noqa: E402

EXPECTED = json.loads(
    (REPO_ROOT / "tests" / "expected_data.json").read_text(encoding="utf-8"))


def expected_urls(base_url: str):
    return sorted(normalize_url(base_url.rstrip("/") + path)
                  for path in EXPECTED["paths"])


def seed_db(db_path: str, base_url: str) -> None:
    store = Store(db_path)
    home = normalize_url(base_url.rstrip("/") + "/")
    store.add_tasks([(home, kind_for(home))])
    store.close()


def run_workers(site, db_path, count=2, timeout=30.0, **overrides):
    """Run `count` in-process workers in threads; fail on hang/error."""
    defaults = dict(concurrency=2, capacity=4, lease_seconds=10.0,
                    poll_interval=0.05, http_timeout=5.0,
                    max_retries=3, backoff_base=0.05)
    defaults.update(overrides)
    errors = []
    threads = []

    def target(cfg):
        try:
            run_worker(cfg)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    for index in range(count):
        cfg = WorkerConfig(db_path=db_path, base_url=site.base_url,
                           worker_id=f"w{index}", **defaults)
        thread = threading.Thread(target=target, args=(cfg,), daemon=True)
        thread.start()
        threads.append(thread)
    deadline = time.time() + timeout
    for thread in threads:
        thread.join(max(0.1, deadline - time.time()))
    alive = [t for t in threads if t.is_alive()]
    if alive:
        raise AssertionError(f"{len(alive)} worker(s) did not finish")
    if errors:
        raise AssertionError(f"worker errors: {errors!r}")


def wait_for(predicate, timeout=10.0, interval=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class CrawlerTestCase(unittest.TestCase):
    def setUp(self):
        # sandbox-friendly: keep test databases inside the workspace
        self.tmp = REPO_ROOT / ".test-tmp" / uuid.uuid4().hex[:12]
        self.tmp.mkdir(parents=True, exist_ok=True)
        self.db = str(self.tmp / "crawl.db")
        self.site = MockSite(0)
        self.site.__enter__()
        self.addCleanup(self.site.__exit__)
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def url(self, path: str) -> str:
        return normalize_url(self.site.base_url.rstrip("/") + path)

    def assert_matches_expected(self):
        store = Store(self.db)
        try:
            task_urls = sorted(row["url"] for row in store.all_tasks())
            self.assertEqual(task_urls, expected_urls(self.site.base_url),
                             "task set differs from expected")
            results = store.all_results()
            self.assertEqual(len(results), len(EXPECTED["paths"]),
                             "result row count differs (dup or missing)")
            items = {}
            for row in results:
                data = json.loads(row["data"])
                if "item" in data:
                    items[data["item"]["sku"]] = data["item"]
            self.assertEqual(items, EXPECTED["items"])
        finally:
            store.close()


class TestFullCrawl(CrawlerTestCase):
    def test_full_crawl_matches_expected(self):
        seed_db(self.db, self.site.base_url)
        run_workers(self.site, self.db, count=3)
        report = build_report(self.db)
        self.assertTrue(report.complete, report.render())
        self.assert_matches_expected()
        # dedup: every page fetched exactly once despite duplicate/cyclic links
        for path, count in self.site.state.request_counts.items():
            self.assertEqual(count, 1, f"{path} fetched {count} times")


class TestClaiming(CrawlerTestCase):
    def test_no_double_claim_and_lease_expiry(self):
        store = Store(self.db)
        store.add_tasks([(self.url(f"/item/books-{i}"), "detail")
                         for i in range(4)])
        first = store.claim("w1", 10, lease_seconds=0.2)
        self.assertEqual(len(first), 4)
        # a second worker must not steal live leases
        self.assertEqual(store.claim("w2", 10, lease_seconds=0.2), [])
        # after expiry the tasks become claimable again
        time.sleep(0.3)
        second = store.claim("w2", 10, lease_seconds=10.0)
        self.assertEqual(len(second), 4)
        # fencing token bumped on re-claim
        seqs = {t.task_id: t.claim_seq for t in second}
        self.assertTrue(all(seq == 2 for seq in seqs.values()))
        store.close()

    def test_stale_worker_commit_rejected(self):
        store = Store(self.db)
        url = self.url("/item/books-0")
        store.add_tasks([(url, "detail")])
        stale = store.claim("w1", 1, lease_seconds=0.2)[0]
        time.sleep(0.3)
        fresh = store.claim("w2", 1, lease_seconds=10.0)[0]
        self.assertEqual(stale.task_id, fresh.task_id)
        ok = store.commit_done(fresh.task_id, fresh.claim_seq, "w2",
                               "detail", url, {"item": {"sku": "books-0"}},
                               [])
        self.assertTrue(ok)
        # the stale worker's late result must not commit
        ok = store.commit_done(stale.task_id, stale.claim_seq, "w1",
                               "detail", url, {"item": {"sku": "STALE"}}, [])
        self.assertFalse(ok)
        self.assertEqual(store.commit_retry(
            stale.task_id, stale.claim_seq, "w1", "late",
            3, 0.1, 1.0), "stale")
        results = store.all_results()
        self.assertEqual(len(results), 1)
        self.assertEqual(json.loads(results[0]["data"])["item"]["sku"],
                         "books-0")
        store.close()

    def test_late_result_rejected_with_live_workers(self):
        """Worker A blocks on a gated page; its lease expires; worker B
        re-claims and completes; A's late commit is rejected."""
        seed_db(self.db, self.site.base_url)
        gated = "/item/books-0"
        self._ctrl(f"/__ctrl/gate?name=g1&path={gated}")
        overrides = dict(lease_seconds=1.0, capacity=1, concurrency=1,
                         http_timeout=10.0)
        threads = [
            threading.Thread(target=run_workers,
                             args=(self.site, self.db),
                             kwargs=dict(count=1, timeout=25, **overrides),
                             daemon=True)
            for _ in range(2)]
        for thread in threads:
            thread.start()
        # wait until the gated page has been hit AND the expired lease was
        # re-claimed by the other worker (fencing token bumped), then
        # release both blocked requests
        self.assertTrue(wait_for(
            lambda: self.site.state.request_counts.get(gated, 0) >= 1))
        store = Store(self.db)
        reclaimed = wait_for(
            lambda: store.get_task_by_url(self.url(gated))["claim_seq"] >= 2,
            timeout=10)
        store.close()
        self.assertTrue(reclaimed, "lease was never re-claimed")
        self._ctrl("/__ctrl/open?name=g1")
        for thread in threads:
            thread.join(30)
            self.assertFalse(thread.is_alive())
        report = build_report(self.db)
        self.assertTrue(report.complete, report.render())
        self.assert_matches_expected()
        # fetched twice (both workers) but committed exactly once
        self.assertEqual(self.site.state.request_counts[gated], 2)
        store = Store(self.db)
        row = store.get_task_by_url(self.url(gated))
        self.assertEqual(row["status"], "done")
        self.assertEqual(len(store.all_results()), len(EXPECTED["paths"]))
        store.close()

    def _ctrl(self, path):
        import urllib.request
        with urllib.request.urlopen(self.site.base_url + path) as resp:
            resp.read()


class TestRetry(CrawlerTestCase):
    def test_503_retry_then_success(self):
        seed_db(self.db, self.site.base_url)
        self._ctrl("/__ctrl/flaky?path=/item/books-1&times=2&status=503")
        run_workers(self.site, self.db, count=2)
        report = build_report(self.db)
        self.assertTrue(report.complete, report.render())
        store = Store(self.db)
        row = store.get_task_by_url(self.url("/item/books-1"))
        self.assertEqual(row["status"], "done")
        self.assertEqual(row["attempts"], 2, "two 503s must be recorded")
        store.close()
        self.assertEqual(self.site.state.request_counts["/item/books-1"], 3)
        self.assert_matches_expected()

    def test_retry_exhaustion_marks_failed(self):
        seed_db(self.db, self.site.base_url)
        self._ctrl("/__ctrl/flaky?path=/item/books-1&times=99&status=503")
        run_workers(self.site, self.db, count=1, max_retries=2)
        report = build_report(self.db)
        self.assertFalse(report.complete)
        self.assertEqual(len(report.failed), 1)
        failed = report.failed[0]
        self.assertTrue(failed["url"].endswith("/item/books-1"))
        self.assertEqual(failed["attempts"], 3)  # 1 initial + 2 retries
        self.assertIn("503", failed["last_error"])
        # everything else still completed
        self.assertEqual(report.counts["done"], len(EXPECTED["paths"]) - 1)

    def test_timeout_then_retry_success(self):
        seed_db(self.db, self.site.base_url)
        gated = "/item/games-0"
        self._ctrl(f"/__ctrl/gate?name=slow&path={gated}")
        releaser = threading.Thread(target=self._open_gate_after_timeout,
                                    args=("slow", gated), daemon=True)
        releaser.start()
        run_workers(self.site, self.db, count=2, http_timeout=0.3,
                    backoff_base=0.05)
        report = build_report(self.db)
        self.assertTrue(report.complete, report.render())
        store = Store(self.db)
        row = store.get_task_by_url(self.url(gated))
        self.assertEqual(row["status"], "done")
        self.assertGreaterEqual(row["attempts"], 1, "timeout must be recorded")
        store.close()
        self.assert_matches_expected()

    def _open_gate_after_timeout(self, name, path):
        # release the gate only after the client-side timeout was recorded
        store = Store(self.db)
        url = self.url(path)
        ok = wait_for(
            lambda: (store.get_task_by_url(url) or {"attempts": 0})
            ["attempts"] >= 1,
            timeout=15)
        store.close()
        if not ok:
            raise AssertionError("timeout was never recorded")
        self._ctrl(f"/__ctrl/open?name={name}")

    def test_empty_queue_waits_for_scheduled_retry(self):
        """Queue empty but a retry is scheduled: not complete, not claimable."""
        store = Store(self.db)
        url = self.url("/item/books-0")
        store.add_tasks([(url, "detail")])
        task = store.claim("w1", 1, lease_seconds=10.0)[0]
        outcome = store.commit_retry(task.task_id, task.claim_seq, "w1",
                                     "http 503", max_retries=3,
                                     backoff_base=60.0, backoff_cap=60.0)
        self.assertEqual(outcome, "retry")
        self.assertFalse(store.all_finished(),
                         "must not declare completion while a retry waits")
        self.assertEqual(store.claim("w2", 10, lease_seconds=10.0), [],
                         "backoff task must not be claimable yet")
        store.close()

    def _ctrl(self, path):
        import urllib.request
        with urllib.request.urlopen(self.site.base_url + path) as resp:
            resp.read()


class TestCrashRecovery(CrawlerTestCase):
    """Kill a real worker process at each of the three crash points, then
    restart on the same database and require the no-fault outcome."""

    CRASH_TARGET = "books-0"
    CRASH_PATH = "/item/books-0"

    def _run_worker_process(self, crash_spec="", extra_env=None):
        env = dict(os.environ)
        if crash_spec:
            env["CRAWLER_CRASH"] = crash_spec
        if extra_env:
            env.update(extra_env)
        cmd = [sys.executable, "-m", "crawler", "worker",
               "--db", self.db, "--base-url", self.site.base_url,
               "--worker-id", "crashy",
               "--concurrency", "1", "--capacity", "2",
               "--lease-seconds", "2", "--poll-interval", "0.05",
               "--backoff-base", "0.05"]
        return subprocess.run(cmd, env=env, cwd=REPO_ROOT,
                              capture_output=True, text=True, timeout=120)

    def _crash_and_recover(self, point):
        seed_db(self.db, self.site.base_url)
        proc = self._run_worker_process(f"{point}@{self.CRASH_TARGET}")
        self.assertEqual(proc.returncode, CRASH_EXIT_CODE,
                         f"expected injected crash, got {proc.returncode}: "
                         f"{proc.stderr[-500:]}")
        store = Store(self.db)
        row = store.get_task_by_url(self.url(self.CRASH_PATH))
        results_before = {r["task_id"] for r in store.all_results()}
        store.close()
        if point == "post_commit":
            self.assertEqual(row["status"], "done",
                             "post_commit crash: commit must be durable")
            self.assertIn(task_id_for(self.url(self.CRASH_PATH)),
                          results_before)
        else:
            self.assertNotEqual(row["status"], "done",
                                f"{point} crash: task must not be done")
            self.assertNotIn(task_id_for(self.url(self.CRASH_PATH)),
                             results_before,
                             f"{point} crash: uncommitted result must "
                             f"be rolled back")
        # recover on the same database
        proc2 = self._run_worker_process()
        self.assertEqual(proc2.returncode, 0, proc2.stderr[-500:])
        report = build_report(self.db)
        self.assertTrue(report.complete, report.render())
        self.assert_matches_expected()
        fetches = self.site.state.request_counts[self.CRASH_PATH]
        if point == "post_commit":
            self.assertEqual(fetches, 1,
                             "committed task must not be re-fetched")
        else:
            self.assertEqual(fetches, 2,
                             "rolled-back task must be re-fetched once")

    def test_crash_after_response(self):
        self._crash_and_recover("response")

    def test_crash_after_save_before_commit(self):
        self._crash_and_recover("pre_commit")

    def test_crash_after_commit_before_ack(self):
        self._crash_and_recover("post_commit")


if __name__ == "__main__":
    unittest.main()

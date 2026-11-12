"""SQLite-backed task store with lease-based claiming and fencing tokens.

Design notes:
- Task identity: sha1 of the normalized URL. The site is fixed during one
  crawl, so a URL always yields the same page; INSERT OR IGNORE gives us
  persistent dedup across restarts.
- Claiming: a worker leases a task; the lease bumps `claim_seq` which acts
  as a fencing token. A stale worker whose lease expired (and whose task
  was re-claimed) cannot commit, because every commit verifies `claim_seq`
  inside the same transaction.
- Atomicity: result row + discovered child tasks + parent status flip are
  written in a single BEGIN IMMEDIATE ... COMMIT transaction. A crash
  before COMMIT rolls everything back; a crash after COMMIT is fully
  durable, so recovery converges to the no-fault outcome either way.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id       TEXT PRIMARY KEY,
    url           TEXT NOT NULL,
    kind          TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_retry_at REAL NOT NULL DEFAULT 0,
    lease_owner   TEXT,
    lease_until   REAL,
    claim_seq     INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    discovered_by TEXT,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS results (
    task_id    TEXT PRIMARY KEY,
    url        TEXT NOT NULL,
    kind       TEXT NOT NULL,
    data       TEXT NOT NULL,
    fetched_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

STATUS_PENDING = "pending"
STATUS_LEASED = "leased"
STATUS_DONE = "done"
STATUS_FAILED = "failed"


def task_id_for(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


@dataclass
class ClaimedTask:
    task_id: str
    url: str
    kind: str
    claim_seq: int
    attempts: int


class Store:
    """One instance per process/thread (owns its sqlite connection)."""

    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        self.conn = sqlite3.connect(self.db_path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def add_tasks(self, tasks, discovered_by=None) -> int:
        """tasks: iterable of (url, kind). Returns number of NEW tasks."""
        now = time.time()
        added = 0
        with self.conn:
            for url, kind in tasks:
                cur = self.conn.execute(
                    "INSERT OR IGNORE INTO tasks"
                    " (task_id, url, kind, status, discovered_by, created_at,"
                    "  updated_at) VALUES (?,?,?,?,?,?,?)",
                    (task_id_for(url), url, kind, STATUS_PENDING,
                     discovered_by, now, now),
                )
                added += cur.rowcount
        return added

    def task_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]

    def claim(self, worker_id: str, limit: int, lease_seconds: float):
        """Lease up to `limit` claimable tasks. Re-claims expired leases."""
        now = time.time()
        lease_until = now + lease_seconds
        claimed = []
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            rows = self.conn.execute(
                "SELECT task_id, url, kind, attempts FROM tasks"
                " WHERE (status=? AND next_retry_at<=?)"
                "    OR (status=? AND lease_until<?)"
                " ORDER BY created_at LIMIT ?",
                (STATUS_PENDING, now, STATUS_LEASED, now, limit),
            ).fetchall()
            for row in rows:
                cur = self.conn.execute(
                    "UPDATE tasks SET status=?, lease_owner=?, lease_until=?,"
                    " claim_seq=claim_seq+1, updated_at=?"
                    " WHERE task_id=?"
                    "   AND ((status=? AND next_retry_at<=?)"
                    "        OR (status=? AND lease_until<?))",
                    (STATUS_LEASED, worker_id, lease_until, now,
                     row["task_id"],
                     STATUS_PENDING, now, STATUS_LEASED, now),
                )
                if cur.rowcount == 1:
                    seq = self.conn.execute(
                        "SELECT claim_seq FROM tasks WHERE task_id=?",
                        (row["task_id"],),
                    ).fetchone()[0]
                    claimed.append(ClaimedTask(
                        task_id=row["task_id"], url=row["url"],
                        kind=row["kind"], claim_seq=seq,
                        attempts=row["attempts"],
                    ))
            self.conn.commit()
            return claimed
        except BaseException:
            self.conn.rollback()
            raise

    def commit_done(self, task_id, claim_seq, worker_id, kind, url,
                    result_data, children, before_commit=None) -> bool:
        """Atomically store result + children + mark done.

        Returns False when the fencing token no longer matches (stale
        worker); nothing is written in that case. `before_commit` is a
        test hook invoked after all writes but before COMMIT
        (crash point: saved-but-not-committed).
        """
        now = time.time()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT claim_seq, lease_owner, status FROM tasks"
                " WHERE task_id=?",
                (task_id,),
            ).fetchone()
            if (row is None or row["status"] != STATUS_LEASED
                    or row["claim_seq"] != claim_seq
                    or row["lease_owner"] != worker_id):
                self.conn.rollback()
                return False
            self.conn.execute(
                "INSERT INTO results (task_id, url, kind, data, fetched_at)"
                " VALUES (?,?,?,?,?) ON CONFLICT(task_id) DO NOTHING",
                (task_id, url, kind,
                 json.dumps(result_data, ensure_ascii=False), now),
            )
            for child_url, child_kind in children:
                self.conn.execute(
                    "INSERT OR IGNORE INTO tasks"
                    " (task_id, url, kind, status, discovered_by, created_at,"
                    "  updated_at) VALUES (?,?,?,?,?,?,?)",
                    (task_id_for(child_url), child_url, child_kind,
                     STATUS_PENDING, task_id, now, now),
                )
            self.conn.execute(
                "UPDATE tasks SET status=?, lease_owner=NULL, lease_until=NULL,"
                " last_error=NULL, updated_at=? WHERE task_id=?",
                (STATUS_DONE, now, task_id),
            )
            if before_commit is not None:
                before_commit()
            self.conn.commit()
            return True
        except BaseException:
            self.conn.rollback()
            raise

    def commit_retry(self, task_id, claim_seq, worker_id, error,
                     max_retries, backoff_base, backoff_cap) -> str:
        """Record a failed attempt. Returns 'retry' | 'failed' | 'stale'."""
        now = time.time()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT claim_seq, lease_owner, status, attempts FROM tasks"
                " WHERE task_id=?",
                (task_id,),
            ).fetchone()
            if (row is None or row["status"] != STATUS_LEASED
                    or row["claim_seq"] != claim_seq
                    or row["lease_owner"] != worker_id):
                self.conn.rollback()
                return "stale"
            attempts = row["attempts"] + 1
            if attempts > max_retries:
                self.conn.execute(
                    "UPDATE tasks SET status=?, attempts=?, last_error=?,"
                    " lease_owner=NULL, lease_until=NULL, updated_at=?"
                    " WHERE task_id=?",
                    (STATUS_FAILED, attempts, error, now, task_id),
                )
                self.conn.commit()
                return "failed"
            delay = min(backoff_cap, backoff_base * (2 ** (attempts - 1)))
            self.conn.execute(
                "UPDATE tasks SET status=?, attempts=?, next_retry_at=?,"
                " last_error=?, lease_owner=NULL, lease_until=NULL,"
                " updated_at=? WHERE task_id=?",
                (STATUS_PENDING, attempts, now + delay, error, now, task_id),
            )
            self.conn.commit()
            return "retry"
        except BaseException:
            self.conn.rollback()
            raise

    def counts(self):
        rows = self.conn.execute(
            "SELECT status, COUNT(*) c FROM tasks GROUP BY status").fetchall()
        out = {STATUS_PENDING: 0, STATUS_LEASED: 0,
               STATUS_DONE: 0, STATUS_FAILED: 0}
        for row in rows:
            out[row["status"]] = row["c"]
        return out

    def all_finished(self) -> bool:
        """True only when nothing is pending or leased anywhere."""
        row = self.conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status IN (?,?)",
            (STATUS_PENDING, STATUS_LEASED),
        ).fetchone()
        return row[0] == 0

    def failed_tasks(self):
        return self.conn.execute(
            "SELECT task_id, url, attempts, last_error FROM tasks"
            " WHERE status=? ORDER BY url",
            (STATUS_FAILED,),
        ).fetchall()

    def missing_results(self):
        """Tasks marked done but lacking a result row (must never happen)."""
        return self.conn.execute(
            "SELECT t.task_id, t.url FROM tasks t"
            " LEFT JOIN results r ON r.task_id = t.task_id"
            " WHERE t.status=? AND r.task_id IS NULL",
            (STATUS_DONE,),
        ).fetchall()

    def all_tasks(self):
        return self.conn.execute(
            "SELECT task_id, url, kind, status, attempts, last_error,"
            " lease_owner FROM tasks ORDER BY created_at, url").fetchall()

    def all_results(self):
        return self.conn.execute(
            "SELECT task_id, url, kind, data FROM results ORDER BY url"
        ).fetchall()

    def get_task_by_url(self, url):
        return self.conn.execute(
            "SELECT * FROM tasks WHERE url=?", (url,)).fetchone()

"""SQLite manifest schema and CRUD operations."""

import sqlite3
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime, timezone

log = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS files (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    rel_path     TEXT NOT NULL,
    data_root    TEXT NOT NULL,
    file_size    INTEGER NOT NULL,
    mtime_ns     INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    par2_name    TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'ok',
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at   TEXT NOT NULL DEFAULT (datetime('now')),
    verified_at  TEXT,
    UNIQUE(data_root, rel_path)
);
CREATE INDEX IF NOT EXISTS idx_content_hash ON files(content_hash);
CREATE INDEX IF NOT EXISTS idx_par2_name ON files(par2_name);

CREATE TABLE IF NOT EXISTS runs (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at            TEXT NOT NULL,
    finished_at           TEXT,
    files_scanned         INTEGER DEFAULT 0,
    files_created         INTEGER DEFAULT 0,
    files_verified        INTEGER DEFAULT 0,
    files_damaged         INTEGER DEFAULT 0,
    files_repaired        INTEGER DEFAULT 0,
    files_moved           INTEGER DEFAULT 0,
    files_deleted         INTEGER DEFAULT 0,
    files_truncated       INTEGER DEFAULT 0,
    parity_recreated      INTEGER DEFAULT 0,
    orphan_parity_cleaned INTEGER DEFAULT 0,
    errors                TEXT
);
"""


class Manifest:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._in_transaction = False
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript(SCHEMA_SQL)
        # Migrate existing databases that lack newer columns
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(runs)").fetchall()}
        for col in ("files_truncated", "parity_recreated", "orphan_parity_cleaned"):
            if col not in cols:
                self.conn.execute(f"ALTER TABLE runs ADD COLUMN {col} INTEGER DEFAULT 0")
        self.conn.commit()

    def close(self):
        self.conn.close()

    def _commit(self):
        """Commit unless inside a transaction() block."""
        if not self._in_transaction:
            self.conn.commit()

    @contextmanager
    def transaction(self):
        """Batch multiple writes into a single transaction for performance."""
        if self._in_transaction:
            yield self
            return
        self._in_transaction = True
        try:
            yield self
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        finally:
            self._in_transaction = False

    # -- File operations --

    def get_file(self, data_root: str, rel_path: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM files WHERE data_root = ? AND rel_path = ?",
            (data_root, rel_path),
        ).fetchone()
        return dict(row) if row else None

    def get_all_files(self, data_root: str | None = None) -> list[dict]:
        if data_root:
            rows = self.conn.execute(
                "SELECT * FROM files WHERE data_root = ?", (data_root,)
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM files").fetchall()
        return [dict(r) for r in rows]

    def iter_all_files(self, data_root: str | None = None) -> Iterator[dict]:
        """Yield file rows one at a time via cursor to avoid loading all into memory."""
        if data_root:
            cursor = self.conn.execute(
                "SELECT * FROM files WHERE data_root = ?", (data_root,)
            )
        else:
            cursor = self.conn.execute("SELECT * FROM files")
        for row in cursor:
            yield dict(row)

    def get_files_by_status(self, *statuses: str) -> list[dict]:
        placeholders = ",".join("?" * len(statuses))
        rows = self.conn.execute(
            f"SELECT * FROM files WHERE status IN ({placeholders})", statuses
        ).fetchall()
        return [dict(r) for r in rows]

    def get_files_by_hash(self, content_hash: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM files WHERE content_hash = ?", (content_hash,)
        ).fetchall()
        return [dict(r) for r in rows]

    def has_par2_name(self, par2_name: str) -> bool:
        """Check if any file entry references the given par2_name."""
        row = self.conn.execute(
            "SELECT 1 FROM files WHERE par2_name = ? LIMIT 1", (par2_name,)
        ).fetchone()
        return row is not None

    def upsert_file(self, data_root: str, rel_path: str, file_size: int,
                    mtime_ns: int, content_hash: str, par2_name: str,
                    status: str = "ok"):
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """INSERT INTO files (data_root, rel_path, file_size, mtime_ns,
                                  content_hash, par2_name, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(data_root, rel_path) DO UPDATE SET
                   file_size = excluded.file_size,
                   mtime_ns = excluded.mtime_ns,
                   content_hash = excluded.content_hash,
                   par2_name = excluded.par2_name,
                   status = excluded.status,
                   updated_at = excluded.updated_at
            """,
            (data_root, rel_path, file_size, mtime_ns, content_hash, par2_name, status, now, now),
        )
        self._commit()

    def update_path(self, file_id: int, new_rel_path: str, new_data_root: str):
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE files SET rel_path = ?, data_root = ?, updated_at = ? WHERE id = ?",
            (new_rel_path, new_data_root, now, file_id),
        )
        self._commit()

    def update_mtime(self, file_id: int, mtime_ns: int):
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE files SET mtime_ns = ?, updated_at = ? WHERE id = ?",
            (mtime_ns, now, file_id),
        )
        self._commit()

    def update_status(self, file_id: int, status: str):
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE files SET status = ?, updated_at = ? WHERE id = ?",
            (status, now, file_id),
        )
        self._commit()

    def mark_verified(self, file_id: int):
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE files SET verified_at = ?, updated_at = ? WHERE id = ?",
            (now, now, file_id),
        )
        self._commit()

    def delete_file(self, file_id: int):
        self.conn.execute("DELETE FROM files WHERE id = ?", (file_id,))
        self._commit()

    # -- Run operations --

    def start_run(self) -> int:
        now = datetime.now(timezone.utc).isoformat()
        cur = self.conn.execute(
            "INSERT INTO runs (started_at) VALUES (?)", (now,)
        )
        self._commit()
        return cur.lastrowid

    def finish_run(self, run_id: int, stats: dict):
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE runs SET
                finished_at = ?, files_scanned = ?, files_created = ?,
                files_verified = ?, files_damaged = ?, files_repaired = ?,
                files_moved = ?, files_deleted = ?, files_truncated = ?,
                parity_recreated = ?, orphan_parity_cleaned = ?,
                errors = ?
               WHERE id = ?""",
            (
                now,
                stats.get("files_scanned", 0),
                stats.get("files_created", 0),
                stats.get("files_verified", 0),
                stats.get("files_damaged", 0),
                stats.get("files_repaired", 0),
                stats.get("files_moved", 0),
                stats.get("files_deleted", 0),
                stats.get("files_truncated", 0),
                stats.get("parity_recreated", 0),
                stats.get("orphan_parity_cleaned", 0),
                stats.get("errors"),
                run_id,
            ),
        )
        self._commit()

    def get_last_run(self) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

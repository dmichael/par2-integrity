"""Tests for manifest.py — SQLite schema and CRUD operations."""

import tempfile
import unittest
from pathlib import Path

from par2integrity.manifest import Manifest


class TestManifest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = Path(self.tmpdir) / "test.db"
        self.m = Manifest(self.db_path)

    def tearDown(self):
        self.m.close()

    def test_schema_creates_tables(self):
        # Verify both tables exist
        rows = self.m.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        names = [r["name"] for r in rows]
        self.assertIn("files", names)
        self.assertIn("runs", names)

    def test_upsert_and_get_file(self):
        self.m.upsert_file("photos", "2024/img.jpg", 10000, 1700000000000000000,
                           "aabb" * 16, "aabb1234567890ab.par2")
        rec = self.m.get_file("photos", "2024/img.jpg")
        self.assertIsNotNone(rec)
        self.assertEqual(rec["data_root"], "photos")
        self.assertEqual(rec["rel_path"], "2024/img.jpg")
        self.assertEqual(rec["file_size"], 10000)
        self.assertEqual(rec["content_hash"], "aabb" * 16)
        self.assertEqual(rec["status"], "ok")

    def test_upsert_updates_existing(self):
        self.m.upsert_file("photos", "img.jpg", 100, 111, "hash1" + "0" * 59, "h1.par2")
        self.m.upsert_file("photos", "img.jpg", 200, 222, "hash2" + "0" * 59, "h2.par2")
        rec = self.m.get_file("photos", "img.jpg")
        self.assertEqual(rec["file_size"], 200)
        self.assertEqual(rec["mtime_ns"], 222)
        self.assertEqual(rec["content_hash"], "hash2" + "0" * 59)

    def test_get_file_missing(self):
        rec = self.m.get_file("photos", "nonexistent.jpg")
        self.assertIsNone(rec)

    def test_get_all_files(self):
        self.m.upsert_file("photos", "a.jpg", 100, 1, "h1" + "0" * 62, "p1.par2")
        self.m.upsert_file("photos", "b.jpg", 200, 2, "h2" + "0" * 62, "p2.par2")
        self.m.upsert_file("docs", "c.pdf", 300, 3, "h3" + "0" * 62, "p3.par2")

        all_files = self.m.get_all_files()
        self.assertEqual(len(all_files), 3)

        photos_only = self.m.get_all_files(data_root="photos")
        self.assertEqual(len(photos_only), 2)

    def test_get_files_by_hash(self):
        shared_hash = "deadbeef" * 8
        self.m.upsert_file("photos", "a.jpg", 100, 1, shared_hash, "p1.par2")
        self.m.upsert_file("docs", "copy.jpg", 100, 2, shared_hash, "p1.par2")
        self.m.upsert_file("photos", "b.jpg", 200, 3, "other" + "0" * 59, "p2.par2")

        matches = self.m.get_files_by_hash(shared_hash)
        self.assertEqual(len(matches), 2)

    def test_update_path(self):
        self.m.upsert_file("photos", "old/path.jpg", 100, 1, "h" * 64, "p.par2")
        rec = self.m.get_file("photos", "old/path.jpg")
        self.m.update_path(rec["id"], "new/path.jpg", "photos")
        # Old path gone
        self.assertIsNone(self.m.get_file("photos", "old/path.jpg"))
        # New path present
        moved = self.m.get_file("photos", "new/path.jpg")
        self.assertIsNotNone(moved)
        self.assertEqual(moved["id"], rec["id"])

    def test_update_mtime(self):
        self.m.upsert_file("photos", "a.jpg", 100, 111, "h" * 64, "p.par2")
        rec = self.m.get_file("photos", "a.jpg")
        self.m.update_mtime(rec["id"], 999)
        updated = self.m.get_file("photos", "a.jpg")
        self.assertEqual(updated["mtime_ns"], 999)

    def test_update_status(self):
        self.m.upsert_file("photos", "a.jpg", 100, 1, "h" * 64, "p.par2")
        rec = self.m.get_file("photos", "a.jpg")
        self.assertEqual(rec["status"], "ok")
        self.m.update_status(rec["id"], "damaged")
        updated = self.m.get_file("photos", "a.jpg")
        self.assertEqual(updated["status"], "damaged")

    def test_mark_verified(self):
        self.m.upsert_file("photos", "a.jpg", 100, 1, "h" * 64, "p.par2")
        rec = self.m.get_file("photos", "a.jpg")
        self.assertIsNone(rec["verified_at"])
        self.m.mark_verified(rec["id"])
        updated = self.m.get_file("photos", "a.jpg")
        self.assertIsNotNone(updated["verified_at"])

    def test_delete_file(self):
        self.m.upsert_file("photos", "a.jpg", 100, 1, "h" * 64, "p.par2")
        rec = self.m.get_file("photos", "a.jpg")
        self.m.delete_file(rec["id"])
        self.assertIsNone(self.m.get_file("photos", "a.jpg"))

    def test_run_lifecycle(self):
        run_id = self.m.start_run()
        self.assertIsInstance(run_id, int)

        self.m.finish_run(run_id, {
            "files_scanned": 100,
            "files_created": 5,
            "files_verified": 90,
            "files_damaged": 1,
            "files_repaired": 0,
            "files_moved": 2,
            "files_deleted": 3,
            "errors": "some error",
        })

        last = self.m.get_last_run()
        self.assertIsNotNone(last)
        self.assertEqual(last["id"], run_id)
        self.assertEqual(last["files_scanned"], 100)
        self.assertEqual(last["files_damaged"], 1)
        self.assertIsNotNone(last["finished_at"])

    def test_get_last_run_empty(self):
        self.assertIsNone(self.m.get_last_run())

    def test_unique_constraint(self):
        """Upsert with same data_root+rel_path should update, not duplicate."""
        self.m.upsert_file("photos", "a.jpg", 100, 1, "h1" + "0" * 62, "p1.par2")
        self.m.upsert_file("photos", "a.jpg", 200, 2, "h2" + "0" * 62, "p2.par2")
        all_files = self.m.get_all_files()
        self.assertEqual(len(all_files), 1)


if __name__ == "__main__":
    unittest.main()

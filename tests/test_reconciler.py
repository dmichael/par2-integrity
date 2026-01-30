"""Tests for reconciler.py — change detection, move matching, action decisions.

All par2 operations are mocked since we're testing classification logic,
not the par2 binary itself.
"""

import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from par2integrity.config import Config
from par2integrity.manifest import Manifest
from par2integrity.reconciler import reconcile
from par2integrity.scanner import FileInfo


def _hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


HASH_A = _hash(b"content_a")
HASH_B = _hash(b"content_b")


class ReconcilerTestBase(unittest.TestCase):
    """Base class that sets up a temp dir, config, and manifest."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.parity_dir = os.path.join(self.tmpdir, "parity")
        os.makedirs(self.parity_dir)

        self._saved_env = {}
        for key in ("DATA_ROOT", "PARITY_ROOT", "MIN_FILE_SIZE",
                     "EXCLUDE_PATTERNS", "VERIFY_PERCENT"):
            self._saved_env[key] = os.environ.pop(key, None)

        os.environ["DATA_ROOT"] = os.path.join(self.tmpdir, "data")
        os.environ["PARITY_ROOT"] = self.parity_dir
        os.environ["MIN_FILE_SIZE"] = "0"
        os.environ["EXCLUDE_PATTERNS"] = ""
        os.environ["VERIFY_PERCENT"] = "100"

        self.config = Config()
        self.manifest = Manifest(self.config.db_path)

    def tearDown(self):
        self.manifest.close()
        for key, val in self._saved_env.items():
            if val is not None:
                os.environ[key] = val
            else:
                os.environ.pop(key, None)

    def _make_file_info(self, data_root: str, rel_path: str,
                        size: int = 100, mtime_ns: int = 1000) -> FileInfo:
        abs_path = self.config.data_root / data_root / rel_path
        return FileInfo(abs_path=abs_path, data_root=data_root,
                        rel_path=rel_path, size=size, mtime_ns=mtime_ns)


class TestNewFiles(ReconcilerTestBase):
    @patch("par2integrity.reconciler.create_parity", return_value=True)
    @patch("par2integrity.reconciler.sha256_file", return_value=HASH_A)
    def test_new_file_creates_parity(self, mock_hash, mock_create):
        fi = self._make_file_info("photos", "img.jpg")
        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.files_scanned, 1)
        self.assertEqual(stats.files_created, 1)
        mock_create.assert_called_once()

        # File should be in manifest now
        rec = self.manifest.get_file("photos", "img.jpg")
        self.assertIsNotNone(rec)
        self.assertEqual(rec["content_hash"], HASH_A)

    @patch("par2integrity.reconciler.create_parity", return_value=False)
    @patch("par2integrity.reconciler.sha256_file", return_value=HASH_A)
    def test_new_file_parity_failure_records_error(self, mock_hash, mock_create):
        fi = self._make_file_info("photos", "img.jpg")
        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.files_created, 0)
        self.assertEqual(len(stats.errors), 1)
        self.assertIn("parity create failed", stats.errors[0])


class TestUnchangedFiles(ReconcilerTestBase):
    @patch("par2integrity.reconciler.verify_parity", return_value="ok")
    def test_unchanged_file_verified(self, mock_verify):
        # Pre-populate manifest
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "par2name.par2")
        fi = self._make_file_info("photos", "img.jpg", size=100, mtime_ns=1000)
        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.files_verified, 1)
        self.assertEqual(stats.files_damaged, 0)
        mock_verify.assert_called_once()

    @patch("par2integrity.reconciler.verify_parity", return_value="damaged")
    def test_unchanged_file_damaged(self, mock_verify):
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "par2name.par2")
        fi = self._make_file_info("photos", "img.jpg", size=100, mtime_ns=1000)
        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.files_damaged, 1)
        rec = self.manifest.get_file("photos", "img.jpg")
        self.assertEqual(rec["status"], "damaged")


class TestModifiedFiles(ReconcilerTestBase):
    @patch("par2integrity.reconciler.delete_parity")
    @patch("par2integrity.reconciler.create_parity", return_value=True)
    @patch("par2integrity.reconciler.sha256_file", return_value=HASH_B)
    def test_modified_file_regenerates_parity(self, mock_hash, mock_create, mock_delete):
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "old.par2")
        # Same path, different size/mtime
        fi = self._make_file_info("photos", "img.jpg", size=200, mtime_ns=2000)
        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.files_created, 1)
        mock_delete.assert_called_once_with(self.config, HASH_A)
        mock_create.assert_called_once()

        rec = self.manifest.get_file("photos", "img.jpg")
        self.assertEqual(rec["content_hash"], HASH_B)
        self.assertEqual(rec["file_size"], 200)


class TestTouchedFiles(ReconcilerTestBase):
    @patch("par2integrity.reconciler.sha256_file", return_value=HASH_A)
    def test_touched_file_updates_mtime_only(self, mock_hash):
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "p.par2")
        # Same size, different mtime, but hash will match
        fi = self._make_file_info("photos", "img.jpg", size=100, mtime_ns=2000)
        stats = reconcile(self.config, self.manifest, [fi])

        # No parity creation — just mtime update
        self.assertEqual(stats.files_created, 0)
        rec = self.manifest.get_file("photos", "img.jpg")
        self.assertEqual(rec["mtime_ns"], 2000)
        self.assertEqual(rec["content_hash"], HASH_A)


class TestMoveDetection(ReconcilerTestBase):
    @patch("par2integrity.reconciler.sha256_file", return_value=HASH_A)
    def test_move_detected_by_hash(self, mock_hash):
        # Old path in manifest, not on disk
        self.manifest.upsert_file("photos", "old/img.jpg", 100, 1000,
                                  HASH_A, "p.par2")
        # New path on disk, same hash
        fi = self._make_file_info("photos", "new/img.jpg", size=100, mtime_ns=2000)
        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.files_moved, 1)
        self.assertEqual(stats.files_created, 0)
        self.assertEqual(stats.files_deleted, 0)

        # Old path should be gone, new path should exist
        self.assertIsNone(self.manifest.get_file("photos", "old/img.jpg"))
        moved = self.manifest.get_file("photos", "new/img.jpg")
        self.assertIsNotNone(moved)
        self.assertEqual(moved["content_hash"], HASH_A)

    @patch("par2integrity.reconciler.sha256_file", return_value=HASH_A)
    def test_cross_root_move(self, mock_hash):
        """File moved from one data root to another."""
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "p.par2")
        fi = self._make_file_info("documents", "img.jpg", size=100, mtime_ns=2000)
        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.files_moved, 1)
        self.assertIsNone(self.manifest.get_file("photos", "img.jpg"))
        moved = self.manifest.get_file("documents", "img.jpg")
        self.assertIsNotNone(moved)


class TestDeletion(ReconcilerTestBase):
    @patch("par2integrity.reconciler.delete_parity")
    def test_disappeared_file_cleaned_up(self, mock_delete):
        self.manifest.upsert_file("photos", "gone.jpg", 100, 1000,
                                  HASH_A, "p.par2")
        # Empty scan — file is gone
        stats = reconcile(self.config, self.manifest, [])

        self.assertEqual(stats.files_deleted, 1)
        mock_delete.assert_called_once_with(self.config, HASH_A)
        self.assertIsNone(self.manifest.get_file("photos", "gone.jpg"))

    @patch("par2integrity.reconciler.delete_parity")
    def test_shared_hash_parity_preserved(self, mock_delete):
        """If another file shares the hash, parity should NOT be deleted."""
        self.manifest.upsert_file("photos", "gone.jpg", 100, 1000,
                                  HASH_A, "p.par2")
        self.manifest.upsert_file("photos", "still_here.jpg", 100, 1000,
                                  HASH_A, "p.par2")

        fi = self._make_file_info("photos", "still_here.jpg", size=100, mtime_ns=1000)

        with patch("par2integrity.reconciler.verify_parity", return_value="ok"):
            stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.files_deleted, 1)
        # Parity should NOT be deleted because still_here.jpg uses the same hash
        mock_delete.assert_not_called()


class TestVerifyOnly(ReconcilerTestBase):
    @patch("par2integrity.reconciler.verify_parity", return_value="ok")
    @patch("par2integrity.reconciler.sha256_file", return_value=HASH_A)
    def test_verify_only_no_creates(self, mock_hash, mock_verify):
        """verify_only mode should not create parity or detect deletions."""
        # New file on disk
        fi = self._make_file_info("photos", "new.jpg")
        stats = reconcile(self.config, self.manifest, [fi], verify_only=True)

        self.assertEqual(stats.files_created, 0)
        self.assertEqual(stats.files_deleted, 0)
        # File should NOT be in manifest
        self.assertIsNone(self.manifest.get_file("photos", "new.jpg"))


class TestVerifyPercent(ReconcilerTestBase):
    @patch("par2integrity.reconciler.verify_parity", return_value="ok")
    def test_sampling_limits_verifications(self, mock_verify):
        os.environ["VERIFY_PERCENT"] = "50"
        self.config = Config()

        # Add 10 unchanged files
        files = []
        for i in range(10):
            name = f"img_{i}.jpg"
            self.manifest.upsert_file("photos", name, 100, 1000,
                                      f"{i:064x}", "p.par2")
            files.append(self._make_file_info("photos", name, size=100, mtime_ns=1000))

        stats = reconcile(self.config, self.manifest, files)

        # With 50%, should verify ~5 files (sampling is random but bounded)
        self.assertEqual(stats.files_verified, 5)


if __name__ == "__main__":
    unittest.main()

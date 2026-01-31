"""Tests for reconciler.py — change detection, move matching, action decisions.

All par2 operations are mocked since we're testing classification logic,
not the par2 binary itself.
"""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from par2integrity.config import Config
from par2integrity.manifest import Manifest
from par2integrity.reconciler import reconcile
from par2integrity.scanner import FileInfo
from tests.helpers import HASH_A, HASH_B, EnvSnapshot


class ReconcilerTestBase(unittest.TestCase):
    """Base class that sets up a temp dir, config, and manifest."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.parity_dir = os.path.join(self.tmpdir, "parity")
        os.makedirs(self.parity_dir)

        self._env = EnvSnapshot(["DATA_ROOT", "PARITY_ROOT", "MIN_FILE_SIZE",
                                 "EXCLUDE_PATTERNS", "VERIFY_PERCENT"])

        os.environ["DATA_ROOT"] = os.path.join(self.tmpdir, "data")
        os.environ["PARITY_ROOT"] = self.parity_dir
        os.environ["MIN_FILE_SIZE"] = "0"
        os.environ["EXCLUDE_PATTERNS"] = ""
        os.environ["VERIFY_PERCENT"] = "100"

        self.config = Config()
        self.manifest = Manifest(self.config.db_path)

    def tearDown(self):
        self.manifest.close()
        self._env.restore()

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

    @patch("par2integrity.reconciler.sha256_file", return_value=HASH_B)
    @patch("par2integrity.reconciler.verify_parity", return_value="damaged")
    def test_unchanged_file_damaged(self, mock_verify, mock_hash):
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


class TestModifiedParityFailure(ReconcilerTestBase):
    @patch("par2integrity.reconciler.delete_parity")
    @patch("par2integrity.reconciler.create_parity", return_value=False)
    @patch("par2integrity.reconciler.sha256_file", return_value=HASH_B)
    def test_failed_create_preserves_old_parity(self, mock_hash, mock_create, mock_delete):
        """If new parity creation fails, old parity must NOT be deleted."""
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "old.par2")
        fi = self._make_file_info("photos", "img.jpg", size=200, mtime_ns=2000)
        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.files_created, 0)
        self.assertEqual(len(stats.errors), 1)
        # Old parity must not have been deleted
        mock_delete.assert_not_called()
        # Manifest should still have the old hash so the file is retried next scan
        rec = self.manifest.get_file("photos", "img.jpg")
        self.assertEqual(rec["content_hash"], HASH_A)
        self.assertEqual(rec["file_size"], 100)

    @patch("par2integrity.reconciler.delete_parity")
    @patch("par2integrity.reconciler.create_parity", return_value=True)
    @patch("par2integrity.reconciler.sha256_file", return_value=HASH_B)
    def test_successful_create_deletes_old_parity(self, mock_hash, mock_create, mock_delete):
        """Old parity should only be deleted after new parity succeeds."""
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "old.par2")
        fi = self._make_file_info("photos", "img.jpg", size=200, mtime_ns=2000)
        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.files_created, 1)
        mock_delete.assert_called_once_with(self.config, HASH_A)
        rec = self.manifest.get_file("photos", "img.jpg")
        self.assertEqual(rec["content_hash"], HASH_B)


class TestMoveDetectionPreference(ReconcilerTestBase):
    @patch("par2integrity.reconciler.delete_parity")
    @patch("par2integrity.reconciler.sha256_file", return_value=HASH_A)
    def test_prefers_same_data_root(self, mock_hash, mock_delete):
        """Move detection should prefer a candidate in the same data_root."""
        # Two copies of same content in different roots, both will disappear
        self.manifest.upsert_file("documents", "old.jpg", 100, 1000,
                                  HASH_A, "p.par2")
        self.manifest.upsert_file("photos", "old.jpg", 100, 1000,
                                  HASH_A, "p.par2")
        # New path appears in photos — should match the photos candidate
        fi = self._make_file_info("photos", "new.jpg", size=100, mtime_ns=2000)
        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.files_moved, 1)
        # The photos/old.jpg entry should have been updated to photos/new.jpg
        moved = self.manifest.get_file("photos", "new.jpg")
        self.assertIsNotNone(moved)
        # photos/old.jpg should be gone (it was the one that moved)
        self.assertIsNone(self.manifest.get_file("photos", "old.jpg"))
        # documents/old.jpg is cleaned up as a deletion in Phase 4
        self.assertIsNone(self.manifest.get_file("documents", "old.jpg"))
        self.assertEqual(stats.files_deleted, 1)


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


class TestTruncationDetection(ReconcilerTestBase):
    @patch("par2integrity.reconciler.delete_parity")
    def test_truncated_file_preserved_not_deleted(self, mock_delete):
        """File exists on disk as 0 bytes, not in scan results.
        Should be marked truncated, not deleted. Parity preserved."""
        # Create the physical file (0 bytes — below MIN_FILE_SIZE)
        data_dir = Path(self.config.data_root) / "photos"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "zeroed.jpg").write_bytes(b"")

        # Pre-populate manifest as if this file was previously tracked
        self.manifest.upsert_file("photos", "zeroed.jpg", 100, 1000,
                                  HASH_A, "p.par2")

        # Empty scan — file exists on disk but filtered out by scanner
        stats = reconcile(self.config, self.manifest, [])

        self.assertEqual(stats.files_truncated, 1)
        self.assertEqual(stats.files_deleted, 0)
        mock_delete.assert_not_called()

        rec = self.manifest.get_file("photos", "zeroed.jpg")
        self.assertIsNotNone(rec)
        self.assertEqual(rec["status"], "truncated")

    @patch("par2integrity.reconciler.delete_parity")
    def test_truly_deleted_file_still_cleaned_up(self, mock_delete):
        """File does not exist on disk. Should be deleted as before."""
        self.manifest.upsert_file("photos", "gone.jpg", 100, 1000,
                                  HASH_A, "p.par2")

        # Empty scan — file is truly gone (no physical file)
        stats = reconcile(self.config, self.manifest, [])

        self.assertEqual(stats.files_deleted, 1)
        self.assertEqual(stats.files_truncated, 0)
        mock_delete.assert_called_once_with(self.config, HASH_A)
        self.assertIsNone(self.manifest.get_file("photos", "gone.jpg"))

    @patch("par2integrity.reconciler.delete_parity")
    def test_excluded_directory_cleaned_up(self, mock_delete):
        """File inside a newly-excluded directory should be deleted from manifest."""
        os.environ["EXCLUDE_PATTERNS"] = "#recycle"
        self.config = Config()

        # Create the physical file inside an excluded directory
        data_dir = Path(self.config.data_root) / "photos" / "#recycle"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "old.jpg").write_bytes(b"x" * 100)

        # Pre-populate manifest as if tracked before #recycle was excluded
        self.manifest.upsert_file("photos", "#recycle/old.jpg", 100, 1000,
                                  HASH_A, "p.par2")

        stats = reconcile(self.config, self.manifest, [])

        self.assertEqual(stats.files_deleted, 1)
        self.assertEqual(stats.files_truncated, 0)
        self.assertIsNone(self.manifest.get_file("photos", "#recycle/old.jpg"))

    @patch("par2integrity.reconciler.delete_parity")
    def test_excluded_data_root_cleaned_up(self, mock_delete):
        """Top-level data root matching exclude pattern should be cleaned from manifest."""
        os.environ["EXCLUDE_PATTERNS"] = "#recycle"
        self.config = Config()

        # Create the physical file inside the excluded data root
        data_dir = Path(self.config.data_root) / "#recycle"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "old.jpg").write_bytes(b"x" * 100)

        # Pre-populate manifest as if tracked before #recycle was excluded
        self.manifest.upsert_file("#recycle", "old.jpg", 100, 1000,
                                  HASH_A, "p.par2")

        stats = reconcile(self.config, self.manifest, [])

        self.assertEqual(stats.files_deleted, 1)
        self.assertEqual(stats.files_truncated, 0)
        self.assertIsNone(self.manifest.get_file("#recycle", "old.jpg"))


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


class TestMissingParityRecreation(ReconcilerTestBase):
    @patch("par2integrity.reconciler.create_parity", return_value=True)
    @patch("par2integrity.reconciler.verify_parity", return_value="missing_parity")
    @patch("par2integrity.reconciler.sha256_file", return_value=HASH_A)
    def test_missing_parity_recreated(self, mock_hash, mock_verify, mock_create):
        """When parity is missing and hash matches, parity should be re-created."""
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "par2name.par2")
        fi = self._make_file_info("photos", "img.jpg", size=100, mtime_ns=1000)
        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.files_verified, 1)
        self.assertEqual(stats.parity_recreated, 1)
        self.assertEqual(stats.files_damaged, 0)
        mock_create.assert_called_once()
        rec = self.manifest.get_file("photos", "img.jpg")
        self.assertEqual(rec["status"], "ok")

    @patch("par2integrity.reconciler.delete_parity")
    @patch("par2integrity.reconciler.create_parity", return_value=True)
    @patch("par2integrity.reconciler.verify_parity", return_value="missing_parity")
    @patch("par2integrity.reconciler.sha256_file", return_value=HASH_B)
    def test_missing_parity_hash_mismatch(self, mock_hash, mock_verify,
                                          mock_create, mock_delete):
        """File changed without mtime update — parity created for new hash."""
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "par2name.par2")
        fi = self._make_file_info("photos", "img.jpg", size=100, mtime_ns=1000)
        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.parity_recreated, 1)
        mock_create.assert_called_once()
        mock_delete.assert_called_once_with(self.config, HASH_A)
        rec = self.manifest.get_file("photos", "img.jpg")
        self.assertEqual(rec["content_hash"], HASH_B)

    @patch("par2integrity.reconciler.verify_parity", return_value="missing_parity")
    def test_missing_parity_verify_only_logs_error(self, mock_verify):
        """In verify_only mode, missing parity should log a specific error."""
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "par2name.par2")
        fi = self._make_file_info("photos", "img.jpg", size=100, mtime_ns=1000)
        stats = reconcile(self.config, self.manifest, [fi], verify_only=True)

        self.assertEqual(stats.files_verified, 1)
        self.assertEqual(len(stats.errors), 1)
        self.assertIn("missing parity", stats.errors[0])


class TestOrphanParityCleanup(ReconcilerTestBase):
    @patch("par2integrity.reconciler.verify_parity", return_value="ok")
    def test_orphan_parity_cleaned_up(self, mock_verify):
        """Par2 files with no manifest entry should be removed after scan."""
        # Create orphan par2 files on disk
        orphan_hash = "ab" + "0" * 62
        orphan_dir = self.config.hash_dir / "ab"
        orphan_dir.mkdir(parents=True, exist_ok=True)
        (orphan_dir / f"{orphan_hash[:16]}.par2").write_bytes(b"fake")
        (orphan_dir / f"{orphan_hash[:16]}.vol000+01.par2").write_bytes(b"fake")

        # Add a real file so the scan isn't empty
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, self.config.par2_name_for_hash(HASH_A))
        fi = self._make_file_info("photos", "img.jpg", size=100, mtime_ns=1000)

        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.orphan_parity_cleaned, 1)
        self.assertFalse((orphan_dir / f"{orphan_hash[:16]}.par2").exists())
        self.assertFalse((orphan_dir / f"{orphan_hash[:16]}.vol000+01.par2").exists())

    @patch("par2integrity.reconciler.verify_parity", return_value="ok")
    def test_referenced_parity_preserved(self, mock_verify):
        """Par2 files with manifest entries should NOT be removed."""
        par2_name = self.config.par2_name_for_hash(HASH_A)
        par2_dir = self.config.par2_dir_for_hash(HASH_A)
        par2_dir.mkdir(parents=True, exist_ok=True)
        (par2_dir / par2_name).write_bytes(b"fake")

        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, par2_name)
        fi = self._make_file_info("photos", "img.jpg", size=100, mtime_ns=1000)

        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.orphan_parity_cleaned, 0)
        self.assertTrue((par2_dir / par2_name).exists())


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


class TestDamagedHashFallback(ReconcilerTestBase):
    """Phase 3 hash-check fallback: par2 filename mismatch causes false
    'damaged' results for deduped/renamed files. The reconciler now hashes
    the file to confirm before marking it damaged."""

    @patch("par2integrity.reconciler.sha256_file", return_value=HASH_A)
    @patch("par2integrity.reconciler.verify_parity", return_value="damaged")
    def test_damaged_false_positive_corrected_by_hash(self, mock_verify, mock_hash):
        """verify returns 'damaged' but hash matches manifest → not damaged."""
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "par2name.par2")
        fi = self._make_file_info("photos", "img.jpg", size=100, mtime_ns=1000)
        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.files_verified, 1)
        self.assertEqual(stats.files_damaged, 0)
        rec = self.manifest.get_file("photos", "img.jpg")
        self.assertEqual(rec["status"], "ok")

    @patch("par2integrity.reconciler.sha256_file", return_value=HASH_B)
    @patch("par2integrity.reconciler.verify_parity", return_value="damaged")
    def test_damaged_confirmed_by_hash_mismatch(self, mock_verify, mock_hash):
        """verify returns 'damaged' and hash doesn't match → genuinely damaged."""
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "par2name.par2")
        fi = self._make_file_info("photos", "img.jpg", size=100, mtime_ns=1000)
        stats = reconcile(self.config, self.manifest, [fi])

        self.assertEqual(stats.files_verified, 1)
        self.assertEqual(stats.files_damaged, 1)
        rec = self.manifest.get_file("photos", "img.jpg")
        self.assertEqual(rec["status"], "damaged")


if __name__ == "__main__":
    unittest.main()

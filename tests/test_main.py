"""Tests for main.py — repair command safety checks."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from par2integrity.config import Config
from par2integrity.manifest import Manifest
from par2integrity.main import cmd_repair
from tests.helpers import HASH_A, HASH_B, EnvSnapshot


class RepairTestBase(unittest.TestCase):
    """Base class that sets up a temp dir, config, manifest, and a real file on disk."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.parity_dir = os.path.join(self.tmpdir, "parity")
        os.makedirs(self.parity_dir)

        self._env = EnvSnapshot(["DATA_ROOT", "PARITY_ROOT", "MIN_FILE_SIZE",
                                 "EXCLUDE_PATTERNS", "VERIFY_PERCENT"])

        self.data_dir = os.path.join(self.tmpdir, "data")
        os.environ["DATA_ROOT"] = self.data_dir
        os.environ["PARITY_ROOT"] = self.parity_dir
        os.environ["MIN_FILE_SIZE"] = "0"
        os.environ["EXCLUDE_PATTERNS"] = ""
        os.environ["VERIFY_PERCENT"] = "100"

        self.config = Config()
        self.manifest = Manifest(self.config.db_path)

        # Create a physical file for repair to find
        self.file_dir = Path(self.data_dir) / "photos"
        self.file_dir.mkdir(parents=True, exist_ok=True)
        self.file_path = self.file_dir / "img.jpg"
        self.file_path.write_bytes(b"content_a")

    def tearDown(self):
        self.manifest.close()
        self._env.restore()


class TestRepairHashCheck(RepairTestBase):
    @patch("par2integrity.main.create_parity", return_value=True)
    @patch("par2integrity.main.delete_parity")
    @patch("par2integrity.main.repair_file")
    @patch("par2integrity.main.sha256_file", return_value=HASH_A)
    def test_repair_skips_when_file_matches_hash(self, mock_hash, mock_repair,
                                                  mock_delete, mock_create):
        """File is fine, parity is bad — repair should be skipped, parity re-created."""
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "p.par2", status="damaged")

        result = cmd_repair(self.config, self.manifest)

        # repair_file should NOT have been called
        mock_repair.assert_not_called()
        # Parity should be deleted and re-created
        mock_delete.assert_called_once_with(self.config, HASH_A)
        mock_create.assert_called_once_with(self.config, self.file_path, HASH_A)
        # Status should be ok
        rec = self.manifest.get_file("photos", "img.jpg")
        self.assertEqual(rec["status"], "ok")
        self.assertEqual(result, 0)

    @patch("par2integrity.main.verify_parity", return_value="ok")
    @patch("par2integrity.main.repair_file", return_value=True)
    @patch("par2integrity.main.sha256_file", return_value=HASH_B)
    def test_repair_proceeds_when_file_differs(self, mock_hash, mock_repair,
                                                mock_verify):
        """File is actually damaged — repair should proceed normally."""
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "p.par2", status="damaged")

        result = cmd_repair(self.config, self.manifest)

        mock_repair.assert_called_once()
        rec = self.manifest.get_file("photos", "img.jpg")
        self.assertEqual(rec["status"], "ok")


class TestRepairPostVerifyReset(RepairTestBase):
    @patch("par2integrity.main.verify_parity", return_value="damaged")
    @patch("par2integrity.main.repair_file", return_value=True)
    @patch("par2integrity.main.sha256_file", return_value=HASH_B)
    def test_repair_success_verify_fail_resets_to_damaged(self, mock_hash,
                                                          mock_repair,
                                                          mock_verify):
        """Repair succeeds but post-repair verify fails — status goes back to damaged."""
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "p.par2", status="damaged")

        result = cmd_repair(self.config, self.manifest)

        mock_repair.assert_called_once()
        rec = self.manifest.get_file("photos", "img.jpg")
        self.assertEqual(rec["status"], "damaged")


class TestRepairPicksUpRepairedStatus(RepairTestBase):
    @patch("par2integrity.main.verify_parity", return_value="ok")
    @patch("par2integrity.main.repair_file", return_value=True)
    @patch("par2integrity.main.sha256_file", return_value=HASH_B)
    def test_repair_retries_stuck_repaired_status(self, mock_hash, mock_repair,
                                                   mock_verify):
        """Files stuck in 'repaired' status from a crashed run should be retried."""
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "p.par2", status="repaired")

        result = cmd_repair(self.config, self.manifest)

        mock_repair.assert_called_once()
        rec = self.manifest.get_file("photos", "img.jpg")
        self.assertEqual(rec["status"], "ok")


class TestRepairRunTracking(RepairTestBase):
    @patch("par2integrity.main.verify_parity", return_value="ok")
    @patch("par2integrity.main.repair_file", return_value=True)
    @patch("par2integrity.main.sha256_file", return_value=HASH_B)
    def test_repair_records_run_in_history(self, mock_hash, mock_repair, mock_verify):
        """Repair operations should be recorded in the runs table."""
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "p.par2", status="damaged")

        cmd_repair(self.config, self.manifest)

        last_run = self.manifest.get_last_run()
        self.assertIsNotNone(last_run)
        self.assertIsNotNone(last_run["finished_at"])
        self.assertEqual(last_run["files_repaired"], 1)

    def test_repair_no_damaged_skips_run(self):
        """When no damaged files exist, no run should be recorded."""
        cmd_repair(self.config, self.manifest)

        self.assertIsNone(self.manifest.get_last_run())


class TestRepairDedupSafety(RepairTestBase):
    @patch("par2integrity.main.create_parity", return_value=True)
    @patch("par2integrity.main.delete_parity")
    @patch("par2integrity.main.sha256_file", return_value=HASH_A)
    def test_shared_hash_parity_not_deleted(self, mock_hash, mock_delete, mock_create):
        """When two files share a hash and parity is corrupt, don't delete shared parity."""
        # Two files share the same hash, both damaged
        self.manifest.upsert_file("photos", "img.jpg", 100, 1000,
                                  HASH_A, "p.par2", status="damaged")
        # Second file in a different data root
        other_dir = Path(self.data_dir) / "docs"
        other_dir.mkdir(parents=True, exist_ok=True)
        (other_dir / "copy.jpg").write_bytes(b"content_a")
        self.manifest.upsert_file("docs", "copy.jpg", 100, 1000,
                                  HASH_A, "p.par2", status="damaged")

        cmd_repair(self.config, self.manifest)

        # Parity should NOT have been deleted since files share the hash
        mock_delete.assert_not_called()
        # But create should still be called to ensure parity exists
        self.assertTrue(mock_create.call_count >= 1)
        # Both files should be ok
        self.assertEqual(self.manifest.get_file("photos", "img.jpg")["status"], "ok")
        self.assertEqual(self.manifest.get_file("docs", "copy.jpg")["status"], "ok")


if __name__ == "__main__":
    unittest.main()

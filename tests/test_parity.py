"""Tests for parity.py — temp dir safety, create/verify/repair/delete."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

from par2integrity.config import Config
from par2integrity.parity import create_parity, verify_parity, delete_parity
from tests.helpers import EnvSnapshot


class ParityTestBase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self._env = EnvSnapshot(["DATA_ROOT", "PARITY_ROOT", "PAR2_REDUNDANCY"])

        os.environ["DATA_ROOT"] = os.path.join(self.tmpdir, "data")
        os.environ["PARITY_ROOT"] = os.path.join(self.tmpdir, "parity")
        os.environ["PAR2_REDUNDANCY"] = "10"

        self.config = Config()
        self.config.parity_root.mkdir(parents=True, exist_ok=True)

        # Create a fake source file
        self.source_dir = Path(self.tmpdir) / "data" / "photos"
        self.source_dir.mkdir(parents=True)
        self.source_file = self.source_dir / "test.jpg"
        self.source_file.write_bytes(b"x" * 1000)

        self.content_hash = "ab" * 32  # 64 hex chars

    def tearDown(self):
        self._env.restore()


class TestCreateParity(ParityTestBase):
    def _fake_par2_success(self, args, **kwargs):
        """Simulate par2 creating output files in the temp directory."""
        # Find the output path from args (it's the par2 path argument)
        for arg in args:
            if arg.endswith(".par2"):
                out = Path(arg)
                out.write_bytes(b"fake par2 data")
                # par2 also creates volume files
                vol = out.parent / out.name.replace(".par2", ".vol000+01.par2")
                vol.write_bytes(b"fake vol data")
                break
        return MagicMock(returncode=0, stdout="", stderr="")

    @patch("par2integrity.parity._run_par2")
    def test_success_moves_to_final_dir(self, mock_run):
        mock_run.side_effect = self._fake_par2_success

        result = create_parity(self.config, self.source_file, self.content_hash)

        self.assertTrue(result)
        par2_dir = self.config.par2_dir_for_hash(self.content_hash)
        par2_name = self.config.par2_name_for_hash(self.content_hash)
        self.assertTrue((par2_dir / par2_name).exists())
        # Volume file should also be moved
        vol_name = par2_name.replace(".par2", ".vol000+01.par2")
        self.assertTrue((par2_dir / vol_name).exists())

    @patch("par2integrity.parity._run_par2")
    def test_success_cleans_up_temp_dir(self, mock_run):
        mock_run.side_effect = self._fake_par2_success

        # Count dirs in parity root before
        before = set(self.config.parity_root.iterdir())
        create_parity(self.config, self.source_file, self.content_hash)
        after = set(self.config.parity_root.iterdir())

        # Only the by_hash dir should be new — no leftover tmp dirs
        new_dirs = after - before
        for d in new_dirs:
            self.assertFalse(d.name.startswith("tmp"), f"Temp dir left behind: {d}")

    @patch("par2integrity.parity._run_par2")
    def test_failure_leaves_no_partial_files(self, mock_run):
        def fake_fail(args, **kwargs):
            # Simulate par2 writing a partial file then failing
            for arg in args:
                if arg.endswith(".par2"):
                    Path(arg).write_bytes(b"partial")
                    break
            return MagicMock(returncode=1, stdout="", stderr="error")

        mock_run.side_effect = fake_fail

        result = create_parity(self.config, self.source_file, self.content_hash)

        self.assertFalse(result)
        # Final location should have NO par2 files
        par2_dir = self.config.par2_dir_for_hash(self.content_hash)
        if par2_dir.exists():
            par2_files = list(par2_dir.glob("*.par2"))
            self.assertEqual(par2_files, [], "Partial par2 files left in final dir")

    @patch("par2integrity.parity._run_par2")
    def test_failure_cleans_up_temp_dir(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")

        create_parity(self.config, self.source_file, self.content_hash)

        # No temp dirs should remain in parity root
        for entry in self.config.parity_root.iterdir():
            self.assertFalse(entry.name.startswith("tmp"), f"Temp dir left behind: {entry}")

    @patch("par2integrity.parity._run_par2")
    def test_skips_if_parity_exists(self, mock_run):
        par2_dir = self.config.par2_dir_for_hash(self.content_hash)
        par2_dir.mkdir(parents=True, exist_ok=True)
        par2_name = self.config.par2_name_for_hash(self.content_hash)
        (par2_dir / par2_name).write_bytes(b"existing parity")

        result = create_parity(self.config, self.source_file, self.content_hash)

        self.assertTrue(result)
        mock_run.assert_not_called()


class TestVerifyParity(ParityTestBase):
    @patch("par2integrity.parity._run_par2")
    def test_ok(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        par2_dir = self.config.par2_dir_for_hash(self.content_hash)
        par2_dir.mkdir(parents=True, exist_ok=True)
        (par2_dir / self.config.par2_name_for_hash(self.content_hash)).write_bytes(b"par2")

        result = verify_parity(self.config, self.source_file, self.content_hash)
        self.assertEqual(result, "ok")

    @patch("par2integrity.parity._run_par2")
    def test_damaged(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1)
        par2_dir = self.config.par2_dir_for_hash(self.content_hash)
        par2_dir.mkdir(parents=True, exist_ok=True)
        (par2_dir / self.config.par2_name_for_hash(self.content_hash)).write_bytes(b"par2")

        result = verify_parity(self.config, self.source_file, self.content_hash)
        self.assertEqual(result, "damaged")

    def test_missing_parity(self):
        result = verify_parity(self.config, self.source_file, self.content_hash)
        self.assertEqual(result, "missing_parity")


class TestDeleteParity(ParityTestBase):
    def test_deletes_all_par2_files(self):
        par2_dir = self.config.par2_dir_for_hash(self.content_hash)
        par2_dir.mkdir(parents=True, exist_ok=True)
        par2_name = self.config.par2_name_for_hash(self.content_hash)
        stem = par2_name.replace(".par2", "")

        (par2_dir / par2_name).write_bytes(b"base")
        (par2_dir / f"{stem}.vol000+01.par2").write_bytes(b"vol1")
        (par2_dir / f"{stem}.vol001+02.par2").write_bytes(b"vol2")

        delete_parity(self.config, self.content_hash)

        remaining = list(par2_dir.glob("*")) if par2_dir.exists() else []
        self.assertEqual(remaining, [])

    def test_leaves_other_hash_files(self):
        par2_dir = self.config.par2_dir_for_hash(self.content_hash)
        par2_dir.mkdir(parents=True, exist_ok=True)
        par2_name = self.config.par2_name_for_hash(self.content_hash)

        (par2_dir / par2_name).write_bytes(b"target")
        (par2_dir / "other_file.par2").write_bytes(b"keep me")

        delete_parity(self.config, self.content_hash)

        remaining = list(par2_dir.glob("*"))
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0].name, "other_file.par2")


if __name__ == "__main__":
    unittest.main()

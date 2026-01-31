"""Tests for scanner.py — filesystem walk, filtering, hashing."""

import hashlib
import os
import tempfile
import unittest
from pathlib import Path

from par2integrity.config import Config
from par2integrity.scanner import scan_data_roots, sha256_file, should_exclude
from tests.helpers import EnvSnapshot


class TestShouldExclude(unittest.TestCase):
    def test_exact_match(self):
        self.assertTrue(should_exclude(".DS_Store", [".DS_Store"]))

    def test_glob_match(self):
        self.assertTrue(should_exclude("foo.tmp", ["*.tmp"]))
        self.assertTrue(should_exclude("data.partial", ["*.partial"]))

    def test_no_match(self):
        self.assertFalse(should_exclude("photo.jpg", ["*.tmp", ".DS_Store"]))

    def test_empty_patterns(self):
        self.assertFalse(should_exclude("anything", []))


class TestSha256File(unittest.TestCase):
    def test_known_hash(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"hello world\n")
            f.flush()
            path = Path(f.name)
        try:
            got = sha256_file(path)
            expected = hashlib.sha256(b"hello world\n").hexdigest()
            self.assertEqual(got, expected)
        finally:
            path.unlink()

    def test_empty_file(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = Path(f.name)
        try:
            got = sha256_file(path)
            expected = hashlib.sha256(b"").hexdigest()
            self.assertEqual(got, expected)
        finally:
            path.unlink()


class TestScanDataRoots(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self._env = EnvSnapshot([
            "DATA_ROOT", "PARITY_ROOT", "MIN_FILE_SIZE", "EXCLUDE_PATTERNS",
        ])

        os.environ["DATA_ROOT"] = self.tmpdir
        os.environ["PARITY_ROOT"] = os.path.join(self.tmpdir, "_parity")
        os.environ["MIN_FILE_SIZE"] = "0"
        os.environ["EXCLUDE_PATTERNS"] = ".DS_Store,*.tmp"

    def tearDown(self):
        self._env.restore()

    def _write_file(self, rel_path: str, content: bytes = b"x" * 100):
        full = Path(self.tmpdir) / rel_path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_bytes(content)
        return full

    def test_finds_files_in_data_roots(self):
        self._write_file("photos/vacation/img1.jpg")
        self._write_file("photos/vacation/img2.jpg")
        self._write_file("documents/report.pdf")

        cfg = Config()
        results = scan_data_roots(cfg)
        rel_paths = {(fi.data_root, fi.rel_path) for fi in results}

        self.assertIn(("photos", "vacation/img1.jpg"), rel_paths)
        self.assertIn(("photos", "vacation/img2.jpg"), rel_paths)
        self.assertIn(("documents", "report.pdf"), rel_paths)
        self.assertEqual(len(results), 3)

    def test_excludes_patterns(self):
        self._write_file("photos/.DS_Store")
        self._write_file("photos/img.jpg")
        self._write_file("photos/cache.tmp")

        cfg = Config()
        results = scan_data_roots(cfg)
        names = {fi.rel_path for fi in results}

        self.assertIn("img.jpg", names)
        self.assertNotIn(".DS_Store", names)
        self.assertNotIn("cache.tmp", names)

    def test_min_file_size_filter(self):
        os.environ["MIN_FILE_SIZE"] = "50"
        self._write_file("photos/small.jpg", b"x" * 10)
        self._write_file("photos/big.jpg", b"x" * 100)

        cfg = Config()
        results = scan_data_roots(cfg)
        names = {fi.rel_path for fi in results}

        self.assertIn("big.jpg", names)
        self.assertNotIn("small.jpg", names)

    def test_ignores_top_level_files(self):
        """Files directly in /data/ (not in a subdirectory) should be ignored."""
        # This is a file at the data root level, not inside a data_root subdir
        Path(self.tmpdir, "stray_file.txt").write_bytes(b"x" * 100)
        self._write_file("photos/real.jpg")

        cfg = Config()
        results = scan_data_roots(cfg)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].rel_path, "real.jpg")

    def test_stat_info_populated(self):
        self._write_file("photos/img.jpg", b"test content")

        cfg = Config()
        results = scan_data_roots(cfg)
        self.assertEqual(len(results), 1)

        fi = results[0]
        self.assertEqual(fi.data_root, "photos")
        self.assertEqual(fi.rel_path, "img.jpg")
        self.assertEqual(fi.size, 12)
        self.assertGreater(fi.mtime_ns, 0)
        self.assertTrue(fi.abs_path.exists())

    def test_empty_data_root(self):
        cfg = Config()
        results = scan_data_roots(cfg)
        self.assertEqual(results, [])

    def test_excluded_top_level_dir_skipped(self):
        """A top-level data root matching an exclude pattern should be skipped entirely."""
        os.environ["EXCLUDE_PATTERNS"] = "#recycle"
        self._write_file("#recycle/old.jpg")
        self._write_file("photos/real.jpg")

        cfg = Config()
        results = scan_data_roots(cfg)
        roots = {fi.data_root for fi in results}

        self.assertEqual(len(results), 1)
        self.assertNotIn("#recycle", roots)
        self.assertIn("photos", roots)


if __name__ == "__main__":
    unittest.main()

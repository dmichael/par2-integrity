"""Tests for config.py — env var loading, defaults, path generation."""

import os
import unittest
from pathlib import Path

from par2integrity.config import Config


class TestConfigDefaults(unittest.TestCase):
    def setUp(self):
        # Clear any env vars that might interfere
        self._saved = {}
        for key in ("RUN_MODE", "CRON_SCHEDULE", "PAR2_REDUNDANCY", "PAR2_TIMEOUT",
                     "MIN_FILE_SIZE", "MAX_FILE_SIZE",
                     "VERIFY_PERCENT", "LOG_LEVEL", "NOTIFY_WEBHOOK", "EXCLUDE_PATTERNS",
                     "DATA_ROOT", "PARITY_ROOT"):
            self._saved[key] = os.environ.pop(key, None)

    def tearDown(self):
        for key, val in self._saved.items():
            if val is not None:
                os.environ[key] = val
            else:
                os.environ.pop(key, None)

    def test_defaults(self):
        cfg = Config()
        self.assertEqual(cfg.run_mode, "cron")
        self.assertEqual(cfg.cron_schedule, "0 2 1 * *")
        self.assertEqual(cfg.par2_redundancy, 10)
        self.assertEqual(cfg.par2_timeout, 3600)
        self.assertEqual(cfg.min_file_size, 4096)
        self.assertEqual(cfg.max_file_size, 53687091200)
        self.assertEqual(cfg.verify_percent, 100)
        self.assertEqual(cfg.log_level, "INFO")
        self.assertEqual(cfg.notify_webhook, "")
        self.assertEqual(cfg.data_root, Path("/data"))
        self.assertEqual(cfg.parity_root, Path("/parity"))
        self.assertEqual(cfg.db_path, Path("/parity/_db/manifest.db"))

    def test_exclude_patterns_default(self):
        cfg = Config()
        self.assertIn(".DS_Store", cfg.exclude_patterns)
        self.assertIn("Thumbs.db", cfg.exclude_patterns)
        self.assertIn("*.tmp", cfg.exclude_patterns)
        self.assertIn("*.partial", cfg.exclude_patterns)

    def test_env_overrides(self):
        os.environ["RUN_MODE"] = "manual"
        os.environ["PAR2_REDUNDANCY"] = "25"
        os.environ["MIN_FILE_SIZE"] = "0"
        os.environ["VERIFY_PERCENT"] = "50"
        os.environ["LOG_LEVEL"] = "DEBUG"
        os.environ["NOTIFY_WEBHOOK"] = "https://example.com/hook"
        os.environ["EXCLUDE_PATTERNS"] = "*.log,*.bak"

        cfg = Config()
        self.assertEqual(cfg.run_mode, "manual")
        self.assertEqual(cfg.par2_redundancy, 25)
        self.assertEqual(cfg.min_file_size, 0)
        self.assertEqual(cfg.verify_percent, 50)
        self.assertEqual(cfg.log_level, "DEBUG")
        self.assertEqual(cfg.notify_webhook, "https://example.com/hook")
        self.assertEqual(cfg.exclude_patterns, ["*.log", "*.bak"])

    def test_exclude_patterns_empty(self):
        os.environ["EXCLUDE_PATTERNS"] = ""
        cfg = Config()
        self.assertEqual(cfg.exclude_patterns, [])


class TestConfigPathGeneration(unittest.TestCase):
    def setUp(self):
        self._saved = {}
        for key in ("DATA_ROOT", "PARITY_ROOT"):
            self._saved[key] = os.environ.pop(key, None)

    def tearDown(self):
        for key, val in self._saved.items():
            if val is not None:
                os.environ[key] = val
            else:
                os.environ.pop(key, None)

    def test_par2_dir_for_hash(self):
        cfg = Config()
        h = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        d = cfg.par2_dir_for_hash(h)
        self.assertEqual(d, Path("/parity/by_hash/ab"))

    def test_par2_name_for_hash(self):
        cfg = Config()
        h = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        name = cfg.par2_name_for_hash(h)
        self.assertEqual(name, "abcdef1234567890.par2")

    def test_par2_dir_uses_first_two_chars(self):
        cfg = Config()
        self.assertEqual(cfg.par2_dir_for_hash("ff" + "0" * 62), Path("/parity/by_hash/ff"))
        self.assertEqual(cfg.par2_dir_for_hash("00" + "f" * 62), Path("/parity/by_hash/00"))


if __name__ == "__main__":
    unittest.main()

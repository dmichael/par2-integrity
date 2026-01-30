"""Environment variable loading and defaults."""

import os
from pathlib import Path


class Config:
    def __init__(self):
        self.run_mode = os.environ.get("RUN_MODE", "cron")
        self.cron_schedule = os.environ.get("CRON_SCHEDULE", "0 2 * * *")
        self.par2_redundancy = int(os.environ.get("PAR2_REDUNDANCY", "10"))
        self.min_file_size = int(os.environ.get("MIN_FILE_SIZE", "4096"))
        self.verify_percent = int(os.environ.get("VERIFY_PERCENT", "100"))
        self.log_level = os.environ.get("LOG_LEVEL", "INFO")
        self.notify_webhook = os.environ.get("NOTIFY_WEBHOOK", "")

        raw_excludes = os.environ.get("EXCLUDE_PATTERNS", ".DS_Store,Thumbs.db,*.tmp,*.partial,.parity,#recycle,#archive,#trash")
        self.exclude_patterns = [p.strip() for p in raw_excludes.split(",") if p.strip()]

        self.data_root = Path(os.environ.get("DATA_ROOT", "/data"))
        self.parity_root = Path(os.environ.get("PARITY_ROOT", "/parity"))
        self.db_path = self.parity_root / "_db" / "manifest.db"
        self.hash_dir = self.parity_root / "by_hash"

    def par2_dir_for_hash(self, content_hash: str) -> Path:
        """Return the parity storage directory for a given content hash."""
        prefix = content_hash[:2]
        return self.hash_dir / prefix

    def par2_name_for_hash(self, content_hash: str) -> str:
        """Return the par2 base filename for a given content hash."""
        return f"{content_hash[:16]}.par2"

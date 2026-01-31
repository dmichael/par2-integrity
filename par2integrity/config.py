"""Environment variable loading and defaults."""

import os
import sys
from pathlib import Path


def _int_env(name: str, default: int, *, min_val: int | None = None,
             max_val: int | None = None) -> int:
    raw = os.environ.get(name, str(default))
    try:
        val = int(raw)
    except ValueError:
        sys.exit(f"Invalid {name}={raw!r} — expected an integer")
    if min_val is not None and val < min_val:
        sys.exit(f"Invalid {name}={val} — must be >= {min_val}")
    if max_val is not None and val > max_val:
        sys.exit(f"Invalid {name}={val} — must be <= {max_val}")
    return val


class Config:
    def __init__(self):
        self.run_mode = os.environ.get("RUN_MODE", "cron")
        self.cron_schedule = os.environ.get("CRON_SCHEDULE", "0 2 1 * *")
        self.par2_redundancy = _int_env("PAR2_REDUNDANCY", 10, min_val=1, max_val=100)
        self.par2_timeout = _int_env("PAR2_TIMEOUT", 3600, min_val=0)
        self.min_file_size = _int_env("MIN_FILE_SIZE", 4096, min_val=0)
        self.max_file_size = _int_env("MAX_FILE_SIZE", 53687091200, min_val=0)  # 50 GiB default
        self.verify_percent = _int_env("VERIFY_PERCENT", 100, min_val=0, max_val=100)
        self.log_level = os.environ.get("LOG_LEVEL", "INFO")
        self.notify_webhook = os.environ.get("NOTIFY_WEBHOOK", "")

        raw_excludes = os.environ.get("EXCLUDE_PATTERNS", ".DS_Store,Thumbs.db,*.tmp,*.partial,.parity,#recycle,#archive,#trash,*.zip,*.tar,*.tar.gz,*.tgz,*.tar.bz2,*.tbz2,*.tar.xz,*.txz,*.rar,*.7z")
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

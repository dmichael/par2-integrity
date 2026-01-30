"""Logging setup, JSON run logs, and optional webhook notifications."""

import json
import logging
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

from .config import Config
from .reconciler import RunStats

log = logging.getLogger(__name__)


def setup_logging(config: Config):
    level = getattr(logging, config.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )


def write_run_log(config: Config, run_id: int, stats: RunStats):
    """Write a JSON log file for this run to /parity/_logs/."""
    log_dir = config.parity_root / "_logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_file = log_dir / f"run_{run_id}_{now}.json"

    data = {
        "run_id": run_id,
        "timestamp": now,
        **stats.to_dict(),
    }
    log_file.write_text(json.dumps(data, indent=2))
    log.info("Run log written: %s", log_file)


def print_summary(stats: RunStats):
    """Print a human-readable summary to stdout."""
    d = stats.to_dict()
    print("\n=== PAR2 Integrity Run Summary ===")
    print(f"  Files scanned:  {d['files_scanned']}")
    print(f"  Parity created: {d['files_created']}")
    print(f"  Verified:       {d['files_verified']}")
    print(f"  Damaged:        {d['files_damaged']}")
    print(f"  Repaired:       {d['files_repaired']}")
    print(f"  Moved:          {d['files_moved']}")
    print(f"  Deleted:        {d['files_deleted']}")
    print(f"  Truncated:      {d['files_truncated']}")
    if d["errors"]:
        print(f"  Errors:\n    {d['errors']}")
    print("==================================\n")


def notify_webhook(config: Config, stats: RunStats):
    """Send a POST to the configured webhook URL with run stats."""
    if not config.notify_webhook:
        return

    payload = json.dumps(stats.to_dict()).encode()
    req = urllib.request.Request(
        config.notify_webhook,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            log.info("Webhook notified (%d)", resp.status)
    except urllib.error.URLError as e:
        log.error("Webhook failed: %s", e)

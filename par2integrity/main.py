"""CLI entry point: scan, verify-only, report, repair."""

import argparse
import fcntl
import logging
import sys

from .config import Config
from .manifest import Manifest
from .parity import repair_file, verify_parity
from .reconciler import reconcile, RunStats
from .reporter import setup_logging, write_run_log, print_summary, notify_webhook
from .scanner import scan_data_roots

log = logging.getLogger(__name__)


def cmd_scan(config: Config, manifest: Manifest):
    """Full scan: detect changes, create parity, verify, report."""
    run_id = manifest.start_run()
    log.info("Starting scan (run %d)", run_id)

    scanned = scan_data_roots(config)
    stats = reconcile(config, manifest, scanned)

    manifest.finish_run(run_id, stats.to_dict())
    write_run_log(config, run_id, stats)
    print_summary(stats)
    notify_webhook(config, stats)

    if stats.files_damaged > 0:
        log.warning("Damaged files detected: %d", stats.files_damaged)
        return 1
    return 0


def cmd_verify(config: Config, manifest: Manifest):
    """Verify-only mode: check parity on all known files, no modifications."""
    run_id = manifest.start_run()
    log.info("Starting verify-only (run %d)", run_id)

    scanned = scan_data_roots(config)
    stats = reconcile(config, manifest, scanned, verify_only=True)

    manifest.finish_run(run_id, stats.to_dict())
    write_run_log(config, run_id, stats)
    print_summary(stats)
    notify_webhook(config, stats)

    if stats.files_damaged > 0:
        log.warning("Damaged files detected: %d", stats.files_damaged)
        return 1
    return 0


def cmd_repair(config: Config, manifest: Manifest):
    """Attempt to repair all files marked as damaged."""
    damaged = [f for f in manifest.get_all_files() if f["status"] == "damaged"]
    if not damaged:
        print("No damaged files found in manifest.")
        return 0

    stats = RunStats()
    for rec in damaged:
        abs_path = config.data_root / rec["data_root"] / rec["rel_path"]
        log.info("Attempting repair: %s", abs_path)

        if not abs_path.exists():
            log.error("File not found: %s", abs_path)
            stats.errors.append(f"not found: {abs_path}")
            continue

        if repair_file(config, abs_path, rec["content_hash"]):
            stats.files_repaired += 1
            manifest.update_status(rec["id"], "repaired")
            # Re-verify after repair
            result = verify_parity(config, abs_path, rec["content_hash"])
            if result == "ok":
                manifest.update_status(rec["id"], "ok")
                manifest.mark_verified(rec["id"])
            else:
                log.warning("Post-repair verify failed: %s → %s", abs_path, result)
        else:
            stats.errors.append(f"repair failed: {abs_path}")

    print_summary(stats)
    return 0 if not stats.errors else 1


def cmd_report(config: Config, manifest: Manifest):
    """Print a report of the current manifest state."""
    all_files = manifest.get_all_files()
    last_run = manifest.get_last_run()

    print(f"\n=== PAR2 Integrity Report ===")
    print(f"  Total tracked files: {len(all_files)}")

    by_status = {}
    for f in all_files:
        by_status.setdefault(f["status"], []).append(f)
    for status, files in sorted(by_status.items()):
        print(f"  {status}: {len(files)}")

    if last_run:
        print(f"\n  Last run: {last_run['started_at']} → {last_run.get('finished_at', 'in progress')}")
        print(f"    Scanned: {last_run['files_scanned']}, Created: {last_run['files_created']}, "
              f"Verified: {last_run['files_verified']}, Damaged: {last_run['files_damaged']}")

    # List damaged files
    damaged = by_status.get("damaged", [])
    if damaged:
        print(f"\n  Damaged files:")
        for f in damaged:
            print(f"    - {f['data_root']}/{f['rel_path']}")

    print("=============================\n")
    return 0


def main():
    parser = argparse.ArgumentParser(
        prog="par2-integrity",
        description="PAR2-based file integrity protection",
    )
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("scan", help="Full scan: detect changes, create parity, verify")
    sub.add_parser("verify", help="Verify-only: check parity, no changes to parity store")
    sub.add_parser("repair", help="Repair all damaged files")
    sub.add_parser("report", help="Print manifest status report")

    args = parser.parse_args()

    config = Config()
    setup_logging(config)

    if not args.command:
        parser.print_help()
        return 1

    # Lock to prevent overlapping runs (e.g. cron fires while still indexing)
    lock_path = config.parity_root / "_db" / "run.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = open(lock_path, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log.warning("Another run is already in progress — skipping")
        return 0

    manifest = Manifest(config.db_path)
    try:
        commands = {
            "scan": cmd_scan,
            "verify": cmd_verify,
            "repair": cmd_repair,
            "report": cmd_report,
        }
        return commands[args.command](config, manifest)
    finally:
        manifest.close()
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()


if __name__ == "__main__":
    sys.exit(main())

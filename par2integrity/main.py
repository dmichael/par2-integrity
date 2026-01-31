"""CLI entry point: scan, verify-only, report, repair."""

import argparse
import fcntl
import logging
import sys

from .config import Config
from .manifest import Manifest
from .parity import create_parity, delete_parity, repair_file, verify_parity
from .reconciler import reconcile, RunStats
from .reporter import setup_logging, write_run_log, print_summary, notify_webhook
from .scanner import scan_data_roots, sha256_file

log = logging.getLogger(__name__)


def _run_scan(config: Config, manifest: Manifest, *, verify_only: bool):
    """Shared implementation for scan and verify-only commands."""
    label = "verify-only" if verify_only else "scan"
    run_id = manifest.start_run()
    log.info("Starting %s (run %d)", label, run_id)

    scanned = scan_data_roots(config)
    stats = reconcile(config, manifest, scanned, verify_only=verify_only)

    manifest.finish_run(run_id, stats.to_dict())
    write_run_log(config, run_id, stats)
    print_summary(stats)
    notify_webhook(config, stats)

    if stats.files_damaged > 0:
        log.warning("Damaged files detected: %d", stats.files_damaged)
    if not verify_only and stats.files_truncated > 0:
        log.warning("Truncated files detected: %d", stats.files_truncated)

    if stats.files_damaged > 0 or (not verify_only and stats.files_truncated > 0):
        return 1
    return 0


def cmd_scan(config: Config, manifest: Manifest):
    """Full scan: detect changes, create parity, verify, report."""
    return _run_scan(config, manifest, verify_only=False)


def cmd_verify(config: Config, manifest: Manifest):
    """Verify-only mode: check parity on all known files, no modifications."""
    return _run_scan(config, manifest, verify_only=True)


def cmd_repair(config: Config, manifest: Manifest):
    """Attempt to repair all files marked as damaged or stuck in repaired state."""
    damaged = manifest.get_files_by_status("damaged", "repaired")
    if not damaged:
        print("No damaged files found in manifest.")
        return 0

    run_id = manifest.start_run()
    log.info("Starting repair (run %d): %d files", run_id, len(damaged))

    stats = RunStats()
    for rec in damaged:
        abs_path = config.data_root / rec["data_root"] / rec["rel_path"]
        log.info("Attempting repair: %s", abs_path)

        if not abs_path.exists():
            log.error("File not found: %s", abs_path)
            stats.errors.append(f"not found: {abs_path}")
            continue

        # Hash-check: verify the file actually needs repair
        try:
            current_hash = sha256_file(abs_path)
        except OSError as e:
            log.error("Cannot hash %s: %s", abs_path, e)
            stats.errors.append(f"hash error: {abs_path}: {e}")
            continue

        if current_hash == rec["content_hash"]:
            # File is fine — parity is corrupt, not the file
            log.warning("File matches manifest hash, parity is corrupt: %s", abs_path)
            # Only delete parity if no other files share this hash
            other_refs = manifest.get_files_by_hash(rec["content_hash"])
            if len(other_refs) <= 1:
                delete_parity(config, rec["content_hash"])
            if create_parity(config, abs_path, rec["content_hash"]):
                manifest.update_status(rec["id"], "ok")
                manifest.mark_verified(rec["id"])
                log.info("Re-created parity for: %s", abs_path)
            else:
                stats.errors.append(f"parity re-create failed: {abs_path}")
            continue

        if repair_file(config, abs_path, rec["content_hash"]):
            stats.files_repaired += 1
            # Re-verify after repair — set final status atomically
            result = verify_parity(config, abs_path, rec["content_hash"])
            if result == "ok":
                manifest.update_status(rec["id"], "ok")
                manifest.mark_verified(rec["id"])
            else:
                log.warning("Post-repair verify failed: %s → %s", abs_path, result)
                manifest.update_status(rec["id"], "damaged")
        else:
            stats.errors.append(f"repair failed: {abs_path}")

    manifest.finish_run(run_id, stats.to_dict())
    print_summary(stats)
    return 0 if not stats.errors else 1


def cmd_report(config: Config, manifest: Manifest):
    """Print a report of the current manifest state."""
    all_files = manifest.get_all_files()
    last_run = manifest.get_last_run()

    print("\n=== PAR2 Integrity Report ===")
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
        print("\n  Damaged files:")
        for f in damaged:
            print(f"    - {f['data_root']}/{f['rel_path']}")

    # List truncated files
    truncated = by_status.get("truncated", [])
    if truncated:
        print("\n  Truncated files (restore from backup):")
        for f in truncated:
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

    log.info("Config: redundancy=%d%%, timeout=%ds, min_size=%d, max_size=%s, "
             "verify=%d%%, exclude=%s",
             config.par2_redundancy, config.par2_timeout,
             config.min_file_size,
             config.max_file_size if config.max_file_size else "unlimited",
             config.verify_percent,
             ",".join(config.exclude_patterns))

    if not args.command:
        parser.print_help()
        return 1

    # Lock to prevent overlapping runs (e.g. cron fires while still indexing)
    lock_path = config.parity_root / "_db" / "run.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = open(lock_path, "w")
    try:
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
    finally:
        lock_file.close()


if __name__ == "__main__":
    sys.exit(main())

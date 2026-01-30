"""Change detection, move matching, and action decisions."""

import logging
import random

from .config import Config
from .manifest import Manifest
from .parity import create_parity, verify_parity, delete_parity
from .scanner import FileInfo, sha256_file

log = logging.getLogger(__name__)


class RunStats:
    def __init__(self):
        self.files_scanned = 0
        self.files_created = 0
        self.files_verified = 0
        self.files_damaged = 0
        self.files_repaired = 0
        self.files_moved = 0
        self.files_deleted = 0
        self.files_truncated = 0
        self.errors: list[str] = []

    def to_dict(self) -> dict:
        return {
            "files_scanned": self.files_scanned,
            "files_created": self.files_created,
            "files_verified": self.files_verified,
            "files_damaged": self.files_damaged,
            "files_repaired": self.files_repaired,
            "files_moved": self.files_moved,
            "files_deleted": self.files_deleted,
            "files_truncated": self.files_truncated,
            "errors": "\n".join(self.errors) if self.errors else None,
        }


def reconcile(config: Config, manifest: Manifest,
              scanned_files: list[FileInfo], *, verify_only: bool = False) -> RunStats:
    """Run the full scan pipeline: classify, execute actions, return stats."""
    stats = RunStats()

    # Build a set of (data_root, rel_path) seen on disk
    seen_on_disk: set[tuple[str, str]] = set()

    # Phase 1: Classify each scanned file
    unchanged: list[tuple[FileInfo, dict]] = []
    needs_hash: list[FileInfo] = []

    for fi in scanned_files:
        stats.files_scanned += 1
        seen_on_disk.add((fi.data_root, fi.rel_path))

        existing = manifest.get_file(fi.data_root, fi.rel_path)
        if existing:
            if existing["mtime_ns"] == fi.mtime_ns and existing["file_size"] == fi.size:
                # Unchanged — schedule for verification
                unchanged.append((fi, existing))
            else:
                # mtime or size differ — need to hash to know if modified or just touched
                needs_hash.append(fi)
        else:
            # New path — need to hash
            needs_hash.append(fi)

    # Phase 2: Hash files that need it and classify further
    for fi in needs_hash:
        try:
            content_hash = sha256_file(fi.abs_path)
        except OSError as e:
            log.error("Cannot hash %s: %s", fi.abs_path, e)
            stats.errors.append(f"hash error: {fi.abs_path}: {e}")
            continue

        existing = manifest.get_file(fi.data_root, fi.rel_path)

        if existing:
            # Path exists in manifest with different mtime/size
            if existing["content_hash"] == content_hash:
                # Touched only — mtime changed but content same
                log.debug("Touched (mtime only): %s/%s", fi.data_root, fi.rel_path)
                manifest.update_mtime(existing["id"], fi.mtime_ns)
            else:
                # Truly modified — regenerate parity
                log.info("Modified: %s/%s", fi.data_root, fi.rel_path)
                if not verify_only:
                    par2_name = config.par2_name_for_hash(content_hash)
                    # Delete old parity if no other files reference it
                    old_hash = existing["content_hash"]
                    other_refs = manifest.get_files_by_hash(old_hash)
                    if len(other_refs) <= 1:
                        delete_parity(config, old_hash)

                    if create_parity(config, fi.abs_path, content_hash):
                        manifest.upsert_file(
                            fi.data_root, fi.rel_path, fi.size,
                            fi.mtime_ns, content_hash, par2_name,
                        )
                        stats.files_created += 1
                    else:
                        stats.errors.append(f"parity create failed: {fi.abs_path}")
        else:
            # New path — check if it's a move (same hash exists for a disappeared path)
            moved = _try_match_move(config, manifest, fi, content_hash, seen_on_disk)
            if moved:
                stats.files_moved += 1
                log.info("Moved: %s → %s/%s", moved, fi.data_root, fi.rel_path)
            elif not verify_only:
                # Truly new file — create parity
                par2_name = config.par2_name_for_hash(content_hash)
                if create_parity(config, fi.abs_path, content_hash):
                    manifest.upsert_file(
                        fi.data_root, fi.rel_path, fi.size,
                        fi.mtime_ns, content_hash, par2_name,
                    )
                    stats.files_created += 1
                    log.info("New: %s/%s", fi.data_root, fi.rel_path)
                else:
                    stats.errors.append(f"parity create failed: {fi.abs_path}")

    # Phase 3: Verify unchanged files (sampling if verify_percent < 100)
    if unchanged:
        to_verify = unchanged
        if config.verify_percent < 100:
            sample_size = max(1, len(unchanged) * config.verify_percent // 100)
            to_verify = random.sample(unchanged, sample_size)

        for fi, rec in to_verify:
            result = verify_parity(config, fi.abs_path, rec["content_hash"])
            stats.files_verified += 1
            if result == "ok":
                manifest.mark_verified(rec["id"])
            elif result == "damaged":
                stats.files_damaged += 1
                manifest.update_status(rec["id"], "damaged")
                log.warning("DAMAGED: %s/%s", fi.data_root, fi.rel_path)
            else:
                stats.errors.append(f"verify {result}: {fi.abs_path}")

    # Phase 4: Detect deletions and truncations — manifest entries not seen on disk
    if not verify_only:
        all_manifest = manifest.get_all_files()
        for rec in all_manifest:
            key = (rec["data_root"], rec["rel_path"])
            if key not in seen_on_disk:
                abs_path = config.data_root / rec["data_root"] / rec["rel_path"]
                if abs_path.exists():
                    # File still on disk but filtered out (truncated below MIN_FILE_SIZE)
                    log.warning("Truncated: %s/%s", rec["data_root"], rec["rel_path"])
                    manifest.update_status(rec["id"], "truncated")
                    stats.files_truncated += 1
                else:
                    # File truly gone from disk
                    log.info("Deleted: %s/%s", rec["data_root"], rec["rel_path"])
                    other_refs = manifest.get_files_by_hash(rec["content_hash"])
                    if len(other_refs) <= 1:
                        delete_parity(config, rec["content_hash"])
                    manifest.delete_file(rec["id"])
                    stats.files_deleted += 1

    return stats


def _try_match_move(config: Config, manifest: Manifest, fi: FileInfo,
                    content_hash: str, seen_on_disk: set[tuple[str, str]]) -> str | None:
    """Check if this new-path file matches a disappeared entry by hash.

    Returns the old path string if a move was matched, else None.
    """
    candidates = manifest.get_files_by_hash(content_hash)
    for cand in candidates:
        cand_key = (cand["data_root"], cand["rel_path"])
        if cand_key not in seen_on_disk:
            # This manifest entry's file is gone from disk — it's a move
            old_path = f"{cand['data_root']}/{cand['rel_path']}"
            manifest.update_path(cand["id"], fi.rel_path, fi.data_root)
            manifest.update_mtime(cand["id"], fi.mtime_ns)
            return old_path
    return None

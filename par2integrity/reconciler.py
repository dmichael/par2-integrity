"""Change detection, move matching, and action decisions."""

import logging
import random
from pathlib import Path

from .config import Config
from .manifest import Manifest
from .parity import create_parity, verify_parity, delete_parity
from .scanner import FileInfo, sha256_file, should_exclude

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
        self.parity_recreated = 0
        self.orphan_parity_cleaned = 0
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
            "parity_recreated": self.parity_recreated,
            "orphan_parity_cleaned": self.orphan_parity_cleaned,
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
    with manifest.transaction():
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
                        if create_parity(config, fi.abs_path, content_hash):
                            # Delete old parity only after new parity succeeds
                            _safe_delete_parity(config, manifest, existing["content_hash"])
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

        with manifest.transaction():
            for fi, rec in to_verify:
                result = verify_parity(config, fi.abs_path, rec["content_hash"])
                stats.files_verified += 1
                if result == "ok":
                    manifest.mark_verified(rec["id"])
                elif result == "damaged":
                    stats.files_damaged += 1
                    manifest.update_status(rec["id"], "damaged")
                    log.warning("DAMAGED: %s/%s", fi.data_root, fi.rel_path)
                elif result == "missing_parity":
                    if verify_only:
                        log.warning("Missing parity (verify-only, cannot re-create): %s/%s",
                                    fi.data_root, fi.rel_path)
                        stats.errors.append(f"missing parity: {fi.abs_path}")
                    else:
                        _handle_missing_parity(config, manifest, fi, rec, stats)
                else:
                    stats.errors.append(f"verify {result}: {fi.abs_path}")

    # Phase 4: Detect deletions and truncations — manifest entries not seen on disk
    # Note: iter_all_files() holds an open cursor while we modify the same table.
    # This is safe under WAL journal mode with a single connection (flock-enforced).
    if not verify_only:
        for rec in manifest.iter_all_files():
            key = (rec["data_root"], rec["rel_path"])
            if key not in seen_on_disk:
                abs_path = config.data_root / rec["data_root"] / rec["rel_path"]
                if abs_path.exists():
                    # File still on disk — check why it was filtered out
                    # Check data root and all path components against exclude patterns
                    rel_parts = Path(rec["rel_path"]).parts
                    if (should_exclude(rec["data_root"], config.exclude_patterns)
                            or any(should_exclude(part, config.exclude_patterns)
                                   for part in rel_parts)):
                        log.info("Excluded: %s/%s", rec["data_root"], rec["rel_path"])
                        _delete_file_and_parity(config, manifest, rec, stats)
                    elif _exceeds_max_file_size(config, abs_path):
                        log.info("Exceeds max file size: %s/%s", rec["data_root"], rec["rel_path"])
                        _delete_file_and_parity(config, manifest, rec, stats)
                    else:
                        # File exists and isn't excluded — likely truncated below MIN_FILE_SIZE
                        log.warning("Truncated: %s/%s", rec["data_root"], rec["rel_path"])
                        manifest.update_status(rec["id"], "truncated")
                        stats.files_truncated += 1
                else:
                    # File truly gone from disk
                    log.info("Deleted: %s/%s", rec["data_root"], rec["rel_path"])
                    _delete_file_and_parity(config, manifest, rec, stats)

    # Phase 5: Clean up orphan parity files
    if not verify_only:
        _cleanup_orphan_parity(config, manifest, stats)

    return stats


def _handle_missing_parity(config: Config, manifest: Manifest,
                           fi: FileInfo, rec: dict, stats: RunStats):
    """Re-create parity for a file whose par2 files are missing."""
    try:
        content_hash = sha256_file(fi.abs_path)
    except OSError as e:
        stats.errors.append(f"hash error during parity re-create: {fi.abs_path}: {e}")
        return
    if content_hash == rec["content_hash"]:
        # Content matches manifest — just re-create parity
        if create_parity(config, fi.abs_path, content_hash):
            log.info("Re-created missing parity: %s/%s", fi.data_root, fi.rel_path)
            stats.parity_recreated += 1
            manifest.mark_verified(rec["id"])
        else:
            stats.errors.append(f"parity re-create failed: {fi.abs_path}")
    else:
        # Sneaky modification — file changed without mtime update
        log.warning("Sneaky modification (hash mismatch): %s/%s", fi.data_root, fi.rel_path)
        par2_name = config.par2_name_for_hash(content_hash)
        if create_parity(config, fi.abs_path, content_hash):
            # Old parity is already missing — only clean up manifest refs
            _safe_delete_parity(config, manifest, rec["content_hash"])
            manifest.upsert_file(
                fi.data_root, fi.rel_path, fi.size,
                fi.mtime_ns, content_hash, par2_name,
            )
            stats.parity_recreated += 1
        else:
            stats.errors.append(f"parity create failed (sneaky mod): {fi.abs_path}")


def _safe_delete_parity(config: Config, manifest: Manifest, content_hash: str):
    """Delete parity for a hash only if no other manifest entries reference it."""
    other_refs = manifest.get_files_by_hash(content_hash)
    if len(other_refs) <= 1:
        delete_parity(config, content_hash)


def _delete_file_and_parity(config: Config, manifest: Manifest,
                            rec: dict, stats: RunStats):
    """Remove a manifest entry and its parity if no other files share the hash."""
    _safe_delete_parity(config, manifest, rec["content_hash"])
    manifest.delete_file(rec["id"])
    stats.files_deleted += 1


def _try_match_move(config: Config, manifest: Manifest, fi: FileInfo,
                    content_hash: str, seen_on_disk: set[tuple[str, str]]) -> str | None:
    """Check if this new-path file matches a disappeared entry by hash.

    Prefers candidates in the same data_root for more intuitive move logs.
    Returns the old path string if a move was matched, else None.
    """
    candidates = manifest.get_files_by_hash(content_hash)
    disappeared = [c for c in candidates
                   if (c["data_root"], c["rel_path"]) not in seen_on_disk]
    if not disappeared:
        return None
    # Prefer same data_root so intra-volume moves match first
    disappeared.sort(key=lambda c: c["data_root"] != fi.data_root)
    best = disappeared[0]
    old_path = f"{best['data_root']}/{best['rel_path']}"
    manifest.update_path(best["id"], fi.rel_path, fi.data_root)
    manifest.update_mtime(best["id"], fi.mtime_ns)
    return old_path


def _cleanup_orphan_parity(config: Config, manifest: Manifest, stats: RunStats):
    """Remove par2 files that have no corresponding manifest entry.

    Walks config.hash_dir looking for *.par2 base files (excluding vol files),
    and checks the manifest for references via the par2_name index.
    """
    hash_dir = config.hash_dir
    if not hash_dir.is_dir():
        return

    for prefix_dir in sorted(hash_dir.iterdir()):
        if not prefix_dir.is_dir():
            continue
        for par2_file in sorted(prefix_dir.iterdir()):
            # Only consider base par2 files, not vol files
            if not par2_file.name.endswith(".par2"):
                continue
            if ".vol" in par2_file.name:
                continue
            par2_name = par2_file.name
            if not manifest.has_par2_name(par2_name):
                # Orphan — reconstruct a hash prefix that produces the correct paths
                stem = par2_name.replace(".par2", "")
                pseudo_hash = stem.ljust(64, "0")
                delete_parity(config, pseudo_hash)
                log.info("Cleaned orphan parity: %s/%s", prefix_dir.name, par2_name)
                stats.orphan_parity_cleaned += 1


def _exceeds_max_file_size(config: Config, abs_path: Path) -> bool:
    """Check if a file exceeds the configured maximum size."""
    try:
        size = abs_path.stat().st_size
    except OSError:
        return False
    return bool(config.max_file_size and size > config.max_file_size)

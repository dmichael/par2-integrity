"""Filesystem walk and stat collection."""

import hashlib
import logging
import os
from fnmatch import fnmatch
from pathlib import Path

from .config import Config

log = logging.getLogger(__name__)

HASH_BUF_SIZE = 1 << 20  # 1 MiB


class FileInfo:
    __slots__ = ("abs_path", "data_root", "rel_path", "size", "mtime_ns")

    def __init__(self, abs_path: Path, data_root: str, rel_path: str,
                 size: int, mtime_ns: int):
        self.abs_path = abs_path
        self.data_root = data_root
        self.rel_path = rel_path
        self.size = size
        self.mtime_ns = mtime_ns


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(HASH_BUF_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _should_exclude(name: str, patterns: list[str]) -> bool:
    return any(fnmatch(name, p) for p in patterns)


def scan_data_roots(config: Config) -> list[FileInfo]:
    """Walk all directories under data_root, return FileInfo for each eligible file."""
    results = []
    data_root = config.data_root

    if not data_root.is_dir():
        log.warning("Data root %s does not exist or is not a directory", data_root)
        return results

    # Each top-level dir under /data/ is a "data_root" label (e.g. photos, documents)
    for entry in sorted(data_root.iterdir()):
        if not entry.is_dir():
            continue
        root_label = entry.name
        log.info("Scanning data root: %s", root_label)
        count = 0
        for dirpath, dirnames, filenames in os.walk(entry):
            # Filter excluded directories in-place
            dirnames[:] = [
                d for d in dirnames
                if not _should_exclude(d, config.exclude_patterns)
            ]
            for fname in sorted(filenames):
                if _should_exclude(fname, config.exclude_patterns):
                    continue
                full = Path(dirpath) / fname
                try:
                    st = full.stat()
                except OSError as e:
                    log.warning("Cannot stat %s: %s", full, e)
                    continue
                if st.st_size < config.min_file_size:
                    continue
                if config.max_file_size and st.st_size > config.max_file_size:
                    log.debug("Skipping (too large): %s (%d bytes)", full, st.st_size)
                    continue
                rel = str(full.relative_to(entry))
                results.append(FileInfo(
                    abs_path=full,
                    data_root=root_label,
                    rel_path=rel,
                    size=st.st_size,
                    mtime_ns=st.st_mtime_ns,
                ))
                count += 1
                if count % 100 == 0:
                    log.info("Scanned %d files in %s...", count, root_label)
        log.info("Found %d eligible files in %s", count, root_label)

    return results

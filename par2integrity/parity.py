"""par2 subprocess wrapper for create, verify, and repair."""

import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

from .config import Config

log = logging.getLogger(__name__)


def _run_par2(args: list[str], timeout: int = 3600) -> subprocess.CompletedProcess:
    log.debug("Running: %s", " ".join(args))
    result = subprocess.run(
        args,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        log.debug("par2 stdout: %s", result.stdout[-500:] if result.stdout else "")
        log.debug("par2 stderr: %s", result.stderr[-500:] if result.stderr else "")
    return result


def create_parity(config: Config, source_file: Path, content_hash: str) -> bool:
    """Create par2 parity files for a source file.

    Returns True on success.
    """
    par2_dir = config.par2_dir_for_hash(content_hash)
    par2_dir.mkdir(parents=True, exist_ok=True)
    par2_name = config.par2_name_for_hash(content_hash)
    par2_path = par2_dir / par2_name

    if par2_path.exists():
        log.debug("Parity already exists: %s", par2_path)
        return True

    # Write to a temp directory first, then move on success.
    # This prevents partial par2 files from being left behind if interrupted.
    tmp_dir = tempfile.mkdtemp(dir=config.parity_root)
    tmp_par2 = Path(tmp_dir) / par2_name

    try:
        # -B sets the basepath so par2 stores only the filename, not the full path.
        args = [
            "par2", "create",
            "-q",  # quiet
            f"-r{config.par2_redundancy}",
            "-B", str(source_file.parent),
            str(tmp_par2),
            str(source_file),
        ]

        result = _run_par2(args, timeout=config.par2_timeout)
        if result.returncode == 0:
            # Move all generated par2 files to final location
            for f in Path(tmp_dir).iterdir():
                shutil.move(str(f), str(par2_dir / f.name))
            log.debug("Created parity: %s", par2_path)
            return True

        log.error("Failed to create parity for %s (rc=%d): %s",
                  source_file, result.returncode, result.stderr.strip())
        return False
    except subprocess.TimeoutExpired:
        log.error("Timed out creating parity for %s (timeout=%ds)",
                  source_file, config.par2_timeout)
        return False
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def verify_parity(config: Config, source_file: Path, content_hash: str) -> str:
    """Verify a file against its par2 parity.

    Returns one of: "ok", "damaged", "missing_parity", "error"
    """
    par2_dir = config.par2_dir_for_hash(content_hash)
    par2_name = config.par2_name_for_hash(content_hash)
    par2_path = par2_dir / par2_name

    if not par2_path.exists():
        log.warning("Missing parity file: %s", par2_path)
        return "missing_parity"

    args = [
        "par2", "verify",
        "-q",
        "-B", str(source_file.parent),
        str(par2_path),
        str(source_file),
    ]

    try:
        result = _run_par2(args, timeout=config.par2_timeout)
    except subprocess.TimeoutExpired:
        log.error("Timed out verifying %s (timeout=%ds)",
                  source_file, config.par2_timeout)
        return "error"
    if result.returncode == 0:
        return "ok"
    # par2cmdline returns 1 for repairable damage, other codes for worse
    if result.returncode == 1:
        return "damaged"
    log.error("par2 verify error for %s (rc=%d): %s",
              source_file, result.returncode, result.stderr.strip())
    return "error"


def repair_file(config: Config, source_file: Path, content_hash: str) -> bool:
    """Attempt to repair a damaged file using par2 parity.

    Returns True on success.
    """
    par2_dir = config.par2_dir_for_hash(content_hash)
    par2_name = config.par2_name_for_hash(content_hash)
    par2_path = par2_dir / par2_name

    if not par2_path.exists():
        log.error("Cannot repair — missing parity: %s", par2_path)
        return False

    args = [
        "par2", "repair",
        "-q",
        "-B", str(source_file.parent),
        str(par2_path),
        str(source_file),
    ]

    try:
        result = _run_par2(args, timeout=config.par2_timeout)
    except subprocess.TimeoutExpired:
        log.error("Timed out repairing %s (timeout=%ds)",
                  source_file, config.par2_timeout)
        return False
    if result.returncode == 0:
        log.info("Successfully repaired: %s", source_file)
        return True

    log.error("Repair failed for %s (rc=%d): %s",
              source_file, result.returncode, result.stderr.strip())
    return False


def delete_parity(config: Config, content_hash: str):
    """Remove par2 files for a given content hash."""
    par2_dir = config.par2_dir_for_hash(content_hash)
    par2_name = config.par2_name_for_hash(content_hash)

    # par2 creates: base.par2, base.vol000+01.par2, base.vol001+02.par2, etc.
    removed = 0
    if par2_dir.is_dir():
        stem = par2_name.replace(".par2", "")
        for f in par2_dir.iterdir():
            if f.name == par2_name or f.name.startswith(stem + "."):
                f.unlink()
                removed += 1

    if removed:
        log.debug("Removed %d parity files for hash %s", removed, content_hash[:16])
        # Clean up empty directory
        try:
            par2_dir.rmdir()
        except OSError:
            pass  # directory not empty, that's fine

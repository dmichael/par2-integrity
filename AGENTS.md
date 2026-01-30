# par2-integrity — Agent Guide

## Module map

```
par2integrity/
  config.py      — Config class, loads all settings from env vars
  scanner.py     — Filesystem walk, stat collection, SHA-256 hashing
  reconciler.py  — Change detection, move matching, orchestrates parity/manifest actions
  manifest.py    — SQLite database for file tracking and run history
  parity.py      — par2 subprocess wrapper (create, verify, repair, delete)
  reporter.py    — Logging setup, run log files, summary printing, webhook notification
  main.py        — CLI entry point, argparse, flock locking
```

## Data flow (scan command)

```
scanner.scan_data_roots()          # Walk /data/*, stat every file
        │
        ▼
reconciler.reconcile()
  Phase 1: Classify              # Compare mtime_ns + size against manifest
    ├─ unchanged → verify list    #   Same mtime/size → skip hashing
    └─ changed/new → hash list   #   Different or unknown → needs SHA-256
  Phase 2: Hash & act
    ├─ touched (hash matches)    #   Update mtime only
    ├─ modified (hash differs)   #   Delete old parity, create new
    ├─ moved (hash in manifest,  #   Update path, no re-hash or re-create
    │         old path gone)
    └─ new                       #   Create parity
  Phase 3: Verify unchanged      # par2 verify on sample (VERIFY_PERCENT)
  Phase 4: Detect deletions      # Manifest entries not seen on disk → clean up
        │
        ▼
reporter                          # Log summary, write run log, webhook
```

## Key invariants

- **Parity keyed by content hash, not path.** `par2_dir_for_hash()` uses the first 2 hex chars as a prefix directory. `par2_name_for_hash()` uses the first 16 hex chars as the filename. Duplicates and moved files share parity.
- **mtime/size fast path.** Unchanged files skip SHA-256 entirely — only `stat()` is needed. Hashing only runs when mtime or size differs from the manifest.
- **Dedup-safe deletion.** Before deleting parity, `get_files_by_hash()` checks if other manifest entries reference the same hash.
- **flock prevents concurrent runs.** A lock file at `/parity/_db/run.lock` with `LOCK_EX | LOCK_NB` ensures only one scan/verify/repair runs at a time.
- **Atomic parity creation.** par2 files are created in a temp directory and moved to the final location on success. Interrupted creates leave no partial files.

## Data model

**Manifest** — SQLite at `/parity/_db/manifest.db`. Key tables:
- `files` — data_root, rel_path, file_size, mtime_ns, content_hash, par2_name, status, last_verified
- `runs` — run history with timestamps and stats

**Parity storage** — `/parity/by_hash/{2-char-prefix}/{hash[:16]}.par2` plus vol files.

## Testing

```sh
python3 -m unittest discover -s tests -v
```

All tests mock `_run_par2` — no par2 binary needed. Tests use `tempfile.mkdtemp` for isolated filesystem state. No network access required.

## Docker

- **Base:** `python:3.13-alpine3.21`
- **par2cmdline-turbo:** Built from source during image build (SIMD-optimized)
- **Entrypoint:** `entrypoint.sh` — cron mode (default) runs an initial scan then starts `crond`; manual mode passes args directly to `python -m par2integrity.main`
- **PID 1:** `tini` handles signal forwarding and zombie reaping
- **Volumes:** `/data/*` (read-only data mounts), `/parity` (read-write parity storage)

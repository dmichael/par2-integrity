# par2-integrity

Automated PAR2-based file integrity protection for data stored on a NAS or any Docker host. Detects bit rot, silent corruption, and accidental modifications by maintaining par2 parity data alongside SHA-256 checksums in a SQLite manifest.

## How it works

Each scan walks every file under `/data/`, hashes it with SHA-256, and stores the result in a SQLite manifest. New or changed files get par2 parity files created (grouped by content hash, so duplicates and moved files share parity). On subsequent scans, files are verified against their par2 parity data. Damaged files are flagged in the manifest and can be repaired.

## Quick start

```sh
git clone https://github.com/dmichael/par2-integrity.git
cd par2-integrity
cp docker-compose.example.yml docker-compose.yml
# Edit docker-compose.yml — add your data volumes and parity path
docker compose up --build -d
```

## Configuration

All settings are environment variables set in `docker-compose.yml`:

| Variable | Default | Description |
|---|---|---|
| `RUN_MODE` | `cron` | `cron` for scheduled scans, `manual` for one-shot CLI |
| `CRON_SCHEDULE` | `0 2 * * *` | Cron expression (cron mode only) |
| `PAR2_REDUNDANCY` | `10` | Parity redundancy percentage |
| `MIN_FILE_SIZE` | `4096` | Skip files smaller than this (bytes) |
| `VERIFY_PERCENT` | `100` | Percentage of files to verify per scan |
| `LOG_LEVEL` | `INFO` | Python log level |
| `EXCLUDE_PATTERNS` | `.DS_Store,Thumbs.db,*.tmp,*.partial` | Comma-separated fnmatch patterns to skip |
| `NOTIFY_WEBHOOK` | _(empty)_ | URL to POST a JSON summary after each scan |

## Volume mounts

Mount each directory you want to protect under `/data/<label>` read-only:

```yaml
volumes:
  - /mnt/photos:/data/photos:ro
  - /mnt/documents:/data/documents:ro
  - /mnt/parity:/parity
```

The `/parity` volume stores the SQLite manifest, par2 files, and run logs. It must be read-write.

## Commands

In cron mode (`RUN_MODE=cron`), scans run automatically on schedule and once at startup.

For manual one-shot commands, set `RUN_MODE=manual` or use `docker compose run`:

```sh
# Full scan — detect changes, create parity, verify
docker compose run --rm -e RUN_MODE=manual par2-integrity scan

# Verify only — check parity without creating new par2 files
docker compose run --rm -e RUN_MODE=manual par2-integrity verify

# Repair — attempt to fix all files flagged as damaged
docker compose run --rm -e RUN_MODE=manual par2-integrity repair

# Report — print manifest status summary
docker compose run --rm -e RUN_MODE=manual par2-integrity report
```

## How repair works

Data volumes are mounted read-only by default. To repair, re-mount the affected volume as read-write with `-v`:

```sh
# Check what's damaged
docker compose run --rm -e RUN_MODE=manual par2-integrity report

# Repair with the affected volume mounted read-write
docker compose run --rm \
  -v /path/to/photos:/data/photos:rw \
  -e RUN_MODE=manual \
  par2-integrity repair
```

The `-v` flag overrides the read-only mount for that run only. Your `docker-compose.yml` stays unchanged.

## Synology NAS

Enable SSH access under Control Panel → Terminal & SNMP, then SSH into your NAS.

### DSM 7.2+ (Container Manager)

**Prerequisite:** Install **Container Manager** from Package Center.

```sh
cd /volume1/docker
git clone https://github.com/dmichael/par2-integrity.git
cd par2-integrity
cp docker-compose.example.yml docker-compose.yml
# Edit docker-compose.yml with your volume paths
sudo docker compose up --build -d
```

- `--build` is required on first run and after updates to rebuild the image.
- The container appears under **Container Manager → Container**. Project support varies by DSM version.
- `restart: unless-stopped` keeps the container running across NAS reboots.
- **Updating:**
  ```sh
  cd /volume1/docker/par2-integrity
  git pull
  sudo docker compose up --build -d
  ```

### DSM 6.x (Docker package)

**Prerequisite:** Install the **Docker** package from Package Center.

```sh
cd /volume1/docker
git clone https://github.com/dmichael/par2-integrity.git
cd par2-integrity
cp docker-compose.example.yml docker-compose.yml
# Edit docker-compose.yml with your volume paths
sudo docker-compose up --build -d
```

- DSM 6 uses `docker-compose` (hyphenated), not `docker compose`.
- The container appears under **Docker → Container**. There is no Project support.
- `restart: unless-stopped` keeps the container running across NAS reboots.
- **Updating:**
  ```sh
  cd /volume1/docker/par2-integrity
  git pull
  sudo docker-compose up --build -d
  ```

## Performance

Each scan has four phases with different costs:

| Phase | What it does | I/O cost |
|---|---|---|
| **Walk** | `stat()` every file, compare mtime/size against manifest | Metadata only — lightweight |
| **Hash** | SHA-256 files with changed mtime or size, and new files | Reads only changed files — skipped entirely when nothing changed |
| **Verify** | `par2 verify` on unchanged files | Reads the full file from disk — this is the expensive phase |
| **Cleanup** | Remove manifest entries for deleted files | None (manifest-only) |

**First scan** is the most expensive: every file is new, so every file gets hashed and has par2 parity created. Parity creation is CPU-bound (par2cmdline-turbo uses SIMD) and I/O-bound (reads the file, writes par2 blocks).

**Subsequent scans** are dominated by the verify phase. `par2 verify` reads the entire file to check it against its parity blocks — there's no cheaper way to detect bit rot. The walk and hash phases are near-free on a stable collection since the mtime/size check skips hashing for unchanged files.

**Tuning with `VERIFY_PERCENT`:** Set this below 100 to verify only a random sample each scan. At `VERIFY_PERCENT=10`, each scan verifies 10% of unchanged files, achieving full coverage over roughly 10 scan cycles. This is the primary knob for controlling scan duration on large collections.

**Monitoring:** Each scan logs a summary with file counts. Run `report` to see the current manifest state. Scan logs are written to `/parity/_logs/`.

## Design notes

- **Hash-based parity** — par2 files are keyed by content SHA-256, not path. Renamed or moved files reuse existing parity without regeneration.
- **Read-only data mounts** — the container never writes to your data unless you explicitly enable `:rw` for repair.
- **Move detection** — if a file moves, the manifest updates the path without re-hashing or re-creating parity.
- **No external dependencies** — only needs Docker. The image builds par2cmdline-turbo from source; Python standard library handles everything else.
- **Single-process lock** — an `flock`-based lock prevents overlapping scans from cron and manual runs.

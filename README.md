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

Data volumes are mounted read-only by default. To repair damaged files:

1. Run `report` to see which files are damaged.
2. Edit `docker-compose.yml` and change `:ro` to `:rw` on the affected volume.
3. Run `repair`.
4. Change the volume back to `:ro`.

## Synology NAS

SSH into your NAS and clone the repo:

```sh
cd /volume1/docker
git clone https://github.com/dmichael/par2-integrity.git
cd par2-integrity
cp docker-compose.example.yml docker-compose.yml
# Edit docker-compose.yml with your volume paths
sudo docker compose up --build -d
```

Notes:

- **`--build` is required** on first run and after `git pull` to rebuild the image.
- **Older DSM versions** use `docker-compose` (hyphenated) instead of `docker compose`.
- The container appears under **Container Manager** (or the Docker package on older DSM). Older DSM does not support Projects — the container is listed directly.
- `restart: unless-stopped` keeps the container running across NAS reboots.
- **Updating:**
  ```sh
  cd /volume1/docker/par2-integrity
  git pull
  sudo docker compose up --build -d
  ```

## Design notes

- **Hash-based parity** — par2 files are keyed by content SHA-256, not path. Renamed or moved files reuse existing parity without regeneration.
- **Read-only data mounts** — the container never writes to your data unless you explicitly enable `:rw` for repair.
- **Move detection** — if a file moves, the manifest updates the path without re-hashing or re-creating parity.
- **No external dependencies** — only needs Docker. The image builds par2cmdline-turbo from source; Python standard library handles everything else.
- **Single-process lock** — an `flock`-based lock prevents overlapping scans from cron and manual runs.

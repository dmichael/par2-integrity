# Code Review Notes

Reviewed 2026-01-31. All 81 tests passing.

## Actionable Items

### Low Priority

1. **`manifest.py` — Repeated timestamp construction**
   `datetime.now(timezone.utc).isoformat()` appears 6 times. A `_now()` helper would reduce repetition.

2. **`reconciler.py:279` — Orphan cleanup pseudo-hash coupling**
   `pseudo_hash = stem.ljust(64, "0")` reconstructs a fake hash from the par2 filename stem, relying on `par2_name_for_hash` using `hash[:16]` and `par2_dir_for_hash` using `hash[:2]`. If either function's slicing changes, this breaks silently. Add a comment noting the coupling.

3. **No `__main__.py`**
   Entrypoint uses `python -m par2integrity.main`. Convention is a `par2integrity/__main__.py` so `python -m par2integrity` works.

### Nits

4. **`config.py` — No min/max cross-validation**
   Setting `MIN_FILE_SIZE=5000` and `MAX_FILE_SIZE=1000` is accepted without error. A one-line check in `__init__` would catch misconfiguration.

5. **`scanner.py:79`, `reconciler.py:292` — `max_file_size=0` means "unlimited" implicitly**
   The `if config.max_file_size and ...` pattern relies on 0 being falsy. The convention is only visible in the log line in `main.py:170`. Consider documenting or using `None` as the sentinel.

6. **`main.py:181` — Lock file opened without context manager**
   Works correctly (closed in `finally`), but a comment explaining why `with` isn't used (file must stay open for flock duration) would help future readers.

7. **Status values are bare strings**
   `"ok"`, `"damaged"`, `"truncated"`, `"repaired"` are string literals scattered across modules. At current scale this is manageable, but a typo would be a silent bug.

8. **`reporter.py:56-59` — Conditional stat printing**
   `parity_recreated` and `orphan_parity_cleaned` only print when non-zero, while other stats always print. Minor inconsistency.

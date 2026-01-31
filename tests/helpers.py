"""Shared test utilities — env management and constants."""

import hashlib
import os


def _hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


HASH_A = _hash(b"content_a")
HASH_B = _hash(b"content_b")


class EnvSnapshot:
    """Save and restore environment variables for test isolation."""

    def __init__(self, keys: list[str]):
        self._saved = {}
        for key in keys:
            self._saved[key] = os.environ.pop(key, None)

    def restore(self):
        for key, val in self._saved.items():
            if val is not None:
                os.environ[key] = val
            else:
                os.environ.pop(key, None)

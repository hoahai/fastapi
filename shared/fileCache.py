from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
from threading import Lock

try:
    import fcntl
except Exception:  # pragma: no cover - fallback for non-posix
    fcntl = None


class FileCache:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._thread_lock = Lock()

    @contextmanager
    def lock(self):
        with self._thread_lock:
            if fcntl is None:
                yield
                return
            lock_path = self.path.with_suffix(f"{self.path.suffix}.lock")
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            with open(lock_path, "a", encoding="utf-8") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def load_root(self) -> dict[str, object]:
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except FileNotFoundError:
            return {}
        except (json.JSONDecodeError, OSError, ValueError):
            return {}
        if not isinstance(data, dict):
            return {}
        return data

    def write_root(self, data: dict[str, object]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(data, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, self.path)


def normalize_tenant_key(tenant_id: str | None) -> str:
    if not tenant_id:
        return "default"
    value = tenant_id.strip().lower()
    return value or "default"

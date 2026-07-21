from __future__ import annotations

from threading import RLock
from time import monotonic
from typing import Any, Callable


class TTLCache:
    def __init__(self):
        self._items: dict[str, tuple[float, Any]] = {}
        self._lock = RLock()

    def get(self, key: str):
        with self._lock:
            item = self._items.get(key)
            if not item:
                return None
            expires_at, value = item
            if expires_at <= monotonic():
                self._items.pop(key, None)
                return None
            return value

    def set(self, key: str, value: Any, ttl_seconds: int = 30):
        with self._lock:
            self._items[key] = (monotonic() + max(1, ttl_seconds), value)
        return value

    def get_or_set(self, key: str, factory: Callable[[], Any], ttl_seconds: int = 30):
        value = self.get(key)
        if value is not None:
            return value
        return self.set(key, factory(), ttl_seconds)

    def delete(self, key: str):
        with self._lock:
            self._items.pop(key, None)

    def clear(self, prefix: str | None = None):
        with self._lock:
            if prefix is None:
                self._items.clear()
                return
            for key in list(self._items):
                if key.startswith(prefix):
                    self._items.pop(key, None)


cache = TTLCache()

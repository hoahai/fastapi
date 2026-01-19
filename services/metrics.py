# services/metrics.py

import threading
from collections import defaultdict


class Metrics:
    """
    Thread-safe in-memory metrics collector.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = defaultdict(int)
        self._timings: dict[str, list[float]] = defaultdict(list)

    def inc(self, key: str, value: int = 1) -> None:
        with self._lock:
            self._counters[key] += value

    def observe(self, key: str, value: float) -> None:
        with self._lock:
            self._timings[key].append(value)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "timings": {
                    key: {
                        "count": len(values),
                        "avg": sum(values) / len(values) if values else 0.0,
                        "max": max(values) if values else 0.0,
                    }
                    for key, values in self._timings.items()
                },
            }


# Global singleton
GLOBAL_METRICS = Metrics()

"""Execution and lock-wait counters, and the thread-safe collector that maintains them."""

import threading
from dataclasses import dataclass


@dataclass
class Stats:
    """Counters accumulated over the lifetime of a :class:`Database` (or :class:`MSQLite`) instance.

    :ivar executions: statements executed (``execute``, ``executemany`` and ``executescript`` each count once)
    :ivar retries: lock attempts that failed (each is followed by a backoff and retry, or by the final error) across ``BEGIN``, statements and ``COMMIT``
    :ivar max_execution_s: longest single statement execution, in seconds
    :ivar max_wait_s: longest total lock wait within one transaction, in seconds
    """

    executions: int = 0
    retries: int = 0
    max_execution_s: float | None = None
    max_wait_s: float | None = None


class StatsCollector:
    """Owns a :class:`Stats` and updates it under a lock, since transactions on several threads report into one instance."""

    def __init__(self) -> None:
        self.stats = Stats()
        self._lock = threading.Lock()

    def record_execution(self, execution_s: float) -> None:
        """Count one statement and keep the longest execution time."""
        with self._lock:
            self.stats.executions += 1
            if self.stats.max_execution_s is None or execution_s > self.stats.max_execution_s:
                self.stats.max_execution_s = execution_s

    def record_retry(self) -> None:
        """Count one failed lock attempt."""
        with self._lock:
            self.stats.retries += 1

    def record_wait(self, waited_s: float) -> None:
        """Keep the longest total lock wait seen within one transaction."""
        with self._lock:
            if self.stats.max_wait_s is None or waited_s > self.stats.max_wait_s:
                self.stats.max_wait_s = waited_s

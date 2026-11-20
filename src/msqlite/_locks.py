"""A lock for state that must stay usable in a forked child."""

import os
import threading
from types import TracebackType


class ForkSafeLock:
    """
    A mutex like ``threading.Lock`` that a forked child never inherits in the locked state.

    ``fork`` copies only the calling thread. A plain lock held by any other thread at that moment is copied
    locked and nothing in the child will ever release it, so the child hangs the first time it needs the lock.
    This one keeps a lock per process id: a child's first use creates a new, unlocked lock of its own.
    """

    def __init__(self) -> None:
        self._locks: dict[int, threading.Lock] = {}

    def _current(self) -> threading.Lock:
        pid = os.getpid()
        lock = self._locks.get(pid)
        if lock is None:
            # setdefault is atomic, so threads racing through a new process's first use all get the same lock.
            lock = self._locks.setdefault(pid, threading.Lock())
            for other_pid in list(self._locks):
                if other_pid != pid:
                    self._locks.pop(other_pid, None)  # an ancestor's lock: unusable here, and its pid may be reused
        return lock

    def __enter__(self) -> None:
        self._current().acquire()

    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None) -> None:
        self._current().release()

"""The lock-retry engine shared by ``BEGIN``, single statements and ``COMMIT``.

:class:`RetryPolicy` is the arithmetic (deadline, retry limit, backoff, busy timeout) with no I/O, so it can be
reasoned about and tested on its own. :class:`LockRetrier` runs an attempt under that policy: it classifies the
error, keeps the per-transaction :class:`WaitState` up to date, reports to the stats and the ``on_retry`` hook,
and sleeps.
"""

import random
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass

from ._errors import MSQLiteMaxRetriesError, MSQLiteTimeoutError, is_lock_error, is_stale_snapshot_error
from ._log import log
from ._stats import StatsCollector
from ._types import OnRetry


@dataclass
class WaitState:
    """Lock-wait bookkeeping for one transaction: shared by its ``BEGIN``, statements and ``COMMIT``.

    :ivar waited_s: time spent in failed attempts and backoff sleeps so far
    :ivar retries: failed attempts so far (every attempt but the last has failed, so this is also the attempt count)
    """

    waited_s: float = 0.0
    retries: int = 0


@dataclass(frozen=True)
class RetryPolicy:
    """How long, and how often, to wait for a lock within one transaction.

    :ivar timeout_s: maximum total wait before :class:`MSQLiteTimeoutError`; ``None`` waits forever
    :ivar retry_limit: maximum retries before :class:`MSQLiteMaxRetriesError`; ``None`` is unbounded
    :ivar retry_scale: base unit of the backoff sleep; the first retry sleeps a random time in
        ``[0, 2 * retry_scale)`` and the range doubles on each further retry
    :ivar retry_cap_s: upper bound of the backoff sleep range
    :ivar busy_timeout_s: SQLite's own busy timeout per attempt, absorbed inside the engine without a Python round trip
    """

    timeout_s: float | None = 30.0
    retry_limit: int | None = None
    retry_scale: float = 0.01
    retry_cap_s: float = 1.0
    busy_timeout_s: float = 5.0

    def __post_init__(self) -> None:
        if self.retry_scale <= 0.0:
            raise ValueError("retry_scale must be positive")
        if self.retry_cap_s <= 0.0:
            raise ValueError("retry_cap_s must be positive")
        if self.timeout_s is not None and self.timeout_s < 0.0:
            raise ValueError("timeout_s must not be negative")
        if self.busy_timeout_s < 0.0:
            raise ValueError("busy_timeout_s must not be negative")

    def remaining_s(self, state: WaitState) -> float | None:
        """Time left before the deadline, or ``None`` when there is no deadline."""
        if self.timeout_s is None:
            return None
        return max(0.0, self.timeout_s - state.waited_s)

    def busy_timeout_ms(self, state: WaitState) -> int:
        """SQLite busy timeout for the next attempt, capped so one attempt cannot wait past the deadline."""
        wanted_s = self.busy_timeout_s
        remaining_s = self.remaining_s(state)
        if remaining_s is not None:
            wanted_s = min(wanted_s, remaining_s)
        return int(wanted_s * 1000.0)

    def check_budget(self, state: WaitState) -> None:
        """Raise the deadline or retry-limit error if ``state`` has exhausted this policy."""
        if self.retry_limit is not None and state.retries > self.retry_limit:
            raise MSQLiteMaxRetriesError(state.waited_s, state.retries, self.retry_limit)
        if self.timeout_s is not None and state.retries > 0 and state.waited_s >= self.timeout_s:
            raise MSQLiteTimeoutError(state.waited_s, state.retries)

    def backoff_s(self, state: WaitState) -> float:
        """Jittered exponential sleep for the retry just counted in ``state``, bounded by the cap and the deadline."""
        sleep_s = random.random() * min(self.retry_scale * (2.0**state.retries), self.retry_cap_s)
        remaining_s = self.remaining_s(state)
        if remaining_s is not None:
            sleep_s = min(sleep_s, remaining_s)
        return sleep_s


class LockRetrier:
    """Runs an attempt until it succeeds, fails with something other than a lock error, or exhausts the policy.

    One instance per :class:`Database`; the per-transaction state travels in a :class:`WaitState`.
    """

    def __init__(self, policy: RetryPolicy, stats: StatsCollector, on_retry: OnRetry | None, label: str):
        """
        :param policy: the limits and backoff arithmetic
        :param stats: where failed attempts are counted
        :param on_retry: optional user callback invoked before each backoff sleep
        :param label: identifies the database in log records
        """
        self.policy = policy
        self._stats = stats
        self._on_retry = on_retry
        self._label = label

    def run[T](self, state: WaitState, attempt: Callable[[], T]) -> T:
        """
        Call ``attempt`` until it returns.

        A ``SQLITE_BUSY``/``SQLITE_LOCKED`` error is counted, reported and followed by a backoff sleep, then the
        attempt is repeated. Any other error, including ``SQLITE_BUSY_SNAPSHOT`` (which a retry can never fix),
        propagates at once. The budget is also checked before each attempt, so a transaction whose earlier
        statements used up the deadline fails without another attempt.
        """
        while True:
            self.policy.check_budget(state)
            started = time.monotonic()
            try:
                return attempt()
            except sqlite3.OperationalError as e:
                if not is_lock_error(e) or is_stale_snapshot_error(e):
                    raise
                self._backoff(state, time.monotonic() - started, e)

    def _backoff(self, state: WaitState, failed_attempt_s: float, error: sqlite3.OperationalError) -> None:
        """Account for a failed attempt, raise if the budget is spent, otherwise notify and sleep."""
        state.waited_s += failed_attempt_s
        state.retries += 1
        self._stats.record_retry()
        self.policy.check_budget(state)
        sleep_s = self.policy.backoff_s(state)
        if self._on_retry is not None:
            self._on_retry(state.retries, state.waited_s, error)
        log.debug(f'lock retry {state.retries} for "{self._label}" after {state.waited_s:.3f} s: {error}')
        time.sleep(sleep_s)
        state.waited_s += sleep_s

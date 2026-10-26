"""Connection ownership for :class:`Database`.

A :class:`ConnectionManager` opens and configures connections and decides who keeps them: with ``persistent=True``
each thread owns one connection, reused across its transactions and closed when the thread ends; otherwise a
connection lives for one transaction. Connections are never shared between threads and never carried across a
fork, since SQLite file locks belong to the process that took them.
"""

import os
import sqlite3
import threading
import weakref
from collections.abc import Sequence
from pathlib import Path

from ._sql import normalize_pragma
from ._types import OnOpen


class ConnectionSlot:
    """
    A connection owned by one thread, tagged with the pid that opened it (connections must not survive a fork).

    A persistent slot is referenced only by its thread's ``threading.local`` (and by a running ``Transaction``),
    so it is collected when the thread ends; :func:`close_dead_thread_connection` then closes its connection.
    """

    def __init__(self, conn: sqlite3.Connection, busy_timeout_ms: int):
        self.conn = conn
        self.pid = os.getpid()
        self.busy_timeout_ms = busy_timeout_ms

    def set_busy_timeout(self, wanted_ms: int) -> None:
        """Apply ``PRAGMA busy_timeout`` only when it differs from what the connection already has."""
        if wanted_ms != self.busy_timeout_ms:
            self.conn.execute(f"PRAGMA busy_timeout = {wanted_ms}")
            self.busy_timeout_ms = wanted_ms

    def close(self) -> None:
        self.conn.close()


def close_dead_thread_connection(conn: sqlite3.Connection, pid: int) -> None:
    """
    Finalizer for a :class:`ConnectionSlot`: close its connection once the owning thread has ended.

    Runs in whichever thread triggers the collection (allowed: connections are opened with
    ``check_same_thread=False``). A connection inherited across a fork belongs to the parent and is left
    alone, as in :meth:`ConnectionManager.close_all`. Closing an already closed connection is a no-op, so a slot
    closed by :meth:`ConnectionManager.close_all` first is harmless here.
    """
    if pid == os.getpid():
        conn.close()


class ConnectionManager:
    """Opens, hands out and closes the connections of one database file."""

    def __init__(self, db_path: Path, *, wal: bool, pragmas: Sequence[str], on_open: OnOpen | None, busy_timeout_s: float, persistent: bool):
        """
        :param db_path: database file path
        :param wal: set ``PRAGMA journal_mode=WAL`` on every new connection
        :param pragmas: extra pragmas applied to every new connection, with or without the ``PRAGMA`` prefix
        :param on_open: called with each newly opened connection after the pragmas are applied
        :param busy_timeout_s: SQLite's busy timeout a new connection starts with
        :param persistent: keep one connection per thread; ``False`` opens one per :meth:`acquire`
        """
        self.db_path = db_path
        self.wal = wal
        self.pragmas = tuple(normalize_pragma(pragma) for pragma in pragmas)
        self.on_open = on_open
        self.busy_timeout_s = busy_timeout_s
        self.persistent = persistent
        self._local = threading.local()
        # Observes the live slots so close_all() can reach every thread's connection; it must not keep them alive,
        # or a finished thread's connection would stay open until close_all(). Each slot's finalizer closes it.
        self._slots: weakref.WeakSet[ConnectionSlot] = weakref.WeakSet()
        self._slots_lock = threading.Lock()

    def acquire(self) -> ConnectionSlot:
        """Return this thread's connection, opening one if needed (or a fresh one when not persistent).

        Lock errors from opening (``PRAGMA journal_mode=WAL`` can be ``SQLITE_BUSY``) propagate so the caller can retry.
        """
        if not self.persistent:
            return self._open()
        slot: ConnectionSlot | None = getattr(self._local, "slot", None)
        if slot is not None and slot.pid != os.getpid():
            # Inherited across a fork: the parent's connection must not be used (or closed) here.
            self._forget(slot)
            slot = None
        if slot is None:
            slot = self._open()
            weakref.finalize(slot, close_dead_thread_connection, slot.conn, slot.pid)  # holds conn and pid, never the slot
            self._local.slot = slot
            with self._slots_lock:
                self._slots.add(slot)
        return slot

    def release(self, slot: ConnectionSlot) -> None:
        """Give a connection back after use (or after a failed ``BEGIN``): closed when not persistent, kept otherwise."""
        if not self.persistent:
            slot.close()

    def close_all(self) -> None:
        """Close every connection opened by this process, in any thread. Safe to call more than once."""
        with self._slots_lock:
            slots = list(self._slots)
            self._slots.clear()
        for slot in slots:
            if slot.pid == os.getpid():
                slot.close()
        self._local = threading.local()

    def _open(self) -> ConnectionSlot:
        """Open and configure a new connection; it is closed again if any configuration step fails."""
        # check_same_thread=False so close_all() can release connections opened by other threads; per-thread use is
        # guaranteed by the threading.local slot, not by sqlite3's check.
        conn = sqlite3.connect(self.db_path, timeout=self.busy_timeout_s, autocommit=True, check_same_thread=False)
        configured = False
        try:
            if self.wal:
                conn.execute("PRAGMA journal_mode=WAL")
            for pragma in self.pragmas:
                conn.execute(f"PRAGMA {pragma}")
            if self.on_open is not None:
                self.on_open(conn)
            configured = True
        finally:
            if not configured:
                conn.close()
        return ConnectionSlot(conn, int(self.busy_timeout_s * 1000.0))

    def _forget(self, slot: ConnectionSlot) -> None:
        """Drop a slot this process must not touch (inherited across a fork) without closing it."""
        self._local.slot = None
        with self._slots_lock:
            self._slots.discard(slot)

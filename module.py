"""
module.py — Persistent (crash-durable) Priority Queue.

A min-priority queue backed by an append-only Write-Ahead Log (WAL)
plus periodic snapshot/compaction. Every mutating operation is fsync'd
before returning, so the queue state survives process crashes and
restarts (same durability idea PostgreSQL uses).

Operations
----------
push(item, priority)     Insert item with given priority (lower = higher precedence).
pop()                   Remove and return (priority, item) of the smallest priority.
peek()                  Return (priority, item) without removing it.
size()                  Number of items currently queued.
is_empty()              True iff size() == 0.
clear()                 Drop every item.
close()                 Flush and release all resources.

Usage
-----
>>> from module import PersistentPriorityQueue
>>> with PersistentPriorityQueue("./queue") as q:
...     q.push("job-A", priority=3)
...     q.push("job-B", priority=1)
...     q.pop()                # -> (1, "job-B")
...     q.peek()               # -> (3, "job-A")

The above state is fully recoverable after a crash or restart.
"""

from __future__ import annotations

import base64
import errno
import heapq
import json
import logging
import os
import pickle
import threading
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    import fcntl  # POSIX-only
    _HAS_FCNTL = True
except ImportError:        # Windows or non-POSIX
    _HAS_FCNTL = False

# A heap entry is (priority, seq, encoded_item). We do NOT store raw items
# in the heap to keep ordering comparisons O(1) even for unorderable items.
Entry = Tuple[int, int, str]

# WAL op-codes
_OP_PUSH = "P"
_OP_POP = "D"
_OP_CLEAR = "C"
_OP_SNAPSHOT = "S"


# --------------------------------------------------------------------------- #
# Item (de)serialization: JSON when lossless, pickled+base64 otherwise.      #
# --------------------------------------------------------------------------- #

# Exact types that JSON round-trips losslessly. Deliberately EXACT-type
# checks (not isinstance): subclasses such as IntEnum, OrderedDict or a
# str subclass carry semantics that JSON would silently flatten.
_JSON_SAFE_TYPES = frozenset({type(None), bool, int, float, str})


def _is_json_safe(value: Any) -> bool:
    """Return True iff `value` survives json.dumps -> json.loads unchanged.

    A bare try/except around json.dumps is NOT sufficient, because JSON
    serializes some Python types *unfaithfully*:

        (4, 5)       -> [4, 5]        (tuple becomes list)
        {1: "a"}     -> {"1": "a"}    (int key becomes string)
        SomeEnum.LOW -> 1             (IntEnum degrades to int)

    Structural, iterative (no RecursionError on deep nesting), cycle-aware.
    Cycles and shared substructure simply return False, routing the item
    to pickle, which handles both correctly. O(size of item).
    """
    stack = [value]
    seen = set()  # ids of visited containers
    while stack:
        node = stack.pop()
        t = type(node)
        if t is list:
            if id(node) in seen:
                return False
            seen.add(id(node))
            stack.extend(node)
        elif t is dict:
            if id(node) in seen:
                return False
            seen.add(id(node))
            for key, child in node.items():
                if type(key) is not str:
                    return False  # json would coerce the key to a string
                stack.append(child)
        elif t not in _JSON_SAFE_TYPES:
            return False
    return True


def _encode_item(item: Any) -> str:
    """Serialize an arbitrary Python object into a JSON-storable string.

    Plain JSON is used when (and only when) the item round-trips
    losslessly, keeping the WAL human-readable. Anything else is pickled
    + base64'd so it comes back *exactly* as pushed.
    """
    if _is_json_safe(item):
        try:
            return json.dumps(["j", item], separators=(",", ":"))
        except Exception:
            pass  # defensive net: e.g. RecursionError on extreme nesting
    raw = pickle.dumps(item, protocol=pickle.HIGHEST_PROTOCOL)
    return json.dumps(["p", base64.b64encode(raw).decode("ascii")],
                      separators=(",", ":"))


def _decode_item(blob: str) -> Any:
    """Reverse of `_encode_item`."""
    tag, payload = json.loads(blob)
    if tag == "j":
        return payload
    if tag == "p":
        return pickle.loads(base64.b64decode(payload))
    raise ValueError(f"unknown item tag: {tag!r}")


# --------------------------------------------------------------------------- #
# Heap helpers (avoid relying on heapq's private _siftup/_siftdown).          #
# --------------------------------------------------------------------------- #
def _sift_down(heap: list, i: int) -> None:
    n = len(heap)
    while True:
        left = 2 * i + 1
        right = 2 * i + 2
        smallest = i
        if left < n and heap[left] < heap[smallest]:
            smallest = left
        if right < n and heap[right] < heap[smallest]:
            smallest = right
        if smallest == i:
            return
        heap[i], heap[smallest] = heap[smallest], heap[i]
        i = smallest


def _sift_up(heap: list, i: int) -> None:
    while i > 0:
        parent = (i - 1) // 2
        if heap[i] < heap[parent]:
            heap[i], heap[parent] = heap[parent], heap[i]
            i = parent
        else:
            return


class PersistentPriorityQueue:
    """Durable, file-backed, thread-safe min-priority queue."""

    def __init__(
        self,
        path: str,
        *,
        compaction_threshold: int = 10_000,
        sync: bool = True,
    ) -> None:
        """
        Parameters
        ----------
        path : str
            Base path (file or directory) used to store the queue. The
            extension-less base is augmented with `.wal`, `.snap`,
            and `.lock` siblings.
        compaction_threshold : int
            After this many WAL records, the in-memory heap is snapshotted
            to disk and the WAL is rotated.
        sync : bool
            If True (default), fsync after every mutating operation.
            Set False only for benchmarks where durability is not required.
        """
        if not path:
            raise ValueError("path must be a non-empty string")

        base = os.path.abspath(path)
        if os.path.isdir(base) or base.endswith(os.sep):
            base = os.path.join(base, "queue")

        self._base = base
        self._wal_path = base + ".wal"
        self._snap_path = base + ".snap"
        self._tmp_snap_path = base + ".snap.tmp"
        self._lock_path = base + ".lock"

        self._compaction_threshold = max(1, compaction_threshold)
        self._sync = sync

        self._lock = threading.RLock()
        self._heap: list[Entry] = []
        self._counter = 0                          # monotonic seq for stable ordering
        self._wal_records_since_snap = 0
        self._closed = False
        self._lock_fd: Optional[int] = None

        self._acquire_file_lock()
        try:
            self._recover()
            self._wal_fd = os.open(
                self._wal_path,
                os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                0o644,
            )
        except Exception:
            self._release_file_lock()
            raise

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    def push(self, item: Any, priority: int) -> None:
        if not isinstance(priority, int) or isinstance(priority, bool):
            raise TypeError("priority must be an int")
        with self._lock:
            self._ensure_open()
            seq = self._counter
            self._counter += 1
            entry = (priority, seq, _encode_item(item))
            heapq.heappush(self._heap, entry)
            self._append_wal(_OP_PUSH, [priority, seq, entry[2]])

    def pop(self) -> Tuple[int, Any]:
        with self._lock:
            self._ensure_open()
            if not self._heap:
                raise IndexError("pop from an empty priority queue")
            priority, seq, blob = heapq.heappop(self._heap)
            self._append_wal(_OP_POP, [priority, seq])
            return priority, _decode_item(blob)

    def peek(self) -> Tuple[int, Any]:
        with self._lock:
            self._ensure_open()
            if not self._heap:
                raise IndexError("peek from an empty priority queue")
            priority, _seq, blob = self._heap[0]
            return priority, _decode_item(blob)

    def size(self) -> int:
        with self._lock:
            return len(self._heap)

    def is_empty(self) -> bool:
        return self.size() == 0

    def clear(self) -> None:
        with self._lock:
            self._ensure_open()
            self._heap.clear()
            self._counter = 0
            self._append_wal(_OP_CLEAR, None)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            try:
                if getattr(self, "_wal_fd", None) is not None:
                    os.close(self._wal_fd)
                    self._wal_fd = None
            finally:
                self._release_file_lock()
                self._closed = True

    # context-manager sugar
    def __enter__(self) -> "PersistentPriorityQueue":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __len__(self) -> int:
        return self.size()

    def __repr__(self) -> str:
        return f"<PersistentPriorityQueue path={self._base!r} size={self.size()}>"

    # ------------------------------------------------------------------ #
    # Internals: WAL, recovery, compaction, locking                      #
    # ------------------------------------------------------------------ #
    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("queue is closed")

    def _append_wal(self, op: str, payload: Any) -> None:
        """Append one record to the WAL and fsync if configured."""
        line = json.dumps({"op": op, "p": payload}, separators=(",", ":")) + "\n"
        os.write(self._wal_fd, line.encode("utf-8"))
        if self._sync:
            os.fsync(self._wal_fd)
        self._wal_records_since_snap += 1
        if self._wal_records_since_snap >= self._compaction_threshold:
            try:
                self._compact()
            except Exception:
                # Compaction is an optimization — never fatal.
                logger.exception("compaction failed; continuing with current WAL")

    def _recover(self) -> None:
        """Rebuild in-memory state from snapshot (if any) + WAL replay."""
        # 1. Load snapshot if present.
        if os.path.exists(self._snap_path):
            try:
                with open(self._snap_path, "rb") as fh:
                    for raw in fh:
                        raw = raw.strip()
                        if not raw:
                            continue
                        priority, seq, blob = json.loads(raw)
                        heapq.heappush(self._heap, (priority, seq, blob))
                        if seq >= self._counter:
                            self._counter = seq + 1
            except (OSError, json.JSONDecodeError):
                logger.exception("snapshot load failed; falling back to WAL only")
                self._heap.clear()
                self._counter = 0

        # 2. Ensure WAL file exists.
        if not os.path.exists(self._wal_path):
            # touch
            with open(self._wal_path, "wb"):
                pass
            return

        # 3. Replay WAL. A torn final line is tolerated: stop replay there.
        with open(self._wal_path, "rb") as fh:
            for raw in fh:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    rec = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning("stopping WAL replay at corrupt/torn record")
                    break
                op = rec.get("op")
                payload = rec.get("p")
                if op == _OP_PUSH:
                    priority, seq, blob = payload
                    heapq.heappush(self._heap, (priority, seq, blob))
                    if seq >= self._counter:
                        self._counter = seq + 1
                    self._wal_records_since_snap += 1
                elif op == _OP_POP:
                    priority, seq = payload
                    self._heap_remove(priority, seq)
                    self._wal_records_since_snap += 1
                elif op == _OP_CLEAR:
                    self._heap.clear()
                    self._counter = 0
                    self._wal_records_since_snap += 1
                elif op == _OP_SNAPSHOT:
                    self._wal_records_since_snap = 0
                else:
                    logger.warning("unknown WAL op %r; ignoring", op)

    def _heap_remove(self, priority: int, seq: int) -> None:
        """Remove the entry whose (priority, seq) matches — used during replay."""
        heap = self._heap
        for i, (p, s, _b) in enumerate(heap):
            if p == priority and s == seq:
                heap[i] = heap[-1]
                heap.pop()
                if i < len(heap):
                    _sift_down(heap, i)
                    _sift_up(heap, i)
                return

    def _compact(self) -> None:
        """Snapshot current state to a fresh file and rotate the WAL.

        Steps (all crash-safe):
          1. A temp snapshot file and fsync.
          2. `os.replace` to atomically swap into place.
          3. Rotate WAL: rename current WAL to .bak, open a fresh one,
             A SNAPSHOT marker, fsync, then unlink the backup.
        If a crash occurs at any step, recovery still works because either
        the (old) WAL is intact, or the new snapshot is now in place and
        the WAL will be replayed from the SNAPSHOT marker forward.
        """
        # 1. Write temp snapshot.
        with open(self._tmp_snap_path, "wb") as fh:
            buf = bytearray()
            for priority, seq, blob in self._heap:
                buf += json.dumps([priority, seq, blob], separators=(",", ":")).encode()
                buf += b"\n"
                if len(buf) >= 65536:
                    fh.write(buf); buf.clear()
            if buf:
                fh.write(buf)
            fh.flush()
            os.fsync(fh.fileno())

        # 2. Atomically install the new snapshot.
        os.replace(self._tmp_snap_path, self._snap_path)

        # 3. Rotate the WAL.
        os.close(self._wal_fd)
        wal_bak = self._wal_path + ".bak"
        if os.path.exists(self._wal_path):
            os.replace(self._wal_path, wal_bak)
        self._wal_fd = os.open(
            self._wal_path,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            0o644,
        )
        marker = json.dumps({"op": _OP_SNAPSHOT, "p": None},
                            separators=(",", ":")) + "\n"
        os.write(self._wal_fd, marker.encode("utf-8"))
        if self._sync:
            os.fsync(self._wal_fd)
        # Safe to drop the backup: new snapshot + new WAL are both durable.
        try:
            if os.path.exists(wal_bak):
                os.remove(wal_bak)
        except OSError:
            logger.warning("could not remove WAL backup %s", wal_bak, exc_info=True)
        self._wal_records_since_snap = 0

    # ---------------------- cross-process locking ---------------------- #
    def _acquire_file_lock(self) -> None:
        if not _HAS_FCNTL:
            return
        self._lock_fd = os.open(
            self._lock_path, os.O_RDWR | os.O_CREAT, 0o644
        )
        try:
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(self._lock_fd)
            self._lock_fd = None
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                raise RuntimeError(
                    f"another process holds the lock for {self._base}"
                ) from exc
            raise

    def _release_file_lock(self) -> None:
        if self._lock_fd is None:
            return
        try:
            if _HAS_FCNTL:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
        finally:
            os.close(self._lock_fd)
            self._lock_fd = None


# --------------------------------------------------------------------------- #
# Tiny CLI for manual smoke-testing: `python module.py`                       #
# --------------------------------------------------------------------------- #
def _demo() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    q = PersistentPriorityQueue("./demo_queue")
    for prio, item in [(3, "low"), (1, "high"), (2, "mid"), (1, "high-2")]:
        q.push(item, prio)
    while not q.is_empty():
        print(q.pop())
    q.close()


if __name__ == "__main__":
    _demo()
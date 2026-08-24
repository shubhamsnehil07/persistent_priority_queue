"""Test suite for the PersistentPriorityQueue."""

import json
import os
import shutil
import tempfile
import threading
from enum import IntEnum
import pytest

from module import PersistentPriorityQueue


@pytest.fixture()
def tmpdir():
    d = tempfile.mkdtemp(prefix="ppq_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


def _path(d):
    return os.path.join(d, "queue")


def test_ordering_and_basic_ops(tmpdir):
    with PersistentPriorityQueue(_path(tmpdir)) as q:
        assert q.is_empty()
        q.push("low", 3)
        q.push("high", 1)
        q.push("mid", 2)
        assert q.size() == 3
        assert q.peek() == (1, "high")
        assert q.pop() == (1, "high")
        assert q.pop() == (2, "mid")
        assert q.pop() == (3, "low")
        assert q.is_empty()


def test_fifo_on_ties(tmpdir):
    """Equal priorities must come out in insertion order (FIFO)."""
    with PersistentPriorityQueue(_path(tmpdir)) as q:
        for i in range(50):
            q.push(i, priority=1)
        out = [q.pop()[1] for _ in range(50)]
        assert out == list(range(50))


def test_durable_across_reopen(tmpdir):
    p = _path(tmpdir)
    with PersistentPriorityQueue(p) as q:
        q.push("a", 2)
        q.push("b", 1)
        q.push("c", 3)
        # crash: do not pop, just close
    with PersistentPriorityQueue(p) as q:
        assert q.size() == 3
        assert q.pop() == (1, "b")
        assert q.pop() == (2, "a")
        assert q.pop() == (3, "c")


def test_pop_then_reopen(tmpdir):
    """POP must be replayed on reopen so the item is not re-delivered."""
    p = _path(tmpdir)
    with PersistentPriorityQueue(p) as q:
        q.push("a", 1)
        q.push("b", 2)
        q.push("c", 3)
        assert q.pop() == (1, "a")
    with PersistentPriorityQueue(p) as q:
        assert q.size() == 2
        assert q.pop() == (2, "b")
        assert q.pop() == (3, "c")


def test_clear_durable(tmpdir):
    p = _path(tmpdir)
    with PersistentPriorityQueue(p) as q:
        q.push("a", 1)
        q.push("b", 2)
        q.clear()
    with PersistentPriorityQueue(p) as q:
        assert q.is_empty()


def test_compaction_roundtrip(tmpdir):
    p = _path(tmpdir)
    with PersistentPriorityQueue(p, compaction_threshold=5) as q:
        for i in range(20):
            q.push(i, i % 4)
        assert os.path.exists(p + ".snap")
        # the snapshot file should have all 20 entries
        with open(p + ".snap") as fh:
            assert sum(1 for _ in fh) == 20
    with PersistentPriorityQueue(p) as q:
        assert q.size() == 20
        seen = sorted(q.pop()[0] for _ in range(20))
        assert seen == sorted(i % 4 for i in range(20))


def test_torn_final_wal_record_is_tolerated(tmpdir):
    """A truncated last WAL record must not corrupt the queue on recovery."""
    p = _path(tmpdir)
    with PersistentPriorityQueue(p) as q:
        q.push("a", 1)
        q.push("b", 2)
    # Corrupt the WAL by appending a half-written line.
    with open(p + ".wal", "ab") as fh:
        fh.write(b'{"op":"P","p":[3,99,"incomple')   # no newline, invalid JSON
    with PersistentPriorityQueue(p) as q:
        # The two good entries survive; the torn record is skipped.
        items = [q.pop()[1] for _ in range(q.size())]
        assert sorted(items) == ["a", "b"]


def test_arbitrary_python_objects(tmpdir):
    """Non-JSON items are transparently pickled+base64'd."""
    obj = {"nested": {"list": [1, 2, 3]}, "tuple": (4, 5)}
    with PersistentPriorityQueue(_path(tmpdir)) as q:
        q.push(obj, 1)
        prio, item = q.pop()
        assert prio == 1 and item == obj


def test_thread_safety(tmpdir):
    """Concurrent pushes/pops from many threads must not corrupt state."""
    p = _path(tmpdir)
    with PersistentPriorityQueue(p, sync=False) as q:
        N_THREADS = 8
        N_PER = 500

        def producer(start):
            for i in range(start, start + N_PER):
                q.push(i, i)

        threads = [threading.Thread(target=producer, args=(t * N_PER,))
                   for t in range(N_THREADS)]
        for t in threads: t.start()
        for t in threads: t.join()

        total = N_THREADS * N_PER
        assert q.size() == total
        out = [q.pop()[1] for _ in range(total)]
        assert sorted(out) == list(range(total))


def test_pop_empty_raises(tmpdir):
    with PersistentPriorityQueue(_path(tmpdir)) as q:
        with pytest.raises(IndexError):
            q.pop()
        with pytest.raises(IndexError):
            q.peek()


def test_close_idempotent(tmpdir):
    q = PersistentPriorityQueue(_path(tmpdir))
    q.close()
    q.close()   # second close must be a no-op


def test_priority_must_be_int(tmpdir):
    with PersistentPriorityQueue(_path(tmpdir)) as q:
        with pytest.raises(TypeError):
            q.push("x", "1")
        with pytest.raises(TypeError):
            q.push("x", 1.0)  # floats disallowed for stable ordering



class Level(IntEnum):
    """Module-level so pickle can reference it (locals are not picklable)."""
    LOW = 1


def test_encoding_tag_selection(tmpdir):
    """JSON-safe items use the readable 'j' path; lossy ones use pickle 'p'."""
    p = _path(tmpdir)
    with PersistentPriorityQueue(p) as q:
        q.push([1, 2, 3], 1)      # plain list -> JSON fast path
        q.push((1, 2, 3), 2)      # tuple      -> pickle path
    with open(p + ".wal", encoding="utf-8") as fh:
        records = [json.loads(line) for line in fh if line.strip()]
    tags = [json.loads(rec["p"][2])[0] for rec in records]
    assert tags == ["j", "p"]


def test_lossy_json_types_round_trip_exactly(tmpdir):
    """Types JSON would silently mutate must come back exactly as pushed."""
    cases = [
        (1, 2, 3),          # tuple -> would become a list
        {"k": (1, 2)},      # nested tuple
        {1: "int-key"},     # int key -> would become "1"
        {"a", "b"},         # set: not JSON-serializable at all
        b"bytes",           # bytes: not JSON-serializable
        Level.LOW,          # IntEnum: would degrade to plain int
    ]
    with PersistentPriorityQueue(_path(tmpdir)) as q:
        for i, case in enumerate(cases):
            q.push(case, i)
        popped = [q.pop()[1] for _ in range(len(cases))]
    assert popped == cases


def test_cyclic_structure_round_trips(tmpdir):
    """Self-referential containers must not hang the safety check."""
    lst = [1, 2]
    lst.append(lst)
    with PersistentPriorityQueue(_path(tmpdir)) as q:
        q.push(lst, 1)
        _prio, out = q.pop()
    assert out[:2] == [1, 2]
    assert out[2] is out  # pickle preserves cycles AND identity


def test_pickled_item_durable_across_reopen(tmpdir):
    p = _path(tmpdir)
    obj = {"priority": 7, "payload": (1, 2, 3)}
    with PersistentPriorityQueue(p) as q:
        q.push(obj, 5)
    with PersistentPriorityQueue(p) as q:
        assert q.pop() == (5, obj)
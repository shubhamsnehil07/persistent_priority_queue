# Persistent Priority Queue

A **crash-durable, file-backed min-priority queue for Python**.

The queue combines an in-memory binary heap with an append-only **Write-Ahead Log (WAL)** and periodic **snapshot/compaction**. Mutating operations are synchronized to disk by default, allowing the queue to recover its state after a process crash or restart.

It is designed to provide the durability guarantees expected from a small persistent data structure while keeping the normal queue operations simple and fast.

## Features

* **Min-priority queue**

  * Lower priority values are served first.
* **Persistent storage**

  * Queue state survives process termination and restart.
* **Write-Ahead Log (WAL)**

  * Every mutation is recorded before the operation completes.
* **Crash recovery**

  * State is reconstructed from a snapshot and WAL during initialization.
* **Periodic compaction**

  * The WAL is periodically compacted into a snapshot to prevent unbounded growth.
* **Atomic snapshots**

  * Snapshots are written to a temporary file and installed using an atomic file replacement.
* **Stable ordering**

  * Items with the same priority are returned in FIFO/insertion order.
* **Arbitrary Python objects**

  * JSON-safe objects use a human-readable JSON representation.
  * Other objects are serialized using `pickle` and Base64.
* **Thread-safe**

  * Operations are protected using an `RLock`.
* **Cross-process protection on POSIX**

  * Uses `fcntl.flock()` to prevent multiple processes from simultaneously opening the same queue.
* **Tolerant WAL recovery**

  * A partially written final WAL record does not invalidate earlier valid records.
* **Context-manager support**

  * Use `with PersistentPriorityQueue(...) as q:`.
* **Simple CLI demo**

  * Run `python module.py` for a basic smoke test.

## Requirements

* Python 3.9+
* `pytest` for running the test suite

The implementation uses Python's standard library for the queue itself.

## Installation

Clone the repository:

```bash
git clone https://github.com/shubhamsnehil07/persistent_priority_queue.git
cd persistent_priority_queue
```

Install the test dependency:

```bash
pip install pytest
```

No additional runtime dependency is required.

## Quick Start

```python
from module import PersistentPriorityQueue

with PersistentPriorityQueue("./queue") as q:
    q.push("job-A", priority=3)
    q.push("job-B", priority=1)
    q.push("job-C", priority=2)

    print(q.peek())
    # (1, "job-B")

    print(q.pop())
    # (1, "job-B")

    print(q.pop())
    # (2, "job-C")

    print(q.size())
    # 1
```

Because the queue is persistent, closing and reopening it restores the remaining items:

```python
from module import PersistentPriorityQueue

with PersistentPriorityQueue("./queue") as q:
    print(q.pop())
```

The queue can therefore be used across multiple executions of a program.

## API

### `PersistentPriorityQueue(path, *, compaction_threshold=10_000, sync=True)`

Create or open a persistent priority queue.

#### Parameters

| Parameter              | Type   | Default | Description                               |
| ---------------------- | ------ | ------: | ----------------------------------------- |
| `path`                 | `str`  |       — | Base path used for persistent queue files |
| `compaction_threshold` | `int`  | `10000` | Number of WAL records before compaction   |
| `sync`                 | `bool` |  `True` | Whether to `fsync()` after each mutation  |

When `path` is a directory, the queue uses a `queue` base name inside that directory.

The implementation creates associated files using the base path:

```text
queue.wal
queue.snap
queue.snap.tmp
queue.lock
```

### `push(item, priority)`

Insert an item into the queue.

```python
q.push("job-A", priority=3)
```

Lower priority values have higher precedence.

```text
priority 1 → first
priority 2 → second
priority 3 → third
```

`priority` must be an integer.

### `pop()`

Remove and return the highest-priority item.

```python
priority, item = q.pop()
```

Example:

```python
q.push("low", 3)
q.push("high", 1)

print(q.pop())
# (1, "high")
```

Raises:

```python
IndexError
```

when the queue is empty.

### `peek()`

Return the next item without removing it.

```python
priority, item = q.peek()
```

Raises `IndexError` when the queue is empty.

### `size()`

Return the number of items currently stored.

```python
print(q.size())
```

### `is_empty()`

Check whether the queue contains no items.

```python
if q.is_empty():
    print("Queue is empty")
```

### `clear()`

Remove all items from the queue.

```python
q.clear()
```

The clear operation is persisted through the WAL and therefore survives reopening.

### `close()`

Flush and release queue resources.

```python
q.close()
```

Calling `close()` multiple times is safe.

### Context Manager

The recommended usage is:

```python
with PersistentPriorityQueue("./queue") as q:
    q.push("task", 1)
```

The queue is automatically closed when leaving the `with` block.

## Priority Ordering

The queue is a **min-priority queue**.

For example:

```python
q.push("A", 3)
q.push("B", 1)
q.push("C", 2)
```

The output order is:

```text
B
C
A
```

Internally, each heap entry contains:

```text
(priority, sequence_number, encoded_item)
```

The sequence number provides stable FIFO behavior for equal priorities.

For example:

```python
for i in range(5):
    q.push(i, priority=1)
```

will return:

```text
0
1
2
3
4
```

## Persistence and WAL

Every mutating operation is appended to a **Write-Ahead Log**.

The supported WAL operations are:

```text
P → PUSH
D → POP/DELETE
C → CLEAR
S → SNAPSHOT
```

The WAL is append-only and records the information necessary to reconstruct the in-memory heap.

With the default:

```python
sync=True
```

the WAL is `fsync()`'d after each mutation.

This means a successful mutation is persisted to the operating system's storage layer before the operation returns.

## Recovery

When the queue is opened, recovery happens in two stages:

1. Load the latest snapshot, if one exists.
2. Replay WAL records after the snapshot.

For example:

```text
queue.snap
     ↓
load snapshot
     ↓
queue.wal
     ↓
replay mutations
     ↓
reconstructed heap
```

This allows the queue to recover after a process restart.

A partially written final WAL record is tolerated. Recovery stops at the malformed record while preserving previously valid records.

## Snapshot and Compaction

The WAL would grow indefinitely if every operation remained in it.

To prevent this, the queue periodically performs **compaction**.

The default threshold is:

```python
compaction_threshold=10_000
```

For example:

```python
q = PersistentPriorityQueue(
    "./queue",
    compaction_threshold=1000
)
```

When the threshold is reached:

1. The current heap is written to a temporary snapshot.
2. The snapshot is flushed and `fsync()`'d.
3. The temporary snapshot is atomically renamed into place.
4. The existing WAL is rotated.
5. A new WAL is created.
6. A snapshot marker is written.
7. The old WAL backup is removed.

This limits WAL growth while maintaining crash-safe recovery.

## Serialization

Queue items can be arbitrary Python objects.

The implementation first checks whether an object can be represented by JSON **without changing its type or value**.

JSON is used for simple values such as:

```python
None
True
42
3.14
"hello"
[1, 2, 3]
{"name": "Alice"}
```

Objects that cannot be safely represented by JSON are serialized using:

```text
pickle → Base64 → JSON string
```

This preserves Python-specific structures such as:

```python
tuple
set
bytes
IntEnum
```

and nested structures containing these types.

Cyclic structures are also handled through the pickle path.

### Example

```python
item = {
    "name": "job",
    "parameters": (1, 2, 3)
}

q.push(item, priority=1)

priority, result = q.pop()

assert result == item
```

## Thread Safety

The queue uses a `threading.RLock` to protect its internal state.

This allows multiple threads to safely perform operations on the same queue instance.

For example:

```python
import threading

def producer(start):
    for i in range(start, start + 100):
        q.push(i, i)

threads = [
    threading.Thread(target=producer, args=(i * 100,))
    for i in range(4)
]

for t in threads:
    t.start()

for t in threads:
    t.join()
```

The test suite specifically verifies concurrent pushes and pops from multiple threads.

## Cross-Process Locking

On POSIX systems, the queue uses `fcntl.flock()` to acquire an exclusive lock.

This prevents two independent processes from opening and modifying the same queue simultaneously.

If another process already holds the lock, opening the queue raises a `RuntimeError`.

On platforms where `fcntl` is unavailable, this file-lock mechanism is disabled.

## Durability Model

With:

```python
sync=True
```

the queue performs:

```text
mutation
   ↓
update in-memory heap
   ↓
append WAL record
   ↓
fsync WAL
   ↓
return
```

This provides significantly stronger durability than a queue that only writes its state when the process exits.

For performance benchmarking, synchronization can be disabled:

```python
q = PersistentPriorityQueue("./queue", sync=False)
```

**Important:** `sync=False` should only be used when the durability guarantee is not required.

## Error Handling

### Empty Queue

```python
q.pop()
```

raises:

```python
IndexError: pop from an empty priority queue
```

Similarly:

```python
q.peek()
```

raises `IndexError` when empty.

### Invalid Priority

Priorities must be integers:

```python
q.push("item", 1.5)
```

raises:

```python
TypeError
```

Boolean values are also rejected even though `bool` is technically a subclass of `int`.

### Closed Queue

Operations performed after:

```python
q.close()
```

raise a `RuntimeError`.

## Running the Tests

Run the complete test suite with:

```bash
pytest -v
```

The tests cover:

* Basic queue operations
* Priority ordering
* FIFO behavior for equal priorities
* Persistence across reopen
* Replay of `pop()` operations
* Durable `clear()`
* Snapshot creation
* Compaction and recovery
* Torn/corrupt final WAL records
* Arbitrary Python objects
* Thread safety
* Empty queue errors
* Idempotent `close()`
* Priority type validation
* JSON vs pickle serialization paths
* Lossy JSON type protection
* Cyclic object serialization
* Persistence of pickled objects

The test suite contains dedicated checks for FIFO ordering and recovery across reopening.

## Project Structure

```text
.
├── module.py
├── test_module.py
└── README.md
```

### `module.py`

Contains the implementation of:

```python
PersistentPriorityQueue
```

including:

* Priority queue operations
* WAL management
* Recovery
* Snapshotting
* Compaction
* Serialization
* Thread synchronization
* POSIX file locking
* Context-manager support

### `test_module.py`

Contains the automated test suite for the implementation.

## Manual Demo

`module.py` includes a small command-line demonstration.

Run:

```bash
python module.py
```

It creates a queue, inserts several priorities, and pops them in priority order.

Expected behavior is approximately:

```text
(1, 'high')
(1, 'high-2')
(2, 'mid')
(3, 'low')
```

## Performance Considerations

Normal queue operations use a binary heap:

* `push()` — approximately **O(log n)**
* `pop()` — approximately **O(log n)**
* `peek()` — **O(1)**
* `size()` — **O(1)**
* `is_empty()` — **O(1)**

WAL appends are approximately **O(1)** apart from filesystem I/O.

Recovery requires reading the snapshot and replaying the WAL.

Compaction requires writing the current heap to disk and therefore costs approximately **O(n)** with respect to the number of queued entries.

## Durability vs Performance

| Configuration | Durability | Performance            |
| ------------- | ---------- | ---------------------- |
| `sync=True`   | Stronger   | Lower write throughput |
| `sync=False`  | Weaker     | Higher throughput      |

For production workloads where losing acknowledged operations after a crash is unacceptable, keep:

```python
sync=True
```

For benchmarks or workloads where durability is less important:

```python
sync=False
```

## Limitations

This implementation intentionally keeps the design relatively small.

Some limitations include:

* One process at a time can own a queue on POSIX systems.
* Cross-process locking relies on `fcntl`, so the locking mechanism is not available on non-POSIX platforms.
* `pickle` serialization should only be used with trusted queue data because unpickling untrusted data can execute arbitrary code.
* `sync=False` does not provide the same durability guarantee as the default configuration.
* The queue stores its active state in memory, so very large queues consume corresponding RAM.
* WAL and snapshot files are implementation details and should not be manually modified while the queue is active.

## Example: Persistent Job Queue

A practical use case is a simple persistent job scheduler:

```python
from module import PersistentPriorityQueue

with PersistentPriorityQueue("./jobs") as jobs:
    jobs.push(
        {"task": "send_email", "user": 101},
        priority=2
    )

    jobs.push(
        {"task": "database_backup"},
        priority=1
    )

    priority, job = jobs.pop()

    print(priority)
    print(job)
```

The highest-priority job will be processed first.

If the application exits and later starts again, unprocessed jobs remain available.

## Design Overview

```text
                 ┌─────────────────────┐
                 │ PersistentPriority  │
                 │       Queue         │
                 └──────────┬──────────┘
                            │
                 ┌──────────▼──────────┐
                 │   In-Memory Heap    │
                 │                     │
                 │ (priority, seq,     │
                 │  encoded_item)      │
                 └──────────┬──────────┘
                            │
                 ┌──────────▼──────────┐
                 │    Write-Ahead Log  │
                 │       (.wal)        │
                 └──────────┬──────────┘
                            │
                  threshold reached
                            │
                 ┌──────────▼──────────┐
                 │      Snapshot       │
                 │       (.snap)       │
                 └─────────────────────┘
```
The key idea is to keep normal operations in memory while persisting every mutation through the WAL and periodically replacing the accumulated history with a compact snapshot.

```

## Author

**Shubham Snehil**

---

Built as a lightweight example of implementing a durable data structure using:

* Binary heaps
* Write-Ahead Logging
* Crash recovery
* Atomic filesystem operations
* Serialization
* File locking
* Thread synchronization
* Automated testing

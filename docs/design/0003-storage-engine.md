# 0003 — LSM storage engine

> Draft written while building Phase 2. Rewrite it in your own words after reading Petrov's
> *Database Internals* chapter 7 and the RocksDB compaction wiki.

## Problem

Each node needs a local, ordered key-value store that:
- takes atomic multi-key batches;
- answers point reads and range scans;
- gives consistent snapshots, which Raft needs to ship a range to a lagging follower;
- never loses a synced write or exposes a half-written one, whatever the crash.

It must also run unchanged inside the deterministic simulator.

## Why an LSM tree and not a B+ tree

- **Writes are sequential appends** (log, then whole new files), not random in-place page
  updates. Raft's apply path is write-heavy.
- **Files are immutable once written.** Snapshots and concurrent readers are cheap: a reader pins
  a set of files and nothing changes under it.
- **This is what TiKV (RocksDB) and CockroachDB (Pebble) run.**

The cost is read amplification (a key may live in several files) and write amplification
(compaction rewrites data). Bloom filters and leveled compaction keep both bounded.

## Structure (`io.distdb.storage.lsm`)

```
write ─▶ WAL (000007.log) ─▶ memtable (skip list) ─▶ frozen memtable ─▶ L0 SSTable
                                                                          │ compaction
                                                   L1 … L6 (non-overlapping, 10× per level)
```

| Piece | File | What it does |
|---|---|---|
| Internal key | `InternalKey` | `userKey ‖ (seq << 8 | kind)`; sorts by user key ascending, then newest first |
| WAL / manifest records | `LogWriter`, `LogReader` | `crc32c | length | payload`; reader stops at the first bad record |
| Memtable | `Memtable` | `ConcurrentSkipListMap`, one writer and lock-free readers |
| SSTable | `TableBuilder`, `TableReader`, `Block`, `BloomFilter` | 4 KB prefix-compressed blocks with restart points, a per-table bloom filter, an index of each block's last key, a checksummed footer |
| Versions | `Version`, `VersionEdit`, `VersionSet` | which files are live at which level; edits appended to `MANIFEST-n`; `CURRENT` names the live manifest |
| Compaction | `Compaction` | LevelDB scoring; L0 is compacted as a whole; deeper levels round-robin, one file at a time |
| Engine | `LsmEngine` | ties it together |

## Invariants

1. **Sequence numbers are unique and increase.** A read at sequence `s` ignores newer entries.
   A batch becomes visible only once all of it is in the memtable, so readers never see half
   a batch.
2. **Newer data lives in shallower places:** memtable, frozen memtable, L0 newest-first, L1,
   and so on. Point reads stop at the first hit. L0 is always compacted entirely, because moving
   only some of its overlapping files down could put an older version above a newer one.
3. **Files in L1+ never overlap within a level.** Compaction outputs are cut only between user
   keys, so one key's versions never span two files at the same level. `Version.apply` checks
   this on every change.
4. **The file layout changes only through a synced manifest edit.** A table that a crashed flush
   or compaction wrote but never recorded is unreferenced, and recovery deletes it.
5. **A log is deleted only after a synced edit says its data is in a table** (`logNumber`).
6. **Recovery restores a prefix of the batch order.** The old log is synced before a new one
   starts, and replay stops at the first damaged record.
7. **Compaction drops a version only if no reader can ever need it:** it is shadowed by a newer
   version at or below the oldest open snapshot. It drops a tombstone only if nothing older can
   exist in deeper levels.

## fsync ordering (the part to know cold)

| Step | Order |
|---|---|
| New log | create → `syncDirectory` → use it (otherwise a synced write could vanish with its file name) |
| Memtable switch | sync old log → close → create new log → `syncDirectory` |
| Flush / compaction | write table → `sync` table → `syncDirectory` → append manifest edit → `sync` manifest → delete old files |
| New manifest (every open) | write `MANIFEST-n` → `sync` → write `CURRENT.tmp` → `sync` → rename to `CURRENT` → `syncDirectory` |
| Recovery | never reuse a file number found on disk, even one the manifest never recorded |

## Concurrency and determinism

- Readers capture `(memtable, frozen memtable, version, sequence)` atomically under a tiny lock
  and reference-count the version. Files are deleted only when no live version holds them.
- Lock order: writeLock → flush or compaction lock → `VersionSet` monitor → viewLock. Versions
  are installed inside the `VersionSet` lock, so they can't be installed out of order.
- Background work goes through `Scheduler`, and all I/O through `FileSystem`. When the writer
  would have to *wait* for background work (memtable full with a flush pending, or too many L0
  files), it does that work itself. That keeps writes bounded in production, and means a
  single-threaded simulator never deadlocks.

## Snapshots

A snapshot is just a sequence number registered with the engine. Reads through it use whatever
files are current at read time, and filter by sequence. That's safe because compaction keeps
every version that the oldest registered snapshot can see.

## MVCC keys (`io.distdb.storage.mvcc.MvccKey`)

`KeyWriter.writeBytes(userKey).writeLong(ts, DESC)`. All versions of a key share a prefix that no
other key has, and the newest version comes first, so a read at `t` is a seek to `key@t`. Phase 7
builds Percolator's data and write columns on this.

## Verification (the Phase 2 gate)

- **`LsmModelTest`:** a random mix of puts, deletes, batches, gets, scans, snapshot reads,
  background steps interleaved at random points, and clean reopens. Every read is compared
  with a `TreeMap`. Tiny size limits force thousands of flushes and compactions. 300k operations
  by default; the gate run used 2M (`-Ddistdb.storage.modelOps=2000000`).
- **`LsmCrashTest`:** many crash cycles per seed. The power is cut at an armed I/O operation
  (mid-append, mid-flush, mid-compaction, mid-recovery) or between calls. After each recovery,
  the contents must equal exactly a prefix of the batch history, ending on a batch boundary,
  and including every batch acknowledged with `sync = true`. 60 seeds by default; the gate run
  used 500.
- **`SimFileSystem`** enforces the `FileSystem` contract and no more. On a crash, unsynced
  data is kept, lost, or torn per page and sector; unsynced creates, renames and deletes may
  revert.
- A real-disk test with four writer threads and a concurrent scanner, then a reopen.

## Not yet (deliberately)

- Block cache (the OS page cache stands in), compression, a range-delete tombstone,
  group commit.
- A real fix for a full disk or I/O error: the engine fails stop, and you reopen it to recover.
- A cap on open tables: one file handle per live table.

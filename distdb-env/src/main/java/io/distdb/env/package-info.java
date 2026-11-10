/**
 * Determinism seams: every source of nondeterminism (time, randomness, network, disk, threads) sits
 * behind an interface here, with the real implementations alongside. The simulator supplies fakes.
 *
 * <p>Core modules (storage, raft, kv, pd, client, txn, sql) must reach the outside world only
 * through {@link io.distdb.env.Clock}, {@link io.distdb.env.RandomSource}, {@link
 * io.distdb.env.Transport}, {@link io.distdb.env.FileSystem} and {@link io.distdb.env.Scheduler}.
 * An ArchUnit test in {@code distdb-sim} enforces this.
 */
package io.distdb.env;

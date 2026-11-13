/**
 * The KV server. {@link io.distdb.kv.Store} is a data node of a sharded cluster: many ranges, each
 * its own Raft group, applied by {@link io.distdb.kv.RangeStateMachine}, sharing one engine and one
 * transport. {@link io.distdb.kv.ReplicatedKv} is a single replicated group (the placement
 * driver's, or a cluster without one), applied by {@link io.distdb.kv.KvStateMachine}. Writes go
 * through the replicated log; reads are linearizable through ReadIndex.
 */
package io.distdb.kv;

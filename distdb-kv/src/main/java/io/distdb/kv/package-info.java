/**
 * The KV server. {@link io.distdb.kv.ReplicatedKv} is one replica of a Raft group: writes go
 * through the replicated log and are applied by {@link io.distdb.kv.KvStateMachine}; reads are
 * linearizable through ReadIndex. A single node is a group of one.
 */
package io.distdb.kv;

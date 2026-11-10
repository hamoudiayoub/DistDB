/**
 * The KV server. In Phase 3 it is one node serving one {@link io.distdb.storage.KvEngine} ({@link
 * io.distdb.kv.KvService}); from Phase 5 it hosts Raft replicas of ranges and applies their
 * committed entries.
 */
package io.distdb.kv;

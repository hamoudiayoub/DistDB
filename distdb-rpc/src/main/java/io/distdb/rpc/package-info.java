/**
 * The wire protocol (Protocol Buffers, in {@code src/main/proto}) and the request/response seams
 * built on it: {@link io.distdb.rpc.KvChannel} on the client side and {@link
 * io.distdb.rpc.KvHandler} on the server side, with gRPC implementations in {@link
 * io.distdb.rpc.grpc}. Raft's one-way messages will use {@link io.distdb.env.Transport} (Phase 4).
 */
package io.distdb.rpc;

package io.distdb.env;

import io.distdb.common.NodeId;

/**
 * One-way, asynchronous messaging between nodes, for Raft traffic.
 *
 * <p>Delivery is deliberately weak, because that is what a real network gives and what the
 * simulator injects: a message may be lost, delayed, duplicated or reordered, but it is never
 * corrupted and never delivered to the wrong node. Raft tolerates all of this by design.
 *
 * <p>Client requests, which need an answer, use the request/response seam instead: {@code
 * KvChannel} and {@code KvHandler} in {@code distdb-rpc}, with retries and request ids in {@code
 * distdb-client}. The production implementation of this interface (gRPC) arrives with Raft; the
 * simulated one (in-memory queues with injected faults) in {@code distdb-sim}.
 */
public interface Transport {

  /** The node this transport sends from. */
  NodeId localNode();

  /**
   * Queues {@code message} for delivery to {@code to} and returns immediately. Never blocks on the
   * network and never reports delivery failure; a caller that needs an answer waits for a reply
   * message and gives up on a timer. The caller must not modify {@code message} afterwards.
   */
  void send(NodeId to, byte[] message);

  /**
   * Installs the handler that receives every inbound message, replacing any previous one. Messages
   * that arrive before a handler is installed are dropped.
   */
  void setHandler(MessageHandler handler);

  /** Receives inbound messages. */
  @FunctionalInterface
  interface MessageHandler {
    /** Called once per delivered message; must not block. */
    void onMessage(NodeId from, byte[] message);
  }
}

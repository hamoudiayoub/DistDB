package io.distdb.env.sim;

import java.io.IOException;

/**
 * Thrown by {@link SimFileSystem} once a crash has been triggered: the node is "dead", and every
 * file operation fails until the test calls {@link SimFileSystem#crash()} and restarts it.
 */
public final class SimulatedCrashException extends IOException {

  private static final long serialVersionUID = 1L;

  public SimulatedCrashException(String message) {
    super(message);
  }
}

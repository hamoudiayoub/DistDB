/**
 * The deterministic simulator: {@link io.distdb.sim.SimNetwork} (the simulated {@link
 * io.distdb.env.Transport}), {@link io.distdb.sim.Life} (a process's timers), and {@link
 * io.distdb.sim.RaftSimulation} with its {@link io.distdb.sim.RaftChecker}. A linearizability
 * checker for the replicated KV service arrives in Phase 5. Depends on everything; nothing depends
 * on it.
 */
package io.distdb.sim;

/**
 * The placement driver: where ranges are and where they should be. {@link
 * io.distdb.pd.PlacementDriver} runs on a Phase 5 replicated group, allocates range ids, keeps a
 * directory and store liveness from heartbeats, and schedules splits and moves (Phase 6). The
 * timestamp oracle joins in Phase 7.
 */
package io.distdb.pd;

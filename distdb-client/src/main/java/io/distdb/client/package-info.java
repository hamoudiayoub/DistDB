/**
 * The KV client: request ids, deadlines and the retry loop that turns typed errors and lost calls
 * into safe retries ({@link io.distdb.client.KvClient}). Range caching and routing arrive in Phase
 * 6.
 */
package io.distdb.client;

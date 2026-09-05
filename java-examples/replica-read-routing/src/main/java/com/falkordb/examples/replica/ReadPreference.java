package com.falkordb.examples.replica;

/**
 * Which nodes are allowed to serve reads.
 *
 * <p>Writes are unaffected by this setting. They always go to the primary, because a replica
 * rejects every write with {@code READONLY You can't write against a read only replica}.
 *
 * <p>The consistency column matters more than the throughput column. Any option that reads from a
 * replica can return data older than a write the primary has already acknowledged, because
 * replication is asynchronous. Choose per query, not once per application.
 */
public enum ReadPreference {

    /**
     * Send every read to the primary.
     *
     * <p>Strongest consistency: a read always observes writes this client has already made. The
     * cost is that replicas serve no read traffic, so their CPU sits idle while you pay for it.
     */
    PRIMARY_ONLY,

    /**
     * Send reads only to replicas, falling back to the primary when no replica is reachable.
     *
     * <p>Keeps read load off the primary so it can spend its capacity on writes. Reads may be
     * stale.
     */
    REPLICA_ONLY,

    /**
     * Rotate reads across the primary and every replica.
     *
     * <p>The highest aggregate utilization, since every node you pay for serves reads. Reads that
     * land on a replica may be stale. Reads that land on the primary will not be, which makes
     * staleness intermittent and therefore easy to miss in testing. Do not use this for a read that
     * must observe a write this client just made. Use {@link ReadPreference#PRIMARY_ONLY} or
     * {@code FalkorGraphFactory.readFromPrimary} for that case.
     */
    ROUND_ROBIN
}

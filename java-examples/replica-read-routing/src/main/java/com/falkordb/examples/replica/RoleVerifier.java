package com.falkordb.examples.replica;

import com.falkordb.Driver;
import redis.clients.jedis.Jedis;

/**
 * Reads the replication role a node reports about itself.
 *
 * <p>This exists because the alternative is a silent misconfiguration. If the endpoint you labelled
 * "primary" is actually a replica, every write fails at runtime. If an endpoint you labelled
 * "replica" is actually a second primary, reads quietly return data from an unrelated dataset that
 * nothing is replicating. Both are cheap to detect at startup and expensive to debug in production.
 *
 * <p>Roles are not static. A Sentinel managed deployment promotes a replica during failover, so a
 * role verified at startup can be wrong later. Verification is therefore a startup sanity check and
 * a diagnostic, not a guarantee. Production code should also handle a write failing with
 * {@code READONLY} by re-resolving the topology.
 */
public final class RoleVerifier {

    /** The role a node reports in {@code INFO replication}. */
    public enum Role {
        PRIMARY,
        REPLICA,
        UNKNOWN
    }

    private RoleVerifier() {}

    /**
     * Reports whether a node is running in Redis Cluster mode.
     *
     * <p>This example routes reads between one primary and its replicas. Cluster mode is a different
     * shape: keys are split across shards, so there is no single primary holding the whole dataset
     * and no single replica mirroring it. Routing built on that assumption would silently read from
     * a node that does not hold the key. Detecting it at startup turns a confusing wrong answer into
     * a clear error.
     *
     * @param driver a driver connected to the node to inspect
     * @return true when the node reports {@code cluster_enabled:1}, false when it reports otherwise
     *     or could not be reached
     */
    public static boolean clusterModeEnabled(Driver driver) {
        try (Jedis connection = driver.getConnection()) {
            String clusterInfo = connection.info("cluster");
            if (clusterInfo == null) {
                return false;
            }
            for (String line : clusterInfo.split("\\r?\\n")) {
                String trimmed = line.trim();
                if (trimmed.startsWith("cluster_enabled:")) {
                    return "1".equals(trimmed.substring("cluster_enabled:".length()).trim());
                }
            }
            return false;
        } catch (Exception unreachable) {
            return false;
        }
    }

    /**
     * Asks a node what role it currently believes it holds.
     *
     * @param driver a driver connected to the node to inspect
     * @return the reported role, or {@link Role#UNKNOWN} if the node could not be reached or the
     *     reply could not be parsed
     */
    public static Role reportedRole(Driver driver) {
        try (Jedis connection = driver.getConnection()) {
            String replicationInfo = connection.info("replication");
            if (replicationInfo == null) {
                return Role.UNKNOWN;
            }
            for (String line : replicationInfo.split("\\r?\\n")) {
                String trimmed = line.trim();
                if (trimmed.startsWith("role:")) {
                    String role = trimmed.substring("role:".length()).trim();
                    if ("master".equalsIgnoreCase(role)) {
                        return Role.PRIMARY;
                    }
                    // Redis still reports the replica role using its historical name.
                    if ("slave".equalsIgnoreCase(role)) {
                        return Role.REPLICA;
                    }
                    return Role.UNKNOWN;
                }
            }
            return Role.UNKNOWN;
        } catch (Exception unreachable) {
            return Role.UNKNOWN;
        }
    }
}

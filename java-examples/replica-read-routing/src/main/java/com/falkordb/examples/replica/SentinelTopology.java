package com.falkordb.examples.replica;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;

/**
 * Asks Sentinel which node is currently the primary and which are its replicas.
 *
 * <p>Hard coding a primary hostname works until the first failover, at which point the node you
 * named is a replica and every write fails. Sentinel already tracks the answer, so ask it rather
 * than keeping a second copy of the truth in your configuration.
 *
 * <p><strong>This resolves a primary/replica deployment only.</strong> Sentinel does not manage
 * Redis Cluster, so there is nothing here that applies to a sharded setup. See the note on cluster
 * mode in {@link FalkorGraphFactory}.
 *
 * <h2>Sentinel settings are not the data settings</h2>
 *
 * <p>The Sentinel port and the data port are configured independently, and real deployments differ.
 * One FalkorDB Cloud instance was observed serving Sentinel over TLS while another served both
 * Sentinel and data in plaintext. TLS and credentials are therefore configured separately for the
 * Sentinel connection and for the data nodes it returns, instead of assuming they match.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * SentinelTopology.Topology topology = SentinelTopology.builder()
 *         .sentinel(Endpoint.of(sentinelHost, 26379, user, password, false))
 *         .dataCredentials(user, password)
 *         .dataTls(false)
 *         .build()
 *         .discover();
 *
 * FalkorGraphFactory factory = FalkorGraphFactory.builder()
 *         .primary(topology.getPrimary())
 *         .replicas(topology.getReplicas())
 *         .readPreference(ReadPreference.ROUND_ROBIN)
 *         .build();
 * }</pre>
 */
public final class SentinelTopology {

    private static final int DEFAULT_TIMEOUT_MILLIS = 5_000;

    private final Endpoint sentinel;
    private final String masterName;
    private final String dataUsername;
    private final String dataPassword;
    private final boolean dataTlsEnabled;
    private final int timeoutMillis;

    private SentinelTopology(Builder builder) {
        if (builder.sentinel == null) {
            throw new IllegalArgumentException("a sentinel endpoint is required");
        }
        this.sentinel = builder.sentinel;
        this.masterName = builder.masterName;
        this.dataUsername = builder.dataUsername;
        this.dataPassword = builder.dataPassword;
        this.dataTlsEnabled = builder.dataTlsEnabled;
        this.timeoutMillis = builder.timeoutMillis;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * The primary and replicas Sentinel reports right now.
     *
     * <p>This is a snapshot, not a subscription. A failover after this call leaves the result stale,
     * so re-run discovery when a write fails with {@code READONLY}.
     *
     * @return the current topology
     * @throws IllegalStateException if Sentinel cannot be reached, knows no monitored primary, or
     *     monitors several and no master name was given to disambiguate
     */
    public Topology discover() {
        JedisClientConfig config = DefaultJedisClientConfig.builder()
                .user(sentinel.getUsername())
                .password(sentinel.getPassword())
                .ssl(sentinel.isTlsEnabled())
                .connectionTimeoutMillis(timeoutMillis)
                .socketTimeoutMillis(timeoutMillis)
                .build();

        try (Jedis sentinelConnection =
                new Jedis(new HostAndPort(sentinel.getHost(), sentinel.getPort()), config)) {

            String resolvedMasterName = resolveMasterName(sentinelConnection);

            List<String> primaryAddress = sentinelConnection.sentinelGetMasterAddrByName(resolvedMasterName);
            if (primaryAddress == null || primaryAddress.size() < 2) {
                throw new IllegalStateException(
                        "Sentinel returned no address for master '" + resolvedMasterName + "'");
            }
            Endpoint primary = dataEndpoint(primaryAddress.get(0), Integer.parseInt(primaryAddress.get(1)));

            List<Endpoint> replicas = new ArrayList<>();
            for (Map<String, String> replica : sentinelConnection.sentinelReplicas(resolvedMasterName)) {
                // Sentinel keeps reporting replicas it believes are down. Routing reads to one
                // would fail every request sent its way, so drop them here.
                String flags = replica.getOrDefault("flags", "");
                if (flags.contains("s_down") || flags.contains("o_down") || flags.contains("disconnected")) {
                    continue;
                }
                String host = replica.get("ip");
                String port = replica.get("port");
                if (host == null || port == null) {
                    continue;
                }
                replicas.add(dataEndpoint(host, Integer.parseInt(port)));
            }

            return new Topology(resolvedMasterName, primary, replicas);
        } catch (IllegalStateException alreadyDescriptive) {
            throw alreadyDescriptive;
        } catch (Exception failure) {
            throw new IllegalStateException(
                    "could not discover topology from Sentinel at " + sentinel + ": " + failure.getMessage(),
                    failure);
        }
    }

    private String resolveMasterName(Jedis sentinelConnection) {
        if (masterName != null) {
            return masterName;
        }
        List<Map<String, String>> monitored = sentinelConnection.sentinelMasters();
        if (monitored.isEmpty()) {
            throw new IllegalStateException("Sentinel at " + sentinel + " monitors no primary");
        }
        if (monitored.size() > 1) {
            List<String> names = new ArrayList<>();
            for (Map<String, String> master : monitored) {
                names.add(master.get("name"));
            }
            throw new IllegalStateException(
                    "Sentinel monitors several primaries " + names + ", so masterName must be set");
        }
        return monitored.get(0).get("name");
    }

    private Endpoint dataEndpoint(String host, int port) {
        return Endpoint.of(host, port, dataUsername, dataPassword, dataTlsEnabled);
    }

    /** A primary and its reachable replicas, as Sentinel reported them at one moment. */
    public static final class Topology {

        private final String masterName;
        private final Endpoint primary;
        private final List<Endpoint> replicas;

        Topology(String masterName, Endpoint primary, List<Endpoint> replicas) {
            this.masterName = masterName;
            this.primary = primary;
            this.replicas = Collections.unmodifiableList(new ArrayList<>(replicas));
        }

        public String getMasterName() {
            return masterName;
        }

        public Endpoint getPrimary() {
            return primary;
        }

        /** Reachable replicas, which may be empty if the primary is running alone. */
        public List<Endpoint> getReplicas() {
            return replicas;
        }

        @Override
        public String toString() {
            return "master '" + masterName + "' primary=" + primary + " replicas=" + replicas;
        }
    }

    /** Collects the Sentinel connection details and the data node settings they imply. */
    public static final class Builder {

        private Endpoint sentinel;
        private String masterName;
        private String dataUsername;
        private String dataPassword;
        private boolean dataTlsEnabled;
        private int timeoutMillis = DEFAULT_TIMEOUT_MILLIS;

        /** Where Sentinel listens, including the TLS and credential settings that port needs. */
        public Builder sentinel(Endpoint sentinelEndpoint) {
            this.sentinel = sentinelEndpoint;
            return this;
        }

        /** The monitored primary to resolve. Optional when Sentinel monitors exactly one. */
        public Builder masterName(String name) {
            this.masterName = name;
            return this;
        }

        /** Credentials for the data nodes, which need not match the Sentinel credentials. */
        public Builder dataCredentials(String username, String password) {
            this.dataUsername = username;
            this.dataPassword = password;
            return this;
        }

        /** Whether the data port requires TLS, which is independent of the Sentinel port. */
        public Builder dataTls(boolean enabled) {
            this.dataTlsEnabled = enabled;
            return this;
        }

        public Builder timeoutMillis(int millis) {
            if (millis < 1) {
                throw new IllegalArgumentException("timeoutMillis must be positive, but was " + millis);
            }
            this.timeoutMillis = millis;
            return this;
        }

        public SentinelTopology build() {
            return new SentinelTopology(this);
        }
    }
}

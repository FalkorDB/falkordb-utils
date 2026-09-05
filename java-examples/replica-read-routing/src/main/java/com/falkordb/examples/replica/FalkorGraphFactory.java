package com.falkordb.examples.replica;

import com.falkordb.Driver;
import com.falkordb.Graph;
import com.falkordb.impl.api.DriverImpl;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hands out FalkorDB graph handles that are already routed to the right node.
 *
 * <p>Writes go to the primary. Reads go wherever {@link ReadPreference} allows. Application code
 * asks for a write handle or a read handle and never repeats the routing decision.
 *
 * <h2>Why a factory</h2>
 *
 * <p>The routing rule is one line of policy and it must hold everywhere. Spread across an
 * application it becomes one forgotten call away from sending a write to a replica, or from sending
 * every read to the primary and leaving replicas idle. Centralising it means the rule is stated
 * once and enforced by construction.
 *
 * <h2>Lifecycle, which is the part most easy to get wrong</h2>
 *
 * <p>Build one factory for the lifetime of the application and share it. Do not build one per
 * request.
 *
 * <p>Each {@link Driver} owns a connection pool, and each {@link Graph} owns a schema cache that
 * maps the integer ids in a compact reply back to label and property names. Both are expensive to
 * create and both are designed to be shared: a graph borrows a pooled connection per call, and its
 * cache is guarded by a lock chosen so it does not pin virtual threads. Creating a factory per
 * request would build a fresh pool and a cold cache every time, which costs far more than the query
 * itself.
 *
 * <p>This class is thread safe. Handles it returns are safe to use concurrently.
 *
 * <h2>Requires jfalkordb 0.11.1 or newer</h2>
 *
 * <p>Earlier versions warmed the schema cache with {@code GRAPH.QUERY}, a write command, so the
 * first replica read that returned a node, a relationship or a path failed with {@code READONLY}.
 * Reads that returned only scalars worked, which made the failure look intermittent. Fixed in
 * 0.11.1.
 *
 * <h2>Primary and replica only, not Redis Cluster</h2>
 *
 * <p>This targets a deployment with one primary holding the whole dataset and replicas mirroring it,
 * which is what FalkorDB Cloud provisions and what Sentinel manages. It does not support Redis
 * Cluster. Under cluster mode the keyspace is sharded across several primaries, so "send this read
 * to a replica instead" is no longer a single decision: the client first has to know which shard
 * owns the key. Routing that ignores sharding returns a wrong answer instead of an error, so the
 * factory checks {@code cluster_enabled} at startup and refuses to build.
 *
 * <h2>Staleness</h2>
 *
 * <p>Replication is asynchronous, so a read served by a replica can return data older than a write
 * the primary has already acknowledged. For a read that must observe your own recent write, use
 * {@link #readFromPrimary}.
 */
public final class FalkorGraphFactory implements AutoCloseable {

    private final Driver primaryDriver;
    private final List<Driver> replicaDrivers;
    private final ReadPreference readPreference;
    private final Endpoint primaryEndpoint;
    private final List<Endpoint> replicaEndpoints;

    // One rotation per preference, each with its own cursor so that mixing preferences at runtime
    // does not make any single rotation unfair.
    private final Map<ReadPreference, Rotation> rotationsByPreference =
            new java.util.EnumMap<>(ReadPreference.class);

    // One Graph per driver and graph name, reused so its schema cache stays warm.
    private final Map<Driver, Map<String, Graph>> graphsByDriver = new ConcurrentHashMap<>();

    /** An ordered set of nodes serving one preference, plus its round robin position. */
    private static final class Rotation {
        private final List<Driver> drivers;
        // Wraps on overflow, which is harmless because it is always reduced modulo the size.
        private final AtomicInteger cursor = new AtomicInteger();

        Rotation(List<Driver> drivers) {
            this.drivers = Collections.unmodifiableList(drivers);
        }

        Driver next() {
            if (drivers.size() == 1) {
                return drivers.get(0);
            }
            return drivers.get(Math.floorMod(cursor.getAndIncrement(), drivers.size()));
        }
    }

    private FalkorGraphFactory(Builder builder) {
        this.readPreference = builder.readPreference;
        this.primaryEndpoint = builder.primaryEndpoint;
        this.replicaEndpoints = Collections.unmodifiableList(new ArrayList<>(builder.replicaEndpoints));

        this.primaryDriver = createDriver(builder.primaryEndpoint, builder);

        List<Driver> replicas = new ArrayList<>(builder.replicaEndpoints.size());
        for (Endpoint replicaEndpoint : builder.replicaEndpoints) {
            replicas.add(createDriver(replicaEndpoint, builder));
        }
        this.replicaDrivers = Collections.unmodifiableList(replicas);

        for (ReadPreference preference : ReadPreference.values()) {
            rotationsByPreference.put(preference, new Rotation(buildRotation(preference)));
        }

        if (builder.verifyRolesOnStartup) {
            rejectClusterMode();
            verifyRoles();
        }
    }

    /**
     * Fails fast when the primary is running in Redis Cluster mode.
     *
     * <p>Everything in this class assumes one primary holding the whole dataset with replicas
     * mirroring it. Under cluster mode the keyspace is sharded, so a read routed to a node that does
     * not own the key returns a wrong answer rather than an error. Better to refuse at startup than
     * to serve that quietly.
     */
    private void rejectClusterMode() {
        if (RoleVerifier.clusterModeEnabled(primaryDriver)) {
            throw new IllegalStateException(
                    "node " + primaryEndpoint + " reports cluster_enabled:1. This example routes reads "
                            + "across a primary and its replicas and does not support Redis Cluster, "
                            + "where the keyspace is sharded across several primaries.");
        }
    }

    private List<Driver> buildRotation(ReadPreference preference) {
        List<Driver> rotation = new ArrayList<>();
        switch (preference) {
            case PRIMARY_ONLY:
                rotation.add(primaryDriver);
                break;
            case REPLICA_ONLY:
                rotation.addAll(replicaDrivers);
                // Falling back to the primary beats failing every read outright. Visible through
                // readEndpoints() so the fallback is never silent.
                if (rotation.isEmpty()) {
                    rotation.add(primaryDriver);
                }
                break;
            case ROUND_ROBIN:
                rotation.add(primaryDriver);
                rotation.addAll(replicaDrivers);
                break;
            default:
                throw new IllegalStateException("unhandled read preference: " + preference);
        }
        return rotation;
    }

    private static Driver createDriver(Endpoint endpoint, Builder builder) {
        return DriverImpl.create(
                endpoint.getHost(),
                endpoint.getPort(),
                endpoint.getUsername(),
                endpoint.getPassword(),
                endpoint.isTlsEnabled(),
                builder.connectionTimeoutMillis,
                builder.socketTimeoutMillis,
                builder.poolMaxTotal,
                builder.poolMaxIdle,
                builder.poolMaxWait);
    }

    /**
     * A graph handle bound to the primary. Use for every write.
     *
     * @param graphName the graph to address
     * @return a handle that always targets the primary
     */
    public Graph write(String graphName) {
        return graphFor(primaryDriver, graphName);
    }

    /**
     * A graph handle bound to a node chosen by the configured {@link ReadPreference}.
     *
     * <p>Issue reads through {@code readOnlyQuery}, which sends {@code GRAPH.RO_QUERY}. A replica
     * rejects {@code GRAPH.QUERY} even when the query only reads, because the command is flagged as
     * a write.
     *
     * @param graphName the graph to address
     * @return a handle targeting whichever node is next in the read rotation
     */
    public Graph read(String graphName) {
        return read(graphName, readPreference);
    }

    /**
     * A graph handle for a single read, overriding the configured preference.
     *
     * <p>Consistency is a property of the query, not of the application. A dashboard aggregate can
     * tolerate a stale replica read. A balance check immediately after a debit cannot. This
     * overload lets one factory serve both.
     *
     * @param graphName the graph to address
     * @param preference which nodes may serve this particular read
     * @return a handle targeting whichever node is next in that preference's rotation
     */
    public Graph read(String graphName, ReadPreference preference) {
        if (preference == null) {
            throw new IllegalArgumentException("preference must not be null");
        }
        return graphFor(rotationsByPreference.get(preference).next(), graphName);
    }

    /**
     * A graph handle bound to the primary, for a read that must observe your own recent write.
     *
     * <p>This is the escape hatch from asynchronous replication. Reading your own write through
     * {@link #read} can miss it when the read lands on a replica that has not caught up yet.
     *
     * @param graphName the graph to address
     * @return a handle that always targets the primary
     */
    public Graph readFromPrimary(String graphName) {
        return graphFor(primaryDriver, graphName);
    }

    private Graph graphFor(Driver driver, String graphName) {
        if (graphName == null || graphName.trim().isEmpty()) {
            throw new IllegalArgumentException("graphName must not be null or blank");
        }
        return graphsByDriver
                .computeIfAbsent(driver, unused -> new ConcurrentHashMap<>())
                .computeIfAbsent(graphName, driver::graph);
    }

    /**
     * Asks every configured node what replication role it reports.
     *
     * <p>Useful as a health check and to confirm a routing configuration before load is applied.
     * Roles change during failover, so treat the answer as a point in time observation.
     *
     * @return the role reported by each endpoint, primary first
     */
    public Map<Endpoint, RoleVerifier.Role> reportedRoles() {
        Map<Endpoint, RoleVerifier.Role> roles = new java.util.LinkedHashMap<>();
        roles.put(primaryEndpoint, RoleVerifier.reportedRole(primaryDriver));
        for (int i = 0; i < replicaEndpoints.size(); i++) {
            roles.put(replicaEndpoints.get(i), RoleVerifier.reportedRole(replicaDrivers.get(i)));
        }
        return roles;
    }

    /**
     * How many read commands each node has served since it started.
     *
     * <p>This is the server's own count of {@code graph.RO_QUERY} calls, taken from
     * {@code INFO commandstats}. It answers the question the client cannot answer honestly about
     * itself: did the reads actually land where the routing intended.
     *
     * <p>Counters are cumulative and cannot be reset without {@code CONFIG RESETSTAT}, which
     * managed platforms often withhold from the application user. Take a reading before and after a
     * workload and use the difference.
     *
     * @return calls served per endpoint, primary first, or -1 for a node that could not be reached
     */
    public Map<Endpoint, Long> readCallsServed() {
        Map<Endpoint, Long> callsByEndpoint = new java.util.LinkedHashMap<>();
        callsByEndpoint.put(primaryEndpoint, roQueryCalls(primaryDriver));
        for (int i = 0; i < replicaEndpoints.size(); i++) {
            callsByEndpoint.put(replicaEndpoints.get(i), roQueryCalls(replicaDrivers.get(i)));
        }
        return callsByEndpoint;
    }

    private static long roQueryCalls(Driver driver) {
        try (redis.clients.jedis.Jedis connection = driver.getConnection()) {
            String commandStats = connection.info("commandstats");
            if (commandStats == null) {
                return -1L;
            }
            for (String line : commandStats.split("\\r?\\n")) {
                // Format: cmdstat_graph.RO_QUERY:calls=15,usec=10775,...
                if (line.startsWith("cmdstat_graph.RO_QUERY:")) {
                    for (String field : line.substring(line.indexOf(':') + 1).split(",")) {
                        if (field.startsWith("calls=")) {
                            return Long.parseLong(field.substring("calls=".length()));
                        }
                    }
                }
            }
            // The command exists but has never been called on this node.
            return 0L;
        } catch (Exception unreachable) {
            return -1L;
        }
    }

    private void verifyRoles() {
        RoleVerifier.Role primaryRole = RoleVerifier.reportedRole(primaryDriver);
        if (primaryRole == RoleVerifier.Role.REPLICA) {
            throw new IllegalStateException("endpoint configured as primary reports role:slave: " + primaryEndpoint
                    + ". Every write would fail with READONLY.");
        }
        for (int i = 0; i < replicaEndpoints.size(); i++) {
            RoleVerifier.Role replicaRole = RoleVerifier.reportedRole(replicaDrivers.get(i));
            if (replicaRole == RoleVerifier.Role.PRIMARY) {
                throw new IllegalStateException("endpoint configured as replica reports role:master: "
                        + replicaEndpoints.get(i)
                        + ". Reads would return data from an unrelated dataset.");
            }
        }
    }

    /** The endpoints serving reads under the given preference, in rotation order. */
    public List<Endpoint> readEndpoints(ReadPreference preference) {
        List<Endpoint> endpoints = new ArrayList<>();
        for (Driver driver : rotationsByPreference.get(preference).drivers) {
            endpoints.add(endpointFor(driver));
        }
        return endpoints;
    }

    /** The endpoints serving reads under the configured preference, in rotation order. */
    public List<Endpoint> readEndpoints() {
        return readEndpoints(readPreference);
    }

    private Endpoint endpointFor(Driver driver) {
        if (driver == primaryDriver) {
            return primaryEndpoint;
        }
        int replicaIndex = replicaDrivers.indexOf(driver);
        if (replicaIndex < 0) {
            throw new IllegalStateException("driver is not part of this factory");
        }
        return replicaEndpoints.get(replicaIndex);
    }

    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public void close() {
        Exception firstFailure = null;
        List<Driver> allDrivers = new ArrayList<>();
        allDrivers.add(primaryDriver);
        allDrivers.addAll(replicaDrivers);
        for (Driver driver : allDrivers) {
            try {
                driver.close();
            } catch (Exception failure) {
                // Keep closing the rest, then report. Leaking a pool is worse than a late throw.
                if (firstFailure == null) {
                    firstFailure = failure;
                }
            }
        }
        graphsByDriver.clear();
        if (firstFailure != null) {
            throw new IllegalStateException("failed to close one or more drivers", firstFailure);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Collects configuration and validates it before any connection is opened. */
    public static final class Builder {

        private Endpoint primaryEndpoint;
        private final List<Endpoint> replicaEndpoints = new ArrayList<>();
        private ReadPreference readPreference = ReadPreference.ROUND_ROBIN;
        private boolean verifyRolesOnStartup = true;

        private int connectionTimeoutMillis = 2000;
        // 0 means no client side read deadline, matching the driver default. A graph query can
        // legitimately run longer than any fixed timeout, and cutting the connection does not stop
        // the server executing it.
        private int socketTimeoutMillis = 0;
        private int poolMaxTotal = 16;
        private int poolMaxIdle = 16;
        private Duration poolMaxWait = Duration.ofSeconds(5);

        public Builder primary(Endpoint endpoint) {
            this.primaryEndpoint = endpoint;
            return this;
        }

        public Builder replica(Endpoint endpoint) {
            this.replicaEndpoints.add(endpoint);
            return this;
        }

        /** Adds every replica at once, which is what {@link SentinelTopology} returns. */
        public Builder replicas(Collection<Endpoint> endpoints) {
            if (endpoints != null) {
                this.replicaEndpoints.addAll(endpoints);
            }
            return this;
        }

        /**
         * Takes the primary and replicas straight from Sentinel discovery.
         *
         * <p>Preferred over naming hosts by hand, because a failover changes which node is the
         * primary and a hard coded name does not follow.
         */
        public Builder topology(SentinelTopology.Topology topology) {
            return primary(topology.getPrimary()).replicas(topology.getReplicas());
        }

        public Builder readPreference(ReadPreference preference) {
            this.readPreference = preference;
            return this;
        }

        /** Disable the startup role check. Only sensible in a test that fakes the topology. */
        public Builder verifyRolesOnStartup(boolean verify) {
            this.verifyRolesOnStartup = verify;
            return this;
        }

        /**
         * Maximum pooled connections per node.
         *
         * <p>Size this at or above the number of application threads that will issue queries
         * concurrently. A pool smaller than the thread count makes threads queue on the pool, which
         * looks exactly like a slow database and is a common way to misdiagnose one. The driver
         * default is 8.
         */
        public Builder poolMaxTotal(int maxTotal) {
            this.poolMaxTotal = maxTotal;
            return this;
        }

        public Builder poolMaxIdle(int maxIdle) {
            this.poolMaxIdle = maxIdle;
            return this;
        }

        public Builder poolMaxWait(Duration maxWait) {
            this.poolMaxWait = maxWait;
            return this;
        }

        public Builder connectionTimeoutMillis(int timeoutMillis) {
            this.connectionTimeoutMillis = timeoutMillis;
            return this;
        }

        public Builder socketTimeoutMillis(int timeoutMillis) {
            this.socketTimeoutMillis = timeoutMillis;
            return this;
        }

        public FalkorGraphFactory build() {
            if (primaryEndpoint == null) {
                throw new IllegalStateException("a primary endpoint is required");
            }
            if (readPreference == ReadPreference.REPLICA_ONLY && replicaEndpoints.isEmpty()) {
                throw new IllegalStateException(
                        "REPLICA_ONLY was requested but no replica endpoint was configured");
            }
            if (poolMaxIdle > poolMaxTotal) {
                throw new IllegalStateException("poolMaxIdle must not exceed poolMaxTotal");
            }
            return new FalkorGraphFactory(this);
        }
    }
}

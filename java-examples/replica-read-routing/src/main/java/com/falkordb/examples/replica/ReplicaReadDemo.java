package com.falkordb.examples.replica;

import com.falkordb.Graph;
import com.falkordb.Record;
import com.falkordb.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A runnable walkthrough of {@link FalkorGraphFactory} against a primary and a replica.
 *
 * <p>Defaults match the two local containers used during development:
 *
 * <pre>
 *   docker run -d --name fkr-primary -p 6399:6379 falkordb/falkordb:latest
 *   docker run -d --name fkr-replica -p 6400:6379 falkordb/falkordb:latest
 *   docker exec fkr-replica redis-cli REPLICAOF &lt;primary-host&gt; 6379
 * </pre>
 *
 * <p>Override with system properties, for example:
 *
 * <pre>
 *   -Dprimary.host=... -Dprimary.port=6379 -Dreplica.host=... -Dtls=true -Duser=falkordb -Dpassword=...
 * </pre>
 */
public final class ReplicaReadDemo {

    private static final String GRAPH_NAME = "replica_read_demo";
    private static final int WORKER_THREADS = 4;
    private static final int READS_PER_WORKER = 250;

    public static void main(String[] args) throws Exception {
        FalkorGraphFactory.Builder builder = FalkorGraphFactory.builder()
                .readPreference(ReadPreference.ROUND_ROBIN)
                // Sized at or above the worker count so threads never queue on the pool.
                .poolMaxTotal(WORKER_THREADS * 2)
                .poolMaxIdle(WORKER_THREADS * 2);

        String sentinelHost = System.getProperty("sentinel.host");
        if (sentinelHost != null && !sentinelHost.trim().isEmpty()) {
            // Preferred path. Sentinel already knows which node is primary, so do not keep a
            // second copy of that answer in configuration where a failover can invalidate it.
            SentinelTopology.Topology topology = SentinelTopology.builder()
                    .sentinel(Endpoint.of(
                            sentinelHost,
                            Integer.parseInt(System.getProperty("sentinel.port", "26379")),
                            user(),
                            password(),
                            Boolean.parseBoolean(System.getProperty("sentinel.tls", "false"))))
                    .masterName(System.getProperty("sentinel.master"))
                    .dataCredentials(user(), password())
                    .dataTls(tlsEnabled())
                    .build()
                    .discover();

            System.out.println("discovered via Sentinel at " + sentinelHost);
            System.out.println("  " + topology);
            System.out.println();
            builder.topology(topology);
        } else {
            Endpoint primary = endpointFromProperties("primary", 6399);
            List<Endpoint> replicas = replicaEndpointsFromProperties();
            System.out.println("primary  : " + primary);
            System.out.println("replicas : " + replicas);
            System.out.println();
            builder.primary(primary).replicas(replicas);
        }

        try (FalkorGraphFactory factory = builder.build()) {
            reportTopology(factory);
            seedData(factory);
            demonstrateStaleness(factory);
            runConcurrentReads(factory);
            cleanUp(factory);
        }
    }

    /** Confirms each endpoint really holds the role the configuration claims. */
    private static void reportTopology(FalkorGraphFactory factory) {
        System.out.println("-- topology --");
        for (Map.Entry<Endpoint, RoleVerifier.Role> entry : factory.reportedRoles().entrySet()) {
            System.out.printf("   %-28s %s%n", entry.getKey(), entry.getValue());
        }
        System.out.println("   reads rotate over: " + factory.readEndpoints());
        System.out.println();
    }

    /** Writes go through the write handle, which always targets the primary. */
    private static void seedData(FalkorGraphFactory factory) {
        System.out.println("-- seeding through the primary --");
        Graph writeGraph = factory.write(GRAPH_NAME);
        writeGraph.query("MATCH (n) DETACH DELETE n");
        writeGraph.query(
                "UNWIND range(1, 200) AS i "
                        + "CREATE (m:Machine {machineId: i, name: 'machine-' + i}) "
                        + "CREATE (m)-[:EMITS]->(:Reading {value: i * 10})");
        // An index keeps the demo read on the fast path. Without it the lookup degrades to a
        // label scan, which is the single most common cause of a slow graph read.
        writeGraph.query("CREATE INDEX FOR (m:Machine) ON (m.machineId)");
        System.out.println("   created 200 machines with one reading each");
        System.out.println();
    }

    /**
     * Shows the consistency tradeoff rather than describing it.
     *
     * <p>A write is made to the primary and read back immediately two ways. The primary read must
     * observe it. The routed read may not, because replication is asynchronous.
     */
    private static void demonstrateStaleness(FalkorGraphFactory factory) {
        System.out.println("-- read your writes --");
        long marker = System.currentTimeMillis();
        Map<String, Object> params = new HashMap<>();
        params.put("marker", marker);
        factory.write(GRAPH_NAME).query("CREATE (:Marker {stamp: $marker})", params);

        // Read both ways immediately, with no pause, so replication has the least chance to catch
        // up. REPLICA_ONLY is forced here: leaving it to the default rotation could land on the
        // primary, and the demonstration would prove nothing.
        boolean primarySeesIt = countMarkers(factory.readFromPrimary(GRAPH_NAME), marker) > 0;
        boolean replicaSeesIt =
                countMarkers(factory.read(GRAPH_NAME, ReadPreference.REPLICA_ONLY), marker) > 0;

        System.out.println("   immediately after the write:");
        System.out.println("     readFromPrimary sees it : " + primarySeesIt);
        System.out.println("     replica read sees it    : " + replicaSeesIt);
        if (replicaSeesIt) {
            System.out.println("     ^ the replica kept up this time. Over a loopback link the window");
            System.out.println("       is often under a millisecond, but it is never guaranteed to be");
            System.out.println("       zero, and it widens with distance and write volume.");
        } else {
            System.out.println("     ^ replication had not caught up: this is the staleness window");
        }
        System.out.println("   use readFromPrimary when a read must observe your own write");
        System.out.println();
    }

    private static long countMarkers(Graph graph, long marker) {
        Map<String, Object> params = new HashMap<>();
        params.put("marker", marker);
        ResultSet resultSet = graph.readOnlyQuery("MATCH (m:Marker {stamp: $marker}) RETURN count(m)", params);
        Record record = resultSet.iterator().next();
        return ((Number) record.getValue(0)).longValue();
    }

    /**
     * Drives reads from several threads at once, which is the case the factory exists for.
     *
     * <p>Handles are fetched per read so the rotation advances. The underlying drivers and graphs
     * are reused by the factory, so this does not create connections per call.
     */
    private static void runConcurrentReads(FalkorGraphFactory factory) throws Exception {
        System.out.println("-- concurrent reads across " + WORKER_THREADS + " threads --");
        Map<Endpoint, Long> callsBefore = factory.readCallsServed();
        ExecutorService pool = Executors.newFixedThreadPool(WORKER_THREADS);
        AtomicInteger completed = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();

        long startedAt = System.nanoTime();
        for (int worker = 0; worker < WORKER_THREADS; worker++) {
            final int workerIndex = worker;
            pool.submit(() -> {
                for (int i = 0; i < READS_PER_WORKER; i++) {
                    Map<String, Object> params = new HashMap<>();
                    // Vary the anchor so the demo reads across the data rather than one hot node.
                    params.put("machineId", 1 + ((i * 7 + workerIndex * 31) % 200));
                    try {
                        // Returning a node exercises the schema cache, which is the path that
                        // failed against replicas before jfalkordb 0.11.1.
                        ResultSet resultSet = factory.read(GRAPH_NAME)
                                .readOnlyQuery(
                                        "MATCH (m:Machine {machineId: $machineId})-[:EMITS]->(r:Reading) RETURN m, r",
                                        params);
                        resultSet.iterator().forEachRemaining(record -> {});
                        completed.incrementAndGet();
                    } catch (RuntimeException failure) {
                        if (failed.getAndIncrement() == 0) {
                            System.out.println("   first failure: " + failure.getMessage());
                        }
                    }
                }
            });
        }
        pool.shutdown();
        if (!pool.awaitTermination(2, TimeUnit.MINUTES)) {
            throw new IllegalStateException("reads did not finish within the timeout");
        }
        double elapsedSeconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;

        System.out.printf("   completed %,d reads in %.2fs (%,.0f reads/sec)%n",
                completed.get(), elapsedSeconds, completed.get() / elapsedSeconds);
        System.out.println("   failed: " + failed.get());

        // Server side truth. The client believes it rotated; this is the database confirming it.
        Map<Endpoint, Long> callsAfter = factory.readCallsServed();
        System.out.println("   reads served per node, counted by the server:");
        for (Map.Entry<Endpoint, Long> entry : callsAfter.entrySet()) {
            long before = callsBefore.getOrDefault(entry.getKey(), 0L);
            long served = entry.getValue() < 0 || before < 0 ? -1 : entry.getValue() - before;
            System.out.printf("     %-24s %s%n", entry.getKey(),
                    served < 0 ? "unavailable" : String.format("%,d", served));
        }
        System.out.println();
    }

    private static void cleanUp(FalkorGraphFactory factory) {
        // Delete only this demo's graph. Never call flushdb here: it would drop every graph on the
        // instance, including ones this demo did not create.
        factory.write(GRAPH_NAME).deleteGraph();
        System.out.println("-- removed graph '" + GRAPH_NAME + "' --");
    }

    private static Endpoint endpointFromProperties(String prefix, int defaultPort) {
        String host = System.getProperty(prefix + ".host", "localhost");
        int port = Integer.parseInt(System.getProperty(prefix + ".port", String.valueOf(defaultPort)));
        return Endpoint.of(host, port, user(), password(), tlsEnabled());
    }

    /**
     * Reads any number of replicas.
     *
     * <p>Accepts {@code -Dreplica.hosts=hostA:6379,hostB:6379} for several nodes, or the single
     * {@code -Dreplica.host} and {@code -Dreplica.port} pair. A port may be omitted per host, in
     * which case {@code replica.port} applies, defaulting to 6400 to match the local containers.
     */
    private static List<Endpoint> replicaEndpointsFromProperties() {
        int defaultPort = Integer.parseInt(System.getProperty("replica.port", "6400"));
        String hostList = System.getProperty("replica.hosts");

        if (hostList == null || hostList.trim().isEmpty()) {
            return List.of(endpointFromProperties("replica", defaultPort));
        }

        List<Endpoint> replicas = new ArrayList<>();
        for (String entry : hostList.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            // Split on the last colon so an IPv6 literal in brackets survives.
            int separator = trimmed.lastIndexOf(':');
            String host = separator > 0 ? trimmed.substring(0, separator) : trimmed;
            int port = separator > 0
                    ? Integer.parseInt(trimmed.substring(separator + 1))
                    : defaultPort;
            replicas.add(Endpoint.of(host, port, user(), password(), tlsEnabled()));
        }
        if (replicas.isEmpty()) {
            throw new IllegalArgumentException("replica.hosts was set but contained no usable entry");
        }
        return replicas;
    }

    private static String user() {
        return System.getProperty("user");
    }

    private static String password() {
        return System.getProperty("password");
    }

    private static boolean tlsEnabled() {
        return Boolean.parseBoolean(System.getProperty("tls", "false"));
    }

    private ReplicaReadDemo() {}
}

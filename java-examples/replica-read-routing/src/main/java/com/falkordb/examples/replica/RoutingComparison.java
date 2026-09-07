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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

/**
 * Compares routing modes under sustained load and reports per node CPU.
 *
 * <p>Not part of the example itself. This produces the numbers quoted in the README, and lets you
 * reproduce them on your own instance rather than taking them on trust.
 *
 * <h2>Why each measurement runs for a fixed time</h2>
 *
 * <p>A fixed read count finishes in under a second at high concurrency, which is useless for two
 * reasons. Server side dashboards sample on an interval far coarser than that, so the load never
 * appears on a graph. And a sub second sample is dominated by whatever the JIT and the connection
 * pool happen to be doing. Running each mode for a set number of seconds gives a steady state you
 * can watch on a metrics page.
 *
 * <h2>Options</h2>
 *
 * <ul>
 *   <li>{@code -Dmachines} machines to seed, default 660,000, which is roughly 2M graph nodes
 *   <li>{@code -Dseconds} load duration per mode, default 60
 *   <li>{@code -Dthreads} client thread counts to sweep, default {@code 4,16,64}
 *   <li>{@code -Dmodes} routing modes and their order, default {@code PRIMARY_ONLY,ROUND_ROBIN}
 *   <li>{@code -Dparams} distinct parameter values, default 10,000
 *   <li>{@code -Ddrop} delete the graph when finished, default false so it can be reused
 * </ul>
 */
public final class RoutingComparison {

    private static final String GRAPH = "routing_comparison";
    private static final int SEED_BATCH = 5_000;

    public static void main(String[] args) throws Exception {
        int machines = Integer.parseInt(System.getProperty("machines", "660000"));
        int seconds = Integer.parseInt(System.getProperty("seconds", "60"));
        int distinctParams = Integer.parseInt(System.getProperty("params", "10000"));
        List<Integer> threadCounts = threadCounts();
        List<ReadPreference> modes = modes();

        SentinelTopology.Topology topology = SentinelTopology.builder()
                .sentinel(Endpoint.of(
                        System.getProperty("sentinel.host"),
                        Integer.parseInt(System.getProperty("sentinel.port", "26379")),
                        System.getProperty("user"),
                        System.getProperty("password"),
                        Boolean.parseBoolean(System.getProperty("sentinel.tls", "false"))))
                .dataCredentials(System.getProperty("user"), System.getProperty("password"))
                .dataTls(Boolean.parseBoolean(System.getProperty("tls", "false")))
                .build()
                .discover();

        System.out.println(topology);
        List<Endpoint> allNodes = new ArrayList<>();
        allNodes.add(topology.getPrimary());
        allNodes.addAll(topology.getReplicas());

        int maxThreads = threadCounts.get(threadCounts.size() - 1);
        long loadSeconds = (long) seconds * threadCounts.size() * modes.size();
        System.out.printf(
                "plan: %d thread counts x %d modes x %ds = %d min %02ds of load, plus seeding%n",
                threadCounts.size(), modes.size(), seconds, loadSeconds / 60, loadSeconds % 60);

        try (FalkorGraphFactory factory = FalkorGraphFactory.builder()
                .topology(topology)
                // The pool must never be the bottleneck, or this measures commons-pool2 rather
                // than FalkorDB. The jfalkordb default of 8 is far below the top of the sweep.
                .poolMaxTotal(maxThreads * 2)
                .poolMaxIdle(maxThreads * 2)
                .build()) {

            seed(factory, machines);
            warmUp(factory, distinctParams);

            List<String> csv = new ArrayList<>();
            csv.add("threads,mode,reads,seconds,throughput_qps,latency_ms,"
                    + "primary_cpu_s,replica_cpu_s,primary_cores,replica_cores,failures");

            for (int threads : threadCounts) {
                for (ReadPreference preference : modes) {
                    csv.add(measure(factory, preference, allNodes, threads, seconds, distinctParams));
                }
            }

            System.out.println("\n===== CSV =====");
            csv.forEach(System.out::println);

            if (Boolean.parseBoolean(System.getProperty("drop", "false"))) {
                factory.write(GRAPH).deleteGraph();
                System.out.println("\nremoved graph '" + GRAPH + "'");
            } else {
                System.out.println("\nkept graph '" + GRAPH + "', rerun to reuse it without reseeding");
            }
        }
    }

    private static List<ReadPreference> modes() {
        String configured = System.getProperty("modes", "PRIMARY_ONLY,ROUND_ROBIN");
        List<ReadPreference> modes = new ArrayList<>();
        for (String entry : configured.split(",")) {
            modes.add(ReadPreference.valueOf(entry.trim()));
        }
        return modes;
    }

    private static List<Integer> threadCounts() {
        String configured = System.getProperty("threads", "4,16,64");
        List<Integer> counts = new ArrayList<>();
        for (String entry : configured.split(",")) {
            counts.add(Integer.parseInt(entry.trim()));
        }
        return counts;
    }

    /**
     * Builds the dataset, skipping the work if the graph already holds it.
     *
     * <p>Seeding a couple of million nodes across a network takes minutes, so repeated runs against
     * the same instance should not pay for it again.
     */
    private static void seed(FalkorGraphFactory factory, int machines) {
        Graph writer = factory.write(GRAPH);

        long existing = countMachines(writer);
        if (existing == machines) {
            System.out.printf("graph already holds %,d machines, skipping seed%n", existing);
            return;
        }
        if (existing > 0) {
            System.out.printf("graph holds %,d machines but %,d wanted, rebuilding%n", existing, machines);
            writer.deleteGraph();
            writer = factory.write(GRAPH);
        }

        writer.query("CREATE INDEX FOR (m:Machine) ON (m.machineId)");
        writer.query("CREATE INDEX FOR (m:Machine) ON (m.line)");

        long startedAt = System.nanoTime();
        for (int start = 0; start < machines; start += SEED_BATCH) {
            int end = Math.min(start + SEED_BATCH, machines) - 1;
            Map<String, Object> params = new HashMap<>();
            params.put("start", start);
            params.put("end", end);
            writer.query(
                    "UNWIND range($start, $end) AS i "
                            + "CREATE (m:Machine {machineId: i, line: i % 200, "
                            + "  site: 'site-' + toString(i % 40), status: 'running'}) "
                            + "CREATE (m)-[:HAS_READING]->(:Reading {value: i % 97, kind: 'temperature'}) "
                            + "CREATE (m)-[:HAS_READING]->(:Reading {value: i % 89, kind: 'vibration'})",
                    params);

            int done = end + 1;
            if (done % 100_000 == 0 || done == machines) {
                System.out.printf("  seeded %,d of %,d machines%n", done, machines);
            }
        }

        System.out.printf(
                "seeded %,d machines and %,d readings in %.0fs, about %,d nodes total%n",
                machines, machines * 2L, (System.nanoTime() - startedAt) / 1e9, machines * 3L);
    }

    private static long countMachines(Graph graph) {
        try {
            ResultSet result = graph.readOnlyQuery("MATCH (m:Machine) RETURN count(m) AS total");
            for (Record record : result) {
                Object total = record.getValue("total");
                return total instanceof Number ? ((Number) total).longValue() : 0L;
            }
            return 0L;
        } catch (Exception noGraphYet) {
            return 0L;
        }
    }

    private static void warmUp(FalkorGraphFactory factory, int distinctParams) {
        for (int i = 0; i < 300; i++) {
            runQuery(factory.read(GRAPH, ReadPreference.ROUND_ROBIN), i, distinctParams);
        }
        System.out.println("warmed the plan cache and the client schema cache on every node");
    }

    private static ResultSet runQuery(Graph graph, int seed, int distinctParams) {
        Map<String, Object> params = new HashMap<>();
        // One parameterised shape so the server plan cache stays warm and only the parameter
        // varies, which is what a real workload looks like. Distinct literals would instead
        // measure the planner and thrash a 25 entry cache.
        params.put("id", Math.floorMod(seed, distinctParams));
        return graph.readOnlyQuery(
                "MATCH (m:Machine {machineId: $id})-[:HAS_READING]->(r:Reading) "
                        + "RETURN m.line, m.site, r.kind, r.value",
                params);
    }

    private static String measure(
            FalkorGraphFactory factory,
            ReadPreference preference,
            List<Endpoint> allNodes,
            int threads,
            int seconds,
            int distinctParams)
            throws Exception {

        System.out.printf("%n=== %d threads, %s, running %ds ===%n", threads, preference, seconds);

        Map<Endpoint, Double> cpuBefore = cpuSeconds(allNodes);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger failures = new AtomicInteger();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        LongAdder reads = new LongAdder();

        long startNanos = System.nanoTime();
        long deadline = startNanos + TimeUnit.SECONDS.toNanos(seconds);

        for (int worker = 0; worker < threads; worker++) {
            final int workerId = worker;
            pool.submit(() -> {
                Graph graph = factory.read(GRAPH, preference);
                // Offset by a prime so threads do not march through the same ids in lockstep.
                int counter = workerId * 7_919;
                while (System.nanoTime() < deadline) {
                    try {
                        runQuery(graph, counter++, distinctParams);
                        reads.increment();
                    } catch (Exception failure) {
                        failures.incrementAndGet();
                        firstFailure.compareAndSet(null, failure.getMessage());
                    }
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(seconds + 120L, TimeUnit.SECONDS);

        double elapsedSeconds = (System.nanoTime() - startNanos) / 1e9;
        Map<Endpoint, Double> cpuAfter = cpuSeconds(allNodes);

        long totalReads = reads.sum();
        double throughput = totalReads / elapsedSeconds;
        // Mean service time seen by one thread, which is what bounds a blocking client.
        double latencyMillis = (elapsedSeconds * 1000.0 * threads) / Math.max(totalReads, 1);

        System.out.printf("  %,d reads in %.1fs = %,.0f reads/sec (failures: %d)%n",
                totalReads, elapsedSeconds, throughput, failures.get());
        System.out.printf("  mean latency: %.3f ms%n", latencyMillis);
        if (firstFailure.get() != null) {
            System.out.println("  first failure: " + firstFailure.get());
        }

        List<Double> cpuDeltas = new ArrayList<>();
        for (Endpoint node : allNodes) {
            double delta = cpuAfter.getOrDefault(node, 0.0) - cpuBefore.getOrDefault(node, 0.0);
            cpuDeltas.add(delta);
            // Cores in use is the readable form. Against THREAD_COUNT=2, approaching 2.0 means
            // the node has no query capacity left.
            System.out.printf("  cpu %-16s %6.1f core-s = %.2f cores busy%n",
                    shortName(node), delta, delta / elapsedSeconds);
        }

        double primaryCpu = cpuDeltas.get(0);
        double replicaCpu = cpuDeltas.size() > 1 ? cpuDeltas.get(1) : 0.0;
        return String.format(
                "%d,%s,%d,%.3f,%.0f,%.3f,%.2f,%.2f,%.2f,%.2f,%d",
                threads,
                preference,
                totalReads,
                elapsedSeconds,
                throughput,
                latencyMillis,
                primaryCpu,
                replicaCpu,
                primaryCpu / elapsedSeconds,
                replicaCpu / elapsedSeconds,
                failures.get());
    }

    private static String shortName(Endpoint node) {
        String host = node.getHost();
        int dot = host.indexOf('.');
        return dot > 0 ? host.substring(0, dot) : host;
    }

    /** Cumulative process CPU per node. The ACL user cannot RESETSTAT, so read deltas. */
    private static Map<Endpoint, Double> cpuSeconds(List<Endpoint> nodes) {
        Map<Endpoint, Double> byNode = new HashMap<>();
        for (Endpoint node : nodes) {
            try (Jedis connection = new Jedis(
                    new HostAndPort(node.getHost(), node.getPort()),
                    DefaultJedisClientConfig.builder()
                            .user(node.getUsername())
                            .password(node.getPassword())
                            .ssl(node.isTlsEnabled())
                            .build())) {

                double total = 0;
                for (String line : connection.info("cpu").split("\\r?\\n")) {
                    String trimmed = line.trim();
                    if (trimmed.startsWith("used_cpu_user:") || trimmed.startsWith("used_cpu_sys:")) {
                        total += Double.parseDouble(trimmed.split(":")[1]);
                    }
                }
                byNode.put(node, total);
            } catch (Exception unreachable) {
                byNode.put(node, 0.0);
            }
        }
        return byNode;
    }
}

package com.falkordb.examples.replica;

import com.falkordb.Graph;
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
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

/**
 * Compares PRIMARY_ONLY against ROUND_ROBIN on the same dataset and reports per node CPU.
 *
 * <p>Not part of the example. Used to produce the numbers quoted in the README, on real hardware
 * rather than a loopback pair.
 */
public final class RoutingComparison {

    private static final String GRAPH = "routing_comparison";
    private static final int MACHINES = 20_000;
    private static final int TOTAL_READS = 12_000;
    private static final int DISTINCT_PARAMS = 1_000;

    public static void main(String[] args) throws Exception {
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

        List<Integer> threadCounts = threadCounts();
        int maxThreads = threadCounts.get(threadCounts.size() - 1);

        try (FalkorGraphFactory factory = FalkorGraphFactory.builder()
                .topology(topology)
                // The pool must never be the bottleneck, or this measures commons-pool2
                // rather than FalkorDB. The jfalkordb default of 8 is below the top of the sweep.
                .poolMaxTotal(maxThreads * 2)
                .poolMaxIdle(maxThreads * 2)
                .build()) {

            seed(factory);
            // Warm the server plan cache and the client schema cache on every node so the
            // comparison measures steady state rather than first-call costs.
            warmUp(factory);

            List<String> csv = new ArrayList<>();
            csv.add("threads,mode,reads,seconds,throughput_qps,latency_ms,primary_cpu_s,replica_cpu_s");

            for (int threads : threadCounts) {
                for (ReadPreference preference :
                        List.of(ReadPreference.PRIMARY_ONLY, ReadPreference.ROUND_ROBIN)) {
                    csv.add(measure(factory, preference, allNodes, threads));
                }
            }

            System.out.println("\n===== CSV =====");
            csv.forEach(System.out::println);

            factory.write(GRAPH).deleteGraph();
            System.out.println("\nremoved graph '" + GRAPH + "'");
        }
    }

    /** Defaults to a sweep, so the point at which throughput starts to move is visible. */
    private static List<Integer> threadCounts() {
        String configured = System.getProperty("threads", "1,2,4,8,16,32,64");
        List<Integer> counts = new ArrayList<>();
        for (String entry : configured.split(",")) {
            counts.add(Integer.parseInt(entry.trim()));
        }
        return counts;
    }

    private static void seed(FalkorGraphFactory factory) {
        Graph writer = factory.write(GRAPH);
        try {
            writer.deleteGraph();
        } catch (Exception noSuchGraph) {
            // First run on a clean instance.
        }
        writer = factory.write(GRAPH);
        writer.query("CREATE INDEX FOR (m:Machine) ON (m.machineId)");

        int batchSize = 2_000;
        for (int start = 0; start < MACHINES; start += batchSize) {
            Map<String, Object> params = new HashMap<>();
            params.put("start", start);
            params.put("end", start + batchSize - 1);
            writer.query(
                    "UNWIND range($start, $end) AS i "
                            + "CREATE (m:Machine {machineId: i, line: i % 20}) "
                            + "CREATE (m)-[:HAS_READING]->(:Reading {value: i % 97}) "
                            + "CREATE (m)-[:HAS_READING]->(:Reading {value: i % 89})",
                    params);
        }
        System.out.println("seeded " + MACHINES + " machines, " + (MACHINES * 2) + " readings");
    }

    private static void warmUp(FalkorGraphFactory factory) {
        for (int i = 0; i < 200; i++) {
            runQuery(factory.read(GRAPH, ReadPreference.ROUND_ROBIN), i);
        }
    }

    private static ResultSet runQuery(Graph graph, int seed) {
        Map<String, Object> params = new HashMap<>();
        // A single parameterised shape, so the server plan cache stays warm. Only the parameter
        // varies, which is what a real workload looks like. Distinct literals would instead
        // measure the planner and thrash a 25 entry cache.
        params.put("id", seed % DISTINCT_PARAMS);
        return graph.readOnlyQuery(
                "MATCH (m:Machine {machineId: $id})-[:HAS_READING]->(r:Reading) RETURN m.line, r.value",
                params);
    }

    private static String measure(
            FalkorGraphFactory factory, ReadPreference preference, List<Endpoint> allNodes, int threads)
            throws Exception {

        int readsPerWorker = TOTAL_READS / threads;
        int totalReads = readsPerWorker * threads;

        Map<Endpoint, Double> cpuBefore = cpuSeconds(allNodes);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger failures = new AtomicInteger();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        long startNanos = System.nanoTime();

        for (int worker = 0; worker < threads; worker++) {
            final int workerId = worker;
            pool.submit(() -> {
                Graph graph = factory.read(GRAPH, preference);
                for (int i = 0; i < readsPerWorker; i++) {
                    try {
                        runQuery(graph, workerId * readsPerWorker + i);
                    } catch (Exception failure) {
                        failures.incrementAndGet();
                        firstFailure.compareAndSet(null, failure.getMessage());
                    }
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.MINUTES);

        double elapsedSeconds = (System.nanoTime() - startNanos) / 1e9;
        Map<Endpoint, Double> cpuAfter = cpuSeconds(allNodes);

        double throughput = totalReads / elapsedSeconds;
        double latencyMillis = (elapsedSeconds * 1000.0) / readsPerWorker;

        System.out.printf("%n=== %2d threads, %s ===%n", threads, preference);
        System.out.printf("  %,d reads in %.2fs = %,.0f reads/sec (failures: %d)%n",
                totalReads, elapsedSeconds, throughput, failures.get());
        System.out.printf("  mean latency per thread: %.3f ms%n", latencyMillis);
        if (firstFailure.get() != null) {
            System.out.println("  first failure: " + firstFailure.get());
        }

        List<Double> cpuDeltas = new ArrayList<>();
        for (Endpoint node : allNodes) {
            double delta = cpuAfter.getOrDefault(node, 0.0) - cpuBefore.getOrDefault(node, 0.0);
            cpuDeltas.add(delta);
            System.out.printf("  cpu %-58s %.2f core-s%n", node, delta);
        }

        return String.format(
                "%d,%s,%d,%.3f,%.0f,%.3f,%.2f,%.2f",
                threads,
                preference,
                totalReads,
                elapsedSeconds,
                throughput,
                latencyMillis,
                cpuDeltas.get(0),
                cpuDeltas.size() > 1 ? cpuDeltas.get(1) : 0.0);
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

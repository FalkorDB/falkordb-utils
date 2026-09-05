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
    private static final int WORKERS = 4;
    private static final int READS_PER_WORKER = 3_000;
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

        try (FalkorGraphFactory factory = FalkorGraphFactory.builder()
                .topology(topology)
                .poolMaxTotal(WORKERS * 2)
                .poolMaxIdle(WORKERS * 2)
                .build()) {

            seed(factory);
            // Warm the query plan cache and the client schema cache on every node so the
            // comparison measures steady state rather than first-call costs.
            warmUp(factory);

            for (ReadPreference preference : List.of(ReadPreference.PRIMARY_ONLY, ReadPreference.ROUND_ROBIN)) {
                measure(factory, preference, allNodes);
            }

            factory.write(GRAPH).deleteGraph();
            System.out.println("\nremoved graph '" + GRAPH + "'");
        }
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

    private static void measure(
            FalkorGraphFactory factory, ReadPreference preference, List<Endpoint> allNodes) throws Exception {

        Map<Endpoint, Double> cpuBefore = cpuSeconds(allNodes);

        ExecutorService pool = Executors.newFixedThreadPool(WORKERS);
        AtomicInteger failures = new AtomicInteger();
        long startNanos = System.nanoTime();

        for (int worker = 0; worker < WORKERS; worker++) {
            final int workerId = worker;
            pool.submit(() -> {
                Graph graph = factory.read(GRAPH, preference);
                for (int i = 0; i < READS_PER_WORKER; i++) {
                    try {
                        runQuery(graph, workerId * READS_PER_WORKER + i);
                    } catch (Exception failure) {
                        failures.incrementAndGet();
                    }
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.MINUTES);

        double elapsedSeconds = (System.nanoTime() - startNanos) / 1e9;
        Map<Endpoint, Double> cpuAfter = cpuSeconds(allNodes);

        int totalReads = WORKERS * READS_PER_WORKER;
        System.out.printf("%n=== %s ===%n", preference);
        System.out.printf("  %,d reads in %.2fs = %,.0f reads/sec (failures: %d)%n",
                totalReads, elapsedSeconds, totalReads / elapsedSeconds, failures.get());
        System.out.printf("  mean latency per thread: %.3f ms%n",
                (elapsedSeconds * 1000.0) / READS_PER_WORKER);
        for (Endpoint node : allNodes) {
            double delta = cpuAfter.getOrDefault(node, 0.0) - cpuBefore.getOrDefault(node, 0.0);
            System.out.printf("  cpu %-58s %.2f core-s%n", node, delta);
        }
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

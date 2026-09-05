package com.falkordb.examples.replica;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Offline tests for Sentinel discovery.
 *
 * <p>These cover the argument handling and the failure message. The happy path needs a live Sentinel
 * and is exercised by running {@link ReplicaReadDemo} against a real deployment.
 */
class SentinelTopologyTest {

    private static Endpoint anySentinel() {
        return Endpoint.of("sentinel.example", 26379, "falkordb", "secret", false);
    }

    @Test
    @DisplayName("a sentinel endpoint is required")
    void sentinelEndpointIsRequired() {
        IllegalArgumentException failure =
                assertThrows(IllegalArgumentException.class, () -> SentinelTopology.builder().build());
        assertTrue(failure.getMessage().contains("sentinel"));
    }

    @Test
    @DisplayName("a non positive timeout is rejected")
    void timeoutMustBePositive() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SentinelTopology.builder().sentinel(anySentinel()).timeoutMillis(0));
    }

    @Test
    @DisplayName("an unreachable sentinel fails with the address in the message")
    void unreachableSentinelIsReported() {
        SentinelTopology topology = SentinelTopology.builder()
                // Port 1 on the loopback refuses immediately, so this stays fast.
                .sentinel(Endpoint.of("127.0.0.1", 1, null, null, false))
                .timeoutMillis(250)
                .build();

        IllegalStateException failure = assertThrows(IllegalStateException.class, topology::discover);
        assertTrue(
                failure.getMessage().contains("127.0.0.1:1"),
                "the message should name the sentinel that failed, but was: " + failure.getMessage());
    }

    @Test
    @DisplayName("a topology exposes the primary and its replicas")
    void topologyExposesNodes() {
        Endpoint primary = Endpoint.local("primary.example", 6379);
        Endpoint replica = Endpoint.local("replica.example", 6379);

        SentinelTopology.Topology topology =
                new SentinelTopology.Topology("master", primary, List.of(replica));

        assertEquals("master", topology.getMasterName());
        assertEquals(primary, topology.getPrimary());
        assertEquals(List.of(replica), topology.getReplicas());
    }

    @Test
    @DisplayName("a topology does not alias the list it was given")
    void topologyCopiesReplicaList() {
        List<Endpoint> mutable = new ArrayList<>();
        mutable.add(Endpoint.local("replica.example", 6379));

        SentinelTopology.Topology topology =
                new SentinelTopology.Topology("master", Endpoint.local("primary.example", 6379), mutable);

        mutable.clear();

        assertEquals(1, topology.getReplicas().size(), "clearing the caller's list must not empty the topology");
        assertThrows(
                UnsupportedOperationException.class,
                () -> topology.getReplicas().add(Endpoint.local("sneaky.example", 6379)));
    }

    @Test
    @DisplayName("a topology feeds the factory builder directly")
    void topologyConfiguresTheFactory() {
        Endpoint primary = Endpoint.local("primary.example", 6379);
        Endpoint replica = Endpoint.local("replica.example", 6379);
        SentinelTopology.Topology topology =
                new SentinelTopology.Topology("master", primary, List.of(replica));

        try (FalkorGraphFactory factory = FalkorGraphFactory.builder()
                .topology(topology)
                .readPreference(ReadPreference.ROUND_ROBIN)
                // Jedis connects lazily, so nothing here touches the network.
                .verifyRolesOnStartup(false)
                .build()) {

            assertEquals(List.of(primary, replica), factory.readEndpoints());
        }
    }

    @Test
    @DisplayName("a null replica collection is tolerated")
    void nullReplicaCollectionIsIgnored() {
        try (FalkorGraphFactory factory = FalkorGraphFactory.builder()
                .primary(Endpoint.local("primary.example", 6379))
                .replicas(null)
                .verifyRolesOnStartup(false)
                .build()) {

            assertEquals(1, factory.readEndpoints().size());
        }
    }
}

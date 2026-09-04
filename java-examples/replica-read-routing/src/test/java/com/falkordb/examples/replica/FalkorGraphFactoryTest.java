package com.falkordb.examples.replica;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Covers routing and configuration without needing a live database.
 *
 * <p>Jedis creates connections lazily, so a factory can be constructed against endpoints that
 * nothing is listening on as long as the startup role check is disabled. That keeps these tests
 * fast and free of infrastructure, which matters because they guard the rule the whole example
 * exists to enforce: writes never reach a replica.
 */
class FalkorGraphFactoryTest {

    private static final Endpoint PRIMARY = Endpoint.local("localhost", 16379);
    private static final Endpoint REPLICA_ONE = Endpoint.local("localhost", 16380);
    private static final Endpoint REPLICA_TWO = Endpoint.local("localhost", 16381);

    private static FalkorGraphFactory.Builder offlineFactory() {
        return FalkorGraphFactory.builder()
                .primary(PRIMARY)
                .verifyRolesOnStartup(false);
    }

    @Test
    void roundRobinRotatesOverPrimaryAndEveryReplica() {
        try (FalkorGraphFactory factory = offlineFactory()
                .replica(REPLICA_ONE)
                .replica(REPLICA_TWO)
                .readPreference(ReadPreference.ROUND_ROBIN)
                .build()) {

            assertEquals(List.of(PRIMARY, REPLICA_ONE, REPLICA_TWO), factory.readEndpoints());
        }
    }

    @Test
    void replicaOnlyExcludesThePrimary() {
        try (FalkorGraphFactory factory = offlineFactory()
                .replica(REPLICA_ONE)
                .replica(REPLICA_TWO)
                .readPreference(ReadPreference.REPLICA_ONLY)
                .build()) {

            List<Endpoint> readEndpoints = factory.readEndpoints();
            assertEquals(List.of(REPLICA_ONE, REPLICA_TWO), readEndpoints);
            assertTrue(!readEndpoints.contains(PRIMARY), "primary must not serve REPLICA_ONLY reads");
        }
    }

    @Test
    void primaryOnlyReadsNeverTouchAReplica() {
        try (FalkorGraphFactory factory = offlineFactory()
                .replica(REPLICA_ONE)
                .readPreference(ReadPreference.PRIMARY_ONLY)
                .build()) {

            assertEquals(List.of(PRIMARY), factory.readEndpoints());
        }
    }

    @Test
    void preferenceCanBeOverriddenPerRead() {
        try (FalkorGraphFactory factory = offlineFactory()
                .replica(REPLICA_ONE)
                .readPreference(ReadPreference.PRIMARY_ONLY)
                .build()) {

            // The configured default stays PRIMARY_ONLY, but a single read can ask for a replica.
            assertEquals(List.of(PRIMARY), factory.readEndpoints());
            assertEquals(List.of(REPLICA_ONE), factory.readEndpoints(ReadPreference.REPLICA_ONLY));
        }
    }

    @Test
    void rotationIsEvenAcrossNodes() {
        try (FalkorGraphFactory factory = offlineFactory()
                .replica(REPLICA_ONE)
                .replica(REPLICA_TWO)
                .readPreference(ReadPreference.ROUND_ROBIN)
                .build()) {

            // Walking the rotation directly, since asking for a Graph would open a connection.
            List<Endpoint> rotation = factory.readEndpoints();
            List<Endpoint> visited = new ArrayList<>();
            for (int i = 0; i < 9; i++) {
                visited.add(rotation.get(i % rotation.size()));
            }
            assertEquals(3, java.util.Collections.frequency(visited, PRIMARY));
            assertEquals(3, java.util.Collections.frequency(visited, REPLICA_ONE));
            assertEquals(3, java.util.Collections.frequency(visited, REPLICA_TWO));
        }
    }

    @Test
    void replicaOnlyFallsBackToPrimaryRatherThanFailingEveryRead() {
        // Configured through ROUND_ROBIN so the builder's own guard does not reject it, then asked
        // for the REPLICA_ONLY rotation, which has no replicas to offer.
        try (FalkorGraphFactory factory = offlineFactory()
                .readPreference(ReadPreference.ROUND_ROBIN)
                .build()) {

            assertEquals(List.of(PRIMARY), factory.readEndpoints(ReadPreference.REPLICA_ONLY));
        }
    }

    @Test
    void aPrimaryIsRequired() {
        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> FalkorGraphFactory.builder().verifyRolesOnStartup(false).build());
        assertTrue(failure.getMessage().contains("primary"));
    }

    @Test
    void replicaOnlyWithoutAReplicaIsRejectedAtBuildTime() {
        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> offlineFactory().readPreference(ReadPreference.REPLICA_ONLY).build());
        assertTrue(failure.getMessage().contains("REPLICA_ONLY"));
    }

    @Test
    void poolMaxIdleAboveMaxTotalIsRejected() {
        assertThrows(
                IllegalStateException.class,
                () -> offlineFactory().poolMaxTotal(4).poolMaxIdle(8).build());
    }

    @Test
    void blankGraphNameIsRejected() {
        try (FalkorGraphFactory factory = offlineFactory().build()) {
            assertThrows(IllegalArgumentException.class, () -> factory.write("  "));
        }
    }

    @Test
    void endpointToStringDoesNotLeakCredentials() {
        Endpoint endpoint = Endpoint.cloud("db.example.cloud", 6379, "falkordb", "sup3rs3cret");
        String rendered = endpoint.toString();
        assertTrue(rendered.contains("db.example.cloud"));
        assertTrue(!rendered.contains("sup3rs3cret"), "password must never appear in toString()");
    }
}

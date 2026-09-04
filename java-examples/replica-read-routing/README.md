# Replica read routing for FalkorDB, Java example

Writes go to the primary. Reads are spread across the primary and its replicas. The routing rule
lives in one small factory instead of being repeated, and forgotten, throughout an application.

This is reference code meant to be copied and adapted.

## Why bother

A replica that only exists for failover still costs money and still burns nothing. Measured on a
primary and replica pair, running the same 12,000 read queries from 4 client threads:

| Reads routed to | Throughput | Primary worker CPU | Replica worker CPU |
|---|---|---|---|
| Primary only | 8,787 q/s | 1.12 s | **0.00 s** |
| Primary and replica | 8,770 q/s | 0.58 s | 0.63 s |

Two things are worth reading carefully.

**Throughput did not improve.** With 4 client threads and a blocking client there are at most 4
queries in flight, and a single node handles that without breaking a sweat. Adding read capacity
does not help when nothing was queueing for it.

**CPU per node halved.** That is the actual win. The same work is spread over hardware you are
already paying for, and each node keeps far more headroom for spikes and for failover. Read routing
is a utilization and cost story, not a latency story, until a single node is genuinely saturated.

## Requires jfalkordb 0.11.1 or newer

Earlier versions warmed the client side schema cache using `GRAPH.QUERY`, which is a write command,
so the first replica read that returned a node, a relationship or a path failed with:

```
READONLY You can't write against a read only replica.
```

Reads returning only scalars worked, which made the failure look intermittent rather than
deterministic. Fixed in [JFalkorDB#414](https://github.com/FalkorDB/JFalkorDB/pull/414) and released
in 0.11.1.

## Quick start

```java
try (FalkorGraphFactory factory = FalkorGraphFactory.builder()
        .primary(Endpoint.cloud("primary-host", 6379, "falkordb", password))
        .replica(Endpoint.cloud("replica-host", 6379, "falkordb", password))
        .readPreference(ReadPreference.ROUND_ROBIN)
        .poolMaxTotal(16)
        .build()) {

    // Writes always go to the primary.
    factory.write("factory_floor")
           .query("CREATE (:Machine {machineId: $id})", Map.of("id", 1));

    // Reads rotate across the primary and every replica.
    ResultSet machines = factory.read("factory_floor")
           .readOnlyQuery("MATCH (m:Machine {machineId: $id}) RETURN m", Map.of("id", 1));

    // A read that must observe your own write goes to the primary.
    ResultSet fresh = factory.readFromPrimary("factory_floor")
           .readOnlyQuery("MATCH (m:Machine {machineId: $id}) RETURN m", Map.of("id", 1));
}
```

## Read preferences

| Preference | Reads served by | Consistency |
|---|---|---|
| `PRIMARY_ONLY` | Primary | Always observes your own writes |
| `REPLICA_ONLY` | Replicas, falling back to the primary if none are reachable | May be stale |
| `ROUND_ROBIN` | Primary and all replicas | Intermittently stale, which is the harder case |

Consistency belongs to the query, not to the application, so the preference can be overridden per
call:

```java
factory.read("factory_floor", ReadPreference.REPLICA_ONLY);
```

A dashboard aggregate tolerates a stale read. A balance check straight after a debit does not.

## Staleness is real and `ROUND_ROBIN` hides it

Replication is asynchronous. A read served by a replica can return data older than a write the
primary has already acknowledged.

`ROUND_ROBIN` is the most dangerous setting for this, because roughly half of reads land on the
primary and observe the write correctly. The bug then appears intermittently and usually not on a
developer machine, where the replication window over loopback is often under a millisecond. Use
`readFromPrimary` for any read that must observe a recent write.

## Verifying reads land where you intended

The client can only report what it tried to do. `readCallsServed()` asks each node how many
`graph.RO_QUERY` calls it has served, so the database confirms the routing:

```java
Map<Endpoint, Long> before = factory.readCallsServed();
// ... run some reads ...
Map<Endpoint, Long> after = factory.readCallsServed();
```

Counters are cumulative and need `CONFIG RESETSTAT` to zero, which managed platforms commonly
withhold from the application user. Take a before and after reading and subtract.

The demo output shows what an even rotation looks like:

```
   reads served per node, counted by the server:
     localhost:6399           502
     localhost:6400           502
```

## Things that will bite you

**Size the connection pool at or above your thread count.** The driver defaults to 8. With more
application threads than pooled connections, threads block waiting on the pool, and the symptom is
indistinguishable from a slow database. This is a common way to misdiagnose one.

**Use `readOnlyQuery` for reads.** It sends `GRAPH.RO_QUERY`. A replica rejects `GRAPH.QUERY` even
when the query only reads, because the command is flagged as a write.

**Build one factory and share it.** Each driver owns a connection pool and each graph handle owns a
schema cache. Both are built to be shared across threads. Creating a factory per request builds a
fresh pool and a cold cache every time, which costs far more than the query.

**Roles change during failover.** The startup check catches a configuration that has the primary and
replica the wrong way round, but a Sentinel promotion later will invalidate it. Production code
should treat a write failing with `READONLY` as a signal to re-resolve the topology.

**Index the properties you anchor reads on.** Without an index the same query degrades from
`Node By Index Scan` to `Node By Label Scan`, and at any real scale that dominates everything else.

## Running the demo

Start a primary and a replica:

```bash
docker run -d --name fkr-primary -p 6399:6379 falkordb/falkordb:latest
docker run -d --name fkr-replica -p 6400:6379 falkordb/falkordb:latest
docker exec fkr-replica redis-cli REPLICAOF <primary-container-ip> 6379
```

Run it:

```bash
cd java-examples/replica-read-routing
./mvnw compile exec:java -Dexec.mainClass=com.falkordb.examples.replica.ReplicaReadDemo
```

Against FalkorDB Cloud:

```bash
./mvnw compile exec:java \
  -Dexec.mainClass=com.falkordb.examples.replica.ReplicaReadDemo \
  -Dprimary.host=node-0.your-instance.cloud -Dprimary.port=6379 \
  -Dreplica.host=node-1.your-instance.cloud -Dreplica.port=6379 \
  -Dtls=true -Duser=falkordb -Dpassword=...
```

The demo creates and then deletes a graph named `replica_read_demo`. It deletes only that graph and
never calls `FLUSHDB`, which would drop every graph on the instance.

## Files

| File | Purpose |
|---|---|
| `FalkorGraphFactory.java` | The factory. Routing, driver and handle lifecycle, diagnostics |
| `ReadPreference.java` | Which nodes may serve reads, and the consistency each implies |
| `Endpoint.java` | Host, port, credentials and TLS for one node |
| `RoleVerifier.java` | Asks a node which replication role it reports |
| `ReplicaReadDemo.java` | Runnable walkthrough of all of the above |

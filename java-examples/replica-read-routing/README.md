# Replica read routing for FalkorDB, Java example

Writes go to the primary. Reads are spread across the primary and its replicas. The routing rule
lives in one small factory instead of being repeated, and forgotten, throughout an application.

This is reference code meant to be copied and adapted.

## Scope: primary and replica, not Redis Cluster

This targets a deployment with **one primary holding the whole dataset and one or more replicas
mirroring it**. That is what FalkorDB Cloud provisions and what Sentinel manages.

It does **not** support Redis Cluster. Under cluster mode the keyspace is sharded across several
primaries, so "send this read to a replica instead" stops being a single decision: the client has to
resolve which shard owns the key before it can choose a node. Routing that ignores sharding returns
a wrong answer rather than an error, which is the worst possible failure mode. The factory therefore
checks `cluster_enabled` at startup and refuses to build against a cluster.

## Why bother

A replica that only exists for failover still costs money and still burns nothing. Measured on
FalkorDB Cloud (AWS us-east-2, 2 pods, `THREAD_COUNT=2` per node) with the Java client on a
c4.xlarge in the same region, running the same 12,000 read queries from 4 client threads against a
20,000 machine graph:

| Reads routed to | Throughput | Mean latency | Primary CPU | Replica CPU |
|---|---|---|---|---|
| Primary only | 2,986 q/s | 1.340 ms | 1.65 core-s | **0.01 core-s** |
| Primary and replica | 2,999 q/s | 1.334 ms | 0.97 core-s | 0.98 core-s |

Two things are worth reading carefully.

**Throughput did not improve.** With 4 client threads and a blocking client there are at most 4
queries in flight, so throughput is just `threads / latency`. Here that is `4 / 0.001334 s` = 2,998
q/s, which is within a rounding error of the measured 2,999. Round trip time to the database was
1.18 ms, so almost all of that latency is network, not query execution. Adding read capacity cannot
help when the client is blocked on the wire rather than queueing behind a busy server.

**CPU per node halved.** That is the actual win. The same work is spread over hardware you are
already paying for, and each node keeps far more headroom for spikes and for failover. Read routing
is a utilization and cost story, not a latency story, until a single node is actually saturated.

Two honest caveats. Total CPU across both nodes rose slightly, from 1.66 to 1.95 core-seconds,
because a second connection pool and a second schema cache are not free. And these numbers say
nothing about a workload with more concurrency: raise the client thread count until a single node
saturates and the throughput column starts to move.

Reproduce this on your own instance with `RoutingComparison`, described under
[Files](#files).

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

Let Sentinel tell you the topology. Naming the primary by hand works until the first failover, after
which the node you named is a replica and every write fails.

```java
SentinelTopology.Topology topology = SentinelTopology.builder()
        .sentinel(Endpoint.of(sentinelHost, 26379, "falkordb", password, sentinelUsesTls))
        .dataCredentials("falkordb", password)
        .dataTls(dataUsesTls)
        .build()
        .discover();

try (FalkorGraphFactory factory = FalkorGraphFactory.builder()
        .topology(topology)
        .readPreference(ReadPreference.ROUND_ROBIN)
        .poolMaxTotal(16)
        .build()) {
    ...
}
```

Note that TLS is configured separately for the Sentinel port and the data port. They are independent
settings and real instances differ: one FalkorDB Cloud instance was observed serving Sentinel over
TLS, another served both Sentinel and data in plaintext. Do not assume they match.

Discovery is a snapshot, not a subscription. Re-run it when a write fails with `READONLY`, which is
how a failover announces itself to a client holding a stale view.

If you would rather name the nodes explicitly:

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

Against FalkorDB Cloud, letting Sentinel find the nodes:

```bash
./mvnw compile exec:java \
  -Dexec.mainClass=com.falkordb.examples.replica.ReplicaReadDemo \
  -Dsentinel.host=singlezonesentinellblb.your-instance.cloud -Dsentinel.port=26379 \
  -Dsentinel.tls=false -Dtls=false \
  -Duser=falkordb -Dpassword=...
```

Set `-Dsentinel.tls` and `-Dtls` to match your instance, and add `-Dsentinel.master=<name>` if
Sentinel monitors more than one primary.

Naming the nodes explicitly instead:

```bash
./mvnw compile exec:java \
  -Dexec.mainClass=com.falkordb.examples.replica.ReplicaReadDemo \
  -Dprimary.host=node-0.your-instance.cloud -Dprimary.port=6379 \
  -Dreplica.host=node-1.your-instance.cloud -Dreplica.port=6379 \
  -Dtls=true -Duser=falkordb -Dpassword=...
```

With more than one replica, list them under `replica.hosts` instead. Each entry may carry its own
port, and any entry without one falls back to `replica.port`:

```bash
./mvnw compile exec:java \
  -Dexec.mainClass=com.falkordb.examples.replica.ReplicaReadDemo \
  -Dprimary.host=node-0.your-instance.cloud -Dprimary.port=6379 \
  -Dreplica.hosts=node-1.your-instance.cloud:6379,node-2.your-instance.cloud:6379 \
  -Dtls=true -Duser=falkordb -Dpassword=...
```

Point these at the individual node endpoints, not at a Sentinel or load balancer address. The whole
point is choosing which node serves each read, so the client needs to reach each node directly.

The demo creates and then deletes a graph named `replica_read_demo`. It deletes only that graph and
never calls `FLUSHDB`, which would drop every graph on the instance.

## Files

| File | Purpose |
|---|---|
| `FalkorGraphFactory.java` | The factory. Routing, driver and handle lifecycle, diagnostics |
| `ReadPreference.java` | Which nodes may serve reads, and the consistency each implies |
| `Endpoint.java` | Host, port, credentials and TLS for one node |
| `SentinelTopology.java` | Asks Sentinel which node is primary and which are replicas |
| `RoleVerifier.java` | Asks a node its replication role, and detects cluster mode |
| `ReplicaReadDemo.java` | Runnable walkthrough of all of the above |
| `RoutingComparison.java` | Measures primary only against rotating, and reports per node CPU |

### Reproducing the CPU measurement

`RoutingComparison` seeds a 20,000 machine graph, runs the same read workload once with
`PRIMARY_ONLY` and once with `ROUND_ROBIN`, and reports throughput alongside the CPU each node
burned. It drops its graph afterwards and never touches anything else on the instance.

```bash
./mvnw compile exec:java \
  -Dexec.mainClass=com.falkordb.examples.replica.RoutingComparison \
  -Dsentinel.host=singlezonesentinellblb.your-instance.cloud -Dsentinel.port=26379 \
  -Dsentinel.tls=false -Dtls=false \
  -Duser=falkordb -Dpassword=...
```

Run it from a machine in the same region as the database. At this concurrency throughput is bounded
by round trip time, so a client sitting in another region measures the distance between them rather
than anything about routing.

CPU is read from `INFO cpu` as a before and after delta, because the FalkorDB Cloud ACL user is not
permitted to run `CONFIG RESETSTAT` and the counters are cumulative.

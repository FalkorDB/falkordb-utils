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
c4.xlarge in the same region. Each measurement is 12,000 one hop indexed reads against a 20,000
machine graph, reported as the median of 3 runs.

| Client threads | Primary only | Primary and replica | Throughput change | Rejected queries, primary only |
|---|---|---|---|---|
| 1 | 791 q/s | 796 q/s | +1% | 0 |
| 2 | 1,526 q/s | 1,560 q/s | +2% | 0 |
| 4 | 3,083 q/s | 3,020 q/s | -2% | 0 |
| 8 | 6,122 q/s | 6,106 q/s | 0% | 0 |
| 16 | 10,524 q/s | 12,072 q/s | **+15%** | 0 |
| 32 | 14,693 q/s | 20,623 q/s | **+40%** | 0 |
| 64 | 16,760 q/s | 28,612 q/s | **+71%** | **50** |

There are three separate results here, and conflating them is how benchmarks mislead.

**Below 8 threads, routing buys you nothing in throughput.** With a blocking client, throughput is
`threads / latency`. Round trip time to the database was 1.18 ms and measured latency was about
1.30 ms, so the client spends nearly all its time on the wire rather than waiting behind a busy
server. Adding read capacity cannot help when nothing is queueing for it. At this concurrency,
putting the client in the same region as the database matters far more than adding a replica.

**Below 8 threads you still halve CPU per node.** At 4 threads the primary burned 1.61 core-seconds
and the replica 0.01. Rotating made it 0.97 and 0.91. Identical throughput, the same work spread
over hardware you already pay for, and roughly double the headroom on each node for spikes and for
failover.

**Above 16 threads the throughput gain is real, and by 64 threads primary only starts failing.**
Once the primary is the bottleneck, the second node is worth up to 71%. At 64 threads the primary
rejected about 50 of 12,000 queries with:

```
JedisDataException: Max pending queries exceeded
```

That is `MAX_QUEUED_QUERIES=50` being hit on the primary while the replica sat completely idle.
Rotating rejected nothing at the same load. This is the sharpest form of the argument: primary only
does not merely waste a node, it drops requests while that node is doing nothing.

One honest caveat. Total CPU across both nodes rose slightly, for example 1.62 to 1.88
core-seconds at 4 threads, because a second connection pool and a second schema cache are not free.

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

`RoutingComparison` seeds a 20,000 machine graph, then sweeps client thread counts running the same
read workload once with `PRIMARY_ONLY` and once with `ROUND_ROBIN`, reporting throughput, latency,
rejected queries and the CPU each node burned. It prints a CSV block at the end for charting. It
drops its graph afterwards and never touches anything else on the instance.

```bash
./mvnw compile exec:java \
  -Dexec.mainClass=com.falkordb.examples.replica.RoutingComparison \
  -Dsentinel.host=singlezonesentinellblb.your-instance.cloud -Dsentinel.port=26379 \
  -Dsentinel.tls=false -Dtls=false \
  -Duser=falkordb -Dpassword=...
```

The sweep defaults to `1,2,4,8,16,32,64` threads. Narrow it with `-Dthreads=4,8`.

Run it from a machine in the same region as the database. At low concurrency throughput is bounded
by round trip time, so a client sitting in another region measures the distance between them rather
than anything about routing.

CPU is read from `INFO cpu` as a before and after delta, because the FalkorDB Cloud ACL user is not
permitted to run `CONFIG RESETSTAT` and the counters are cumulative.

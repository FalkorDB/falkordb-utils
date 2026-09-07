# FalkorDB Utils, internal runbook

Internal notes for FalkorDB engineers and solutions engineers. Not customer facing. Customer facing
documentation lives next to each example, for instance
[`java-examples/replica-read-routing/README.md`](../java-examples/replica-read-routing/README.md).

This file collects the procedures we want repeatable. Add a new section per topic.

| Section | Topic |
|---|---|
| [Read from replica](#read-from-replica) | Reproducing the Java read routing benchmark on FalkorDB Cloud |

---

# Read from replica

## TL;DR

Routing reads across the primary and the replica roughly doubles read throughput on a two pod
instance, but **only once the primary is saturated**. Below about 8 client threads it changes
nothing.

Measured at 64 client threads, **2 cores per database pod**, **4 core client**:

| | Primary only | Rotating |
|---|---|---|
| Throughput | 14,345 reads/sec | **26,396 reads/sec** |
| Replica cores busy | **0.00 of 2** | **1.83 of 2** |
| Rejected in 5 minutes | **117,217** | **0** |

What to say and not say:

- **Say** the ceiling is 2x, set by 2 cores per pod, and we measured 1.84x
- **Say** primary only dropped 117,217 requests while the replica was idle
- **Do not** quote localhost or Docker numbers, see [Where the numbers must come from](#where-the-numbers-must-come-from)
- **Do not** present the 18 passing tests as evidence for throughput or replication, see [The test suite](#the-test-suite)
- **Do not** promise failover behaviour, it was never tested

## Test environment

Reproduce on this or state clearly that you did not.

| | Spec |
|---|---|
| Database | FalkorDB Cloud, 2 pods, one primary and one replica |
| **Cores per database pod** | **2** |
| `THREAD_COUNT` | 2 per node |
| `MAX_QUEUED_QUERIES` | 50 per node |
| `maxmemory` | 5.25 GB per node, `noeviction` |
| Region | AWS us-east-2 |
| Client | EC2 c4.xlarge, same region, Ubuntu 24.04 |
| **Client cores** | **4 vCPU**, 7 GB RAM |
| Client runtime | OpenJDK 17, Maven, jfalkordb 0.11.1 |
| Dataset | about 1,980,000 nodes, 338 MB |
| Network | Plaintext on Sentinel and data ports, 1.18 ms round trip |

The core counts are the two numbers people forget to record and then cannot explain their results.
**2 cores per pod** is why the ceiling is 2x and why `cores busy` saturates at 1.88 rather than
climbing further. **4 client cores** is why 64 client threads is a fair test rather than a client
side queueing artifact, since the threads block on the network rather than competing for CPU.

## What this proves

That a replica bought for failover is idle capacity, and that routing reads across the primary and
the replica converts it into throughput without changing the data model or the queries.

The measured claim, at 64 client threads against a two pod instance with 2 cores per pod:

| | Primary only | Rotating |
|---|---|---|
| Throughput | 14,345 reads/sec | 26,396 reads/sec |
| Mean latency | 4.46 ms | 2.43 ms |
| Primary cores busy | 1.88 of 2 | 1.84 of 2 |
| Replica cores busy | 0.00 of 2 | 1.83 of 2 |
| Rejected queries in 5 minutes | 117,217 | 0 |

## Where the numbers must come from

**Do not quote localhost or Docker numbers.** A loopback run has sub 0.1 ms round trip time and no
pod CPU limits, which flatters routing at low thread counts and hides the queueing behaviour that
produces the rejection result. We ran a local Docker setup first and its numbers were not usable for
anything customer facing.

Every figure in the example README came from:

- the database on **FalkorDB Cloud**, two pods, one primary and one replica, **2 cores per pod**
- the client on an **EC2 box in the same region as the database**, **4 vCPU**
- **90 second** stages for the sweep, **300 second** stages for the 64 thread confirmation

Always record both core counts alongside a result. Without them a throughput number cannot be
compared against anything and the 2x ceiling cannot be justified.

Same region matters. At low concurrency throughput is `threads / latency` and latency is dominated
by round trip time, so a client in another region measures the distance between the two rather than
anything about routing.

## Prerequisites

On the client box:

```bash
sudo apt-get update && sudo apt-get install -y openjdk-17-jdk maven redis-tools
```

`redis-tools` is not needed to run the benchmark. It is worth having because probing the instance by
hand is the fastest way to answer the questions in the next section.

## Probe the instance before you run anything

The two facts that change per instance are whether ports are TLS and what the Sentinel master is
called. Getting either wrong wastes a lot of time, because a TLS attempt against a plaintext port
**hangs rather than failing**.

```bash
# plaintext Sentinel. NOAUTH means plaintext and reachable
redis-cli -h <sentinel-host> -p 26379 PING

# if that hangs, it is TLS
redis-cli --tls -h <sentinel-host> -p 26379 PING

# who is primary, who is replica
redis-cli -h <sentinel-host> -p 26379 --user <user> --pass <password> --no-auth-warning \
  SENTINEL masters
```

Sentinel and the data ports are configured separately and **do not have to match**. One instance we
tested had TLS on Sentinel and one had plaintext on both. `SentinelTopology` takes separate TLS and
credential settings for exactly this reason.

Also record `THREAD_COUNT` and `MAX_QUEUED_QUERIES`, because they set the ceiling and explain the
rejections:

```bash
redis-cli -h <node-host> -p 6379 --user <user> --pass <password> --no-auth-warning \
  GRAPH.CONFIG GET '*'
```

Cloud instances we measured ran `THREAD_COUNT=2`, so one node executes two queries at a time. That
puts the theoretical ceiling for a second node at 2x. We measured 1.84x, which is the number to
quote. If a customer instance has a higher `THREAD_COUNT` the ceiling is the same 2x, but it takes
more client threads to reach it.

## Run it

```bash
git clone https://github.com/FalkorDB/falkordb-utils.git
cd falkordb-utils/java-examples/replica-read-routing

./mvnw compile exec:java \
  -Dexec.mainClass=com.falkordb.examples.replica.RoutingComparison \
  -Dmachines=660000 -Dseconds=90 -Dthreads=4,16,64 \
  -Dsentinel.host=<sentinel-host> -Dsentinel.port=26379 \
  -Dsentinel.tls=false -Dtls=false \
  -Duser=<user> -Dpassword=<password>
```

Seeding is idempotent, so a rerun reuses the graph instead of rebuilding it. 660,000 machines is
about 1,980,000 nodes and consumed 338 MB, so check `maxmemory` headroom but do not expect trouble.

Run it under `nohup` and poll the log. A full sweep is about 9 minutes of load and an SSH drop
otherwise kills it.

## Verify it before you believe it

Three checks, in order of how often they have caught something.

**Confirm the split actually happened.** If routing silently fell back to the primary the throughput
numbers are meaningless. `ReplicaReadDemo` reports the per node counts, and the run should show an
even split rather than everything on one node.

**Rule out ordering bias.** `PRIMARY_ONLY` runs first by default, so progressive cache warming would
flatter rotating. Rerun with the order reversed and confirm the gain survives:

```bash
-Dmodes=ROUND_ROBIN,PRIMARY_ONLY
```

We did this and the gain held, 35% at 32 threads and 63% at 64.

**Sanity check the shape of the result.** A gain at low thread counts would be the suspicious
outcome, not a gain at high thread counts. Below about 8 threads nothing is queueing, so no routing
change can help and we measure none. If a run shows rotating winning at 1 or 2 threads, something is
wrong with the run.

## Reading the dashboard

Use the **commands per second** panel. During rotating, `graph.RO_QUERY` should appear on both nodes
at similar rates.

**Do not trust a tooltip on the CPU panel.** It is sampled coarsely enough that the cursor can land
in a trough and report a busy node as idle. We hit this during a rotating stage. The panel showed
the replica at 0.05% while the node was in fact at 1.83 of 2 cores busy. Take `INFO cpu` as a delta
over a window instead:

```bash
# used_cpu_user + used_cpu_sys, before and after, divided by the interval
redis-cli -h <node-host> -p 6379 --user <user> --pass <password> --no-auth-warning INFO cpu
```

The benchmark does this internally. It reads deltas rather than resetting counters because the
FalkorDB Cloud ACL user is not permitted to run `CONFIG RESETSTAT`.

If you are capturing screenshots for a customer, allow about 90 seconds after a mode switch before
capturing, so the second connection pool has filled and the plateau has settled.

## The test suite

```bash
./mvnw test
```

Currently 18 tests, all passing. 11 in `FalkorGraphFactoryTest` and 7 in `SentinelTopologyTest`.

Be precise about what they cover, because it is easy to overstate. They verify **routing logic and
Sentinel response parsing offline**. They check that round robin rotates evenly across the primary
and every replica, that `REPLICA_ONLY` excludes the primary, that `PRIMARY_ONLY` never touches a
replica, that a per read override wins over the default, that `REPLICA_ONLY` falls back to the
primary rather than failing every read, that invalid configuration is rejected at build time, and
that `toString` does not leak credentials.

They do **not** touch a database. Nothing in the suite verifies real replication, staleness,
failover, or the throughput claims. That evidence comes from running `ReplicaReadDemo` and
`RoutingComparison` against a real instance, which is why the raw run logs are committed under
`java-examples/replica-read-routing/docs/`.

## Regenerating the charts

```bash
python3 java-examples/replica-read-routing/docs/make_artifacts.py
```

It reads the committed `benchmark-run.txt` and rewrites the PNGs and the workbook. It needs
`matplotlib` and `openpyxl` and no database access.

## Traps we already hit

Listed because each one cost real time.

**Maven picks a compiler plugin from 2013.** Maven 3.8.7 defaults `maven-compiler-plugin` to 3.1,
which predates the `release` flag, and the build dies with `Source option 5 is no longer supported`.
The pom pins 3.13.0. Pin it in any new example pom.

**SSM may not be usable.** Our client box had no IAM instance profile attached, so the SSM agent
could never register, and the account had no instance profiles to attach. Fall back to SSH rather
than trying to fix IAM. Check for a profile before planning around SSM.

**`pgrep -f <pattern>` matches the shell running it.** A wait loop like
`while pgrep -f RoutingComparison; do sleep 5; done` never exits, because the bash process running
that command contains the pattern in its own command line. This cost us a full benchmark run. Match
on something absent from the wait command, or check the log for a completion marker instead.

**Piping Maven hides its exit code.** `mvn ... | grep ...` makes `$?` the exit code of `grep`. Use
`${PIPESTATUS[0]}`.

**Cluster mode is out of scope and now fails loudly.** `FalkorGraphFactory` calls `rejectClusterMode`
at startup and throws on `cluster_enabled:1`. The routing model here is one primary with replicas.
Note that the check runs inside the role verification block, so disabling verification also disables
the cluster guard.

## Teardown

Leaving a two pod cloud instance and an EC2 box running is the expensive part of this exercise.

```bash
aws ec2 stop-instances --instance-ids <instance-id> --profile <profile> --region <region>
aws ec2 describe-instances --instance-ids <instance-id> --profile <profile> --region <region> \
  --query 'Reservations[0].Instances[0].State.Name' --output text
```

Stop the client first so nothing is holding connections, then delete the cloud instance from the
FalkorDB Cloud console. There is no CLI for the cloud instance, it has to be the console.

The benchmark keeps its graph by default so reruns skip seeding. Pass `-Ddrop=true` if you need to
leave the instance clean instead.

## Not yet covered

Worth knowing before you promise any of it to a customer.

- **Failover is untested.** We never killed the primary to watch Sentinel promote the replica and
  see how the factory behaves during and after the switch. This is the most obvious gap.
- **No Python equivalent.** The example is Java only.
- **TLS was not exercised end to end.** Both instances we measured had plaintext data ports, so
  credentials crossed the wire unencrypted. The code supports TLS on Sentinel and data ports
  independently, but the benchmark numbers were not gathered over TLS and TLS will cost some
  throughput.
- **One replica only.** The code accepts many replicas and rotates over all of them, and the unit
  tests cover that, but we have only measured one.

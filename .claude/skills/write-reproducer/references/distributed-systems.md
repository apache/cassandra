# Distributed Systems Bug Repros

Distributed bugs involve multiple nodes, network conditions, clock behavior, or distributed coordination. They require higher tiers on the minimality lattice (Tier 5-7).

## Key principle: always try a lower tier first

Many "distributed" bugs can actually be triggered on a single node. Try single-instance first. If the bug disappears, that's valuable: it means the bug genuinely requires distributed coordination, and you now know the minimum instance count.

## Jepsen test structure

Jepsen is the canonical framework for distributed correctness testing. A Jepsen test has:

- **Control node**: orchestrates the test
- **DB nodes**: run the system under test (typically 5 nodes)
- **Generator**: produces a stream of operations (reads, writes, CAS)
- **Client**: translates operations into system-specific API calls
- **Nemesis**: injects faults (partitions, kills, clock skew, disk corruption)
- **Checker**: validates the history (linearizability, sequential consistency, etc.)

```clojure
;; Jepsen test skeleton (Clojure)
(deftest register-test
  (let [test (jepsen/run!
               (merge tests/noop-test
                      {:name      "register"
                       :nodes     ["n1" "n2" "n3" "n4" "n5"]
                       :client    (register-client)
                       :nemesis   (nemesis/partition-random-halves)
                       :generator (gen/phases
                                    (->> (gen/mix [r w cas])
                                         (gen/stagger 1/10)
                                         (gen/nemesis
                                           (gen/seq (cycle [(gen/sleep 5)
                                                           {:type :info :f :start}
                                                           (gen/sleep 5)
                                                           {:type :info :f :stop}])))
                                         (gen/time-limit 60)))
                       :checker   (checker/linearizable
                                    {:model (model/cas-register)
                                     :algorithm :linear})}))]
    (is (:valid? (:results test)))))
```

Jepsen saves histories to `store/<test-name>/<date>/` for offline replay.

## Cassandra in-JVM dtest patterns

For Cassandra-specific distributed bugs, in-JVM dtests are faster than Jepsen because all nodes run in one JVM.

### Basic cluster test
```java
try (Cluster cluster = Cluster.build().withNodes(3)
        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
        .start()) {
    // setup, trigger, oracle
}
```

### Node failure during operation
```java
// TRIGGER: write data, kill a node, verify reads
cluster.coordinator(1).execute("INSERT INTO ks.t (pk, v) VALUES (1, 1)", ConsistencyLevel.ALL);
cluster.get(2).shutdown().get();
Object[][] result = cluster.coordinator(1).execute("SELECT v FROM ks.t WHERE pk = 1", ConsistencyLevel.QUORUM);
assertThat(result[0][0]).isEqualTo(1);
```

### Network partition simulation
```java
// Message filtering to simulate partition
cluster.filters().verbs(Verb.MUTATION_REQ.id).from(1).to(2).drop();
// Now mutations from node 1 won't reach node 2
```

### Bootstrap / decommission / topology change
```java
// Start with 3 nodes, bootstrap a 4th
IInstanceConfig config = cluster.newInstanceConfig();
config.set("auto_bootstrap", true);
cluster.bootstrap(config).startup();
// Run workload during bootstrap, check for data loss
```

### Repair
```java
cluster.get(1).nodetoolResult("repair", "ks").asserts().success();
// Verify data consistency after repair
```

## Cassandra simulator (CEP-10)

The Cassandra simulator provides deterministic execution for Cassandra cluster scenarios. It controls the thread scheduler and network, making races reproducible with a seed.

Key namespace: `org.apache.cassandra.simulator.*`

Seeds are printed on failure. To replay: pass the same seed to get the exact same execution.

## testcontainers patterns

When you need real external services but not a full cluster framework:

```java
// Java
@Testcontainers
class ReproTest {
    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Container
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:16.2-alpine");

    @Test
    void issue_xxx() {
        // Use kafka.getBootstrapServers() and pg.getJdbcUrl()
    }
}
```

Pin images by version (not `:latest`). Use `@Container` for lifecycle management.

## Fault injection patterns

### Network partitions
- **Symmetric**: both sides cannot communicate
- **Asymmetric**: A can send to B but not receive from B
- **Partial**: only specific message types are dropped

### Node failures
- **Clean shutdown**: graceful stop
- **Kill -9**: immediate termination, no cleanup
- **Pause/resume**: simulate GC pause or freeze (SIGSTOP/SIGCONT)

### Clock issues
- **Skew**: offset one node's clock by minutes/hours
- **Drift**: gradually increase clock rate
- **Jump**: sudden clock change (forward or backward)

### Disk issues
- **Full disk**: fill the data directory
- **Slow disk**: inject latency on I/O
- **Corrupt data file**: flip bits in specific files

## What to include in a distributed repro

1. **Topology**: number of nodes, datacenter layout, replication factor
2. **Workload**: operations, rate, consistency levels
3. **Fault scenario**: what fault, when injected, duration
4. **Oracle**: what invariant is violated (linearizability, durability, liveness)
5. **Timing**: when the fault is injected relative to the workload
6. **Version**: exact commit or release version
7. **Configuration**: any non-default settings

## Minimization for distributed repros

Reduce in this order:
1. **Node count**: 5 → 3 → 2 → 1 (if it reproduces at 1, it's not a distributed bug)
2. **Replication factor**: 3 → 2 → 1
3. **Workload size**: 1000 ops → 100 → 10 → 1
4. **Fault complexity**: partition + kill + clock skew → just partition → just kill
5. **Concurrency**: many clients → 2 → 1
6. **Configuration**: remove non-default settings one at a time
7. **Schema**: remove unused columns, tables, indexes

After each reduction, verify the *same* invariant violation occurs.

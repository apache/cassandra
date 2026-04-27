# Advanced Testing Patterns

Advanced patterns for complex testing scenarios in the Cassandra in-JVM dtest framework.

## CMS (Cluster Metadata Service) Testing

### Testing CMS Convergence

Wait for all nodes to reach the same cluster metadata state:

```java
@Test
public void testCMSConvergence() throws IOException {
    try (Cluster cluster = Cluster.build(3).start()) {
        // Perform schema change
        cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                            "{'class': 'SimpleStrategy', 'replication_factor': 2}");

        // Wait for CMS to quiesce
        ClusterUtils.waitForCMSToQuiesce(cluster);

        // Verify all nodes at same epoch
        Epoch epoch1 = ClusterUtils.getCurrentEpoch(cluster.get(1));
        Epoch epoch2 = ClusterUtils.getCurrentEpoch(cluster.get(2));
        Epoch epoch3 = ClusterUtils.getCurrentEpoch(cluster.get(3));

        Assert.assertEquals(epoch1, epoch2);
        Assert.assertEquals(epoch2, epoch3);
    }
}
```

### Pausing CMS Operations

Test scenarios by pausing CMS at specific points:

```java
@Test
public void testPausedCMS() throws Exception {
    try (Cluster cluster = Cluster.build(3).start()) {
        IInvokableInstance cms = cluster.get(1);

        // Pause before adding node
        Callable<Epoch> pauseHandle = ClusterUtils.pauseBeforeCommit(
            cms,
            transformation -> transformation instanceof AddNode
        );

        // Start add node operation in background
        Future<?> addNodeFuture = CompletableFuture.runAsync(() -> {
            IInvokableInstance newNode = ClusterUtils.addInstance(cluster);
            newNode.startup();
        });

        // Wait for pause
        Epoch pausedEpoch = pauseHandle.call();
        System.out.println("Paused at epoch: " + pausedEpoch);

        // Verify transformation hasn't completed
        Epoch currentEpoch = ClusterUtils.getCurrentEpoch(cluster.get(2));
        Assert.assertTrue(currentEpoch.getEpoch() < pausedEpoch.getEpoch());

        // Resume
        ClusterUtils.unpauseCommits(cms);

        // Wait for completion
        addNodeFuture.get(30, TimeUnit.SECONDS);

        // Verify convergence
        ClusterUtils.waitForCMSToQuiesce(cluster);
    }
}
```

### Testing CMS Snapshot

```java
@Test
public void testCMSSnapshot() throws IOException {
    try (Cluster cluster = Cluster.build(2).start()) {
        // Make some changes
        cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                            "{'class': 'SimpleStrategy', 'replication_factor': 1}");

        // Trigger snapshot
        Epoch snapshotEpoch = ClusterUtils.snapshotClusterMetadata(cluster.get(1));

        // Verify snapshot created
        Assert.assertNotNull(snapshotEpoch);
        Assert.assertTrue(snapshotEpoch.getEpoch() > 0);

        // Verify both nodes see snapshot
        Epoch epoch1 = ClusterUtils.getCurrentEpoch(cluster.get(1));
        Assert.assertEquals(snapshotEpoch, epoch1);
    }
}
```

### Fetch Log from CMS

```java
@Test
public void testFetchFromCMS() throws IOException {
    try (Cluster cluster = Cluster.build(3).start()) {
        // Node 3 is behind
        ClusterUtils.dropAllEntriesBeginningAt(cluster.get(3), Epoch.create(5));

        // Perform operations
        cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                            "{'class': 'SimpleStrategy', 'replication_factor': 2}");

        // Get target epoch from leader
        Epoch targetEpoch = ClusterUtils.getCurrentEpoch(cluster.get(1));

        // Fetch log on node 3
        Epoch fetchedEpoch = ClusterUtils.fetchLogFromCMS(cluster.get(3), targetEpoch);

        // Verify node 3 caught up
        Assert.assertEquals(targetEpoch, fetchedEpoch);
    }
}
```

## Accord (CEP-15) Testing

### Testing Accord Transactions

```java
@Test
public void testAccordTransaction() throws IOException {
    try (Cluster cluster = Cluster.build()
                                  .withNodes(3)
                                  .withConfig(config -> config
                                      .set("accord.enabled", "true")
                                      .set("accord.journal_directory", "/tmp/accord"))
                                  .start()) {

        cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                            "{'class': 'SimpleStrategy', 'replication_factor': 2}");
        cluster.schemaChange("CREATE TABLE test.accounts (" +
                            "id int PRIMARY KEY, " +
                            "balance int) " +
                            "WITH accord.enabled = true");

        // Execute Accord transaction
        cluster.coordinator(1).execute(
            "BEGIN TRANSACTION " +
            "INSERT INTO test.accounts (id, balance) VALUES (1, 100); " +
            "INSERT INTO test.accounts (id, balance) VALUES (2, 200); " +
            "COMMIT TRANSACTION",
            ConsistencyLevel.QUORUM
        );

        // Verify transaction completed
        Object[][] rows = cluster.coordinator(2).execute(
            "SELECT * FROM test.accounts",
            ConsistencyLevel.QUORUM
        );

        Assert.assertEquals(2, rows.length);
    }
}
```

### Query Accord Transaction State

```java
@Test
public void testAccordTxnState() throws IOException {
    try (Cluster cluster = buildAccordCluster(3)) {
        // Get TxnId from transaction
        TxnId txnId = ...; // Obtain from transaction execution

        // Query state across cluster
        String state = ClusterUtils.queryTxnStateAsString(cluster, txnId);
        System.out.println("Transaction state:\n" + state);

        // Query specific nodes
        LinkedHashMap<String, SimpleQueryResult> results =
            ClusterUtils.queryTxnState(cluster, txnId, 1, 2);

        for (Map.Entry<String, SimpleQueryResult> entry : results.entrySet()) {
            System.out.println("Node: " + entry.getKey());
            SimpleQueryResult result = entry.getValue();
            while (result.hasNext()) {
                Row row = result.next();
                System.out.println("  " + Arrays.toString(row.toObjectArray()));
            }
        }
    }
}
```

### Wait for Accord Epoch Ready

```java
@Test
public void testAccordEpochReady() throws IOException {
    try (Cluster cluster = buildAccordCluster(3)) {
        // Get current epoch
        long currentEpoch = cluster.get(1).callOnInstance(() -> {
            return ClusterMetadata.current().epoch.getEpoch();
        });

        // Wait for epoch to be ready for reads
        ClusterUtils.awaitAccordEpochReady(cluster, currentEpoch);

        // Now safe to perform Accord operations
    }
}
```

## Testing Topology Changes

### Bootstrap with Streaming

```java
@Test
public void testBootstrapStreaming() throws IOException {
    try (Cluster cluster = Cluster.build(2)
                                  .withConfig(c -> c.with(NETWORK, GOSSIP))
                                  .start()) {

        cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                            "{'class': 'SimpleStrategy', 'replication_factor': 2}");
        cluster.schemaChange("CREATE TABLE test.data (k int PRIMARY KEY, v int)");

        // Insert data
        for (int i = 0; i < 1000; i++) {
            cluster.coordinator(1).execute(
                "INSERT INTO test.data (k, v) VALUES (?, ?)",
                ConsistencyLevel.QUORUM, i, i * 10);
        }

        // Force flush to create SSTables
        cluster.forEach(instance -> instance.runOnInstance(() -> {
            StorageService.instance.forceKeyspaceFlush("test");
        }));

        // Bootstrap new node
        IInvokableInstance newNode = ClusterUtils.addInstance(cluster, config -> {
            config.set("auto_bootstrap", true)
                  .with(NETWORK, GOSSIP);
        });

        // Monitor log for streaming
        long mark = newNode.logs().mark();
        newNode.startup();

        // Wait for bootstrap
        newNode.logs().watchFor(mark, "Bootstrap completed");
        ClusterUtils.awaitRingJoin(cluster.get(1), newNode);

        // Verify data streamed correctly
        int count = newNode.callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open("test").getColumnFamilyStore("data");
            return cfs.getLiveSSTables().size();
        });

        Assert.assertTrue("Expected SSTables on new node", count > 0);
    }
}
```

### Testing Decommission with Pending Writes

```java
@Test
public void testDecommissionWithWrites() throws Exception {
    try (Cluster cluster = Cluster.build(3).start()) {
        cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                            "{'class': 'SimpleStrategy', 'replication_factor': 2}");
        cluster.schemaChange("CREATE TABLE test.data (k int PRIMARY KEY, v int)");

        IInvokableInstance leaving = cluster.get(3);

        // Start continuous writes
        AtomicBoolean stopWrites = new AtomicBoolean(false);
        AtomicInteger writeCount = new AtomicInteger(0);

        Future<?> writeFuture = CompletableFuture.runAsync(() -> {
            int i = 0;
            while (!stopWrites.get()) {
                try {
                    cluster.coordinator(1).execute(
                        "INSERT INTO test.data (k, v) VALUES (?, ?)",
                        ConsistencyLevel.QUORUM, i++, i * 10);
                    writeCount.incrementAndGet();
                    Thread.sleep(10);
                } catch (Exception e) {
                    // Expected during decommission
                }
            }
        });

        // Start decommission
        Future<Boolean> decommissionFuture = CompletableFuture.supplyAsync(() -> {
            return ClusterUtils.decommission(leaving);
        });

        // Wait for decommission
        boolean success = decommissionFuture.get(60, TimeUnit.SECONDS);
        Assert.assertTrue("Decommission failed", success);

        // Stop writes
        stopWrites.set(true);
        writeFuture.get(5, TimeUnit.SECONDS);

        // Verify node left
        ClusterUtils.assertNotInRing(cluster.get(1), leaving);

        System.out.println("Completed writes during decommission: " + writeCount.get());
    }
}
```

### Testing Replace with Data

```java
@Test
public void testReplaceWithData() throws IOException {
    try (Cluster cluster = Cluster.build(3).start()) {
        cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                            "{'class': 'SimpleStrategy', 'replication_factor': 3}");
        cluster.schemaChange("CREATE TABLE test.data (k int PRIMARY KEY, v int)");

        // Insert data
        for (int i = 0; i < 100; i++) {
            cluster.coordinator(1).execute(
                "INSERT INTO test.data (k, v) VALUES (?, ?)",
                ConsistencyLevel.ALL, i, i * 10);
        }

        // Stop node 2 abruptly
        IInvokableInstance failed = cluster.get(2);
        ClusterUtils.stopAbrupt(cluster, failed);

        // Verify data still readable on quorum
        Object[][] rows = cluster.coordinator(1).execute(
            "SELECT COUNT(*) FROM test.data",
            ConsistencyLevel.QUORUM
        );
        Assert.assertEquals(100L, rows[0][0]);

        // Replace failed node
        IInvokableInstance replacement = ClusterUtils.replaceHostAndStart(
            cluster,
            failed
        );

        // Wait for replacement to join
        ClusterUtils.awaitRingJoin(cluster.get(1), replacement);

        // Verify data on replacement
        int count = replacement.callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open("test").getColumnFamilyStore("data");
            return (int) cfs.getMeanRowCount();
        });

        Assert.assertTrue("Expected data on replacement", count > 0);
    }
}
```

## Testing Failure Scenarios

### Network Partition Testing

```java
@Test
public void testNetworkPartition() throws IOException {
    try (Cluster cluster = Cluster.build(3).start()) {
        cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                            "{'class': 'SimpleStrategy', 'replication_factor': 3}");
        cluster.schemaChange("CREATE TABLE test.data (k int PRIMARY KEY, v int)");

        // Partition: node 1,2 | node 3
        cluster.filters().allVerbs()
            .from(1).to(3).drop().on();
        cluster.filters().allVerbs()
            .from(2).to(3).drop().on();
        cluster.filters().allVerbs()
            .from(3).to(1, 2).drop().on();

        // Write to majority partition (should succeed)
        cluster.coordinator(1).execute(
            "INSERT INTO test.data (k, v) VALUES (?, ?)",
            ConsistencyLevel.QUORUM, 1, 10);

        // Try write from minority partition (should timeout)
        try {
            cluster.coordinator(3).execute(
                "INSERT INTO test.data (k, v) VALUES (?, ?)",
                ConsistencyLevel.QUORUM, 2, 20);
            Assert.fail("Expected timeout");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof UnavailableException ||
                            e.getCause() instanceof WriteTimeoutException);
        }

        // Heal partition
        cluster.filters().reset();

        // Wait for gossip to converge
        ClusterUtils.awaitGossipSchemaMatch(cluster);

        // Verify read repair
        Object[][] rows = cluster.coordinator(3).execute(
            "SELECT * FROM test.data WHERE k = 1",
            ConsistencyLevel.ALL
        );

        Assert.assertEquals(1, rows.length);
    }
}
```

### Testing Read Repair

```java
@Test
public void testReadRepair() throws IOException {
    try (Cluster cluster = Cluster.build(3).start()) {
        cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                            "{'class': 'SimpleStrategy', 'replication_factor': 3}");
        cluster.schemaChange("CREATE TABLE test.data (k int PRIMARY KEY, v int) " +
                            "WITH read_repair = 'BLOCKING'");

        // Write to only 2 replicas
        cluster.filters().inbound()
            .to(3)
            .verbs(Verb.MUTATION_REQ.id)
            .drop()
            .on();

        cluster.coordinator(1).execute(
            "INSERT INTO test.data (k, v) VALUES (?, ?)",
            ConsistencyLevel.QUORUM, 1, 10);

        cluster.filters().reset();

        // Read at ALL should trigger read repair
        Object[][] rows = cluster.coordinator(1).execute(
            "SELECT * FROM test.data WHERE k = 1",
            ConsistencyLevel.ALL
        );

        Assert.assertEquals(1, rows.length);

        // Verify node 3 now has data
        SimpleQueryResult result = cluster.get(3).executeInternalWithResult(
            "SELECT v FROM test.data WHERE k = 1"
        );

        Assert.assertTrue(result.hasNext());
        Assert.assertEquals(10, result.next().getInteger("v"));
    }
}
```

### Testing Hinted Handoff

```java
@Test
public void testHintedHandoff() throws Exception {
    try (Cluster cluster = Cluster.build(3)
                                  .withConfig(c -> c
                                      .set("max_hint_window", "10000")
                                      .set("hinted_handoff_enabled", "true"))
                                  .start()) {

        cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                            "{'class': 'SimpleStrategy', 'replication_factor': 3}");
        cluster.schemaChange("CREATE TABLE test.data (k int PRIMARY KEY, v int)");

        // Stop node 3
        ClusterUtils.stopUnchecked(cluster.get(3));

        // Write (will create hints for node 3)
        cluster.coordinator(1).execute(
            "INSERT INTO test.data (k, v) VALUES (?, ?)",
            ConsistencyLevel.ONE, 1, 10);

        // Verify hint created
        int hintCount = cluster.get(1).callOnInstance(() -> {
            HintsCatalog catalog = HintsService.instance.getCatalog();
            return catalog.allHints().size();
        });

        Assert.assertTrue("Expected hints to be created", hintCount > 0);

        // Restart node 3
        cluster.get(3).startup();
        ClusterUtils.awaitRingJoin(cluster.get(1), cluster.get(3));

        // Wait for hints to be delivered
        Thread.sleep(5000);

        // Verify data on node 3
        SimpleQueryResult result = cluster.get(3).executeInternalWithResult(
            "SELECT v FROM test.data WHERE k = 1"
        );

        Assert.assertTrue(result.hasNext());
        Assert.assertEquals(10, result.next().getInteger("v"));
    }
}
```

## Testing Repair

### Full Repair Test

```java
@Test
public void testFullRepair() throws Exception {
    try (Cluster cluster = Cluster.build(3).start()) {
        cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                            "{'class': 'SimpleStrategy', 'replication_factor': 3}");
        cluster.schemaChange("CREATE TABLE test.data (k int PRIMARY KEY, v int)");

        // Create inconsistency
        cluster.filters().inbound().to(3).verbs(Verb.MUTATION_REQ.id).drop().on();

        for (int i = 0; i < 100; i++) {
            cluster.coordinator(1).execute(
                "INSERT INTO test.data (k, v) VALUES (?, ?)",
                ConsistencyLevel.QUORUM, i, i * 10);
        }

        cluster.filters().reset();

        // Verify node 3 is missing data
        int countBefore = cluster.get(3).callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open("test").getColumnFamilyStore("data");
            return cfs.getLiveSSTables().size();
        });

        Assert.assertEquals(0, countBefore);

        // Run repair
        NodeToolResult result = cluster.get(1).nodetoolResult("repair", "test", "data");
        result.asserts().success();

        // Wait for repair to complete
        Thread.sleep(10000);

        // Verify node 3 now has data
        int countAfter = cluster.get(3).callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open("test").getColumnFamilyStore("data");
            return cfs.getLiveSSTables().size();
        });

        Assert.assertTrue("Expected data after repair", countAfter > 0);
    }
}
```

## Multi-Datacenter Patterns

### Testing LOCAL_QUORUM

```java
@Test
public void testLocalQuorum() throws IOException {
    try (Cluster cluster = builder()
                              .withRacks(2, 1, 3)  // 2 DCs, 1 rack each, 3 nodes
                              .start()) {

        cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                            "{'class': 'NetworkTopologyStrategy', " +
                            "'datacenter1': 3, 'datacenter2': 3}");
        cluster.schemaChange("CREATE TABLE test.data (k int PRIMARY KEY, v int)");

        // Partition DC2 from DC1
        for (int dc1Node = 1; dc1Node <= 3; dc1Node++) {
            for (int dc2Node = 4; dc2Node <= 6; dc2Node++) {
                cluster.filters().allVerbs()
                    .from(dc1Node).to(dc2Node).drop().on();
                cluster.filters().allVerbs()
                    .from(dc2Node).to(dc1Node).drop().on();
            }
        }

        // LOCAL_QUORUM write in DC1 should succeed
        cluster.coordinator(1).execute(
            "INSERT INTO test.data (k, v) VALUES (?, ?)",
            ConsistencyLevel.LOCAL_QUORUM, 1, 10);

        // LOCAL_QUORUM read in DC1 should succeed
        Object[][] rows = cluster.coordinator(2).execute(
            "SELECT * FROM test.data WHERE k = 1",
            ConsistencyLevel.LOCAL_QUORUM
        );

        Assert.assertEquals(1, rows.length);

        // EACH_QUORUM should fail (can't reach DC2)
        try {
            cluster.coordinator(1).execute(
                "INSERT INTO test.data (k, v) VALUES (?, ?)",
                ConsistencyLevel.EACH_QUORUM, 2, 20);
            Assert.fail("Expected unavailable");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof UnavailableException);
        }
    }
}
```

## Performance Testing Patterns

### Testing Under Load

```java
@Test
public void testUnderLoad() throws Exception {
    try (Cluster cluster = Cluster.build(3)
                                  .withConfig(c -> c
                                      .set("concurrent_reads", "128")
                                      .set("concurrent_writes", "128"))
                                  .start()) {

        cluster.schemaChange("CREATE KEYSPACE test WITH replication = " +
                            "{'class': 'SimpleStrategy', 'replication_factor': 2}");
        cluster.schemaChange("CREATE TABLE test.data (k int PRIMARY KEY, v int)");

        int numThreads = 10;
        int numOpsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                for (int i = 0; i < numOpsPerThread; i++) {
                    try {
                        int key = threadId * numOpsPerThread + i;
                        cluster.coordinator((key % 3) + 1).execute(
                            "INSERT INTO test.data (k, v) VALUES (?, ?)",
                            ConsistencyLevel.QUORUM, key, key * 10);
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        failureCount.incrementAndGet();
                    }
                }
            }));
        }

        // Wait for all threads
        for (Future<?> future : futures) {
            future.get();
        }

        executor.shutdown();

        System.out.println("Successes: " + successCount.get());
        System.out.println("Failures: " + failureCount.get());

        Assert.assertTrue("Too many failures", failureCount.get() < successCount.get() * 0.01);

        // Verify data
        Object[][] rows = cluster.coordinator(1).execute(
            "SELECT COUNT(*) FROM test.data",
            ConsistencyLevel.QUORUM
        );

        long count = (long) rows[0][0];
        Assert.assertTrue("Expected data written", count > 0);
    }
}
```

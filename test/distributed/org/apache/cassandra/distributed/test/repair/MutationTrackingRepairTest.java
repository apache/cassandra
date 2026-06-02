/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.distributed.test.repair;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.MutationTrackingIncrementalRepairTask;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.replication.migration.KeyspaceMigrationInfo;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.function.Predicate.not;
import static org.apache.cassandra.distributed.api.IMessageFilters.Matcher.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end tests for mutation tracking repair.
 *
 * Each test creates a unique keyspace. Keyspaces are not dropped between tests because
 * dropping a tracked keyspace while background offset broadcasts are in flight causes
 * NoSuchElementException in MutationTrackingService.getOrCreateShards (the broadcast
 * tries to look up the dropped keyspace's metadata). The keyspaces are cleaned up when
 * the cluster is closed at the end of the test class.
 */
public class MutationTrackingRepairTest extends TestBaseImpl
{
    private static final int NUM_NODES = 3;
    private static final List<Integer> ALL_NODES = List.of(1, 2, 3);

    private static Cluster CLUSTER;
    private static ExecutorService executor;
    private static final AtomicInteger ksCounter = new AtomicInteger();

    private String ksName;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build());
        CLUSTER = Cluster.build()
                         .withNodes(NUM_NODES)
                         .withConfig(cfg -> cfg.set("hinted_handoff_enabled", false)
                                               .set("mutation_tracking_sync_timeout", "10s")
                                               .set("request_timeout", "1000ms")
                                               .set("repair.retries.max_attempts", 10)
                                               .set("repair.retries.base_sleep_time", "100ms")
                                               .set("repair.retries.max_sleep_time", "500ms")
                                               .with(Feature.GOSSIP, Feature.NETWORK))
                         .start();
    }

    @AfterClass
    public static void teardownCluster()
    {
        executor.shutdownNow();
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Before
    public void setUp()
    {
        ksName = "mt_repair_" + ksCounter.incrementAndGet();
        CLUSTER.schemaChange("CREATE KEYSPACE " + ksName + " WITH replication = " +
                "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                "AND replication_type='tracked'");
        CLUSTER.schemaChange("CREATE TABLE " + ksName + ".tbl (k int PRIMARY KEY, v int)");
    }

    @After
    public void tearDown()
    {
        CLUSTER.filters().reset();
        CLUSTER.forEach(() ->
            Gossiper.runInGossipStageBlocking(() -> {
                for (var entry : Gossiper.instance.endpointStateMap.entrySet())
                {
                    InetAddressAndPort ep = entry.getKey();
                    EndpointState state = entry.getValue();
                    if (!ep.equals(FBUtilities.getBroadcastAddressAndPort()) && !state.isAlive())
                    {
                        FailureDetector.instance.report(ep);
                        Gossiper.instance.realMarkAlive(ep, state);
                    }
                }
            }));
    }

    private void setupUntracked()
    {
        ksName = "mt_repair_" + ksCounter.incrementAndGet();
        CLUSTER.schemaChange("CREATE KEYSPACE " + ksName + " WITH replication = " +
                "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                "AND replication_type='untracked'");
        CLUSTER.schemaChange("CREATE TABLE " + ksName + ".tbl (k int PRIMARY KEY, v int)");
    }

    private void createTable(String tableName)
    {
        CLUSTER.schemaChange("CREATE TABLE " + ksName + '.' + tableName + " (k int PRIMARY KEY, v int)");
    }

    private void alterKeyspaceToTracked()
    {
        CLUSTER.schemaChange("ALTER KEYSPACE " + ksName + " WITH replication = " +
                "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                "AND replication_type='tracked'");
    }

    private void alterKeyspaceToUntracked()
    {
        CLUSTER.schemaChange("ALTER KEYSPACE " + ksName + " WITH replication = " +
                "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                "AND replication_type='untracked'");
    }

    private void insertData(String tableName, int start, int count)
    {
        for (int i = start; i < start + count; i++)
        {
            CLUSTER.coordinator(1).execute(
                    "INSERT INTO " + ksName + '.' + tableName + " (k, v) VALUES (?, ?)",
                    ConsistencyLevel.ALL, i, i);
        }
    }

    private void insertDataWithInconsistency(String tableName, int start, int count)
    {
        insertDataWithInconsistency(2, tableName, start, count);
    }

    private void insertDataWithInconsistency(int isolatedNode, String tableName, int start, int count)
    {
        // Isolate a node so background reconcilation has some work to do
        CLUSTER.filters().allVerbs().to(isolatedNode).drop();
        CLUSTER.filters().allVerbs().from(isolatedNode).drop();

        for (int i = start; i < start + count; i++)
        {
            CLUSTER.coordinator(1).execute(
                    "INSERT INTO " + ksName + '.' + tableName + " (k, v) VALUES (?, ?)",
                    ConsistencyLevel.QUORUM, i, i);
        }

        // Verify the isolated node is actually missing the data we just wrote
        Object[][] results = CLUSTER.get(isolatedNode).executeInternal(
                "SELECT k FROM " + ksName + '.' + tableName + " WHERE k >= ? AND k < ? ALLOW FILTERING",
                start, start + count);
        assertEquals("Node " + isolatedNode + " should not have data written while isolated",
                0, results.length);
        CLUSTER.filters().reset();
    }

    private void assertDataOnAllNodes(String tableName, List<Integer> keys)
    {
        for (int node = 1; node <= CLUSTER.size(); node++)
        {
            for (int key : keys)
            {
                Object[][] results = CLUSTER.get(node).executeInternal(
                        "SELECT k, v FROM " + ksName + '.' + tableName + " WHERE k = ?", key);
                assertEquals("Node " + node + " missing row k=" + key, 1, results.length);
                assertEquals(key, results[0][0]);
                assertEquals(key, results[0][1]);
            }
        }
    }

    private void assertDataOnAllNodes(String tableName, int start, int count)
    {
        List<Integer> keys = new ArrayList<>(count);
        for (int i = start; i < start + count; i++)
            keys.add(i);
        assertDataOnAllNodes(tableName, keys);
    }

    private NodeToolResult nodetoolRepair(int node, String... args)
    {
        String[] cmd = new String[args.length + 1];
        cmd[0] = "repair";
        System.arraycopy(args, 0, cmd, 1, args.length);
        return CLUSTER.get(node).nodetoolResult(cmd);
    }

    private List<NodeToolResult> repairConcurrently(List<Integer> nodes, String... args)
    {
        List<Future<NodeToolResult>> futures = new ArrayList<>();
        for (int node : nodes)
            futures.add(executor.submit(() -> nodetoolRepair(node, args)));
        List<NodeToolResult> results = new ArrayList<>();
        for (Future<NodeToolResult> f : futures)
        {
            try
            {
                results.add(f.get(60, TimeUnit.SECONDS));
            }
            catch (Exception e)
            {
                throw new RuntimeException("Repair future failed", e);
            }
        }
        return results;
    }

    private void assertAllSuccess(List<NodeToolResult> results)
    {
        for (NodeToolResult r : results)
            r.asserts().success();
    }

    private void assertAllFailure(List<NodeToolResult> results)
    {
        for (NodeToolResult r : results)
            r.asserts().failure();
    }

    private String[] withPR(String... args)
    {
        String[] result = new String[args.length + 1];
        System.arraycopy(args, 0, result, 0, args.length);
        result[args.length] = "-pr";
        return result;
    }

    private void repairResolvingInconsistency(String... args) throws Exception
    {
        repairResolvingInconsistency(2, ALL_NODES, withPR(args));
    }

    private void repairResolvingInconsistency(int isolatedNode, List<Integer> nodes, String... args) throws Exception
    {
        // Dropping messages is to check that repair retries messages if needed
        CLUSTER.filters().allVerbs().to(isolatedNode).drop();
        CLUSTER.filters().allVerbs().from(isolatedNode).drop();

        List<Future<NodeToolResult>> futures = new ArrayList<>();
        for (int node : nodes)
            futures.add(executor.submit(() -> nodetoolRepair(node, args)));

        Thread.sleep(2000);
        assertTrue("Repair should be blocked while node " + isolatedNode + " is isolated",
                futures.stream().allMatch(not(Future::isDone)));

        CLUSTER.filters().reset();

        List<NodeToolResult> results = new ArrayList<>();
        for (Future<NodeToolResult> f : futures)
            results.add(f.get(30, TimeUnit.SECONDS));
        assertAllSuccess(results);

        // Run a second time to make sure repair can be run multiple times without failing
        assertAllSuccess(repairConcurrently(nodes, args));
    }

    private void repairFromNodesSuccess(List<Integer> nodes, String... args)
    {
        String[] prArgs = withPR(args);
        assertAllSuccess(repairConcurrently(nodes, prArgs));
        assertAllSuccess(repairConcurrently(nodes, prArgs));
    }

    private boolean isMigrationInProgress()
    {
        String ks = ksName;
        return CLUSTER.get(1).callOnInstance(() -> {
            ClusterMetadata metadata = ClusterMetadata.current();
            return MutationTrackingIncrementalRepairTask.isMutationTrackingMigrationInProgress(metadata, ks);
        });
    }

    private boolean isMigrationComplete()
    {
        String ks = ksName;
        return CLUSTER.get(1).callOnInstance(() -> {
            ClusterMetadata metadata = ClusterMetadata.current();
            return !metadata.mutationTrackingMigrationState.isMigrating(ks);
        });
    }

    /**
     * Get the primary token range for a node as [start, end] token values.
     * With SimpleStrategy RF=3 and 3 nodes, each node has exactly one primary range.
     */
    private long[] getPrimaryRangeTokens(int node)
    {
        String ks = ksName;
        return CLUSTER.get(node).callOnInstance(() -> {
            var ranges = StorageService.instance.getPrimaryRanges(ks);
            assertEquals(1, ranges.size());
            Range<Token> range = ranges.iterator().next();
            return new long[]{
                    ((Murmur3Partitioner.LongToken) range.left).token(),
                    ((Murmur3Partitioner.LongToken) range.right).token()
            };
        });
    }

    /**
     * Compute which integer keys from [start, start+count) hash into the given token range.
     */
    private List<Integer> keysInTokenRange(int start, int count, long rangeStart, long rangeEnd)
    {
        Range<Token> range = new Range<>(new Murmur3Partitioner.LongToken(rangeStart),
                new Murmur3Partitioner.LongToken(rangeEnd));
        List<Integer> keys = new ArrayList<>();
        for (int i = start; i < start + count; i++)
        {
            Token token = Murmur3Partitioner.instance.getToken(ByteBufferUtil.bytes(i));
            if (range.contains(token))
                keys.add(i);
        }
        return keys;
    }

    private String getBroadcastAddress(int node)
    {
        return CLUSTER.get(node).callOnInstance(() -> FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort());
    }

    private void isolateNode(int nodeToIsolate, int... observerNodes)
    {
        CLUSTER.filters().allVerbs().from(nodeToIsolate).drop();
        CLUSTER.filters().allVerbs().to(nodeToIsolate).drop();

        String isolatedAddress = CLUSTER.get(nodeToIsolate).callOnInstance(
                () -> FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort());
        for (int observer : observerNodes)
        {
            CLUSTER.get(observer).runOnInstance(() -> {
                try
                {
                    InetAddressAndPort neighbor = InetAddressAndPort.getByName(isolatedAddress);
                    FailureDetector.instance.forceConviction(neighbor);
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Test
    public void testBasicRepairHappyPath() throws Exception
    {
        insertDataWithInconsistency("tbl", 0, 100);

        repairResolvingInconsistency(ksName);

        assertDataOnAllNodes("tbl", 0, 100);
    }

    @Test
    public void testRepairSpecificTable() throws Exception
    {
        createTable("tbl1");
        createTable("tbl2");

        // Repair only tbl1
        insertDataWithInconsistency("tbl1", 0, 50);
        repairResolvingInconsistency(ksName, "tbl1");
        assertDataOnAllNodes("tbl1", 0, 50);

        // Repair only tbl2 while tbl1 already has repaired data
        insertDataWithInconsistency("tbl2", 0, 50);
        repairResolvingInconsistency(ksName, "tbl2");
        assertDataOnAllNodes("tbl2", 0, 50);

        // Repair both tables together
        insertDataWithInconsistency("tbl1", 50, 50);
        insertDataWithInconsistency("tbl2", 50, 50);
        repairResolvingInconsistency(ksName, "tbl1", "tbl2");
        assertDataOnAllNodes("tbl1", 0, 100);
        assertDataOnAllNodes("tbl2", 0, 100);
    }

    @Test
    public void testRepairAllTables() throws Exception
    {
        createTable("tbl1");
        createTable("tbl2");
        createTable("tbl3");

        insertDataWithInconsistency("tbl1", 0, 30);
        insertDataWithInconsistency("tbl2", 100, 30);
        insertDataWithInconsistency("tbl3", 200, 30);

        repairResolvingInconsistency(ksName);

        assertDataOnAllNodes("tbl1", 0, 30);
        assertDataOnAllNodes("tbl2", 100, 30);
        assertDataOnAllNodes("tbl3", 200, 30);
    }

    @Test
    public void testForceRepairWithNodeDown()
    {
        insertDataWithInconsistency(3, "tbl", 0, 50);

        isolateNode(2, 1, 3);

        List<Integer> liveNodes = List.of(1, 3);
        assertAllFailure(repairConcurrently(liveNodes, withPR(ksName)));

        repairFromNodesSuccess(liveNodes, ksName, "--force");

        for (int node : liveNodes)
        {
            for (int i = 0; i < 50; i++)
            {
                Object[][] results = CLUSTER.get(node).executeInternal(
                        "SELECT k, v FROM " + ksName + ".tbl WHERE k = ?", i);
                assertEquals("Node " + node + " missing row k=" + i, 1, results.length);
            }
        }
    }

    @Test
    public void testForceRepairWithAllNodesUp() throws Exception
    {
        insertDataWithInconsistency("tbl", 0, 50);

        repairResolvingInconsistency(ksName, "--force");

        assertDataOnAllNodes("tbl", 0, 50);
    }

    @Test
    public void testRepairWithSpecificHosts()
    {
        String addr1 = getBroadcastAddress(1);
        String addr3 = getBroadcastAddress(3);

        insertDataWithInconsistency(3, "tbl", 0, 50);

        // Node 2 is down, so normal repair should fail
        isolateNode(2, 1, 3);

        List<Integer> liveNodes = List.of(1, 3);
        assertAllFailure(repairConcurrently(liveNodes, withPR(ksName)));

        // Repair with --in-hosts scoped to only the live nodes should succeed
        // Note: --in-hosts cannot be combined with -pr
        String[] args = new String[]{ksName, "--in-hosts", addr1 + ',' + addr3};
        assertAllSuccess(repairConcurrently(liveNodes, args));
        assertAllSuccess(repairConcurrently(liveNodes, args));

        for (int node : liveNodes)
        {
            for (int i = 0; i < 50; i++)
            {
                Object[][] results = CLUSTER.get(node).executeInternal(
                        "SELECT k, v FROM " + ksName + ".tbl WHERE k = ?", i);
                assertEquals("Node " + node + " missing row k=" + i, 1, results.length);
            }
        }
    }

    @Test
    public void testMigrationUntrackedToTrackedCompletesViaRepair() throws Exception
    {
        setupUntracked();
        insertDataWithInconsistency("tbl", 0, 100);

        alterKeyspaceToTracked();
        assertTrue("Migration should be in progress after ALTER", isMigrationInProgress());

        repairResolvingInconsistency(ksName);
        assertTrue("Migration should complete after repair", isMigrationComplete());

        assertDataOnAllNodes("tbl", 0, 100);
    }

    @Test
    public void testDataAccessibleDuringMigrationToTracked() throws Exception
    {
        setupUntracked();
        dataAccessibleDuringMigration(() -> alterKeyspaceToTracked());
    }

    @Test
    public void testDataAccessibleAfterSwitchToUntracked()
    {
        insertDataWithInconsistency("tbl", 0, 50);

        alterKeyspaceToUntracked();
        assertFalse("Migration should NOT be in progress (tracked->untracked is instant)",
                    isMigrationInProgress());

        // Read at CL.ALL triggers blocking read repair to fix inconsistencies
        Object[][] results = CLUSTER.coordinator(1).execute(
                "SELECT k, v FROM " + ksName + ".tbl", ConsistencyLevel.ALL);
        assertEquals("Pre-switch data should be readable", 50, results.length);

        // Write and read more data
        insertData("tbl", 50, 50);

        results = CLUSTER.coordinator(1).execute(
                "SELECT k, v FROM " + ksName + ".tbl", ConsistencyLevel.ALL);
        assertEquals("All data should be readable after switch", 100, results.length);

        assertDataOnAllNodes("tbl", 0, 100);
    }

    private void dataAccessibleDuringMigration(Runnable alterKeyspace) throws Exception
    {
        insertDataWithInconsistency("tbl", 0, 50);

        alterKeyspace.run();

        Object[][] results = CLUSTER.coordinator(1).execute(
                "SELECT k, v FROM " + ksName + ".tbl", ConsistencyLevel.ALL);
        assertEquals("Pre-migration data should be readable", 50, results.length);

        insertData("tbl", 50, 50);

        results = CLUSTER.coordinator(1).execute(
                "SELECT k, v FROM " + ksName + ".tbl", ConsistencyLevel.ALL);
        assertEquals("All data should be readable during migration", 100, results.length);

        repairResolvingInconsistency(ksName);
        assertTrue("Migration should complete after repair", isMigrationComplete());

        results = CLUSTER.coordinator(1).execute(
                "SELECT k, v FROM " + ksName + ".tbl", ConsistencyLevel.ALL);
        assertEquals("All data should be readable after migration", 100, results.length);

        insertData("tbl", 100, 50);
        results = CLUSTER.coordinator(1).execute(
                "SELECT k, v FROM " + ksName + ".tbl", ConsistencyLevel.ALL);
        assertEquals("All data including post-migration should be readable", 150, results.length);

        assertDataOnAllNodes("tbl", 0, 150);
    }

    @Test
    public void testMigrationTrackedToUntrackedIsInstant()
    {
        insertData("tbl", 0, 100);

        alterKeyspaceToUntracked();
        assertFalse("Migration should NOT be in progress after ALTER (tracked->untracked is instant)",
                    isMigrationInProgress());
        assertTrue("Migration should be complete", isMigrationComplete());

        assertDataOnAllNodes("tbl", 0, 100);
    }

    @Test
    public void testForceRepairWithDeadNodeDoesNotAdvanceMigration()
    {
        repairWithDeadNodeDoesNotAdvanceMigration(withPR(ksName, "--force"));
    }

    @Test
    public void testInHostsRepairWithDeadNodeDoesNotAdvanceMigration()
    {
        String addr1 = getBroadcastAddress(1);
        String addr3 = getBroadcastAddress(3);
        repairWithDeadNodeDoesNotAdvanceMigration(ksName, "--in-hosts", addr1 + ',' + addr3);
    }

    private void repairWithDeadNodeDoesNotAdvanceMigration(String... repairArgs)
    {
        setupUntracked();
        insertDataWithInconsistency(3, "tbl", 0, 50);

        alterKeyspaceToTracked();
        assertTrue("Migration should be in progress", isMigrationInProgress());

        isolateNode(2, 1, 3);

        List<Integer> liveNodes = List.of(1, 3);
        assertAllSuccess(repairConcurrently(liveNodes, repairArgs));

        String ks = ksName;
        assertTrue("Migration should not advance with dead nodes excluded",
                CLUSTER.get(1).callOnInstance(() -> {
                    ClusterMetadata metadata = ClusterMetadata.current();
                    return metadata.mutationTrackingMigrationState.isMigrating(ks);
                }));
    }

    @Test
    public void testInHostsRepairSucceedsWhenSpecifiedHostIsNetworkBlocked()
    {
        String addr1 = getBroadcastAddress(1);
        String addr3 = getBroadcastAddress(3);

        insertDataWithInconsistency(3, "tbl", 0, 50);

        // Block network to node 2 but do NOT mark it down in gossip
        CLUSTER.filters().allVerbs().from(2).drop();
        CLUSTER.filters().allVerbs().to(2).drop();

        // Repair specifying only live hosts should succeed despite node 2 being blocked
        List<Integer> liveNodes = List.of(1, 3);
        String[] args = new String[]{ksName, "--in-hosts", addr1 + ',' + addr3};
        assertAllSuccess(repairConcurrently(liveNodes, args));
    }

    @Test
    public void testPreviewRepairDoesNotAdvanceMigration() throws Exception
    {
        setupUntracked();
        insertDataWithInconsistency("tbl", 0, 50);

        alterKeyspaceToTracked();
        assertTrue("Migration should be in progress", isMigrationInProgress());

        repairResolvingInconsistency(ksName, "--preview");

        assertTrue("Migration should not advance with preview repair", isMigrationInProgress());
    }

    @Test
    public void testSubrangeRepair() throws Exception
    {
        long[] primaryRange = getPrimaryRangeTokens(1);
        String st = Long.toString(primaryRange[0]);
        String et = Long.toString(primaryRange[1]);

        insertDataWithInconsistency("tbl", 0, 100);

        repairResolvingInconsistency(2, ALL_NODES, ksName, "-st", st, "-et", et);

        List<Integer> keysInRange = keysInTokenRange(0, 100, primaryRange[0], primaryRange[1]);
        assertFalse("Should have keys hashing into node 1's primary range", keysInRange.isEmpty());

        assertDataOnAllNodes("tbl", keysInRange);
    }

    @Test
    public void testSubrangeRepairAdvancesMigrationOnlyForSpecifiedRange() throws Exception
    {
        setupUntracked();
        long[] primaryRange = getPrimaryRangeTokens(1);
        String st = Long.toString(primaryRange[0]);
        String et = Long.toString(primaryRange[1]);

        insertDataWithInconsistency("tbl", 0, 100);

        alterKeyspaceToTracked();
        assertTrue("Full ring should be pending", isMigrationInProgress());

        // During migration, subrange repair uses incremental repair. Running from all nodes
        // on the same subrange causes anti-compaction conflicts, so repair from a single node.
        repairResolvingInconsistency(2, List.of(1), ksName, "-st", st, "-et", et);

        assertTrue("Migration should not be complete after subrange repair",
                isMigrationInProgress());

        // Verify the repaired range is no longer pending but other ranges still are
        String ks = ksName;
        long rangeStart = primaryRange[0];
        long rangeEnd = primaryRange[1];
        CLUSTER.get(1).runOnInstance(() -> {
            ClusterMetadata metadata = ClusterMetadata.current();
            KeyspaceMigrationInfo info = metadata.mutationTrackingMigrationState.getKeyspaceInfo(ks);
            assertNotNull("Migration info should still exist", info);

            Range<Token> repairedRange = new Range<>(new Murmur3Partitioner.LongToken(rangeStart),
                    new Murmur3Partitioner.LongToken(rangeEnd));
            for (var entry : info.pendingRangesPerTable.entrySet())
            {
                for (Range<Token> pending : entry.getValue())
                {
                    assertFalse("Repaired range should not overlap with pending ranges for table " + entry.getKey(),
                            repairedRange.intersects(pending));
                }
            }
        });

        // Verify all keys in the repaired range are present on all nodes
        List<Integer> keysInRange = keysInTokenRange(0, 100, primaryRange[0], primaryRange[1]);
        assertFalse("Should have keys hashing into node 1's primary range", keysInRange.isEmpty());

        assertDataOnAllNodes("tbl", keysInRange);
    }

    @Test
    public void testRepairRejectsMixedMigratedAndPendingRanges()
    {
        setupUntracked();
        insertData("tbl", 0, 50);

        alterKeyspaceToTracked();
        assertTrue("Migration should be in progress after ALTER", isMigrationInProgress());

        long[] primaryRange = getPrimaryRangeTokens(1);
        String st = Long.toString(primaryRange[0]);
        String et = Long.toString(primaryRange[1]);

        // Repair node 1's primary range to advance migration for that subrange only.
        // Run from single node to avoid anti-compaction conflicts during migration IR.
        nodetoolRepair(1, ksName, "-st", st, "-et", et).asserts().success();

        // Now attempt a repair with a range that straddles the migrated/pending boundary.
        // Node 1's primary range has been repaired (no longer pending), but the range
        // immediately after is still pending. A range spanning both should be rejected.
        String straddleSt = Long.toString(primaryRange[1] - 1);
        String straddleEt = Long.toString(primaryRange[1] + 1000);
        NodeToolResult result = nodetoolRepair(1, ksName, "-st", straddleSt, "-et", straddleEt);
        result.asserts().failure();
        assertTrue("Expected partial overlap error but got: " + result.getStderr(),
                result.getStderr().contains("partially overlap with migration pending ranges"));
    }

    @Test
    public void testRepairTimeout()
    {
        insertData("tbl", 0, 50);

        CLUSTER.filters().verbs(Verb.MT_SYNC_REQ.id).to(2).drop();
        CLUSTER.filters().verbs(Verb.MT_SYNC_REQ.id).from(2).drop();

        List<NodeToolResult> results = repairConcurrently(ALL_NODES, withPR(ksName));
        assertAllFailure(results);
        for (NodeToolResult r : results)
            assertTrue("Expected timeout error but got: " + r.getStderr(),
                    r.getStderr().contains("Mutation tracking sync timed out"));
    }

    /**
     * Exercises the onFailure callback in MutationTrackingSyncCoordinator.sendSyncRequests().
     * Unlike testRepairTimeout (which drops MT_SYNC_REQ entirely so the request times out),
     * this test makes the remote handler throw an exception, which sends a FAILURE_RSP back
     * to the coordinator, triggering the onFailure -> fail() path.
     */
    @Test
    public void testSyncFailureResponse()
    {
        insertData("tbl", 0, 50);

        // The matcher throwing causes uncaught exceptions on the receiving nodes' stage threads.
        // These are expected, so filter them out to avoid failing at cluster close.
        CLUSTER.setUncaughtExceptionsFilter((nodeNum, throwable) ->
                throwable.getMessage() != null && throwable.getMessage().contains("sync failure injected"));
        try
        {
            CLUSTER.verbs(Verb.MT_SYNC_REQ).messagesMatching(of(m -> {
                throw new RuntimeException("sync failure injected");
            })).drop();

            List<NodeToolResult> results = repairConcurrently(ALL_NODES, withPR(ksName));
            assertAllFailure(results);
            for (NodeToolResult r : results)
                assertTrue("Expected sync failure error but got: " + r.getStderr(),
                        r.getStderr().contains("Mutation tracking sync failed"));
        }
        finally
        {
            CLUSTER.setUncaughtExceptionsFilter((BiPredicate<Integer, Throwable>) null);
        }
    }

    /**
     * During migration from untracked to tracked, incremental repair runs anti-compaction
     * on SSTables that were written before tracking was enabled. When an SSTable partially
     * overlaps the repair range, anti-compaction must split it by rewriting through
     * SSTableWriter. The "inside repair range" writer gets pendingRepair set to the session ID.
     *
     * SSTableWriter.finalizeMetadata() must tolerate pendingRepair being set on a tracked
     * table during migration. This test uses a narrow subrange to force anti-compaction to
     * split SSTables (rather than just mutating fully-contained ones in place).
     */
    @Test
    public void testMigrationSubrangeRepairAntiCompactionSplitsSSTables() throws Exception
    {
        setupUntracked();

        // Write data and flush so SSTables span the full token ring on each node.
        insertData("tbl", 0, 500);
        for (int i = 1; i <= NUM_NODES; i++)
            CLUSTER.get(i).flush(ksName);

        alterKeyspaceToTracked();
        assertTrue("Migration should be in progress", isMigrationInProgress());

        // Use a subrange that's well within one local range but wide enough to contain
        // data. With 3 nodes, node 2's primary range is approximately
        // (-3074457345618258603, 3074457345618258602]. A range of (0, 3000000000000000000]
        // is fully contained in that range and covers ~16% of the ring, so ~80 of our 500
        // rows should hash into it. SSTables from the flush span the entire ring, so they
        // will NOT be fully contained in this narrow range. Anti-compaction must split them
        // via SSTableWriter, exercising the pendingRepair code path in finalizeMetadata().
        String st = "0";
        String et = "3000000000000000000";

        // Run repair from a single node to avoid anti-compaction conflicts.
        // This should succeed: anti-compaction splits SSTables and the repair completes.
        NodeToolResult result = nodetoolRepair(1, ksName, "-st", st, "-et", et);
        result.asserts().success();
    }

    @Test
    public void testRepairSyncTimeout()
    {
        insertDataWithInconsistency("tbl", 0, 50);

        // Drop only offset broadcasts so MT_SYNC_REQ/RSP can succeed but
        // reconciliation never completes, triggering mutation_tracking_sync_timeout
        CLUSTER.filters().verbs(Verb.MT_BROADCAST_LOG_OFFSETS.id).drop();

        List<NodeToolResult> results = repairConcurrently(ALL_NODES, withPR(ksName));
        assertAllFailure(results);
        for (NodeToolResult r : results)
            assertTrue("Expected sync timeout error but got: " + r.getStderr(),
                    r.getStderr().contains("Mutation tracking sync timed out"));
    }

    /**
     * Verifies that a topology change during an active mutation tracking sync causes
     * the repair to fail with "topology changed during sync".
     *
     * The strategy:
     * 1. Insert data so sync has work to do
     * 2. Drop BROADCAST_LOG_OFFSETS so the sync coordinator stays alive waiting for
     *    offset reconciliation
     * 3. Start repair in a background thread
     * 4. Wait until the sync request has been sent (confirming sync is active)
     * 5. ALTER KEYSPACE to change RF (3 -> 2), which triggers REPLICA_GROUP ->
     *    withUpdatedMetadata -> new Shard instances
     * 6. Turn off the BROADCAST_LOG_OFFSETS filter so offset broadcasts resume,
     *    triggering onOffsetsReceived -> recaptureTargets -> checkForTopologyChange
     *    which detects the identity mismatch and fails the repair
     * 7. Assert the repair failed with the expected topology change message
     */
    @Test
    public void testRepairFailsOnTopologyChange() throws Exception
    {
        // Block offset broadcasts so the sync coordinator stays alive waiting
        IMessageFilters.Filter offsetFilter = CLUSTER.filters().verbs(Verb.MT_BROADCAST_LOG_OFFSETS.id).drop();

        // must use inconsistent data to prevent write process from marking the writes reconciled
        insertDataWithInconsistency("tbl", 0, 50);

        // Use a latch to detect when the sync request has been sent, meaning
        // the sync coordinator is active and tracking shard references
        CountDownLatch syncStarted = new CountDownLatch(1);
        IMessageFilters.Filter syncObserver = CLUSTER.verbs(Verb.MT_SYNC_REQ).messagesMatching(
                (from, to, msg) -> {
                    syncStarted.countDown();
                    return false; // don't drop the message
                }).drop();

        // Start repair in background
        Future<NodeToolResult> repairFuture = executor.submit(() -> nodetoolRepair(1, withPR(ksName)));

        // Wait until sync is active. The latch fires when MT_SYNC_REQ is sent, which
        // happens after shardStates is fully populated in start(), so no additional
        // delay is needed.
        assertTrue("Timed out waiting for sync to start",
                   syncStarted.await(30, TimeUnit.SECONDS));

        // ALTER KEYSPACE to change RF from 3 to 2 — this changes the participants for
        // every range, triggering REPLICA_GROUP -> withUpdatedMetadata -> new Shard instances.
        // The sync coordinator's shardStates still holds references to the old Shard objects.
        CLUSTER.schemaChange("ALTER KEYSPACE " + ksName + " WITH replication = " +
                             "{'class': 'SimpleStrategy', 'replication_factor': 2} " +
                             "AND replication_type='tracked'");

        // Remove the sync observer since it's no longer needed
        syncObserver.off();

        // Turn off the offset broadcast filter so broadcasts resume. When an offset
        // broadcast arrives, it calls onOffsetsReceived -> recaptureTargets ->
        // checkForTopologyChange, which will detect that the current Shard instances
        // (new objects from withUpdatedMetadata) differ from the ones stored in
        // shardStates (reference equality check), and fail the repair.
        offsetFilter.off();

        NodeToolResult result = repairFuture.get(30, TimeUnit.SECONDS);
        result.asserts().failure();
        assertTrue("Expected topology change error but got: " + result.getStderr(),
                   result.getStderr().contains("topology changed during sync"));
    }
}

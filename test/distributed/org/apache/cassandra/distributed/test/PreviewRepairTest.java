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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.shared.RepairResult;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEventType;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PreviewRepairTest extends TestBaseImpl
{
    /**
     * makes sure that the repaired sstables are not matching on the two
     * nodes by disabling autocompaction on node2 and then running an
     * incremental repair
     */
    @Test
    public void testWithMismatchingPending() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(2).withConfig(config -> config.with(GOSSIP).with(NETWORK)).start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            cluster.get(1).callOnInstance(repair(options(false)));
            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            // make sure that all sstables have moved to repaired by triggering a compaction
            // also disables autocompaction on the nodes
            cluster.forEach((node) -> node.runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(cfs));
                cfs.disableAutoCompaction();
            }));
            cluster.get(1).callOnInstance(repair(options(false)));
            // now re-enable autocompaction on node1, this moves the sstables for the new repair to repaired
            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                cfs.enableAutoCompaction();
                FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(cfs));
            });

            //IR and Preview repair can't run concurrently. In case the test is flaky, please check CASSANDRA-15685
            Thread.sleep(1000);

            RepairResult rs = cluster.get(1).callOnInstance(repair(options(true)));
            assertTrue(rs.success); // preview repair should succeed
            assertFalse(rs.wasInconsistent); // and we should see no mismatches
        }
    }

    /**
     * another case where the repaired datasets could mismatch is if an incremental repair finishes just as the preview
     * repair is starting up.
     *
     * This tests this case:
     * 1. we start a preview repair
     * 2. pause the validation requests from node1 -> node2
     * 3. node1 starts its validation
     * 4. run an incremental repair which completes fine
     * 5. node2 resumes its validation
     *
     * Now we will include sstables from the second incremental repair on node2 but not on node1
     * This should fail since we fail any preview repair which is ongoing when an incremental repair finishes (step 4 above)
     */
    @Test
    public void testFinishingIncRepairDuringPreview() throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService es = Executors.newSingleThreadExecutor();
        try(Cluster cluster = init(Cluster.build(2).withConfig(config -> config.with(GOSSIP).with(NETWORK)).start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");

            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            cluster.get(1).callOnInstance(repair(options(false)));

            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            SimpleCondition continuePreviewRepair = new SimpleCondition();
            DelayMessageFilter filter = new DelayMessageFilter(continuePreviewRepair);
            // this pauses the validation request sent from node1 to node2 until we have run a full inc repair below
            cluster.filters().outbound().verbs(Verb.VALIDATION_REQ.id).from(1).to(2).messagesMatching(filter).drop();

            Future<RepairResult> rsFuture = es.submit(() -> cluster.get(1).callOnInstance(repair(options(true))));
            Thread.sleep(1000);
            // this needs to finish before the preview repair is unpaused on node2
            cluster.get(1).callOnInstance(repair(options(false)));
            continuePreviewRepair.signalAll();
            RepairResult rs = rsFuture.get();
            assertFalse(rs.success); // preview repair should have failed
            assertFalse(rs.wasInconsistent); // and no mismatches should have been reported
        }
        finally
        {
            es.shutdown();
        }
    }

    /**
     * Same as testFinishingIncRepairDuringPreview but the previewed range does not intersect the incremental repair
     * so both preview and incremental repair should finish fine (without any mismatches)
     */
    @Test
    public void testFinishingNonIntersectingIncRepairDuringPreview() throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService es = Executors.newSingleThreadExecutor();
        try(Cluster cluster = init(Cluster.build(2).withConfig(config -> config.with(GOSSIP).with(NETWORK)).start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");

            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            assertTrue(cluster.get(1).callOnInstance(repair(options(false))).success);

            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            // pause preview repair validation messages on node2 until node1 has finished
            SimpleCondition continuePreviewRepair = new SimpleCondition();
            DelayMessageFilter filter = new DelayMessageFilter(continuePreviewRepair);
            cluster.filters().outbound().verbs(Verb.VALIDATION_REQ.id).from(1).to(2).messagesMatching(filter).drop();

            // get local ranges to repair two separate ranges:
            List<String> localRanges = cluster.get(1).callOnInstance(() -> {
                List<String> res = new ArrayList<>();
                for (Range<Token> r : StorageService.instance.getLocalReplicas(KEYSPACE).ranges())
                    res.add(r.left.getTokenValue()+ ":"+ r.right.getTokenValue());
                return res;
            });

            assertEquals(2, localRanges.size());
            Future<RepairResult> repairStatusFuture = es.submit(() -> cluster.get(1).callOnInstance(repair(options(true, localRanges.get(0)))));
            Thread.sleep(1000); // wait for node1 to start validation compaction
            // this needs to finish before the preview repair is unpaused on node2
            assertTrue(cluster.get(1).callOnInstance(repair(options(false, localRanges.get(1)))).success);

            continuePreviewRepair.signalAll();
            RepairResult rs = repairStatusFuture.get();
            assertTrue(rs.success); // repair should succeed
            assertFalse(rs.wasInconsistent); // and no mismatches
        }
        finally
        {
            es.shutdown();
        }
    }

    @Test
    public void snapshotTest() throws IOException, InterruptedException
    {
        try(Cluster cluster = init(Cluster.build(3).withConfig(config ->
                                                               config.set("snapshot_on_repaired_data_mismatch", true)
                                                                     .with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            cluster.schemaChange("create table " + KEYSPACE + ".tbl2 (id int primary key, t int)");
            Thread.sleep(1000);

            // populate 2 tables
            insert(cluster.coordinator(1), 0, 100, "tbl");
            insert(cluster.coordinator(1), 0, 100, "tbl2");
            cluster.forEach((n) -> n.flush(KEYSPACE));

            // make sure everything is marked repaired
            cluster.get(1).callOnInstance(repair(options(false)));
            waitMarkedRepaired(cluster);
            // make node2 mismatch
            unmarkRepaired(cluster.get(2), "tbl");
            verifySnapshots(cluster, "tbl", true);
            verifySnapshots(cluster, "tbl2", true);

            AtomicInteger snapshotMessageCounter = new AtomicInteger();
            cluster.filters().verbs(Verb.SNAPSHOT_REQ.id).messagesMatching((from, to, message) -> {
                snapshotMessageCounter.incrementAndGet();
                return false;
            }).drop();
            cluster.get(1).callOnInstance(repair(options(true)));
            verifySnapshots(cluster, "tbl", false);
            // tbl2 should not have a mismatch, so the snapshots should be empty here
            verifySnapshots(cluster, "tbl2", true);
            assertEquals(3, snapshotMessageCounter.get());

            // and make sure that we don't try to snapshot again
            snapshotMessageCounter.set(0);
            cluster.get(3).callOnInstance(repair(options(true)));
            assertEquals(0, snapshotMessageCounter.get());
        }
    }

    private void waitMarkedRepaired(Cluster cluster)
    {
        cluster.forEach(node -> node.runOnInstance(() -> {
            for (String table : Arrays.asList("tbl", "tbl2"))
            {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
                while (true)
                {
                    if (cfs.getLiveSSTables().stream().allMatch(SSTableReader::isRepaired))
                        return;
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                }
            }
        }));
    }

    private void unmarkRepaired(IInvokableInstance instance, String table)
    {
        instance.runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
            try
            {
                cfs.getCompactionStrategyManager().mutateRepaired(cfs.getLiveSSTables(), ActiveRepairService.UNREPAIRED_SSTABLE, null, false);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    private void verifySnapshots(Cluster cluster, String table, boolean shouldBeEmpty)
    {
        cluster.forEach(node -> node.runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
            if(shouldBeEmpty)
            {
                assertTrue(cfs.getSnapshotDetails().isEmpty());
            }
            else
            {
                while (cfs.getSnapshotDetails().isEmpty())
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
        }));
    }

    static class DelayMessageFilter implements IMessageFilters.Matcher
    {
        private final SimpleCondition condition;
        private final AtomicBoolean waitForRepair = new AtomicBoolean(true);

        public DelayMessageFilter(SimpleCondition condition)
        {
            this.condition = condition;
        }
        public boolean matches(int from, int to, IMessage message)
        {
            try
            {
                // only the first validation req should be delayed:
                if (waitForRepair.compareAndSet(true, false))
                    condition.await();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            return false; // don't drop the message
        }
    }

    static void insert(ICoordinator coordinator, int start, int count)
    {
        insert(coordinator, start, count, "tbl");
    }

    static void insert(ICoordinator coordinator, int start, int count, String table)
    {
        for (int i = start; i < start + count; i++)
            coordinator.execute("insert into " + KEYSPACE + "." + table + " (id, t) values (?, ?)", ConsistencyLevel.ALL, i, i);
    }

    /**
     * returns a pair with [repair success, was inconsistent]
     */
    private static IIsolatedExecutor.SerializableCallable<RepairResult> repair(Map<String, String> options)
    {
        return () -> {
            SimpleCondition await = new SimpleCondition();
            AtomicBoolean success = new AtomicBoolean(true);
            AtomicBoolean wasInconsistent = new AtomicBoolean(false);
            StorageService.instance.repair(KEYSPACE, options, ImmutableList.of((tag, event) -> {
                if (event.getType() == ProgressEventType.ERROR)
                {
                    success.set(false);
                    await.signalAll();
                }
                else if (event.getType() == ProgressEventType.NOTIFICATION && event.getMessage().contains("Repaired data is inconsistent"))
                {
                    wasInconsistent.set(true);
                }
                else if (event.getType() == ProgressEventType.COMPLETE)
                    await.signalAll();
            }));
            try
            {
                await.await(1, TimeUnit.MINUTES);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            return new RepairResult(success.get(), wasInconsistent.get());
        };
    }

    private static Map<String, String> options(boolean preview)
    {
        Map<String, String> config = new HashMap<>();
        config.put(RepairOption.INCREMENTAL_KEY, "true");
        config.put(RepairOption.PARALLELISM_KEY, RepairParallelism.PARALLEL.toString());
        if (preview)
            config.put(RepairOption.PREVIEW, PreviewKind.REPAIRED.toString());
        return config;
    }

    private static Map<String, String> options(boolean preview, String range)
    {
        Map<String, String> options = options(preview);
        options.put(RepairOption.RANGES_KEY, range);
        return options;
    }
}

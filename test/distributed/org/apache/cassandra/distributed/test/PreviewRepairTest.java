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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.Condition;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
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
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.RepairResult;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.FinalizePropose;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.repair.messages.ValidationRequest;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.collect.ImmutableList.of;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.api.IMessageFilters.Matcher;
import static org.apache.cassandra.distributed.impl.Instance.deserializeMessage;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.DelayFirstRepairTypeMessageFilter.validationRequest;
import static org.apache.cassandra.net.Verb.VALIDATION_REQ;
import static org.apache.cassandra.service.StorageService.instance;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;
import static org.apache.cassandra.utils.progress.ProgressEventType.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PreviewRepairTest extends TestBaseImpl
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }
    
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
            cluster.get(1).callOnInstance(repair(options(false, false)));
            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            // make sure that all sstables have moved to repaired by triggering a compaction
            // also disables autocompaction on the nodes
            cluster.forEach((node) -> node.runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(cfs));
                cfs.disableAutoCompaction();
            }));
            long[] marks = logMark(cluster);
            cluster.get(1).callOnInstance(repair(options(false, false)));
            // now re-enable autocompaction on node1, this moves the sstables for the new repair to repaired
            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                cfs.enableAutoCompaction();
                FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(cfs));
            });

            waitLogsRepairFullyFinished(cluster, marks);

            RepairResult rs = cluster.get(1).callOnInstance(repair(options(true, false)));
            assertTrue(rs.success); // preview repair should succeed
            assertFalse(rs.wasInconsistent); // and we should see no mismatches
        }
    }

    public static void waitLogsRepairFullyFinished(Cluster cluster, long[] marks) throws TimeoutException
    {
        for (int i = 0; i < cluster.size(); i++)
            cluster.get(i + 1).logs().watchFor(marks[i], "Finalized local repair session");
    }

    public static long[] logMark(Cluster cluster)
    {
        long [] marks = new long[cluster.size()];
        for (int i = 0; i < cluster.size(); i++)
        {
            marks[i] = cluster.get(i + 1).logs().mark();
        }
        return marks;
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
            cluster.get(1).callOnInstance(repair(options(false, false)));

            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            
            Condition previewRepairStarted = newOneTimeCondition();
            Condition continuePreviewRepair = newOneTimeCondition();
            DelayFirstRepairTypeMessageFilter filter = validationRequest(previewRepairStarted, continuePreviewRepair);
            // this pauses the validation request sent from node1 to node2 until we have run a full inc repair below
            cluster.filters().outbound().verbs(VALIDATION_REQ.id).from(1).to(2).messagesMatching(filter).drop();

            Future<RepairResult> rsFuture = es.submit(() -> cluster.get(1).callOnInstance(repair(options(true, false))));
            previewRepairStarted.await();
            // this needs to finish before the preview repair is unpaused on node2
            cluster.get(1).callOnInstance(repair(options(false, false)));
            RepairResult irResult = cluster.get(1).callOnInstance(repair(options(false, false)));
            continuePreviewRepair.signalAll();
            RepairResult rs = rsFuture.get();
            assertFalse(rs.success); // preview repair was started before IR, but has lower priority, so its task will get cancelled
            assertFalse(rs.wasInconsistent); // and no mismatches should have been reported

            assertTrue(irResult.success); // IR was started after preview repair, but has a higher priority, so it'll be allowed to finish
            assertFalse(irResult.wasInconsistent);
        }
        finally
        {
            es.shutdown();
        }
    }

    /**
     * Tests that a IR is running, but not completed before validation compaction starts
     */
    @Test
    public void testConcurrentIncRepairDuringPreview() throws IOException, InterruptedException, ExecutionException
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                                config.with(GOSSIP)
                                                                      .with(NETWORK)).start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            cluster.get(1).callOnInstance(repair(options(false, false)));

            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            Condition previewRepairStarted = newOneTimeCondition();
            Condition continuePreviewRepair = newOneTimeCondition();
            // this pauses the validation request sent from node1 to node2 until the inc repair below has run
            cluster.filters()
                   .outbound()
                   .verbs(VALIDATION_REQ.id)
                   .from(1).to(2)
                   .messagesMatching(validationRequest(previewRepairStarted, continuePreviewRepair))
                   .drop();

            Future<RepairResult> previewResult = cluster.get(1).asyncCallsOnInstance(repair(options(true, false))).call();
            previewRepairStarted.await();

            // trigger IR and wait till it's ready to commit
            Future<RepairResult> irResult = cluster.get(1).asyncCallsOnInstance(repair(options(false, false))).call();
            RepairResult ir = irResult.get();
            assertTrue(ir.success); // IR was submitted after preview repair has acquired sstables, but has higher priority
            assertFalse(ir.wasInconsistent); // not preview, so we don't care about preview notification

            // unblock preview repair and wait for it to complete
            continuePreviewRepair.signalAll();

            RepairResult rs = previewResult.get();
            assertFalse(rs.success); // preview repair was started earlier than IR session; but has smaller priority
            assertFalse(rs.wasInconsistent); // and no mismatches should have been reported
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
            int tokenCount = ClusterUtils.getTokenCount(cluster.get(1));
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");

            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            assertTrue(cluster.get(1).callOnInstance(repair(options(false, false))).success);

            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            // pause preview repair validation messages on node2 until node1 has finished
            Condition previewRepairStarted = newOneTimeCondition();
            Condition continuePreviewRepair = newOneTimeCondition();
            DelayFirstRepairTypeMessageFilter filter = validationRequest(previewRepairStarted, continuePreviewRepair);
            cluster.filters().outbound().verbs(VALIDATION_REQ.id).from(1).to(2).messagesMatching(filter).drop();

            // get local ranges to repair two separate ranges:
            List<String> localRanges = cluster.get(1).callOnInstance(() -> {
                List<String> res = new ArrayList<>();
                for (Range<Token> r : instance.getLocalReplicas(KEYSPACE).ranges())
                    res.add(r.left.getTokenValue()+ ":"+ r.right.getTokenValue());
                return res;
            });

            assertEquals(2 * tokenCount, localRanges.size());
            Future<RepairResult> repairStatusFuture = es.submit(() -> cluster.get(1).callOnInstance(repair(options(true, false, localRanges.get(0)))));
            previewRepairStarted.await(); // wait for node1 to start validation compaction
            // this needs to finish before the preview repair is unpaused on node2
            assertTrue(cluster.get(1).callOnInstance(repair(options(false, false, localRanges.get(1)))).success);

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

    /**
     * Makes sure we can start a non-intersecting preview repair while there are other pending sstables on disk
     */
    @Test
    public void testStartNonIntersectingPreviewRepair() throws IOException, InterruptedException, ExecutionException
    {
        ExecutorService es = Executors.newSingleThreadExecutor();
        try(Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                               config.with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            int tokenCount = ClusterUtils.getTokenCount(cluster.get(1));
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            cluster.get(1).nodetoolResult("repair", KEYSPACE, "tbl").asserts().success();

            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            // pause inc repair validation messages on node2 until node1 has finished
            Condition incRepairStarted = newOneTimeCondition();
            Condition continueIncRepair = newOneTimeCondition();

            DelayFirstRepairTypeMessageFilter filter = DelayFirstRepairTypeMessageFilter.validationRequest(incRepairStarted, continueIncRepair);
            cluster.filters().outbound().verbs(Verb.VALIDATION_REQ.id).from(1).to(2).messagesMatching(filter).drop();

            // get local ranges to repair two separate ranges:
            List<String> localRanges = cluster.get(1).callOnInstance(() -> {
                List<String> res = new ArrayList<>();
                for (Range<Token> r : StorageService.instance.getLocalReplicas(KEYSPACE).ranges())
                    res.add(r.left.getTokenValue()+ ":"+ r.right.getTokenValue());
                return res;
            });

            assertEquals(2 * tokenCount, localRanges.size());
            String [] previewedRange = localRanges.get(0).split(":");
            String [] repairedRange = localRanges.get(1).split(":");
            Future<NodeToolResult> repairStatusFuture = es.submit(() -> cluster.get(1).nodetoolResult("repair", "-st", repairedRange[0], "-et", repairedRange[1], KEYSPACE, "tbl"));
            incRepairStarted.await(); // wait for node1 to start validation compaction
            // now we have pending sstables in range "repairedRange", make sure we can preview "previewedRange"
            cluster.get(1).nodetoolResult("repair", "-vd", "-st", previewedRange[0], "-et", previewedRange[1], KEYSPACE, "tbl")
                          .asserts()
                          .success()
                          .notificationContains("Repaired data is in sync");

            continueIncRepair.signalAll();

            repairStatusFuture.get().asserts().success();
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

            // populate 2 tables
            insert(cluster.coordinator(1), 0, 100, "tbl");
            insert(cluster.coordinator(1), 0, 100, "tbl2");
            cluster.forEach((n) -> n.flush(KEYSPACE));

            // make sure everything is marked repaired
            cluster.get(1).callOnInstance(repair(options(false, false)));
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
            cluster.get(1).callOnInstance(repair(options(true, true)));
            verifySnapshots(cluster, "tbl", false);
            // tbl2 should not have a mismatch, so the snapshots should be empty here
            verifySnapshots(cluster, "tbl2", true);
            assertEquals(3, snapshotMessageCounter.get());

            // and make sure that we don't try to snapshot again
            snapshotMessageCounter.set(0);
            cluster.get(3).callOnInstance(repair(options(true, true)));
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
                assertTrue(cfs.listSnapshots().isEmpty());
            }
            else
            {
                while (cfs.listSnapshots().isEmpty())
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
        }));
    }

    static abstract class DelayFirstRepairMessageFilter implements Matcher
    {
        private final Condition pause;
        private final Condition resume;
        private final AtomicBoolean waitForRepair = new AtomicBoolean(true);

        protected DelayFirstRepairMessageFilter(Condition pause, Condition resume)
        {
            this.pause = pause;
            this.resume = resume;
        }

        protected abstract boolean matchesMessage(RepairMessage message);

        public final boolean matches(int from, int to, IMessage message)
        {
            try
            {
                Message<?> msg = deserializeMessage(message);
                RepairMessage repairMessage = (RepairMessage) msg.payload;
                // only the first message should be delayed:
                if (matchesMessage(repairMessage) && waitForRepair.compareAndSet(true, false))
                {
                    pause.signalAll();
                    resume.await();
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            return false; // don't drop the message
        }
    }

    static class DelayFirstRepairTypeMessageFilter extends DelayFirstRepairMessageFilter
    {
        private final Class<? extends RepairMessage> type;

        public DelayFirstRepairTypeMessageFilter(Condition pause, Condition resume, Class<? extends RepairMessage> type)
        {
            super(pause, resume);
            this.type = type;
        }

        public static DelayFirstRepairTypeMessageFilter validationRequest(Condition pause, Condition resume)
        {
            return new DelayFirstRepairTypeMessageFilter(pause, resume, ValidationRequest.class);
        }

        public static DelayFirstRepairTypeMessageFilter finalizePropose(Condition pause, Condition resume)
        {
            return new DelayFirstRepairTypeMessageFilter(pause, resume, FinalizePropose.class);
        }

        protected boolean matchesMessage(RepairMessage repairMessage)
        {
            return repairMessage.getClass() == type;
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
            Condition await = newOneTimeCondition();
            AtomicBoolean success = new AtomicBoolean(true);
            AtomicBoolean wasInconsistent = new AtomicBoolean(false);
            instance.repair(KEYSPACE, options, of((tag, event) -> {
                if (event.getType() == ERROR)
                {
                    success.set(false);
                    await.signalAll();
                }
                else if (event.getType() == NOTIFICATION && event.getMessage().contains("Repaired data is inconsistent"))
                {
                    wasInconsistent.set(true);
                }
                else if (event.getType() == COMPLETE)
                    await.signalAll();
            }));
            try
            {
                await.await(1, MINUTES);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            return new RepairResult(success.get(), wasInconsistent.get());
        };
    }

    private static Map<String, String> options(boolean preview, boolean full)
    {
        Map<String, String> config = new HashMap<>();
        config.put(RepairOption.INCREMENTAL_KEY, "true");
        config.put(RepairOption.PARALLELISM_KEY, RepairParallelism.PARALLEL.toString());
        if (preview)
            config.put(RepairOption.PREVIEW, PreviewKind.REPAIRED.toString());
        if (full)
            config.put(RepairOption.INCREMENTAL_KEY, "false");
        return config;
    }

    private static Map<String, String> options(boolean preview, boolean full, String range)
    {
        Map<String, String> options = options(preview, full);
        options.put(RepairOption.RANGES_KEY, range);
        return options;
    }
}

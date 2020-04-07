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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.Notification;

import com.google.common.collect.ImmutableList;
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
import org.apache.cassandra.distributed.api.LongTokenRange;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.RepairResult;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
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
    private static final String TABLE = "tbl";

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
            cluster.schemaChange("create table " + KEYSPACE + "." + TABLE + " (id int primary key, t int)");
            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            IInvokableInstance instance = cluster.get(1);
            repair(cluster.get(1), options(false));
            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            // make sure that all sstables have moved to repaired by triggering a compaction
            // also disables autocompaction on the nodes
            cluster.forEach((node) -> node.runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(cfs));
                cfs.disableAutoCompaction();
            }));
            repair(cluster.get(1), options(false));
            // now re-enable autocompaction on node1, this moves the sstables for the new repair to repaired
            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                cfs.enableAutoCompaction();
                FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(cfs));
            });
            RepairResult rs = repair(cluster.get(1), options(true));
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
            cluster.schemaChange("create table " + KEYSPACE + "." + TABLE + " (id int primary key, t int)");

            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            repair(cluster.get(1), options(false));

            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            SimpleCondition continuePreviewRepair = new SimpleCondition();
            DelayMessageFilter filter = new DelayMessageFilter(continuePreviewRepair);
            // this pauses the validation request sent from node1 to node2 until we have run a full inc repair below
            cluster.filters().outbound().verbs(Verb.VALIDATION_REQ.id).from(1).to(2).messagesMatching(filter).drop();

            Future<RepairResult> rsFuture = es.submit(() -> repair(cluster.get(1), options(true)));
            Thread.sleep(1000);
            // this needs to finish before the preview repair is unpaused on node2
            repair(cluster.get(1), options(false));
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
            cluster.schemaChange("create table " + KEYSPACE + "." + TABLE + " (id int primary key, t int)");

            insert(cluster.coordinator(1), 0, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));
            assertTrue(repair(cluster.get(1), options(false)).success);

            insert(cluster.coordinator(1), 100, 100);
            cluster.forEach((node) -> node.flush(KEYSPACE));

            // pause preview repair validation messages on node2 until node1 has finished
            SimpleCondition continuePreviewRepair = new SimpleCondition();
            DelayMessageFilter filter = new DelayMessageFilter(continuePreviewRepair);
            cluster.filters().outbound().verbs(Verb.VALIDATION_REQ.id).from(1).to(2).messagesMatching(filter).drop();

            // get local ranges to repair two separate ranges:
            List<LongTokenRange> localRanges = cluster.get(1).callOnInstance(() -> {
                List<LongTokenRange> res = new ArrayList<>();
                for (Range<Token> r : StorageService.instance.getLocalReplicas(KEYSPACE).ranges())
                    res.add(new LongTokenRange((Long) r.left.getTokenValue(), (Long) r.right.getTokenValue()));
                return res;
            });

            assertEquals(2, localRanges.size());
            Future<RepairResult> repairStatusFuture = es.submit(() -> repair(cluster.get(1), options(true, localRanges.get(0))));
            Thread.sleep(1000); // wait for node1 to start validation compaction
            // this needs to finish before the preview repair is unpaused on node2
            assertTrue(repair(cluster.get(1), options(false, localRanges.get(1))).success);

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

    private static class DelayMessageFilter implements IMessageFilters.Matcher
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

    private static void insert(ICoordinator coordinator, int start, int count)
    {
        for (int i = start; i < start + count; i++)
            coordinator.execute("insert into " + KEYSPACE + ".tbl (id, t) values (?, ?)", ConsistencyLevel.ALL, i, i);
    }

    /**
     * returns a pair with [repair success, was inconsistent]
     */
    private static RepairResult repair(IInvokableInstance instance, List<String> args)
    {
        String[] command = new String[args.size() + 1];
        command[0] = "repair";
        System.arraycopy(args.stream().toArray(String[]::new), 0, command, 1, args.size());
        NodeToolResult result = instance.nodetoolResult(command);
        return new RepairResult(result.getRc() == 0, extractWasInconsistent(result));
    }

    private static boolean extractWasInconsistent(NodeToolResult result)
    {
        //NOTE this is flaky since notifications are lossy.  Ideally the completion messsage would have this since that
        // can be recovered by polling.  One negative of the current APIs is that this would only be exposed to stdout
        boolean wasInconsistent = false;
        int targetOrdinal = NodeToolResult.ProgressEventType.NOTIFICATION.ordinal();
        for (Notification notification : result.getNotifications())
        {
            int type = ((Map<String, Integer>) notification.getUserData()).get("type");
            if (type == targetOrdinal)
            {
                wasInconsistent |= notification.getMessage().contains("Repaired data is inconsistent");
            }
        }
        return wasInconsistent;
    }

    private static List<String> options(boolean preview)
    {
        List<String> options = new ArrayList<>();
        options.add(KEYSPACE);
        options.add(TABLE);
        // defaults are IR + parallel, so don't need to add to options list
        if (preview)
            options.add("--validate");
        return options;
    }

    private static List<String> options(boolean preview, LongTokenRange range)
    {
        List<String> options = options(preview);
        options.add("--start-token");
        options.add(Long.toString(range.minExclusive));
        options.add("--end-token");
        options.add(Long.toString(range.maxInclusive));
        return options;
    }
}

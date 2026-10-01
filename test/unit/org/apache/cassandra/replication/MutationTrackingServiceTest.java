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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.repair.SymmetricRemoteSyncTask;
import org.apache.cassandra.repair.SyncTask;
import org.apache.cassandra.repair.SyncTasks;
import org.apache.cassandra.replication.MutationTrackingService.KeyspaceShards;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.replication.MutationTrackingService.TestAccess.createTestKeyspaceShards;
import static org.apache.cassandra.replication.MutationTrackingService.TestAccess.setKeyspaceShardsUnsafe;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MutationTrackingServiceTest
{
    private static final String TEST_KEYSPACE = "test_ks";
    private static final String TEST_TABLE = "test_table";
    private static final InetAddressAndPort LOCAL = InetAddressAndPort.getByNameUnchecked("127.0.0.1");
    private static final InetAddressAndPort REMOTE = InetAddressAndPort.getByNameUnchecked("127.0.0.2");

    @BeforeClass
    public static void setup() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(TEST_KEYSPACE, KeyspaceParams.simple(1, ReplicationType.tracked), SchemaLoader.standardCFMD(TEST_KEYSPACE, TEST_TABLE));
    }

    @Test
    public void testAlignToShardBoundariesSingleTaskWithinSingleShard()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Create a single shard covering a-z
        Set<Range<Token>> shardRanges = new HashSet<>();
        shardRanges.add(range("a", "z"));
        KeyspaceShards shards =
        createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, shards);

        // Input task completely within the shard
        List<SyncTask> inputTasks = Collections.singletonList(createSyncTask(range("d", "m")));

        Keyspace keyspace = Keyspace.open(TEST_KEYSPACE);
        SyncTasks result = service.alignToShardBoundaries(keyspace, inputTasks);

        // Should have one shard entry
        AtomicInteger entries = new AtomicInteger(0);
        result.apply((shardedTask) -> entries.incrementAndGet());
        assertEquals(1, entries.get());

        // Get all tasks from the result
        List<SyncTask> allTasks = new ArrayList<>();
        result.apply((shardedTask) -> allTasks.add(shardedTask.task));

        assertEquals(1, allTasks.size());

        SyncTask resultTask = allTasks.get(0);
        assertEquals(1, resultTask.rangesToSync.size());
        assertTrue(resultTask.rangesToSync.contains(range("d", "m")));

        // Should have a transfer ID assigned
        assertNotNull(resultTask.getTransferId());
    }

    @Test
    public void testAlignToShardBoundariesSingleTaskSpanningMultipleShards()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Create two shards
        Set<Range<Token>> shardRanges = new HashSet<>();
        shardRanges.add(range("a", "m"));
        shardRanges.add(range("m", "z"));
        KeyspaceShards shards =
        createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, shards);

        // Input task spans both shards
        List<SyncTask> inputTasks = Collections.singletonList(createSyncTask(range("d", "s")));

        Keyspace keyspace = Keyspace.open(TEST_KEYSPACE);
        SyncTasks result = service.alignToShardBoundaries(keyspace, inputTasks);

        // Should be split into two shard entries
        AtomicInteger entries = new AtomicInteger(0);
        result.apply((shardedTask) -> entries.incrementAndGet());
        assertEquals(2, entries.get());

        // Collect all ranges from all tasks
        Set<Range<Token>> allRanges = new HashSet<>();
        result.apply((shardedTask) -> allRanges.addAll(shardedTask.task.rangesToSync));

        // Should contain the two split pieces
        assertEquals(2, allRanges.size());
        assertTrue(allRanges.contains(range("d", "m")));
        assertTrue(allRanges.contains(range("m", "s")));
    }

    @Test
    public void testAlignToShardBoundariesMultipleTasksAcrossMultipleShards()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Create three shards
        Set<Range<Token>> shardRanges = new HashSet<>();
        shardRanges.add(range("a", "h"));
        shardRanges.add(range("h", "p"));
        shardRanges.add(range("p", "z"));
        KeyspaceShards shards =
        createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, shards);

        // Multiple tasks, some spanning shards
        List<SyncTask> inputTasks = Arrays.asList(
        createSyncTask(range("b", "e")),  // Within shard 1
        createSyncTask(range("f", "j")),  // Spans shard 1 and 2
        createSyncTask(range("q", "s"))   // Within shard 3
        );

        Keyspace keyspace = Keyspace.open(TEST_KEYSPACE);
        SyncTasks result = service.alignToShardBoundaries(keyspace, inputTasks);

        // Should have 4 entries (one per sync task)
        AtomicInteger entries = new AtomicInteger(0);
        result.apply((shardedTask) -> entries.incrementAndGet());
        assertEquals(4, entries.get());

        // Collect all ranges from all tasks
        Set<Range<Token>> allRanges = new HashSet<>();
        result.apply((shardedTask) -> allRanges.addAll(shardedTask.task.rangesToSync));

        // Should have split ranges: b-e, f-h (shard 1), h-j (shard 2), q-s (shard 3)
        assertTrue(allRanges.contains(range("b", "e")));
        assertTrue(allRanges.contains(range("f", "h")));
        assertTrue(allRanges.contains(range("h", "j")));
        assertTrue(allRanges.contains(range("q", "s")));
    }

    @Test
    public void testAlignToShardBoundariesTaskWithMultipleRangesSpanningShards()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Create two shards
        Set<Range<Token>> shardRanges = new HashSet<>();
        shardRanges.add(range("a", "m"));
        shardRanges.add(range("m", "z"));
        KeyspaceShards shards =
        createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, shards);

        // Single task with multiple ranges spanning both shards
        List<Range<Token>> ranges = Arrays.asList(
        range("b", "e"),  // Shard 1
        range("f", "p")   // Spans both shards
        );

        SyncTask task = createSyncTask(ranges, LOCAL, REMOTE);
        List<SyncTask> inputTasks = Collections.singletonList(task);

        Keyspace keyspace = Keyspace.open(TEST_KEYSPACE);
        SyncTasks result = service.alignToShardBoundaries(keyspace, inputTasks);

        // Should be split into two shard entries
        AtomicInteger entries = new AtomicInteger(0);
        result.forEach((shardedTask) -> entries.incrementAndGet());
        assertEquals(2, entries.get());

        // Collect all ranges
        Set<Range<Token>> allRanges = new HashSet<>();
        result.forEach((shardedTask) -> allRanges.addAll(shardedTask.rangesToSync));

        // Should have: b-e, f-m (shard 1), m-p (shard 2)
        assertTrue(allRanges.contains(range("b", "e")));
        assertTrue(allRanges.contains(range("f", "m")));
        assertTrue(allRanges.contains(range("m", "p")));
    }

    @Test
    public void testAlignToShardBoundariesPreservesTaskType()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Create two shards
        Set<Range<Token>> shardRanges = new HashSet<>();
        shardRanges.add(range("a", "m"));
        shardRanges.add(range("m", "z"));
        KeyspaceShards shards =
        createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, shards);

        // Task spanning both shards
        List<SyncTask> inputTasks = Collections.singletonList(createSyncTask(range("d", "s")));

        Keyspace keyspace = Keyspace.open(TEST_KEYSPACE);
        SyncTasks result = service.alignToShardBoundaries(keyspace, inputTasks);

        // All resulting tasks should be the same type as the input
        result.apply((shardedTask) -> assertTrue("Task should be SymmetricRemoteSyncTask", shardedTask.task instanceof SymmetricRemoteSyncTask));
    }

    @Test
    public void testOffsetsForAKeyspaceThatNoLongerExistsAreDropped()
    {
        String dropped = "keyspace_this_node_drops";
        SchemaLoader.createKeyspace(dropped, KeyspaceParams.simple(1, ReplicationType.tracked), SchemaLoader.standardCFMD(dropped, TEST_TABLE));

        MutationTrackingService service = MutationTrackingService.TestAccess.create();
        ClusterMetadata created = ClusterMetadata.current();
        MutationTrackingService.TestAccess.onNewClusterMetadata(service, null, created);
        assertNotNull(MutationTrackingService.TestAccess.getKeyspaceShards(service, dropped));
        assertTrue(MutationTrackingService.TestAccess.countLogsFor(service, dropped) > 0);

        SchemaTestUtil.dropKeyspaceIfExist(dropped, true);
        ClusterMetadata afterDrop = ClusterMetadata.current();
        MutationTrackingService.TestAccess.onNewClusterMetadata(service, created, afterDrop);

        CoordinatorLogId logId = CoordinatorLogId.fromLong(CoordinatorLogId.asLong(1, 1));
        Offsets.Immutable offsets = new Offsets.Immutable(logId, new int[]{ 1, 1 });
        Range<Token> range = range("a", "z");

        service.updateReplicatedOffsets(dropped, range, Collections.singletonList(offsets), true, REMOTE);
        service.recordFullyReconciledOffsets(ReconciledLogSnapshot.builder().put(dropped, logId, offsets, range).build());

        assertNull(MutationTrackingService.TestAccess.getKeyspaceShards(service, dropped));
        assertEquals(0, MutationTrackingService.TestAccess.countLogsFor(service, dropped));
    }

    /**
     * Replicas treat unknown coordinator logs in received summaries as locally missing.
     */
    @Test
    public void testUnknownShardSummaryTreatedAsLocallyMissing()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Shard 1 covers range a-m replicated by node 1.
        Set<Range<Token>> shardRanges = Collections.singleton(range("a", "m"));
        KeyspaceShards shards = createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, shards);

        // Foreign coordinator log belongs to host 2 on a foreign shard that node 1 does not replicate.
        CoordinatorLogId foreignLogId = new CoordinatorLogId(2, 1);
        TableId tableId = Keyspace.open(TEST_KEYSPACE).getColumnFamilyStore(TEST_TABLE).metadata().id;

        MutationSummary.Builder summaryBuilder = new MutationSummary.Builder(tableId);
        summaryBuilder.builderForLog(foreignLogId).unreconciled.add(10, 20);
        MutationSummary fullRemoteSummary = summaryBuilder.build();

        Log2OffsetsMap.Mutable missingMutations = new Log2OffsetsMap.Mutable();
        service.collectLocallyMissingMutations(fullRemoteSummary, missingMutations);
        assertTrue(missingMutations.contains(new ShortMutationId(foreignLogId, 10)));
        assertTrue(missingMutations.contains(new ShortMutationId(foreignLogId, 20)));

        try
        {
            service.validateSummaryForParticipant(fullRemoteSummary, 1);
            Assert.fail("Expected IllegalStateException when validating summary with foreign shard for node 1");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("is not replicated by node 1"));
        }
    }

    /**
     * Replicas reject pushing missing mutations to non-participating nodes.
     */
    @Test
    public void testNonParticipantCollectRemotelyMissingThrowsException()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        Set<Range<Token>> shardRanges = Collections.singleton(range("a", "m"));
        KeyspaceShards shards = createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, shards);

        // Shard was created with localNodeId 1 as participant. Node 2 is not a participant.
        Shard shard = shards.lookUp(range("a", "m"));
        CoordinatorLog log = shard.currentLocalLog();
        Offsets offsets = new Offsets.Mutable(log.logId);

        Node2OffsetsMap into = new Node2OffsetsMap();
        org.agrona.collections.IntArrayList remoteNodes = new org.agrona.collections.IntArrayList();
        remoteNodes.addInt(2); // node 2 is not a participant
        try
        {
            service.collectRemotelyMissingMutations(offsets, remoteNodes, into);
            Assert.fail("Expected IllegalStateException when pushing to non-participating host");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("is not a participant of shard"));
        }
    }

    /**
     * Summaries that include foreign shards fail validation for non-participating nodes.
     */
    @Test
    public void testSummaryValidationFailsForNonParticipatingShards()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Shard 1 covers range a-m with participant 1 only.
        // Shard 2 covers range m-z with participant 2 only.
        Map<Range<Token>, Shard> shardMap = new HashMap<>();
        Map<Range<Token>, VersionedEndpoints.ForRange> groups = new HashMap<>();

        AtomicInteger hostLogId = new AtomicInteger(0);
        LongSupplier logId1 = () -> CoordinatorLogId.asLong(1, hostLogId.getAndIncrement());
        LongSupplier logId2 = () -> CoordinatorLogId.asLong(2, hostLogId.getAndIncrement());

        Range<Token> range1 = range("a", "m");
        Shard shard1 = new Shard(1, TEST_KEYSPACE, range1, new Participants(List.of(1)), logId1, (s, l) -> {});
        shard1.currentLocalLog().reconciledOffsets.add(1);
        shardMap.put(range1, shard1);
        groups.put(range1, VersionedEndpoints.forRange(Epoch.EMPTY, EndpointsForRange.empty(range1)));

        Range<Token> range2 = range("m", "z");
        Shard shard2 = new Shard(2, TEST_KEYSPACE, range2, new Participants(List.of(2)), logId2, (s, l) -> {});
        shard2.currentLocalLog().reconciledOffsets.add(2);
        shardMap.put(range2, shard2);
        groups.put(range2, VersionedEndpoints.forRange(Epoch.EMPTY, EndpointsForRange.empty(range2)));

        KeyspaceShards keyspaceShards = new KeyspaceShards(TEST_KEYSPACE, shardMap, new ReplicaGroups(groups));
        setKeyspaceShardsUnsafe(service, TEST_KEYSPACE, keyspaceShards);

        TableId tableId = Keyspace.open(TEST_KEYSPACE).getColumnFamilyStore(TEST_TABLE).metadata().id;
        AbstractBounds<PartitionPosition> fullRange = Range.makeRowRange(range("a", "z"));

        MutationSummary summaryCoveringBothShards = service.createSummaryForRange(fullRange, tableId, false);
        assertEquals(2, summaryCoveringBothShards.size());

        // Verify validation fails when sending summary to node 1 with foreign shard 2
        try
        {
            service.validateSummaryForParticipant(summaryCoveringBothShards, 1);
            Assert.fail("Expected IllegalStateException for node 1");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("is not replicated by node 1"));
        }

        // Verify validation fails when sending summary to node 2 with foreign shard 1
        try
        {
            service.validateSummaryForParticipant(summaryCoveringBothShards, 2);
            Assert.fail("Expected IllegalStateException for node 2");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("is not replicated by node 2"));
        }

        // Sub-range for shard 1 validates for participant 1, but fails for participant 2
        MutationSummary shard1Summary = service.createSummaryForRange(Range.makeRowRange(range1), tableId, false);
        assertEquals(1, shard1Summary.size());
        assertEquals(shard1.currentLocalLog().logId, shard1Summary.get(0).logId());
        service.validateSummaryForParticipant(shard1Summary, 1);
        try
        {
            service.validateSummaryForParticipant(shard1Summary, 2);
            Assert.fail("Expected IllegalStateException for node 2 on shard 1 summary");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("is not replicated by node 2"));
        }

        // Sub-range for shard 2 validates for participant 2, but fails for participant 1
        MutationSummary shard2Summary = service.createSummaryForRange(Range.makeRowRange(range2), tableId, false);
        assertEquals(1, shard2Summary.size());
        assertEquals(shard2.currentLocalLog().logId, shard2Summary.get(0).logId());
        service.validateSummaryForParticipant(shard2Summary, 2);
        try
        {
            service.validateSummaryForParticipant(shard2Summary, 1);
            Assert.fail("Expected IllegalStateException for node 1 on shard 2 summary");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("is not replicated by node 1"));
        }
    }

    private static Token tk(String key)
    {
        return new ByteOrderedPartitioner.BytesToken(ByteBufferUtil.bytes(key));
    }

    private static Range<Token> range(String left, String right)
    {
        return new Range<>(tk(left), tk(right));
    }

    private static SyncTask createSyncTask(Range<Token> range)
    {
        return createSyncTask(Collections.singletonList(range), LOCAL, REMOTE);
    }

    private static SyncTask createSyncTask(List<Range<Token>> ranges, InetAddressAndPort local, InetAddressAndPort remote)
    {
        SharedContext ctx = SharedContext.Global.instance;
        TimeUUID sessionId = TimeUUID.Generator.nextTimeUUID();
        RepairJobDesc desc = new RepairJobDesc(sessionId, TimeUUID.Generator.nextTimeUUID(),TEST_KEYSPACE, TEST_TABLE, ranges);
        return new SymmetricRemoteSyncTask(ctx, desc, local, remote, ranges, PreviewKind.NONE, null);
    }
}

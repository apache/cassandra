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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.repair.SymmetricRemoteSyncTask;
import org.apache.cassandra.repair.SyncTask;
import org.apache.cassandra.repair.SyncTasks;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
        MutationTrackingService.KeyspaceShards shards =
        MutationTrackingService.TestAccess.createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        MutationTrackingService.TestAccess.setKeyspaceShards(service, TEST_KEYSPACE, shards);

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
        MutationTrackingService.KeyspaceShards shards =
        MutationTrackingService.TestAccess.createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        MutationTrackingService.TestAccess.setKeyspaceShards(service, TEST_KEYSPACE, shards);

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
        MutationTrackingService.KeyspaceShards shards =
        MutationTrackingService.TestAccess.createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        MutationTrackingService.TestAccess.setKeyspaceShards(service, TEST_KEYSPACE, shards);

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
        MutationTrackingService.KeyspaceShards shards =
        MutationTrackingService.TestAccess.createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        MutationTrackingService.TestAccess.setKeyspaceShards(service, TEST_KEYSPACE, shards);

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
        MutationTrackingService.KeyspaceShards shards =
        MutationTrackingService.TestAccess.createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        MutationTrackingService.TestAccess.setKeyspaceShards(service, TEST_KEYSPACE, shards);

        // Task spanning both shards
        List<SyncTask> inputTasks = Collections.singletonList(createSyncTask(range("d", "s")));

        Keyspace keyspace = Keyspace.open(TEST_KEYSPACE);
        SyncTasks result = service.alignToShardBoundaries(keyspace, inputTasks);

        // All resulting tasks should be the same type as the input
        result.apply((shardedTask) -> assertTrue("Task should be SymmetricRemoteSyncTask", shardedTask.task instanceof SymmetricRemoteSyncTask));
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

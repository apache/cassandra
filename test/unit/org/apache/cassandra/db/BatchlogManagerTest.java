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
package org.apache.cassandra.db;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Iterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.Util.PartitionerSwitcher;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.UUIDGen;

public class BatchlogManagerTest
{
    private static final String KEYSPACE1 = "BatchlogManagerTest1";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_STANDARD2 = "Standard2";
    private static final String CF_STANDARD3 = "Standard3";
    private static final String CF_STANDARD4 = "Standard4";

    static PartitionerSwitcher sw;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        sw = Util.switchPartitioner(Murmur3Partitioner.instance);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1, 1, BytesType.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2, 1, BytesType.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD3, 1, BytesType.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD4, 1, BytesType.instance));
        System.out.println(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).metadata.partitionKeyColumns());
    }

    @AfterClass
    public static void cleanup()
    {
        sw.close();
    }

    @Before
    public void setUp() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        InetAddress localhost = InetAddress.getByName("127.0.0.1");
        metadata.updateNormalToken(Util.token("A"), localhost);
        metadata.updateHostId(UUIDGen.getTimeUUID(), localhost);
        Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).truncateBlocking();
        Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.LEGACY_BATCHLOG).truncateBlocking();
    }

    @Test
    public void testDelete()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        CFMetaData cfm = cfs.metadata;
        new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), ByteBufferUtil.bytes("1234"))
                .clustering("c")
                .add("val", "val" + 1234)
                .build()
                .applyUnsafe();

        DecoratedKey dk = cfs.decorateKey(ByteBufferUtil.bytes("1234"));
        ImmutableBTreePartition results = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, dk).build());
        Iterator<Row> iter = results.iterator();
        assert iter.hasNext();

        Mutation mutation = new Mutation(PartitionUpdate.fullPartitionDelete(cfm,
                                                         dk,
                                                         FBUtilities.timestampMicros(),
                                                         FBUtilities.nowInSeconds()));
        mutation.applyUnsafe();

        Util.assertEmpty(Util.cmd(cfs, dk).build());
    }

    // TODO: Fix. Currently endlessly looping on BatchLogManager.replayAllFailedBatches
    @Test
    public void testReplay() throws Exception
    {
        long initialAllBatches = BatchlogManager.instance.countAllBatches();
        long initialReplayedBatches = BatchlogManager.instance.getTotalBatchesReplayed();

        CFMetaData cfm = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).metadata;

        // Generate 1000 mutations and put them all into the batchlog.
        // Half (500) ready to be replayed, half not.
        for (int i = 0; i < 1000; i++)
        {
            Mutation m = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), bytes(i))
                    .clustering("name" + i)
                    .add("val", "val" + i)
                    .build();

            long timestamp = i < 500
                           ? (System.currentTimeMillis() - BatchlogManager.instance.getBatchlogTimeout())
                           : (System.currentTimeMillis() + BatchlogManager.instance.getBatchlogTimeout());

            BatchlogManager.getBatchlogMutationFor(Collections.singleton(m),
                                                   UUIDGen.getTimeUUID(timestamp, i),
                                                   MessagingService.current_version)
                           .applyUnsafe();
        }

        // Flush the batchlog to disk (see CASSANDRA-6822).
        Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).forceBlockingFlush();

        assertEquals(1000, BatchlogManager.instance.countAllBatches() - initialAllBatches);
        assertEquals(0, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);

        // Force batchlog replay and wait for it to complete.
        BatchlogManager.instance.startBatchlogReplay().get();

        // Ensure that the first half, and only the first half, got replayed.
        assertEquals(500, BatchlogManager.instance.countAllBatches() - initialAllBatches);
        assertEquals(500, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);

        for (int i = 0; i < 1000; i++)
        {
            UntypedResultSet result = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".\"%s\" WHERE key = intAsBlob(%d)", KEYSPACE1, CF_STANDARD1, i));
            if (i < 500)
            {
                assertEquals(bytes(i), result.one().getBytes("key"));
                assertEquals("name" + i, result.one().getString("name"));
                assertEquals("val" + i, result.one().getString("val"));
            }
            else
            {
                assertTrue(result.isEmpty());
            }
        }

        // Ensure that no stray mutations got somehow applied.
        UntypedResultSet result = QueryProcessor.executeInternal(String.format("SELECT count(*) FROM \"%s\".\"%s\"", KEYSPACE1, CF_STANDARD1));
        assertEquals(500, result.one().getLong("count"));
    }

    @Test
    public void testTruncatedReplay() throws InterruptedException, ExecutionException
    {
        CFMetaData cf2 = Schema.instance.getCFMetaData(KEYSPACE1, CF_STANDARD2);
        CFMetaData cf3 = Schema.instance.getCFMetaData(KEYSPACE1, CF_STANDARD3);
        // Generate 2000 mutations (1000 batchlog entries) and put them all into the batchlog.
        // Each batchlog entry with a mutation for Standard2 and Standard3.
        // In the middle of the process, 'truncate' Standard2.
        for (int i = 0; i < 1000; i++)
        {
            Mutation mutation1 = new RowUpdateBuilder(cf2, FBUtilities.timestampMicros(), bytes(i))
                .clustering("name" + i)
                .add("val", "val" + i)
                .build();
            Mutation mutation2 = new RowUpdateBuilder(cf3, FBUtilities.timestampMicros(), bytes(i))
                .clustering("name" + i)
                .add("val", "val" + i)
                .build();

            List<Mutation> mutations = Lists.newArrayList(mutation1, mutation2);

            // Make sure it's ready to be replayed, so adjust the timestamp.
            long timestamp = System.currentTimeMillis() - BatchlogManager.instance.getBatchlogTimeout();

            if (i == 500)
                SystemKeyspace.saveTruncationRecord(Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard2"),
                                                    timestamp,
                                                    ReplayPosition.NONE);

            // Adjust the timestamp (slightly) to make the test deterministic.
            if (i >= 500)
                timestamp++;
            else
                timestamp--;

            BatchlogManager.getBatchlogMutationFor(mutations,
                                                   UUIDGen.getTimeUUID(timestamp, i),
                                                   MessagingService.current_version)
                           .applyUnsafe();
        }

        // Flush the batchlog to disk (see CASSANDRA-6822).
        Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).forceBlockingFlush();

        // Force batchlog replay and wait for it to complete.
        BatchlogManager.instance.startBatchlogReplay().get();

        // We should see half of Standard2-targeted mutations written after the replay and all of Standard3 mutations applied.
        for (int i = 0; i < 1000; i++)
        {
            UntypedResultSet result = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".\"%s\" WHERE key = intAsBlob(%d)", KEYSPACE1, CF_STANDARD2,i));
            if (i >= 500)
            {
                assertEquals(bytes(i), result.one().getBytes("key"));
                assertEquals("name" + i, result.one().getString("name"));
                assertEquals("val" + i, result.one().getString("val"));
            }
            else
            {
                assertTrue(result.isEmpty());
            }
        }

        for (int i = 0; i < 1000; i++)
        {
            UntypedResultSet result = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".\"%s\" WHERE key = intAsBlob(%d)", KEYSPACE1, CF_STANDARD3, i));
            assertEquals(bytes(i), result.one().getBytes("key"));
            assertEquals("name" + i, result.one().getString("name"));
            assertEquals("val" + i, result.one().getString("val"));
        }
    }

    static Mutation fakeVersion12MutationFor(Collection<Mutation> mutations, long now) throws IOException
    {
        // Serialization can't write version 1.2 mutations, pretend this is old by using random id and written_at and
        // saving it in the legacy batchlog.
        UUID uuid = UUID.randomUUID();
        ByteBuffer writtenAt = LongType.instance.decompose(now);
        int version = MessagingService.VERSION_30;
        ByteBuffer data = BatchlogManager.serializeMutations(mutations, version);

        return new RowUpdateBuilder(SystemKeyspace.LegacyBatchlog, FBUtilities.timestampMicros(), uuid)
            .clustering()
            .add("written_at", writtenAt)
            .add("data", data)
            .add("version", version)
            .build();
    }

    static Mutation fakeVersion20MutationFor(Collection<Mutation> mutations, UUID uuid)
    {
        // Serialization can't write version 1.2 mutations, pretend this is old by saving it in the legacy batchlog.
        int version = MessagingService.VERSION_30;
        ByteBuffer writtenAt = LongType.instance.decompose(UUIDGen.unixTimestamp(uuid));
        return new RowUpdateBuilder(SystemKeyspace.LegacyBatchlog, FBUtilities.timestampMicros(), uuid)
               .clustering()
               .add("data", BatchlogManager.serializeMutations(mutations, version))
               .add("written_at", writtenAt)
               .add("version", version)
               .build();
    }

    @Test
    public void testConversion() throws Exception
    {
        long initialAllBatches = BatchlogManager.instance.countAllBatches();
        long initialReplayedBatches = BatchlogManager.instance.getTotalBatchesReplayed();
        CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE1, CF_STANDARD4);

        // Generate 1000 mutations and put them all into the batchlog.
        // Half (500) ready to be replayed, half not.
        for (int i = 0; i < 1000; i++)
        {
            Mutation mutation = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), bytes(i))
                .clustering("name" + i)
                .add("val", "val" + i)
                .build();

            long timestamp = i < 500
                           ? (System.currentTimeMillis() - BatchlogManager.instance.getBatchlogTimeout())
                           : (System.currentTimeMillis() + BatchlogManager.instance.getBatchlogTimeout());


            fakeVersion12MutationFor(Collections.singleton(mutation), timestamp).applyUnsafe();
        }

        // Add 400 version 2.0 mutations and put them all into the batchlog.
        // Half (200) ready to be replayed, half not.
        for (int i = 1000; i < 1400; i++)
        {
            Mutation mutation = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), bytes(i))
                .clustering("name" + i)
                .add("val", "val" + i)
                .build();

            long timestamp = i < 1200
                           ? (System.currentTimeMillis() - BatchlogManager.instance.getBatchlogTimeout())
                           : (System.currentTimeMillis() + BatchlogManager.instance.getBatchlogTimeout());


            fakeVersion20MutationFor(Collections.singleton(mutation), UUIDGen.getTimeUUID(timestamp, i)).applyUnsafe();
        }

        // Mix in 100 current version mutations, 50 ready for replay.
        for (int i = 1400; i < 1500; i++)
        {
            Mutation mutation = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), bytes(i))
                .clustering("name" + i)
                .add("val", "val" + i)
                .build();

            long timestamp = i < 1450
                           ? (System.currentTimeMillis() - BatchlogManager.instance.getBatchlogTimeout())
                           : (System.currentTimeMillis() + BatchlogManager.instance.getBatchlogTimeout());


            BatchlogManager.getBatchlogMutationFor(Collections.singleton(mutation),
                                                   UUIDGen.getTimeUUID(timestamp, i),
                                                   MessagingService.current_version)
                           .applyUnsafe();
        }

        // Flush the batchlog to disk (see CASSANDRA-6822).
        Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.LEGACY_BATCHLOG).forceBlockingFlush();
        Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).forceBlockingFlush();

        assertEquals(100, BatchlogManager.instance.countAllBatches() - initialAllBatches);
        assertEquals(0, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);

        UntypedResultSet result = QueryProcessor.executeInternal(String.format("SELECT count(*) FROM \"%s\".\"%s\"", SystemKeyspace.NAME, SystemKeyspace.LEGACY_BATCHLOG));
        assertEquals("Count in blog legacy", 1400, result.one().getLong("count"));
        result = QueryProcessor.executeInternal(String.format("SELECT count(*) FROM \"%s\".\"%s\"", SystemKeyspace.NAME, SystemKeyspace.BATCHES));
        assertEquals("Count in blog", 100, result.one().getLong("count"));

        // Force batchlog replay and wait for it to complete.
        BatchlogManager.instance.performInitialReplay();

        // Ensure that the first half, and only the first half, got replayed.
        assertEquals(750, BatchlogManager.instance.countAllBatches() - initialAllBatches);
        assertEquals(750, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);

        for (int i = 0; i < 1500; i++)
        {
            result = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".\"%s\" WHERE key = intAsBlob(%d)", KEYSPACE1, CF_STANDARD4, i));
            if (i < 500 || i >= 1000 && i < 1200 || i >= 1400 && i < 1450)
            {
                assertEquals(bytes(i), result.one().getBytes("key"));
                assertEquals("name" + i, result.one().getString("name"));
                assertEquals("val" + i, result.one().getString("val"));
            }
            else
            {
                assertTrue("Present at " + i, result.isEmpty());
            }
        }

        // Ensure that no stray mutations got somehow applied.
        result = QueryProcessor.executeInternal(String.format("SELECT count(*) FROM \"%s\".\"%s\"", KEYSPACE1, CF_STANDARD4));
        assertEquals(750, result.one().getLong("count"));

        // Ensure batchlog is left as expected.
        result = QueryProcessor.executeInternal(String.format("SELECT count(*) FROM \"%s\".\"%s\"", SystemKeyspace.NAME, SystemKeyspace.BATCHES));
        assertEquals("Count in blog after initial replay", 750, result.one().getLong("count"));
        result = QueryProcessor.executeInternal(String.format("SELECT count(*) FROM \"%s\".\"%s\"", SystemKeyspace.NAME, SystemKeyspace.LEGACY_BATCHLOG));
        assertEquals("Count in blog legacy after initial replay ", 0, result.one().getLong("count"));
    }
}

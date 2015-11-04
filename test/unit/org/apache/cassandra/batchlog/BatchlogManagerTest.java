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
package org.apache.cassandra.batchlog;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

import org.junit.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.Util.PartitionerSwitcher;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.junit.Assert.*;

public class BatchlogManagerTest
{
    private static final String KEYSPACE1 = "BatchlogManagerTest1";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_STANDARD2 = "Standard2";
    private static final String CF_STANDARD3 = "Standard3";
    private static final String CF_STANDARD4 = "Standard4";
    private static final String CF_STANDARD5 = "Standard5";

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
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD4, 1, BytesType.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD5, 1, BytesType.instance));
    }

    @AfterClass
    public static void cleanup()
    {
        sw.close();
    }

    @Before
    @SuppressWarnings("deprecation")
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

    @Test
    public void testReplay() throws Exception
    {
        testReplay(false);
    }

    @Test
    public void testLegacyReplay() throws Exception
    {
        testReplay(true);
    }

    @SuppressWarnings("deprecation")
    private static void testReplay(boolean legacy) throws Exception
    {
        long initialAllBatches = BatchlogManager.instance.countAllBatches();
        long initialReplayedBatches = BatchlogManager.instance.getTotalBatchesReplayed();

        CFMetaData cfm = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).metadata;

        // Generate 1000 mutations (100 batches of 10 mutations each) and put them all into the batchlog.
        // Half batches (50) ready to be replayed, half not.
        for (int i = 0; i < 100; i++)
        {
            List<Mutation> mutations = new ArrayList<>(10);
            for (int j = 0; j < 10; j++)
            {
                mutations.add(new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(i))
                              .clustering("name" + j)
                              .add("val", "val" + j)
                              .build());
            }

            long timestamp = i < 50
                           ? (System.currentTimeMillis() - BatchlogManager.getBatchlogTimeout())
                           : (System.currentTimeMillis() + BatchlogManager.getBatchlogTimeout());

            if (legacy)
                LegacyBatchlogMigrator.store(Batch.createLocal(UUIDGen.getTimeUUID(timestamp, i), timestamp * 1000, mutations), MessagingService.current_version);
            else
                BatchlogManager.store(Batch.createLocal(UUIDGen.getTimeUUID(timestamp, i), timestamp * 1000, mutations));
        }

        if (legacy)
        {
            Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.LEGACY_BATCHLOG).forceBlockingFlush();
            LegacyBatchlogMigrator.migrate();
        }

        // Flush the batchlog to disk (see CASSANDRA-6822).
        Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).forceBlockingFlush();

        assertEquals(100, BatchlogManager.instance.countAllBatches() - initialAllBatches);
        assertEquals(0, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);

        // Force batchlog replay and wait for it to complete.
        BatchlogManager.instance.startBatchlogReplay().get();

        // Ensure that the first half, and only the first half, got replayed.
        assertEquals(50, BatchlogManager.instance.countAllBatches() - initialAllBatches);
        assertEquals(50, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);

        for (int i = 0; i < 100; i++)
        {
            String query = String.format("SELECT * FROM \"%s\".\"%s\" WHERE key = intAsBlob(%d)", KEYSPACE1, CF_STANDARD1, i);
            UntypedResultSet result = executeInternal(query);
            assertNotNull(result);
            if (i < 50)
            {
                Iterator<UntypedResultSet.Row> it = result.iterator();
                assertNotNull(it);
                for (int j = 0; j < 10; j++)
                {
                    assertTrue(it.hasNext());
                    UntypedResultSet.Row row = it.next();

                    assertEquals(ByteBufferUtil.bytes(i), row.getBytes("key"));
                    assertEquals("name" + j, row.getString("name"));
                    assertEquals("val" + j, row.getString("val"));
                }

                assertFalse(it.hasNext());
            }
            else
            {
                assertTrue(result.isEmpty());
            }
        }

        // Ensure that no stray mutations got somehow applied.
        UntypedResultSet result = executeInternal(String.format("SELECT count(*) FROM \"%s\".\"%s\"", KEYSPACE1, CF_STANDARD1));
        assertNotNull(result);
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
            Mutation mutation1 = new RowUpdateBuilder(cf2, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(i))
                .clustering("name" + i)
                .add("val", "val" + i)
                .build();
            Mutation mutation2 = new RowUpdateBuilder(cf3, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(i))
                .clustering("name" + i)
                .add("val", "val" + i)
                .build();

            List<Mutation> mutations = Lists.newArrayList(mutation1, mutation2);

            // Make sure it's ready to be replayed, so adjust the timestamp.
            long timestamp = System.currentTimeMillis() - BatchlogManager.getBatchlogTimeout();

            if (i == 500)
                SystemKeyspace.saveTruncationRecord(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD2),
                                                    timestamp,
                                                    ReplayPosition.NONE);

            // Adjust the timestamp (slightly) to make the test deterministic.
            if (i >= 500)
                timestamp++;
            else
                timestamp--;

            BatchlogManager.store(Batch.createLocal(UUIDGen.getTimeUUID(timestamp, i), FBUtilities.timestampMicros(), mutations));
        }

        // Flush the batchlog to disk (see CASSANDRA-6822).
        Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).forceBlockingFlush();

        // Force batchlog replay and wait for it to complete.
        BatchlogManager.instance.startBatchlogReplay().get();

        // We should see half of Standard2-targeted mutations written after the replay and all of Standard3 mutations applied.
        for (int i = 0; i < 1000; i++)
        {
            UntypedResultSet result = executeInternal(String.format("SELECT * FROM \"%s\".\"%s\" WHERE key = intAsBlob(%d)", KEYSPACE1, CF_STANDARD2,i));
            assertNotNull(result);
            if (i >= 500)
            {
                assertEquals(ByteBufferUtil.bytes(i), result.one().getBytes("key"));
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
            UntypedResultSet result = executeInternal(String.format("SELECT * FROM \"%s\".\"%s\" WHERE key = intAsBlob(%d)", KEYSPACE1, CF_STANDARD3, i));
            assertNotNull(result);
            assertEquals(ByteBufferUtil.bytes(i), result.one().getBytes("key"));
            assertEquals("name" + i, result.one().getString("name"));
            assertEquals("val" + i, result.one().getString("val"));
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testConversion() throws Exception
    {
        long initialAllBatches = BatchlogManager.instance.countAllBatches();
        long initialReplayedBatches = BatchlogManager.instance.getTotalBatchesReplayed();
        CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE1, CF_STANDARD4);

        // Generate 1400 version 2.0 mutations and put them all into the batchlog.
        // Half ready to be replayed, half not.
        for (int i = 0; i < 1400; i++)
        {
            Mutation mutation = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(i))
                .clustering("name" + i)
                .add("val", "val" + i)
                .build();

            long timestamp = i < 700
                           ? (System.currentTimeMillis() - BatchlogManager.getBatchlogTimeout())
                           : (System.currentTimeMillis() + BatchlogManager.getBatchlogTimeout());


            Mutation batchMutation = LegacyBatchlogMigrator.getStoreMutation(Batch.createLocal(UUIDGen.getTimeUUID(timestamp, i),
                                                                                               TimeUnit.MILLISECONDS.toMicros(timestamp),
                                                                                               Collections.singleton(mutation)),
                                                                             MessagingService.VERSION_20);
            assertTrue(LegacyBatchlogMigrator.isLegacyBatchlogMutation(batchMutation));
            LegacyBatchlogMigrator.handleLegacyMutation(batchMutation);
        }

        // Mix in 100 current version mutations, 50 ready for replay.
        for (int i = 1400; i < 1500; i++)
        {
            Mutation mutation = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(i))
                .clustering("name" + i)
                .add("val", "val" + i)
                .build();

            long timestamp = i < 1450
                           ? (System.currentTimeMillis() - BatchlogManager.getBatchlogTimeout())
                           : (System.currentTimeMillis() + BatchlogManager.getBatchlogTimeout());


            BatchlogManager.store(Batch.createLocal(UUIDGen.getTimeUUID(timestamp, i),
                                                    FBUtilities.timestampMicros(),
                                                    Collections.singleton(mutation)));
        }

        // Flush the batchlog to disk (see CASSANDRA-6822).
        Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).forceBlockingFlush();

        assertEquals(1500, BatchlogManager.instance.countAllBatches() - initialAllBatches);
        assertEquals(0, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);

        UntypedResultSet result = executeInternal(String.format("SELECT count(*) FROM \"%s\".\"%s\"", SystemKeyspace.NAME, SystemKeyspace.LEGACY_BATCHLOG));
        assertNotNull(result);
        assertEquals("Count in blog legacy", 0, result.one().getLong("count"));
        result = executeInternal(String.format("SELECT count(*) FROM \"%s\".\"%s\"", SystemKeyspace.NAME, SystemKeyspace.BATCHES));
        assertNotNull(result);
        assertEquals("Count in blog", 1500, result.one().getLong("count"));

        // Force batchlog replay and wait for it to complete.
        BatchlogManager.instance.performInitialReplay();

        // Ensure that the first half, and only the first half, got replayed.
        assertEquals(750, BatchlogManager.instance.countAllBatches() - initialAllBatches);
        assertEquals(750, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);

        for (int i = 0; i < 1500; i++)
        {
            result = executeInternal(String.format("SELECT * FROM \"%s\".\"%s\" WHERE key = intAsBlob(%d)", KEYSPACE1, CF_STANDARD4, i));
            assertNotNull(result);
            if (i < 700 || i >= 1400 && i < 1450)
            {
                assertEquals(ByteBufferUtil.bytes(i), result.one().getBytes("key"));
                assertEquals("name" + i, result.one().getString("name"));
                assertEquals("val" + i, result.one().getString("val"));
            }
            else
            {
                assertTrue("Present at " + i, result.isEmpty());
            }
        }

        // Ensure that no stray mutations got somehow applied.
        result = executeInternal(String.format("SELECT count(*) FROM \"%s\".\"%s\"", KEYSPACE1, CF_STANDARD4));
        assertNotNull(result);
        assertEquals(750, result.one().getLong("count"));

        // Ensure batchlog is left as expected.
        result = executeInternal(String.format("SELECT count(*) FROM \"%s\".\"%s\"", SystemKeyspace.NAME, SystemKeyspace.BATCHES));
        assertNotNull(result);
        assertEquals("Count in blog after initial replay", 750, result.one().getLong("count"));
        result = executeInternal(String.format("SELECT count(*) FROM \"%s\".\"%s\"", SystemKeyspace.NAME, SystemKeyspace.LEGACY_BATCHLOG));
        assertNotNull(result);
        assertEquals("Count in blog legacy after initial replay ", 0, result.one().getLong("count"));
    }

    @Test
    public void testAddBatch() throws IOException
    {
        long initialAllBatches = BatchlogManager.instance.countAllBatches();
        CFMetaData cfm = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD5).metadata;

        long timestamp = (System.currentTimeMillis() - DatabaseDescriptor.getWriteRpcTimeout() * 2) * 1000;
        UUID uuid = UUIDGen.getTimeUUID();

        // Add a batch with 10 mutations
        List<Mutation> mutations = new ArrayList<>(10);
        for (int j = 0; j < 10; j++)
        {
            mutations.add(new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(j))
                          .clustering("name" + j)
                          .add("val", "val" + j)
                          .build());
        }


        BatchlogManager.store(Batch.createLocal(uuid, timestamp, mutations));
        Assert.assertEquals(initialAllBatches + 1, BatchlogManager.instance.countAllBatches());

        String query = String.format("SELECT count(*) FROM %s.%s where id = %s",
                                     SystemKeyspace.NAME,
                                     SystemKeyspace.BATCHES,
                                     uuid);
        UntypedResultSet result = executeInternal(query);
        assertNotNull(result);
        assertEquals(1L, result.one().getLong("count"));
    }

    @Test
    public void testRemoveBatch()
    {
        long initialAllBatches = BatchlogManager.instance.countAllBatches();
        CFMetaData cfm = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD5).metadata;

        long timestamp = (System.currentTimeMillis() - DatabaseDescriptor.getWriteRpcTimeout() * 2) * 1000;
        UUID uuid = UUIDGen.getTimeUUID();

        // Add a batch with 10 mutations
        List<Mutation> mutations = new ArrayList<>(10);
        for (int j = 0; j < 10; j++)
        {
            mutations.add(new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(j))
                          .clustering("name" + j)
                          .add("val", "val" + j)
                          .build());
        }

        // Store the batch
        BatchlogManager.store(Batch.createLocal(uuid, timestamp, mutations));
        Assert.assertEquals(initialAllBatches + 1, BatchlogManager.instance.countAllBatches());

        // Remove the batch
        BatchlogManager.remove(uuid);

        assertEquals(initialAllBatches, BatchlogManager.instance.countAllBatches());

        String query = String.format("SELECT count(*) FROM %s.%s where id = %s",
                                     SystemKeyspace.NAME,
                                     SystemKeyspace.BATCHES,
                                     uuid);
        UntypedResultSet result = executeInternal(query);
        assertNotNull(result);
        assertEquals(0L, result.one().getLong("count"));
    }

    // CASSANRDA-9223
    @Test
    public void testReplayWithNoPeers() throws Exception
    {
        StorageService.instance.getTokenMetadata().removeEndpoint(InetAddress.getByName("127.0.0.1"));

        long initialAllBatches = BatchlogManager.instance.countAllBatches();
        long initialReplayedBatches = BatchlogManager.instance.getTotalBatchesReplayed();

        CFMetaData cfm = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).metadata;

        long timestamp = (System.currentTimeMillis() - DatabaseDescriptor.getWriteRpcTimeout() * 2) * 1000;
        UUID uuid = UUIDGen.getTimeUUID();

        // Add a batch with 10 mutations
        List<Mutation> mutations = new ArrayList<>(10);
        for (int j = 0; j < 10; j++)
        {
            mutations.add(new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(j))
                          .clustering("name" + j)
                          .add("val", "val" + j)
                          .build());
        }
        BatchlogManager.store(Batch.createLocal(uuid, timestamp, mutations));
        assertEquals(1, BatchlogManager.instance.countAllBatches() - initialAllBatches);

        // Flush the batchlog to disk (see CASSANDRA-6822).
        Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).forceBlockingFlush();

        assertEquals(1, BatchlogManager.instance.countAllBatches() - initialAllBatches);
        assertEquals(0, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);

        // Force batchlog replay and wait for it to complete.
        BatchlogManager.instance.startBatchlogReplay().get();

        // Replay should be cancelled as there are no peers in the ring.
        assertEquals(1, BatchlogManager.instance.countAllBatches() - initialAllBatches);
    }
}

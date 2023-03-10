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

import java.util.*;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Lists;

import org.junit.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.Util.PartitionerSwitcher;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.TimeUUID.Generator.atUnixMillis;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
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
        DatabaseDescriptor.daemonInitialization();
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
    public void setUp() throws Exception
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        InetAddressAndPort localhost = InetAddressAndPort.getByName("127.0.0.1");
        metadata.updateNormalToken(Util.token("A"), localhost);
        metadata.updateHostId(UUID.randomUUID(), localhost);
        Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).truncateBlocking();
    }

    @Test
    public void testDelete()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        TableMetadata cfm = cfs.metadata();
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
        long initialAllBatches = BatchlogManager.instance.countAllBatches();
        long initialReplayedBatches = BatchlogManager.instance.getTotalBatchesReplayed();

        TableMetadata cfm = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).metadata();

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
                           ? (currentTimeMillis() - BatchlogManager.getBatchlogTimeout())
                           : (currentTimeMillis() + BatchlogManager.getBatchlogTimeout());

            BatchlogManager.store(Batch.createLocal(atUnixMillis(timestamp, i), timestamp * 1000, mutations));
        }

        // Flush the batchlog to disk (see CASSANDRA-6822).
        Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        assertEquals(100, BatchlogManager.instance.countAllBatches() - initialAllBatches);
        assertEquals(0, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);

        // Force batchlog replay and wait for it to complete.
        BatchlogManager.instance.startBatchlogReplay().get();

        // Ensure that the first half, and only the first half, got replayed.
        assertEquals(50, BatchlogManager.instance.countAllBatches() - initialAllBatches);
        assertEquals(50, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);

        for (int i = 0; i < 100; i++)
        {
            String query = String.format("SELECT * FROM \"%s\".\"%s\" WHERE key = int_as_blob(%d)", KEYSPACE1, CF_STANDARD1, i);
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
        TableMetadata cf2 = Schema.instance.getTableMetadata(KEYSPACE1, CF_STANDARD2);
        TableMetadata cf3 = Schema.instance.getTableMetadata(KEYSPACE1, CF_STANDARD3);
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
            long timestamp = currentTimeMillis() - BatchlogManager.getBatchlogTimeout();

            if (i == 500)
                SystemKeyspace.saveTruncationRecord(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD2),
                                                    timestamp,
                                                    CommitLogPosition.NONE);

            // Adjust the timestamp (slightly) to make the test deterministic.
            if (i >= 500)
                timestamp++;
            else
                timestamp--;

            BatchlogManager.store(Batch.createLocal(atUnixMillis(timestamp, i), FBUtilities.timestampMicros(), mutations));
        }

        // Flush the batchlog to disk (see CASSANDRA-6822).
        Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // Force batchlog replay and wait for it to complete.
        BatchlogManager.instance.startBatchlogReplay().get();

        // We should see half of Standard2-targeted mutations written after the replay and all of Standard3 mutations applied.
        for (int i = 0; i < 1000; i++)
        {
            UntypedResultSet result = executeInternal(String.format("SELECT * FROM \"%s\".\"%s\" WHERE key = int_as_blob(%d)", KEYSPACE1, CF_STANDARD2,i));
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
            UntypedResultSet result = executeInternal(String.format("SELECT * FROM \"%s\".\"%s\" WHERE key = int_as_blob(%d)", KEYSPACE1, CF_STANDARD3, i));
            assertNotNull(result);
            assertEquals(ByteBufferUtil.bytes(i), result.one().getBytes("key"));
            assertEquals("name" + i, result.one().getString("name"));
            assertEquals("val" + i, result.one().getString("val"));
        }
    }

    @Test
    public void testAddBatch()
    {
        long initialAllBatches = BatchlogManager.instance.countAllBatches();
        TableMetadata cfm = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD5).metadata();

        long timestamp = (currentTimeMillis() - DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS) * 2) * 1000;
        TimeUUID uuid = nextTimeUUID();

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
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME,
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
        TableMetadata cfm = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD5).metadata();

        long timestamp = (currentTimeMillis() - DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS) * 2) * 1000;
        TimeUUID uuid = nextTimeUUID();

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
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME,
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
        StorageService.instance.getTokenMetadata().removeEndpoint(InetAddressAndPort.getByName("127.0.0.1"));

        long initialAllBatches = BatchlogManager.instance.countAllBatches();
        long initialReplayedBatches = BatchlogManager.instance.getTotalBatchesReplayed();

        TableMetadata cfm = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).metadata();

        long timestamp = (currentTimeMillis() - DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS) * 2) * 1000;
        TimeUUID uuid = nextTimeUUID();

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
        Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        assertEquals(1, BatchlogManager.instance.countAllBatches() - initialAllBatches);
        assertEquals(0, BatchlogManager.instance.getTotalBatchesReplayed() - initialReplayedBatches);

        // Force batchlog replay and wait for it to complete.
        BatchlogManager.instance.startBatchlogReplay().get();

        // Replay should be cancelled as there are no peers in the ring.
        assertEquals(1, BatchlogManager.instance.countAllBatches() - initialAllBatches);
    }
}

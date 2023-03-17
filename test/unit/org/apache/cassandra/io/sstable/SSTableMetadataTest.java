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
package org.apache.cassandra.io.sstable;

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SSTableMetadataTest
{
    public static final String KEYSPACE1 = "SSTableMetadataTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_STANDARD2 = "Standard2";
    public static final String CF_STANDARD3 = "Standard3";
    public static final String CF_STANDARDCOMPOSITE2 = "StandardComposite2";
    public static final String CF_COUNTER1 = "Counter1";

    /**
     * Max allowed difference between compared SSTable metadata timestamps, in seconds.
     * We use a {@code double} to force the usage of {@link org.junit.Assert#assertEquals(double, double, double)} when
     * comparing integer timestamps, otherwise {@link org.junit.Assert#assertEquals(float, float, float)} would be used.
     */
    public static final double DELTA = 10;

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD3),
                                    TableMetadata.builder(KEYSPACE1, CF_STANDARDCOMPOSITE2)
                                                 .addPartitionKeyColumn("key", AsciiType.instance)
                                                 .addClusteringColumn("name", AsciiType.instance)
                                                 .addClusteringColumn("int", IntegerType.instance)
                                                 .addRegularColumn("val", AsciiType.instance),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF_COUNTER1));
    }

    @Test
    public void testTrackMaxDeletionTime()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard1");
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < 10; i++)
        {
            for (int j = 0; j < 10; j++)
                new RowUpdateBuilder(store.metadata(), timestamp, 10 + j, Integer.toString(i))
                    .clustering(Integer.toString(j))
                    .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

        }

        new RowUpdateBuilder(store.metadata(), timestamp, 10000, "longttl")
            .clustering("col")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();

        Util.flush(store);
        assertEquals(1, store.getLiveSSTables().size());
        int ttltimestamp = (int) (System.currentTimeMillis() / 1000);
        int firstDelTime = 0;
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            firstDelTime = sstable.getSSTableMetadata().maxLocalDeletionTime;
            assertEquals(ttltimestamp + 10000, firstDelTime, DELTA);

        }

        new RowUpdateBuilder(store.metadata(), timestamp, 20000, "longttl2")
        .clustering("col")
        .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
        .build()
        .applyUnsafe();

        ttltimestamp = (int) (System.currentTimeMillis() / 1000);
        Util.flush(store);
        assertEquals(2, store.getLiveSSTables().size());
        List<SSTableReader> sstables = new ArrayList<>(store.getLiveSSTables());
        if (sstables.get(0).getSSTableMetadata().maxLocalDeletionTime < sstables.get(1).getSSTableMetadata().maxLocalDeletionTime)
        {
            assertEquals(sstables.get(0).getSSTableMetadata().maxLocalDeletionTime, firstDelTime);
            assertEquals(sstables.get(1).getSSTableMetadata().maxLocalDeletionTime, ttltimestamp + 20000, DELTA);
        }
        else
        {
            assertEquals(sstables.get(1).getSSTableMetadata().maxLocalDeletionTime, firstDelTime);
            assertEquals(sstables.get(0).getSSTableMetadata().maxLocalDeletionTime, ttltimestamp + 20000, DELTA);
        }

        Util.compact(store, store.getLiveSSTables());
        assertEquals(1, store.getLiveSSTables().size());
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            assertEquals(sstable.getSSTableMetadata().maxLocalDeletionTime, ttltimestamp + 20000, DELTA);
        }
    }

    /**
     * 1. create a row with columns with ttls, 5x100 and 1x1000
     * 2. flush, verify (maxLocalDeletionTime = time+1000)
     * 3. delete column with ttl=1000
     * 4. flush, verify the new sstable (maxLocalDeletionTime = ~now)
     * 5. compact
     * 6. verify resulting sstable has maxLocalDeletionTime = time + 100.
     */
    @Test
    public void testWithDeletes()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard2");
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < 5; i++)
            new RowUpdateBuilder(store.metadata(), timestamp, 100, "deletetest")
                .clustering("deletecolumn" + i)
                .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build()
                .applyUnsafe();


        new RowUpdateBuilder(store.metadata(), timestamp, 1000, "deletetest")
        .clustering("todelete")
        .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
        .build()
        .applyUnsafe();

        Util.flush(store);
        assertEquals(1, store.getLiveSSTables().size());
        int ttltimestamp = (int) (System.currentTimeMillis() / 1000);
        int firstMaxDelTime = 0;
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            firstMaxDelTime = sstable.getSSTableMetadata().maxLocalDeletionTime;
            assertEquals(ttltimestamp + 1000, firstMaxDelTime, DELTA);
        }

        RowUpdateBuilder.deleteRow(store.metadata(), timestamp + 1, "deletetest", "todelete").applyUnsafe();

        Util.flush(store);
        assertEquals(2, store.getLiveSSTables().size());
        boolean foundDelete = false;
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            if (sstable.getSSTableMetadata().maxLocalDeletionTime != firstMaxDelTime)
            {
                assertEquals(sstable.getSSTableMetadata().maxLocalDeletionTime, ttltimestamp, DELTA);
                foundDelete = true;
            }
        }
        assertTrue(foundDelete);
        Util.compact(store, store.getLiveSSTables());
        assertEquals(1, store.getLiveSSTables().size());
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            assertEquals(ttltimestamp + 100, sstable.getSSTableMetadata().maxLocalDeletionTime, DELTA);
        }
    }

    @Test
    public void trackMaxMinColNames() throws CharacterCodingException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard3");
        for (int j = 0; j < 8; j++)
        {
            String key = "row" + j;
            for (int i = 100; i < 150; i++)
            {
                new RowUpdateBuilder(store.metadata(), System.currentTimeMillis(), key)
                    .clustering(j + "col" + i)
                    .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();
            }
        }
        Util.flush(store);
        assertEquals(1, store.getLiveSSTables().size());
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            assertEquals(ByteBufferUtil.string(sstable.getSSTableMetadata().minClusteringValues.get(0)), "0col100");
            assertEquals(ByteBufferUtil.string(sstable.getSSTableMetadata().maxClusteringValues.get(0)), "7col149");
            // make sure the clustering values are minimised
            assertTrue(sstable.getSSTableMetadata().minClusteringValues.get(0).capacity() < 50);
            assertTrue(sstable.getSSTableMetadata().maxClusteringValues.get(0).capacity() < 50);
        }
        String key = "row2";

        for (int i = 101; i < 299; i++)
        {
            new RowUpdateBuilder(store.metadata(), System.currentTimeMillis(), key)
            .clustering(9 + "col" + i)
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }

        Util.flush(store);
        store.forceMajorCompaction();
        assertEquals(1, store.getLiveSSTables().size());
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            assertEquals(ByteBufferUtil.string(sstable.getSSTableMetadata().minClusteringValues.get(0)), "0col100");
            assertEquals(ByteBufferUtil.string(sstable.getSSTableMetadata().maxClusteringValues.get(0)), "9col298");
            // and make sure the clustering values are still minimised after compaction
            assertTrue(sstable.getSSTableMetadata().minClusteringValues.get(0).capacity() < 50);
            assertTrue(sstable.getSSTableMetadata().maxClusteringValues.get(0).capacity() < 50);
        }
    }

    /*@Test
    public void testLegacyCounterShardTracking()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Counter1");

        // A cell with all shards
        CounterContext.ContextState state = CounterContext.ContextState.allocate(1, 1, 1);
        state.writeGlobal(CounterId.fromInt(1), 1L, 1L);
        state.writeLocal(CounterId.fromInt(2), 1L, 1L);
        state.writeRemote(CounterId.fromInt(3), 1L, 1L);

        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterCell(cellname("col"), state.context, 1L, Long.MIN_VALUE));
        new Mutation(Util.dk("k").getKey(), cells).applyUnsafe();
        Util.flush(cfs);
        assertTrue(cfs.getLiveSSTables().iterator().next().getSSTableMetadata().hasLegacyCounterShards);
        cfs.truncateBlocking();

        // A cell with global and remote shards
        state = CounterContext.ContextState.allocate(0, 1, 1);
        state.writeLocal(CounterId.fromInt(2), 1L, 1L);
        state.writeRemote(CounterId.fromInt(3), 1L, 1L);
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterCell(cellname("col"), state.context, 1L, Long.MIN_VALUE));
        new Mutation(Util.dk("k").getKey(), cells).applyUnsafe();
        Util.flush(cfs);
        assertTrue(cfs.getLiveSSTables().iterator().next().getSSTableMetadata().hasLegacyCounterShards);
        cfs.truncateBlocking();

        // A cell with global and local shards
        state = CounterContext.ContextState.allocate(1, 1, 0);
        state.writeGlobal(CounterId.fromInt(1), 1L, 1L);
        state.writeLocal(CounterId.fromInt(2), 1L, 1L);
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterCell(cellname("col"), state.context, 1L, Long.MIN_VALUE));
        new Mutation(Util.dk("k").getKey(), cells).applyUnsafe();
        Util.flush(cfs);
        assertTrue(cfs.getLiveSSTables().iterator().next().getSSTableMetadata().hasLegacyCounterShards);
        cfs.truncateBlocking();

        // A cell with global only
        state = CounterContext.ContextState.allocate(1, 0, 0);
        state.writeGlobal(CounterId.fromInt(1), 1L, 1L);
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterCell(cellname("col"), state.context, 1L, Long.MIN_VALUE));
        new Mutation(Util.dk("k").getKey(), cells).applyUnsafe();
        Util.flush(cfs);
        assertFalse(cfs.getLiveSSTables().iterator().next().getSSTableMetadata().hasLegacyCounterShards);
        cfs.truncateBlocking();
    } */
}

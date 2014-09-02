/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.io.sstable;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.Util.cellname;

public class SSTableMetadataTest
{
    public static final String KEYSPACE1 = "SSTableMetadataTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_STANDARD2 = "Standard2";
    public static final String CF_STANDARD3 = "Standard3";
    public static final String CF_STANDARDCOMPOSITE2 = "StandardComposite2";
    public static final String CF_COUNTER1 = "Counter1";

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        AbstractType<?> compositeMaxMin = CompositeType.getInstance(Arrays.asList(new AbstractType<?>[]{BytesType.instance, IntegerType.instance}));
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD3),
                                    CFMetaData.denseCFMetaData(KEYSPACE1, CF_STANDARDCOMPOSITE2, compositeMaxMin),
                                    CFMetaData.denseCFMetaData(KEYSPACE1, CF_COUNTER1, BytesType.instance).defaultValidator(CounterColumnType.instance));
    }

    @Test
    public void testTrackMaxDeletionTime()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard1");
        long timestamp = System.currentTimeMillis();
        for(int i = 0; i < 10; i++)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            for (int j = 0; j < 10; j++)
                rm.add("Standard1", cellname(Integer.toString(j)),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       10 + j);
            rm.applyUnsafe();
        }
        Mutation rm = new Mutation(KEYSPACE1, Util.dk("longttl").getKey());
        rm.add("Standard1", cellname("col"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               10000);
        rm.applyUnsafe();
        store.forceBlockingFlush();
        assertEquals(1, store.getSSTables().size());
        int ttltimestamp = (int)(System.currentTimeMillis()/1000);
        int firstDelTime = 0;
        for(SSTableReader sstable : store.getSSTables())
        {
            firstDelTime = sstable.getSSTableMetadata().maxLocalDeletionTime;
            assertEquals(ttltimestamp + 10000, firstDelTime, 10);

        }
        rm = new Mutation(KEYSPACE1, Util.dk("longttl2").getKey());
        rm.add("Standard1", cellname("col"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               20000);
        rm.applyUnsafe();
        ttltimestamp = (int) (System.currentTimeMillis()/1000);
        store.forceBlockingFlush();
        assertEquals(2, store.getSSTables().size());
        List<SSTableReader> sstables = new ArrayList<>(store.getSSTables());
        if(sstables.get(0).getSSTableMetadata().maxLocalDeletionTime < sstables.get(1).getSSTableMetadata().maxLocalDeletionTime)
        {
            assertEquals(sstables.get(0).getSSTableMetadata().maxLocalDeletionTime, firstDelTime);
            assertEquals(sstables.get(1).getSSTableMetadata().maxLocalDeletionTime, ttltimestamp + 20000, 10);
        }
        else
        {
            assertEquals(sstables.get(1).getSSTableMetadata().maxLocalDeletionTime, firstDelTime);
            assertEquals(sstables.get(0).getSSTableMetadata().maxLocalDeletionTime, ttltimestamp + 20000, 10);
        }

        Util.compact(store, store.getSSTables());
        assertEquals(1, store.getSSTables().size());
        for(SSTableReader sstable : store.getSSTables())
        {
            assertEquals(sstable.getSSTableMetadata().maxLocalDeletionTime, ttltimestamp + 20000, 10);
        }
    }

    /**
     * 1. create a row with columns with ttls, 5x100 and 1x1000
     * 2. flush, verify (maxLocalDeletionTime = time+1000)
     * 3. delete column with ttl=1000
     * 4. flush, verify the new sstable (maxLocalDeletionTime = ~now)
     * 5. compact
     * 6. verify resulting sstable has maxLocalDeletionTime = time + 100.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testWithDeletes() throws ExecutionException, InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard2");
        long timestamp = System.currentTimeMillis();
        DecoratedKey key = Util.dk("deletetest");
        Mutation rm = new Mutation(KEYSPACE1, key.getKey());
        for (int i = 0; i<5; i++)
            rm.add("Standard2", cellname("deletecolumn" + i),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       100);
        rm.add("Standard2", cellname("todelete"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp,
                   1000);
        rm.applyUnsafe();
        store.forceBlockingFlush();
        assertEquals(1,store.getSSTables().size());
        int ttltimestamp = (int) (System.currentTimeMillis()/1000);
        int firstMaxDelTime = 0;
        for(SSTableReader sstable : store.getSSTables())
        {
            firstMaxDelTime = sstable.getSSTableMetadata().maxLocalDeletionTime;
            assertEquals(ttltimestamp + 1000, firstMaxDelTime, 10);
        }
        rm = new Mutation(KEYSPACE1, key.getKey());
        rm.delete("Standard2", cellname("todelete"), timestamp + 1);
        rm.applyUnsafe();
        store.forceBlockingFlush();
        assertEquals(2,store.getSSTables().size());
        boolean foundDelete = false;
        for(SSTableReader sstable : store.getSSTables())
        {
            if(sstable.getSSTableMetadata().maxLocalDeletionTime != firstMaxDelTime)
            {
                assertEquals(sstable.getSSTableMetadata().maxLocalDeletionTime, ttltimestamp, 10);
                foundDelete = true;
            }
        }
        assertTrue(foundDelete);
        Util.compact(store, store.getSSTables());
        assertEquals(1,store.getSSTables().size());
        for(SSTableReader sstable : store.getSSTables())
        {
            assertEquals(ttltimestamp + 100, sstable.getSSTableMetadata().maxLocalDeletionTime, 10);
        }
    }

    @Test
    public void trackMaxMinColNames() throws CharacterCodingException, ExecutionException, InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard3");
        store.getCompactionStrategy();
        for (int j = 0; j < 8; j++)
        {
            DecoratedKey key = Util.dk("row"+j);
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            for (int i = 100; i<150; i++)
            {
                rm.add("Standard3", cellname(j + "col" + i), ByteBufferUtil.EMPTY_BYTE_BUFFER, System.currentTimeMillis());
            }
            rm.applyUnsafe();
        }
        store.forceBlockingFlush();
        assertEquals(1, store.getSSTables().size());
        for (SSTableReader sstable : store.getSSTables())
        {
            assertEquals(ByteBufferUtil.string(sstable.getSSTableMetadata().minColumnNames.get(0)), "0col100");
            assertEquals(ByteBufferUtil.string(sstable.getSSTableMetadata().maxColumnNames.get(0)), "7col149");
        }
        DecoratedKey key = Util.dk("row2");
        Mutation rm = new Mutation(KEYSPACE1, key.getKey());
        for (int i = 101; i<299; i++)
        {
            rm.add("Standard3", cellname(9 + "col" + i), ByteBufferUtil.EMPTY_BYTE_BUFFER, System.currentTimeMillis());
        }
        rm.applyUnsafe();

        store.forceBlockingFlush();
        store.forceMajorCompaction();
        assertEquals(1, store.getSSTables().size());
        for (SSTableReader sstable : store.getSSTables())
        {
            assertEquals(ByteBufferUtil.string(sstable.getSSTableMetadata().minColumnNames.get(0)), "0col100");
            assertEquals(ByteBufferUtil.string(sstable.getSSTableMetadata().maxColumnNames.get(0)), "9col298");
        }
    }

    @Test
    public void testMaxMinComposites() throws CharacterCodingException, ExecutionException, InterruptedException
    {
        /*
        creates two sstables, columns like this:
        ---------------------
        k   |a0:9|a1:8|..|a9:0
        ---------------------
        and
        ---------------------
        k2  |b0:9|b1:8|..|b9:0
        ---------------------
        meaning max columns are b9 and 9, min is a0 and 0
         */
        Keyspace keyspace = Keyspace.open(KEYSPACE1);

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("StandardComposite2");

        CellNameType type = cfs.getComparator();

        ByteBuffer key = ByteBufferUtil.bytes("k");
        for (int i = 0; i < 10; i++)
        {
            Mutation rm = new Mutation(KEYSPACE1, key);
            CellName colName = type.makeCellName(ByteBufferUtil.bytes("a"+(9-i)), ByteBufferUtil.bytes(i));
            rm.add("StandardComposite2", colName, ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
            rm.applyUnsafe();
        }
        cfs.forceBlockingFlush();

        key = ByteBufferUtil.bytes("k2");
        for (int i = 0; i < 10; i++)
        {
            Mutation rm = new Mutation(KEYSPACE1, key);
            CellName colName = type.makeCellName(ByteBufferUtil.bytes("b"+(9-i)), ByteBufferUtil.bytes(i));
            rm.add("StandardComposite2", colName, ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
            rm.applyUnsafe();
        }
        cfs.forceBlockingFlush();
        cfs.forceMajorCompaction();
        assertEquals(cfs.getSSTables().size(), 1);
        for (SSTableReader sstable : cfs.getSSTables())
        {
            assertEquals("b9", ByteBufferUtil.string(sstable.getSSTableMetadata().maxColumnNames.get(0)));
            assertEquals(9, ByteBufferUtil.toInt(sstable.getSSTableMetadata().maxColumnNames.get(1)));
            assertEquals("a0", ByteBufferUtil.string(sstable.getSSTableMetadata().minColumnNames.get(0)));
            assertEquals(0, ByteBufferUtil.toInt(sstable.getSSTableMetadata().minColumnNames.get(1)));
        }
    }

    @Test
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
        cfs.forceBlockingFlush();
        assertTrue(cfs.getSSTables().iterator().next().getSSTableMetadata().hasLegacyCounterShards);
        cfs.truncateBlocking();

        // A cell with global and remote shards
        state = CounterContext.ContextState.allocate(0, 1, 1);
        state.writeLocal(CounterId.fromInt(2), 1L, 1L);
        state.writeRemote(CounterId.fromInt(3), 1L, 1L);
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterCell(cellname("col"), state.context, 1L, Long.MIN_VALUE));
        new Mutation(Util.dk("k").getKey(), cells).applyUnsafe();
        cfs.forceBlockingFlush();
        assertTrue(cfs.getSSTables().iterator().next().getSSTableMetadata().hasLegacyCounterShards);
        cfs.truncateBlocking();

        // A cell with global and local shards
        state = CounterContext.ContextState.allocate(1, 1, 0);
        state.writeGlobal(CounterId.fromInt(1), 1L, 1L);
        state.writeLocal(CounterId.fromInt(2), 1L, 1L);
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterCell(cellname("col"), state.context, 1L, Long.MIN_VALUE));
        new Mutation(Util.dk("k").getKey(), cells).applyUnsafe();
        cfs.forceBlockingFlush();
        assertTrue(cfs.getSSTables().iterator().next().getSSTableMetadata().hasLegacyCounterShards);
        cfs.truncateBlocking();

        // A cell with global only
        state = CounterContext.ContextState.allocate(1, 0, 0);
        state.writeGlobal(CounterId.fromInt(1), 1L, 1L);
        cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterCell(cellname("col"), state.context, 1L, Long.MIN_VALUE));
        new Mutation(Util.dk("k").getKey(), cells).applyUnsafe();
        cfs.forceBlockingFlush();
        assertFalse(cfs.getSSTables().iterator().next().getSSTableMetadata().hasLegacyCounterShards);
        cfs.truncateBlocking();
    }
}

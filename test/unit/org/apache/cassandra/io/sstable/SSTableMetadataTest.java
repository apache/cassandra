package org.apache.cassandra.io.sstable;
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


import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SSTableMetadataTest extends SchemaLoader
{
    @Test
    public void testTrackMaxDeletionTime() throws ExecutionException, InterruptedException
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard1");
        long timestamp = System.currentTimeMillis();
        for(int i = 0; i < 10; i++)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            RowMutation rm = new RowMutation("Keyspace1", key.key);
            for (int j = 0; j < 10; j++)
                rm.add("Standard1", ByteBufferUtil.bytes(Integer.toString(j)),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       10 + j);
            rm.apply();
        }
        RowMutation rm = new RowMutation("Keyspace1", Util.dk("longttl").key);
        rm.add("Standard1", ByteBufferUtil.bytes("col"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               10000);
        rm.apply();
        store.forceBlockingFlush();
        assertEquals(1, store.getSSTables().size());
        int ttltimestamp = (int)(System.currentTimeMillis()/1000);
        int firstDelTime = 0;
        for(SSTableReader sstable : store.getSSTables())
        {
            firstDelTime = sstable.getSSTableMetadata().maxLocalDeletionTime;
            assertEquals(ttltimestamp + 10000, firstDelTime, 10);

        }
        rm = new RowMutation("Keyspace1", Util.dk("longttl2").key);
        rm.add("Standard1", ByteBufferUtil.bytes("col"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               20000);
        rm.apply();
        ttltimestamp = (int) (System.currentTimeMillis()/1000);
        store.forceBlockingFlush();
        assertEquals(2, store.getSSTables().size());
        List<SSTableReader> sstables = new ArrayList<SSTableReader>(store.getSSTables());
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
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard2");
        long timestamp = System.currentTimeMillis();
        DecoratedKey key = Util.dk("deletetest");
        RowMutation rm = new RowMutation("Keyspace1", key.key);
        for (int i = 0; i<5; i++)
            rm.add("Standard2", ByteBufferUtil.bytes("deletecolumn"+i),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       100);
        rm.add("Standard2", ByteBufferUtil.bytes("todelete"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp,
                   1000);
        rm.apply();
        store.forceBlockingFlush();
        assertEquals(1,store.getSSTables().size());
        int ttltimestamp = (int) (System.currentTimeMillis()/1000);
        int firstMaxDelTime = 0;
        for(SSTableReader sstable : store.getSSTables())
        {
            firstMaxDelTime = sstable.getSSTableMetadata().maxLocalDeletionTime;
            assertEquals(ttltimestamp + 1000, firstMaxDelTime, 10);
        }
        rm = new RowMutation("Keyspace1", key.key);
        rm.delete("Standard2", ByteBufferUtil.bytes("todelete"), timestamp + 1);
        rm.apply();
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
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard3");
        store.getCompactionStrategy();
        for (int j = 0; j < 8; j++)
        {
            DecoratedKey key = Util.dk("row"+j);
            RowMutation rm = new RowMutation("Keyspace1", key.key);
            for (int i = 100; i<150; i++)
            {
                rm.add("Standard3", ByteBufferUtil.bytes(j+"col"+i),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                   System.currentTimeMillis());
            }
            rm.apply();
        }
        store.forceBlockingFlush();
        assertEquals(1, store.getSSTables().size());
        for (SSTableReader sstable : store.getSSTables())
        {
            assertEquals(ByteBufferUtil.string(sstable.getSSTableMetadata().minColumnNames.get(0)), "0col100");
            assertEquals(ByteBufferUtil.string(sstable.getSSTableMetadata().maxColumnNames.get(0)), "7col149");
        }
        DecoratedKey key = Util.dk("row2");
        RowMutation rm = new RowMutation("Keyspace1", key.key);
        for (int i = 101; i<299; i++)
        {
            rm.add("Standard3", ByteBufferUtil.bytes(9+"col"+i),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               System.currentTimeMillis());
        }
        rm.apply();

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
        Keyspace keyspace = Keyspace.open("Keyspace1");

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("StandardComposite2");

        CompositeType ct = CompositeType.getInstance(BytesType.instance, IntegerType.instance);

        ByteBuffer key = ByteBufferUtil.bytes("k");
        for (int i = 0; i < 10; i++)
        {
            RowMutation rm = new RowMutation("Keyspace1", key);
            ByteBuffer colName = ct.builder().add(ByteBufferUtil.bytes("a"+(9-i))).add(ByteBufferUtil.bytes(i)).build();
            rm.add("StandardComposite2", colName, ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
            rm.apply();
        }
        cfs.forceBlockingFlush();

        key = ByteBufferUtil.bytes("k2");
        for (int i = 0; i < 10; i++)
        {
            RowMutation rm = new RowMutation("Keyspace1", key);
            ByteBuffer colName = ct.builder().add(ByteBufferUtil.bytes("b"+(9-i))).add(ByteBufferUtil.bytes(i)).build();
            rm.add("StandardComposite2", colName, ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
            rm.apply();
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
}

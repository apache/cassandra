package org.apache.cassandra.db.compaction;
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



import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.tools.SSTableExpiredBlockers;
import org.apache.cassandra.utils.ByteBufferUtil;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class TTLExpiryTest extends SchemaLoader
{
    @Test
    public void testSimpleExpire() throws ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = Keyspace.open("Keyspace1").getColumnFamilyStore("Standard1");
        cfs.disableAutoCompaction();
        cfs.metadata.gcGraceSeconds(0);
        long timestamp = System.currentTimeMillis();
        RowMutation rm = new RowMutation("Keyspace1", Util.dk("ttl").key);
        rm.add("Standard1", ByteBufferUtil.bytes("col"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);
        rm.add("Standard1", ByteBufferUtil.bytes("col7"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);

        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation("Keyspace1", Util.dk("ttl").key);
                rm.add("Standard1", ByteBufferUtil.bytes("col2"),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       1);
                rm.apply();
        cfs.forceBlockingFlush();
        rm = new RowMutation("Keyspace1", Util.dk("ttl").key);
        rm.add("Standard1", ByteBufferUtil.bytes("col3"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp,
                   1);
        rm.apply();
        cfs.forceBlockingFlush();
        rm = new RowMutation("Keyspace1", Util.dk("ttl").key);
        rm.add("Standard1", ByteBufferUtil.bytes("col311"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp,
                   1);
        rm.apply();

        cfs.forceBlockingFlush();
        Thread.sleep(2000); // wait for ttl to expire
        assertEquals(4, cfs.getSSTables().size());
        cfs.enableAutoCompaction(true);
        assertEquals(0, cfs.getSSTables().size());
    }

    @Test
    public void testNoExpire() throws ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = Keyspace.open("Keyspace1").getColumnFamilyStore("Standard1");
        cfs.disableAutoCompaction();
        cfs.metadata.gcGraceSeconds(0);
        long timestamp = System.currentTimeMillis();
        RowMutation rm = new RowMutation("Keyspace1", Util.dk("ttl").key);
        rm.add("Standard1", ByteBufferUtil.bytes("col"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);
        rm.add("Standard1", ByteBufferUtil.bytes("col7"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);

        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation("Keyspace1", Util.dk("ttl").key);
                rm.add("Standard1", ByteBufferUtil.bytes("col2"),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       1);
                rm.apply();
        cfs.forceBlockingFlush();
        rm = new RowMutation("Keyspace1", Util.dk("ttl").key);
        rm.add("Standard1", ByteBufferUtil.bytes("col3"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp,
                   1);
        rm.apply();
        cfs.forceBlockingFlush();
        DecoratedKey noTTLKey = Util.dk("nottl");
        rm = new RowMutation("Keyspace1", noTTLKey.key);
        rm.add("Standard1", ByteBufferUtil.bytes("col311"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp);
        rm.apply();
        cfs.forceBlockingFlush();
        Thread.sleep(2000); // wait for ttl to expire
        assertEquals(4, cfs.getSSTables().size());
        cfs.enableAutoCompaction(true);
        assertEquals(1, cfs.getSSTables().size());
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        SSTableScanner scanner = sstable.getScanner(DataRange.allData(sstable.partitioner));
        assertTrue(scanner.hasNext());
        while(scanner.hasNext())
        {
            OnDiskAtomIterator iter = scanner.next();
            assertEquals(noTTLKey, iter.getKey());
        }
    }

    @Test
    public void testAggressiveFullyExpired()
    {
        String KEYSPACE1 = "Keyspace1";
        ColumnFamilyStore cfs = Keyspace.open("Keyspace1").getColumnFamilyStore("Standard1");
        cfs.disableAutoCompaction();
        cfs.metadata.gcGraceSeconds(0);

        DecoratedKey ttlKey = Util.dk("ttl");
        RowMutation rm = new RowMutation("Keyspace1", ttlKey.key);
        rm.add("Standard1", ByteBufferUtil.bytes("col1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 1, 1);
        rm.add("Standard1", ByteBufferUtil.bytes("col2"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 3, 1);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KEYSPACE1, ttlKey.key);
        rm.add("Standard1", ByteBufferUtil.bytes("col1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 2, 1);
        rm.add("Standard1", ByteBufferUtil.bytes("col2"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 5, 1);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KEYSPACE1, ttlKey.key);
        rm.add("Standard1", ByteBufferUtil.bytes("col1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 4, 1);
        rm.add("Standard1", ByteBufferUtil.bytes("shadow"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 7, 1);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KEYSPACE1, ttlKey.key);
        rm.add("Standard1", ByteBufferUtil.bytes("shadow"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 6, 3);
        rm.add("Standard1", ByteBufferUtil.bytes("col2"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 8, 1);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        Set<SSTableReader> sstables = Sets.newHashSet(cfs.getSSTables());
        int now = (int)(System.currentTimeMillis() / 1000);
        int gcBefore = now + 2;
        Set<SSTableReader> expired = CompactionController.getFullyExpiredSSTables(
                cfs,
                sstables,
                Collections.EMPTY_SET,
                gcBefore);
        assertEquals(2, expired.size());

        cfs.clearUnsafe();
    }

    @Test
    public void testCheckForExpiredSSTableBlockers() throws InterruptedException
    {
        String KEYSPACE1 = "Keyspace1";
        ColumnFamilyStore cfs = Keyspace.open("Keyspace1").getColumnFamilyStore("Standard1");
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        cfs.metadata.gcGraceSeconds(0);

        RowMutation rm = new RowMutation(KEYSPACE1, Util.dk("test").key);
        rm.add("Standard1", ByteBufferUtil.bytes("col1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, System.currentTimeMillis());
        rm.applyUnsafe();
        cfs.forceBlockingFlush();
        SSTableReader blockingSSTable = cfs.getSSTables().iterator().next();
        for (int i = 0; i < 10; i++)
        {
            rm = new RowMutation(KEYSPACE1, Util.dk("test").key);
            rm.delete("Standard1", System.currentTimeMillis());
            rm.applyUnsafe();
            cfs.forceBlockingFlush();
        }
        Multimap<SSTableReader, SSTableReader> blockers = SSTableExpiredBlockers.checkForExpiredSSTableBlockers(cfs.getSSTables(), (int) (System.currentTimeMillis() / 1000) + 100);
        assertEquals(1, blockers.keySet().size());
        assertTrue(blockers.keySet().contains(blockingSSTable));
        assertEquals(10, blockers.get(blockingSSTable).size());
    }

}

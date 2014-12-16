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

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.BeforeClass;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class TTLExpiryTest
{
    public static final String KEYSPACE1 = "TTLExpiryTest";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testAggressiveFullyExpired()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1");
        cfs.disableAutoCompaction();
        cfs.metadata.gcGraceSeconds(0);

        DecoratedKey ttlKey = Util.dk("ttl");
        Mutation rm = new Mutation(KEYSPACE1, ttlKey.getKey());
        rm.add("Standard1", Util.cellname("col1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 1, 1);
        rm.add("Standard1", Util.cellname("col2"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 3, 1);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        rm = new Mutation(KEYSPACE1, ttlKey.getKey());
        rm.add("Standard1", Util.cellname("col1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 2, 1);
        rm.add("Standard1", Util.cellname("col2"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 5, 1);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        rm = new Mutation(KEYSPACE1, ttlKey.getKey());
        rm.add("Standard1", Util.cellname("col1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 4, 1);
        rm.add("Standard1", Util.cellname("shadow"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 7, 1);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        rm = new Mutation(KEYSPACE1, ttlKey.getKey());
        rm.add("Standard1", Util.cellname("shadow"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 6, 3);
        rm.add("Standard1", Util.cellname("col2"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 8, 1);
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
    public void testSimpleExpire() throws InterruptedException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1");
        cfs.disableAutoCompaction();
        cfs.metadata.gcGraceSeconds(0);
        long timestamp = System.currentTimeMillis();
        Mutation rm = new Mutation(KEYSPACE1, Util.dk("ttl").getKey());
        rm.add("Standard1", Util.cellname("col"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);
        rm.add("Standard1", Util.cellname("col7"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);

        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        rm = new Mutation(KEYSPACE1, Util.dk("ttl").getKey());
                rm.add("Standard1", Util.cellname("col2"),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       1);
                rm.applyUnsafe();
        cfs.forceBlockingFlush();
        rm = new Mutation(KEYSPACE1, Util.dk("ttl").getKey());
        rm.add("Standard1", Util.cellname("col3"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp,
                   1);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();
        rm = new Mutation(KEYSPACE1, Util.dk("ttl").getKey());
        rm.add("Standard1", Util.cellname("col311"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp,
                   1);
        rm.applyUnsafe();

        cfs.forceBlockingFlush();
        Thread.sleep(2000); // wait for ttl to expire
        assertEquals(4, cfs.getSSTables().size());
        cfs.enableAutoCompaction(true);
        assertEquals(0, cfs.getSSTables().size());
    }

    @Test
    public void testNoExpire() throws InterruptedException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1");
        cfs.disableAutoCompaction();
        cfs.metadata.gcGraceSeconds(0);
        long timestamp = System.currentTimeMillis();
        Mutation rm = new Mutation(KEYSPACE1, Util.dk("ttl").getKey());
        rm.add("Standard1", Util.cellname("col"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);
        rm.add("Standard1", Util.cellname("col7"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);

        rm.applyUnsafe();
        cfs.forceBlockingFlush();

        rm = new Mutation(KEYSPACE1, Util.dk("ttl").getKey());
                rm.add("Standard1", Util.cellname("col2"),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       1);
                rm.applyUnsafe();
        cfs.forceBlockingFlush();
        rm = new Mutation(KEYSPACE1, Util.dk("ttl").getKey());
        rm.add("Standard1", Util.cellname("col3"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp,
                   1);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();
        DecoratedKey noTTLKey = Util.dk("nottl");
        rm = new Mutation(KEYSPACE1, noTTLKey.getKey());
        rm.add("Standard1", Util.cellname("col311"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp);
        rm.applyUnsafe();
        cfs.forceBlockingFlush();
        Thread.sleep(2000); // wait for ttl to expire
        assertEquals(4, cfs.getSSTables().size());
        cfs.enableAutoCompaction(true);
        assertEquals(1, cfs.getSSTables().size());
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        ISSTableScanner scanner = sstable.getScanner(DataRange.allData(sstable.partitioner));
        assertTrue(scanner.hasNext());
        while(scanner.hasNext())
        {
            OnDiskAtomIterator iter = scanner.next();
            assertEquals(noTTLKey, iter.getKey());
        }
    }
}

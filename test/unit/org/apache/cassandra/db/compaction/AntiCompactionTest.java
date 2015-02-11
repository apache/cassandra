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
package org.apache.cassandra.db.compaction;

import junit.framework.Assert;
import org.apache.cassandra.utils.concurrent.Refs;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.locator.SimpleStrategy;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.google.common.collect.Iterables;

public class AntiCompactionTest
{
    private static final String KEYSPACE1 = "AntiCompactionTest";
    private static final String CF = "Standard1";


    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.standardCFMD(KEYSPACE1, CF));
    }

    @After
    public void truncateCF()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.truncateBlocking();
    }

    @Test
    public void antiCompactOne() throws Exception
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        Collection<SSTableReader> sstables = store.getUnrepairedSSTables();
        assertEquals(store.getSSTables().size(), sstables.size());
        Range<Token> range = new Range<Token>(new BytesToken("0".getBytes()), new BytesToken("4".getBytes()));
        List<Range<Token>> ranges = Arrays.asList(range);

        Refs<SSTableReader> refs = Refs.ref(sstables);
        long repairedAt = 1000;
        CompactionManager.instance.performAnticompaction(store, ranges, refs, repairedAt);

        assertEquals(2, store.getSSTables().size());
        int repairedKeys = 0;
        int nonRepairedKeys = 0;
        for (SSTableReader sstable : store.getSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
                    if (sstable.isRepaired())
                    {
                        assertTrue(range.contains(row.getKey().getToken()));
                        repairedKeys++;
                    }
                    else
                    {
                        assertFalse(range.contains(row.getKey().getToken()));
                        nonRepairedKeys++;
                    }
                }
            }
        }
        for (SSTableReader sstable : store.getSSTables())
        {
            assertFalse(sstable.isMarkedCompacted());
            assertEquals(1, sstable.selfRef().globalCount());
        }
        assertEquals(0, store.getDataTracker().getCompacting().size());
        assertEquals(repairedKeys, 4);
        assertEquals(nonRepairedKeys, 6);
    }

    @Test
    public void antiCompactionSizeTest() throws InterruptedException, IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.disableAutoCompaction();
        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        long origSize = s.bytesOnDisk();
        Range<Token> range = new Range<Token>(new BytesToken(ByteBufferUtil.bytes(0)), new BytesToken(ByteBufferUtil.bytes(500)));
        Collection<SSTableReader> sstables = cfs.getSSTables();
        CompactionManager.instance.performAnticompaction(cfs, Arrays.asList(range), Refs.tryRef(sstables), 12345);
        long sum = 0;
        for (SSTableReader x : cfs.getSSTables())
            sum += x.bytesOnDisk();
        assertEquals(sum, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(origSize, cfs.metric.liveDiskSpaceUsed.getCount(), 100000);

    }

    private SSTableReader writeFile(ColumnFamilyStore cfs, int count)
    {
        ArrayBackedSortedColumns cf = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        for (int i = 0; i < count; i++)
            cf.addColumn(Util.column(String.valueOf(i), "a", 1));
        File dir = cfs.directories.getDirectoryForNewSSTables();
        String filename = cfs.getTempSSTablePath(dir);

        SSTableWriter writer = SSTableWriter.create(filename, 0, 0);

        for (int i = 0; i < count * 5; i++)
            writer.append(StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(i)), cf);
        return writer.closeAndOpenReader();
    }

    public void generateSStable(ColumnFamilyStore store, String Suffix)
    {
    long timestamp = System.currentTimeMillis();
    for (int i = 0; i < 10; i++)
        {
            DecoratedKey key = Util.dk(Integer.toString(i) + "-" + Suffix);
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            for (int j = 0; j < 10; j++)
                rm.add("Standard1", Util.cellname(Integer.toString(j)),
                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                        timestamp,
                        0);
            rm.apply();
        }
        store.forceBlockingFlush();
    }

    @Test
    public void antiCompactTenSTC() throws InterruptedException, IOException{
        antiCompactTen("SizeTieredCompactionStrategy");
    }

    @Test
    public void antiCompactTenLC() throws InterruptedException, IOException{
        antiCompactTen("LeveledCompactionStrategy");
    }

    public void antiCompactTen(String compactionStrategy) throws InterruptedException, IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.setCompactionStrategyClass(compactionStrategy);
        store.disableAutoCompaction();

        for (int table = 0; table < 10; table++)
        {
            generateSStable(store,Integer.toString(table));
        }
        Collection<SSTableReader> sstables = store.getUnrepairedSSTables();
        assertEquals(store.getSSTables().size(), sstables.size());

        Range<Token> range = new Range<Token>(new BytesToken("0".getBytes()), new BytesToken("4".getBytes()));
        List<Range<Token>> ranges = Arrays.asList(range);

        Refs<SSTableReader> refs = Refs.tryRef(sstables);
        Assert.assertNotNull(refs);
        long repairedAt = 1000;
        CompactionManager.instance.performAnticompaction(store, ranges, refs, repairedAt);
        /*
        Anticompaction will be anti-compacting 10 SSTables but will be doing this two at a time
        so there will be no net change in the number of sstables
         */
        assertEquals(10, store.getSSTables().size());
        int repairedKeys = 0;
        int nonRepairedKeys = 0;
        for (SSTableReader sstable : store.getSSTables())
        {
            ISSTableScanner scanner = sstable.getScanner();
            while (scanner.hasNext())
            {
                SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
                if (sstable.isRepaired())
                {
                    assertTrue(range.contains(row.getKey().getToken()));
                    assertEquals(repairedAt, sstable.getSSTableMetadata().repairedAt);
                    repairedKeys++;
                }
                else
                {
                    assertFalse(range.contains(row.getKey().getToken()));
                    assertEquals(ActiveRepairService.UNREPAIRED_SSTABLE, sstable.getSSTableMetadata().repairedAt);
                    nonRepairedKeys++;
                }
            }
        }
        assertEquals(repairedKeys, 40);
        assertEquals(nonRepairedKeys, 60);
    }

    @Test
    public void shouldMutateRepairedAt() throws InterruptedException, IOException
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        Collection<SSTableReader> sstables = store.getUnrepairedSSTables();
        assertEquals(store.getSSTables().size(), sstables.size());
        Range<Token> range = new Range<Token>(new BytesToken("0".getBytes()), new BytesToken("9999".getBytes()));
        List<Range<Token>> ranges = Arrays.asList(range);

        CompactionManager.instance.performAnticompaction(store, ranges, Refs.tryRef(sstables), 1);

        assertThat(store.getSSTables().size(), is(1));
        assertThat(Iterables.get(store.getSSTables(), 0).isRepaired(), is(true));
        assertThat(Iterables.get(store.getSSTables(), 0).selfRef().globalCount(), is(1));
        assertThat(store.getDataTracker().getCompacting().size(), is(0));
    }


    @Test
    public void shouldSkipAntiCompactionForNonIntersectingRange() throws InterruptedException, IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.disableAutoCompaction();

        for (int table = 0; table < 10; table++)
        {
            generateSStable(store,Integer.toString(table));
        }
        Collection<SSTableReader> sstables = store.getUnrepairedSSTables();
        assertEquals(store.getSSTables().size(), sstables.size());
        
        Range<Token> range = new Range<Token>(new BytesToken("-10".getBytes()), new BytesToken("-1".getBytes()));
        List<Range<Token>> ranges = Arrays.asList(range);

        Refs<SSTableReader> refs = Refs.ref(sstables);
        CompactionManager.instance.performAnticompaction(store, ranges, refs, 0);

        assertThat(store.getSSTables().size(), is(10));
        assertThat(Iterables.get(store.getSSTables(), 0).isRepaired(), is(false));
    }

    private ColumnFamilyStore prepareColumnFamilyStore()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.disableAutoCompaction();
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < 10; i++)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            for (int j = 0; j < 10; j++)
                rm.add("Standard1", Util.cellname(Integer.toString(j)),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       0);
            rm.apply();
        }
        store.forceBlockingFlush();
        return store;
    }

    @After
    public void truncateCfs()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.truncateBlocking();
    }
}

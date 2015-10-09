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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.UpdateBuilder;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class AntiCompactionTest
{
    private static final String KEYSPACE1 = "AntiCompactionTest";
    private static final String CF = "AntiCompactionTest";
    private static CFMetaData cfm;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        cfm = SchemaLoader.standardCFMD(KEYSPACE1, CF);
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    cfm);
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
        Collection<SSTableReader> sstables = getUnrepairedSSTables(store);
        assertEquals(store.getLiveSSTables().size(), sstables.size());
        Range<Token> range = new Range<Token>(new BytesToken("0".getBytes()), new BytesToken("4".getBytes()));
        List<Range<Token>> ranges = Arrays.asList(range);

        int repairedKeys = 0;
        int nonRepairedKeys = 0;
        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            if (txn == null)
                throw new IllegalStateException();
            long repairedAt = 1000;
            CompactionManager.instance.performAnticompaction(store, ranges, refs, txn, repairedAt);
        }

        assertEquals(2, store.getLiveSSTables().size());
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner((RateLimiter) null))
            {
                while (scanner.hasNext())
                {
                    UnfilteredRowIterator row = scanner.next();
                    if (sstable.isRepaired())
                    {
                        assertTrue(range.contains(row.partitionKey().getToken()));
                        repairedKeys++;
                    }
                    else
                    {
                        assertFalse(range.contains(row.partitionKey().getToken()));
                        nonRepairedKeys++;
                    }
                }
            }
        }
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            assertFalse(sstable.isMarkedCompacted());
            assertEquals(1, sstable.selfRef().globalCount());
        }
        assertEquals(0, store.getTracker().getCompacting().size());
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
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(cfs, Arrays.asList(range), refs, txn, 12345);
        }
        long sum = 0;
        long rows = 0;
        for (SSTableReader x : cfs.getLiveSSTables())
        {
            sum += x.bytesOnDisk();
            rows += x.getTotalRows();
        }
        assertEquals(sum, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(rows, 1000 * (1000 * 5));//See writeFile for how this number is derived
        assertEquals(origSize, cfs.metric.liveDiskSpaceUsed.getCount(), 16000000);
    }

    private SSTableReader writeFile(ColumnFamilyStore cfs, int count)
    {
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        String filename = cfs.getSSTablePath(dir);

        try (SSTableTxnWriter writer = SSTableTxnWriter.create(cfs, filename, 0, 0, new SerializationHeader(true, cfm, cfm.partitionColumns(), EncodingStats.NO_STATS)))
        {
            for (int i = 0; i < count; i++)
            {
                UpdateBuilder builder = UpdateBuilder.create(cfm, ByteBufferUtil.bytes(i));
                for (int j = 0; j < count * 5; j++)
                    builder.newRow("c" + j).add("val", "value1");
                writer.append(builder.build().unfilteredIterator());

            }
            Collection<SSTableReader> sstables = writer.finish(true);
            assertNotNull(sstables);
            assertEquals(1, sstables.size());
            return sstables.iterator().next();
        }
    }

    public void generateSStable(ColumnFamilyStore store, String Suffix)
    {
        for (int i = 0; i < 10; i++)
        {
            String localSuffix = Integer.toString(i);
            new RowUpdateBuilder(cfm, System.currentTimeMillis(), localSuffix + "-" + Suffix)
                    .clustering("c")
                    .add("val", "val" + localSuffix)
                    .build()
                    .applyUnsafe();
        }
        store.forceBlockingFlush();
    }

    @Test
    public void antiCompactTenSTC() throws InterruptedException, IOException
    {
        antiCompactTen("SizeTieredCompactionStrategy");
    }

    @Test
    public void antiCompactTenLC() throws InterruptedException, IOException
    {
        antiCompactTen("LeveledCompactionStrategy");
    }

    public void antiCompactTen(String compactionStrategy) throws InterruptedException, IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.disableAutoCompaction();

        for (int table = 0; table < 10; table++)
        {
            generateSStable(store,Integer.toString(table));
        }
        Collection<SSTableReader> sstables = getUnrepairedSSTables(store);
        assertEquals(store.getLiveSSTables().size(), sstables.size());

        Range<Token> range = new Range<Token>(new BytesToken("0".getBytes()), new BytesToken("4".getBytes()));
        List<Range<Token>> ranges = Arrays.asList(range);

        long repairedAt = 1000;
        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(store, ranges, refs, txn, repairedAt);
        }
        /*
        Anticompaction will be anti-compacting 10 SSTables but will be doing this two at a time
        so there will be no net change in the number of sstables
         */
        assertEquals(10, store.getLiveSSTables().size());
        int repairedKeys = 0;
        int nonRepairedKeys = 0;
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner((RateLimiter) null))
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator row = scanner.next())
                    {
                        if (sstable.isRepaired())
                        {
                            assertTrue(range.contains(row.partitionKey().getToken()));
                            assertEquals(repairedAt, sstable.getSSTableMetadata().repairedAt);
                            repairedKeys++;
                        }
                        else
                        {
                            assertFalse(range.contains(row.partitionKey().getToken()));
                            assertEquals(ActiveRepairService.UNREPAIRED_SSTABLE, sstable.getSSTableMetadata().repairedAt);
                            nonRepairedKeys++;
                        }
                    }
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
        Collection<SSTableReader> sstables = getUnrepairedSSTables(store);
        assertEquals(store.getLiveSSTables().size(), sstables.size());
        Range<Token> range = new Range<Token>(new BytesToken("0".getBytes()), new BytesToken("9999".getBytes()));
        List<Range<Token>> ranges = Arrays.asList(range);

        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(store, ranges, refs, txn, 1);
        }

        assertThat(store.getLiveSSTables().size(), is(1));
        assertThat(Iterables.get(store.getLiveSSTables(), 0).isRepaired(), is(true));
        assertThat(Iterables.get(store.getLiveSSTables(), 0).selfRef().globalCount(), is(1));
        assertThat(store.getTracker().getCompacting().size(), is(0));
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
        Collection<SSTableReader> sstables = getUnrepairedSSTables(store);
        assertEquals(store.getLiveSSTables().size(), sstables.size());

        Range<Token> range = new Range<Token>(new BytesToken("-1".getBytes()), new BytesToken("-10".getBytes()));
        List<Range<Token>> ranges = Arrays.asList(range);


        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(store, ranges, refs, txn, 1);
        }

        assertThat(store.getLiveSSTables().size(), is(10));
        assertThat(Iterables.get(store.getLiveSSTables(), 0).isRepaired(), is(false));
    }

    private ColumnFamilyStore prepareColumnFamilyStore()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.disableAutoCompaction();
        for (int i = 0; i < 10; i++)
        {
            new RowUpdateBuilder(cfm, System.currentTimeMillis(), Integer.toString(i))
                .clustering("c")
                .add("val", "val")
                .build()
                .applyUnsafe();
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

    private static Set<SSTableReader> getUnrepairedSSTables(ColumnFamilyStore cfs)
    {
        return ImmutableSet.copyOf(cfs.getTracker().getView().sstables(SSTableSet.LIVE, (s) -> !s.isRepaired()));
    }


}

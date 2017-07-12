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
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
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
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;


public class AntiCompactionTest
{
    private static final String KEYSPACE1 = "AntiCompactionTest";
    private static final String CF = "AntiCompactionTest";
    private static TableMetadata metadata;
    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        metadata = SchemaLoader.standardCFMD(KEYSPACE1, CF).build();
        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1), metadata);
        cfs = Schema.instance.getColumnFamilyStoreInstance(metadata.id);
    }

    @After
    public void truncateCF()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.truncateBlocking();
    }

    private void registerParentRepairSession(UUID sessionID, Collection<Range<Token>> ranges, long repairedAt, UUID pendingRepair) throws IOException
    {
        ActiveRepairService.instance.registerParentRepairSession(sessionID,
                                                                 InetAddress.getByName("10.0.0.1"),
                                                                 Lists.newArrayList(cfs), ranges,
                                                                 pendingRepair != null || repairedAt != UNREPAIRED_SSTABLE,
                                                                 repairedAt, true, PreviewKind.NONE);
    }

    private void antiCompactOne(long repairedAt, UUID pendingRepair) throws Exception
    {
        assert repairedAt != UNREPAIRED_SSTABLE || pendingRepair != null;

        ColumnFamilyStore store = prepareColumnFamilyStore();
        Collection<SSTableReader> sstables = getUnrepairedSSTables(store);
        assertEquals(store.getLiveSSTables().size(), sstables.size());
        Range<Token> range = new Range<Token>(new BytesToken("0".getBytes()), new BytesToken("4".getBytes()));
        List<Range<Token>> ranges = Arrays.asList(range);

        int repairedKeys = 0;
        int pendingKeys = 0;
        int nonRepairedKeys = 0;
        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            if (txn == null)
                throw new IllegalStateException();
            UUID parentRepairSession = pendingRepair == null ? UUID.randomUUID() : pendingRepair;
            registerParentRepairSession(parentRepairSession, ranges, repairedAt, pendingRepair);
            CompactionManager.instance.performAnticompaction(store, ranges, refs, txn, repairedAt, pendingRepair, parentRepairSession);
        }

        assertEquals(2, store.getLiveSSTables().size());
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    UnfilteredRowIterator row = scanner.next();
                    if (sstable.isRepaired() || sstable.isPendingRepair())
                    {
                        assertTrue(range.contains(row.partitionKey().getToken()));
                        repairedKeys += sstable.isRepaired() ? 1 : 0;
                        pendingKeys += sstable.isPendingRepair() ? 1 : 0;
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
        assertEquals(repairedKeys, repairedAt != UNREPAIRED_SSTABLE ? 4 : 0);
        assertEquals(pendingKeys, pendingRepair != NO_PENDING_REPAIR ? 4 : 0);
        assertEquals(nonRepairedKeys, 6);
    }

    @Test
    public void antiCompactOneRepairedAt() throws Exception
    {
        antiCompactOne(1000, NO_PENDING_REPAIR);
    }

    @Test
    public void antiCompactOnePendingRepair() throws Exception
    {
        antiCompactOne(UNREPAIRED_SSTABLE, UUIDGen.getTimeUUID());
    }

    @Ignore
    @Test
    public void antiCompactionSizeTest() throws InterruptedException, IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.disableAutoCompaction();
        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Range<Token> range = new Range<Token>(new BytesToken(ByteBufferUtil.bytes(0)), new BytesToken(ByteBufferUtil.bytes(500)));
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        UUID parentRepairSession = UUID.randomUUID();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(cfs, Arrays.asList(range), refs, txn, 12345, NO_PENDING_REPAIR, parentRepairSession);
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
    }

    private SSTableReader writeFile(ColumnFamilyStore cfs, int count)
    {
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        Descriptor desc = cfs.newSSTableDescriptor(dir);

        try (SSTableTxnWriter writer = SSTableTxnWriter.create(cfs, desc, 0, 0, NO_PENDING_REPAIR, new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS)))
        {
            for (int i = 0; i < count; i++)
            {
                UpdateBuilder builder = UpdateBuilder.create(metadata, ByteBufferUtil.bytes(i));
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
            new RowUpdateBuilder(metadata, System.currentTimeMillis(), localSuffix + "-" + Suffix)
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
        UUID parentRepairSession = UUID.randomUUID();
        registerParentRepairSession(parentRepairSession, ranges, repairedAt, null);
        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(store, ranges, refs, txn, repairedAt, NO_PENDING_REPAIR, parentRepairSession);
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
            try (ISSTableScanner scanner = sstable.getScanner())
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

    private void shouldMutate(long repairedAt, UUID pendingRepair) throws InterruptedException, IOException
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        Collection<SSTableReader> sstables = getUnrepairedSSTables(store);
        assertEquals(store.getLiveSSTables().size(), sstables.size());
        Range<Token> range = new Range<Token>(new BytesToken("0".getBytes()), new BytesToken("9999".getBytes()));
        List<Range<Token>> ranges = Arrays.asList(range);
        UUID parentRepairSession = pendingRepair == null ? UUID.randomUUID() : pendingRepair;
        registerParentRepairSession(parentRepairSession, ranges, repairedAt, pendingRepair);

        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(store, ranges, refs, txn, repairedAt, pendingRepair, parentRepairSession);
        }

        assertThat(store.getLiveSSTables().size(), is(1));
        assertThat(Iterables.get(store.getLiveSSTables(), 0).isRepaired(), is(repairedAt != UNREPAIRED_SSTABLE));
        assertThat(Iterables.get(store.getLiveSSTables(), 0).isPendingRepair(), is(pendingRepair != NO_PENDING_REPAIR));
        assertThat(Iterables.get(store.getLiveSSTables(), 0).selfRef().globalCount(), is(1));
        assertThat(store.getTracker().getCompacting().size(), is(0));
    }

    @Test
    public void shouldMutateRepairedAt() throws InterruptedException, IOException
    {
        shouldMutate(1, NO_PENDING_REPAIR);
    }

    @Test
    public void shouldMutatePendingRepair() throws InterruptedException, IOException
    {
        shouldMutate(UNREPAIRED_SSTABLE, UUIDGen.getTimeUUID());
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
        UUID parentRepairSession = UUID.randomUUID();
        registerParentRepairSession(parentRepairSession, ranges, UNREPAIRED_SSTABLE, null);

        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            CompactionManager.instance.performAnticompaction(store, ranges, refs, txn, 1, NO_PENDING_REPAIR, parentRepairSession);
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
            new RowUpdateBuilder(metadata, System.currentTimeMillis(), Integer.toString(i))
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

    /**
     * If the parent repair session is missing, we should still clean up
     */
    @Test
    public void missingParentRepairSession() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.disableAutoCompaction();

        for (int table = 0; table < 10; table++)
        {
            generateSStable(store,Integer.toString(table));
        }
        Collection<SSTableReader> sstables = getUnrepairedSSTables(store);
        assertEquals(10, sstables.size());

        Range<Token> range = new Range<Token>(new BytesToken("-1".getBytes()), new BytesToken("-10".getBytes()));
        List<Range<Token>> ranges = Arrays.asList(range);

        UUID missingRepairSession = UUIDGen.getTimeUUID();
        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {
            Assert.assertFalse(refs.isEmpty());
            try
            {
                CompactionManager.instance.performAnticompaction(store, ranges, refs, txn, 1, missingRepairSession, missingRepairSession);
                Assert.fail("expected RuntimeException");
            }
            catch (RuntimeException e)
            {
                // expected
            }
            Assert.assertEquals(Transactional.AbstractTransactional.State.ABORTED, txn.state());
            Assert.assertTrue(refs.isEmpty());
        }
    }
}

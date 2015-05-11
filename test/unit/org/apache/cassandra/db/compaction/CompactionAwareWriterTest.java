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

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.primitives.Longs;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MajorLeveledCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.compaction.writers.SplittingSizeTieredCompactionWriter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.SimpleStrategy;

import static org.junit.Assert.assertEquals;

public class CompactionAwareWriterTest
{
    private static String KEYSPACE1 = "CompactionAwareWriterTest";
    private static String CF = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF));

    }

    @Before
    public void clear()
    {
        // avoid one test affecting the next one
        Keyspace ks = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CF);
        cfs.clearUnsafe();
    }

    @Test
    public void testDefaultCompactionWriter()
    {
        Keyspace ks = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CF);
        int rowCount = 1000;
        cfs.disableAutoCompaction();
        populate(cfs, rowCount);
        LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getSSTables(), OperationType.COMPACTION);
        long beforeSize = txn.originals().iterator().next().onDiskLength();
        CompactionAwareWriter writer = new DefaultCompactionWriter(cfs, txn, txn.originals(), false, OperationType.COMPACTION);
        int rows = compact(cfs, txn, writer);
        assertEquals(1, cfs.getSSTables().size());
        assertEquals(rowCount, rows);
        assertEquals(beforeSize, cfs.getSSTables().iterator().next().onDiskLength());
        validateData(cfs, rowCount);
        cfs.truncateBlocking();
    }

    @Test
    public void testMaxSSTableSizeWriter()
    {
        Keyspace ks = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CF);
        cfs.disableAutoCompaction();
        int rowCount = 1000;
        populate(cfs, rowCount);
        LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getSSTables(), OperationType.COMPACTION);
        long beforeSize = txn.originals().iterator().next().onDiskLength();
        int sstableSize = (int)beforeSize/10;
        CompactionAwareWriter writer = new MaxSSTableSizeWriter(cfs, txn, txn.originals(), sstableSize, 0, false, OperationType.COMPACTION);
        int rows = compact(cfs, txn, writer);
        assertEquals(10, cfs.getSSTables().size());
        assertEquals(rowCount, rows);
        validateData(cfs, rowCount);
        cfs.truncateBlocking();
    }
    @Test
    public void testSplittingSizeTieredCompactionWriter()
    {
        Keyspace ks = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CF);
        cfs.disableAutoCompaction();
        int rowCount = 10000;
        populate(cfs, rowCount);
        LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getSSTables(), OperationType.COMPACTION);
        long beforeSize = txn.originals().iterator().next().onDiskLength();
        CompactionAwareWriter writer = new SplittingSizeTieredCompactionWriter(cfs, txn, txn.originals(), OperationType.COMPACTION, 0);
        int rows = compact(cfs, txn, writer);
        long expectedSize = beforeSize / 2;
        List<SSTableReader> sortedSSTables = new ArrayList<>(cfs.getSSTables());

        Collections.sort(sortedSSTables, new Comparator<SSTableReader>()
                                {
                                    @Override
                                    public int compare(SSTableReader o1, SSTableReader o2)
                                    {
                                        return Longs.compare(o2.onDiskLength(), o1.onDiskLength());
                                    }
                                });
        for (SSTableReader sstable : sortedSSTables)
        {
            // we dont create smaller files than this, everything will be in the last file
            if (expectedSize > SplittingSizeTieredCompactionWriter.DEFAULT_SMALLEST_SSTABLE_BYTES)
                assertEquals(expectedSize, sstable.onDiskLength(), expectedSize / 100); // allow 1% diff in estimated vs actual size
            expectedSize /= 2;
        }
        assertEquals(rowCount, rows);
        validateData(cfs, rowCount);
        cfs.truncateBlocking();
    }

    @Test
    public void testMajorLeveledCompactionWriter()
    {
        Keyspace ks = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CF);
        cfs.disableAutoCompaction();
        int rowCount = 20000;
        int targetSSTableCount = 50;
        populate(cfs, rowCount);
        LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getSSTables(), OperationType.COMPACTION);
        long beforeSize = txn.originals().iterator().next().onDiskLength();
        int sstableSize = (int)beforeSize/targetSSTableCount;
        CompactionAwareWriter writer = new MajorLeveledCompactionWriter(cfs, txn, txn.originals(), sstableSize, false, OperationType.COMPACTION);
        int rows = compact(cfs, txn, writer);
        assertEquals(targetSSTableCount, cfs.getSSTables().size());
        int [] levelCounts = new int[5];
        assertEquals(rowCount, rows);
        for (SSTableReader sstable : cfs.getSSTables())
        {
            levelCounts[sstable.getSSTableLevel()]++;
        }
        assertEquals(0, levelCounts[0]);
        assertEquals(10, levelCounts[1]);
        assertEquals(targetSSTableCount - 10, levelCounts[2]); // note that if we want more levels, fix this
        for (int i = 3; i < levelCounts.length; i++)
            assertEquals(0, levelCounts[i]);
        validateData(cfs, rowCount);
        cfs.truncateBlocking();
    }

    private int compact(ColumnFamilyStore cfs, LifecycleTransaction txn, CompactionAwareWriter writer)
    {
        assert txn.originals().size() == 1;
        int rowsWritten = 0;
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(txn.originals()))
        {
            CompactionController controller = new CompactionController(cfs, txn.originals(), cfs.gcBefore(System.currentTimeMillis()));
            ISSTableScanner scanner = scanners.scanners.get(0);
            while(scanner.hasNext())
            {
                AbstractCompactedRow row = new LazilyCompactedRow(controller, Arrays.asList(scanner.next()));
                if (writer.append(row))
                    rowsWritten++;
            }
        }
        Collection<SSTableReader> newSSTables = writer.finish();
        return rowsWritten;
    }

    private void populate(ColumnFamilyStore cfs, int count)
    {
        long timestamp = System.currentTimeMillis();
        byte [] payload = new byte[1000];
        new Random().nextBytes(payload);
        ByteBuffer b = ByteBuffer.wrap(payload);
        for (int i = 0; i < count; i++)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            for (int j = 0; j < 10; j++)
                rm.add(CF,  Util.cellname(Integer.toString(j)),
                        b,
                        timestamp);
            rm.applyUnsafe();
        }
        cfs.forceBlockingFlush();
        if (cfs.getSSTables().size() > 1)
        {
            // we want just one big sstable to avoid doing actual compaction in compact() above
            try
            {
                cfs.forceMajorCompaction();
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        }
        assert cfs.getSSTables().size() == 1 : cfs.getSSTables();
    }
    private void validateData(ColumnFamilyStore cfs, int rowCount)
    {
        for (int i = 0; i < rowCount; i++)
        {
            ColumnFamily cf = cfs.getTopLevelColumns(QueryFilter.getIdentityFilter(Util.dk(Integer.toString(i)), CF, System.currentTimeMillis()), Integer.MAX_VALUE);
            Iterator<Cell> iter = cf.iterator();
            int cellCount = 0;
            while (iter.hasNext())
            {
                Cell c = iter.next();
                assertEquals(Util.cellname(Integer.toString(cellCount)), c.name());
                cellCount++;
            }
            assertEquals(10, cellCount);
        }
    }
}

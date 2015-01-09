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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.primitives.Longs;
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
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
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
    @Test
    public void testDefaultCompactionWriter()
    {
        Keyspace ks = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(CF);
        int rowCount = 1000;
        cfs.disableAutoCompaction();
        populate(cfs, rowCount);
        Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
        long beforeSize = sstables.iterator().next().onDiskLength();
        CompactionAwareWriter writer = new DefaultCompactionWriter(cfs, sstables, sstables, false, OperationType.COMPACTION);
        int rows = compact(cfs, sstables, writer);
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
        Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
        long beforeSize = sstables.iterator().next().onDiskLength();
        int sstableCount = (int)beforeSize/10;
        CompactionAwareWriter writer = new MaxSSTableSizeWriter(cfs, sstables, sstables, sstableCount, 0, false, OperationType.COMPACTION);
        int rows = compact(cfs, sstables, writer);
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
        Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
        long beforeSize = sstables.iterator().next().onDiskLength();
        CompactionAwareWriter writer = new SplittingSizeTieredCompactionWriter(cfs, sstables, sstables, OperationType.COMPACTION, 0);
        int rows = compact(cfs, sstables, writer);
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
            assertEquals(expectedSize, sstable.onDiskLength(), 10000);
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
        int rowCount = 10000;
        populate(cfs, rowCount);
        Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
        long beforeSize = sstables.iterator().next().onDiskLength();
        int sstableCount = (int)beforeSize/100;
        CompactionAwareWriter writer = new MajorLeveledCompactionWriter(cfs, sstables, sstables, sstableCount, false, OperationType.COMPACTION);
        int rows = compact(cfs, sstables, writer);
        assertEquals(100, cfs.getSSTables().size());
        int [] levelCounts = new int[5];
        assertEquals(rowCount, rows);
        for (SSTableReader sstable : cfs.getSSTables())
        {
            levelCounts[sstable.getSSTableLevel()]++;
        }
        assertEquals(0, levelCounts[0]);
        assertEquals(10, levelCounts[1]);
        assertEquals(90, levelCounts[2]);
        for (int i = 3; i < levelCounts.length; i++)
            assertEquals(0, levelCounts[i]);
        validateData(cfs, rowCount);
        cfs.truncateBlocking();
    }

    private int compact(ColumnFamilyStore cfs, Set<SSTableReader> sstables, CompactionAwareWriter writer)
    {
        assert sstables.size() == 1;
        int rowsWritten = 0;
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategy().getScanners(sstables))
        {
            CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(System.currentTimeMillis()));
            ISSTableScanner scanner = scanners.scanners.get(0);
            while(scanner.hasNext())
            {
                AbstractCompactedRow row = new LazilyCompactedRow(controller, Arrays.asList(scanner.next()));
                if (writer.append(row))
                    rowsWritten++;
            }
        }
        Collection<SSTableReader> newSSTables = writer.finish();
        cfs.getDataTracker().markCompactedSSTablesReplaced(sstables, newSSTables, OperationType.COMPACTION);
        return rowsWritten;
    }

    private void populate(ColumnFamilyStore cfs, int count)
    {
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < count; i++)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            for (int j = 0; j < 10; j++)
                rm.add(CF,  Util.cellname(Integer.toString(j)),
                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                        timestamp);
            rm.applyUnsafe();
        }
        cfs.forceBlockingFlush();
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

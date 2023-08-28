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
package org.apache.cassandra.db.compaction.writers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.primitives.Longs;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.compaction.OperationType.COMPACTION;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;

public class CompactionAwareWriterTest extends CQLTester
{
    private static final String KEYSPACE = "cawt_keyspace";
    private static final String TABLE = "cawt_table";

    private static final int ROW_PER_PARTITION = 10;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        // Disabling durable write since we don't care
        schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes=false");
        schemaChange(String.format("CREATE TABLE %s.%s (k int, t int, v blob, PRIMARY KEY (k, t))", KEYSPACE, TABLE));
    }

    @AfterClass
    public static void tearDownClass()
    {
        QueryProcessor.executeInternal("DROP KEYSPACE IF EXISTS " + KEYSPACE);
    }

    private ColumnFamilyStore getColumnFamilyStore()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
    }

    @After
    public void afterTest() {
        Keyspace ks = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(TABLE);
        cfs.truncateBlocking();
    }

    @Test
    public void testDefaultCompactionWriter() throws Throwable
    {
        Keyspace ks = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(TABLE);

        int rowCount = 1000;
        cfs.disableAutoCompaction();
        populate(rowCount);
        LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);
        long beforeSize = txn.originals().iterator().next().onDiskLength();
        CompactionAwareWriter writer = new DefaultCompactionWriter(cfs, cfs.getDirectories(), txn, txn.originals());
        int rows = compact(cfs, txn, writer);
        assertEquals(1, cfs.getLiveSSTables().size());
        assertEquals(rowCount, rows);
        assertEquals(beforeSize, cfs.getLiveSSTables().iterator().next().onDiskLength());
        validateData(cfs, rowCount);
        cfs.truncateBlocking();
    }

    @Test
    public void testMaxSSTableSizeWriter() throws Throwable
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        cfs.disableAutoCompaction();
        int rowCount = 1000;
        populate(rowCount);
        LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);
        long beforeSize = txn.originals().iterator().next().onDiskLength();
        int sstableSize = (int)beforeSize/10;
        CompactionAwareWriter writer = new MaxSSTableSizeWriter(cfs, cfs.getDirectories(), txn, txn.originals(), sstableSize, 0);
        int rows = compact(cfs, txn, writer);
        assertEquals(10, cfs.getLiveSSTables().size());
        assertEquals(rowCount, rows);
        validateData(cfs, rowCount);
        cfs.truncateBlocking();
    }

    @Test
    public void testSplittingSizeTieredCompactionWriter() throws Throwable
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        cfs.disableAutoCompaction();
        int rowCount = 10000;
        populate(rowCount);
        LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);
        long beforeSize = txn.originals().iterator().next().onDiskLength();
        CompactionAwareWriter writer = new SplittingSizeTieredCompactionWriter(cfs, cfs.getDirectories(), txn, txn.originals(), 0);
        int rows = compact(cfs, txn, writer);
        long expectedSize = beforeSize / 2;
        List<SSTableReader> sortedSSTables = new ArrayList<>(cfs.getLiveSSTables());

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
    public void testMajorLeveledCompactionWriter() throws Throwable
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        cfs.disableAutoCompaction();
        int rowCount = 20000;
        int targetSSTableCount = 50;
        populate(rowCount);
        LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);
        long beforeSize = txn.originals().iterator().next().onDiskLength();
        int sstableSize = (int)beforeSize/targetSSTableCount;
        CompactionAwareWriter writer = new MajorLeveledCompactionWriter(cfs, cfs.getDirectories(), txn, txn.originals(), sstableSize);
        int rows = compact(cfs, txn, writer);
        assertEquals(targetSSTableCount, cfs.getLiveSSTables().size());
        int [] levelCounts = new int[5];
        assertEquals(rowCount, rows);
        for (SSTableReader sstable : cfs.getLiveSSTables())
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

    @Test
    public void testMultiDatadirCheck() throws IOException
    {
        createTable("create table %s (id int primary key)");
        Path tmpDir = Files.createTempDirectory("testMultiDatadirCheck");

        Directories.DataDirectory [] dataDirs = new Directories.DataDirectory[] {
        new MockDataDirectory(new File(tmpDir, "1")),
        new MockDataDirectory(new File(tmpDir, "2")),
        new MockDataDirectory(new File(tmpDir, "3")),
        new MockDataDirectory(new File(tmpDir, "4")),
        new MockDataDirectory(new File(tmpDir, "5"))
        };
        Set<SSTableReader> sstables = new HashSet<>();
        for (int i = 0; i < 100; i++)
            sstables.add(MockSchema.sstable(i, 1000, getCurrentColumnFamilyStore()));

        Directories dirs = new Directories(getCurrentColumnFamilyStore().metadata(), dataDirs);
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.COMPACTION, sstables);
        CompactionAwareWriter writer = new MaxSSTableSizeWriter(getCurrentColumnFamilyStore(), dirs, txn, sstables, 2000, 1);
        // init case
        writer.maybeSwitchWriter(null);
    }

    private static class MockDataDirectory extends Directories.DataDirectory
    {
        public MockDataDirectory(File location)
        {
            super(location);
        }

        public long getAvailableSpace()
        {
            return 5000;
        }
    }

    private int compact(ColumnFamilyStore cfs, LifecycleTransaction txn, CompactionAwareWriter writer)
    {
        assert txn.originals().size() == 1;
        int rowsWritten = 0;
        long nowInSec = FBUtilities.nowInSeconds();
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(txn.originals());
             CompactionController controller = new CompactionController(cfs, txn.originals(), cfs.gcBefore(nowInSec));
             CompactionIterator ci = new CompactionIterator(COMPACTION, scanners.scanners, controller, nowInSec, nextTimeUUID()))
        {
            while (ci.hasNext())
            {
                if (writer.append(ci.next()))
                    rowsWritten++;
            }
        }
        writer.finish();
        return rowsWritten;
    }

    private void populate(int count) throws Throwable
    {
        byte [] payload = new byte[5000];
        new Random(42).nextBytes(payload);
        ByteBuffer b = ByteBuffer.wrap(payload);

        for (int i = 0; i < count; i++)
            for (int j = 0; j < ROW_PER_PARTITION; j++)
                execute(String.format("INSERT INTO %s.%s(k, t, v) VALUES (?, ?, ?)", KEYSPACE, TABLE), i, j, b);

        ColumnFamilyStore cfs = getColumnFamilyStore();
        Util.flush(cfs);
        if (cfs.getLiveSSTables().size() > 1)
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
        assert cfs.getLiveSSTables().size() == 1 : cfs.getLiveSSTables();
    }

    private void validateData(ColumnFamilyStore cfs, int rowCount) throws Throwable
    {
        for (int i = 0; i < rowCount; i++)
        {
            Object[][] expected = new Object[ROW_PER_PARTITION][];
            for (int j = 0; j < ROW_PER_PARTITION; j++)
                expected[j] = row(i, j);

            assertRows(execute(String.format("SELECT k, t FROM %s.%s WHERE k = :i", KEYSPACE, TABLE), i), expected);
        }
    }
}

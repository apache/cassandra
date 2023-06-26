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

package org.apache.cassandra.db.compaction.unified;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.ShardManager;
import org.apache.cassandra.db.compaction.ShardManagerDiskAware;
import org.apache.cassandra.db.compaction.ShardManagerNoDisks;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ShardedCompactionWriterTest extends CQLTester
{
    private static final String KEYSPACE = "cawt_keyspace";
    private static final String TABLE = "cawt_table";

    private static final int ROW_PER_PARTITION = 10;

    @BeforeClass
    public static void beforeClass()
    {
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        StorageService.instance.initServer();

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

    @Test
    public void testOneSSTablePerShard() throws Throwable
    {
        // If we set the minSSTableSize ratio to 0.5, because this gets multiplied by the shard size to give the min sstable size,
        // assuming evenly distributed data, it should split at each boundary and so we should end up with numShards sstables
        int numShards = 5;
        int rowCount = 5000;
        testShardedCompactionWriter(numShards, rowCount, numShards, true);
    }


    @Test
    public void testMultipleInputSSTables() throws Throwable
    {
        int numShards = 3;
        int rowCount = 5000;
        testShardedCompactionWriter(numShards, rowCount, numShards, false);
    }

    private void testShardedCompactionWriter(int numShards, int rowCount, int numOutputSSTables, boolean majorCompaction) throws Throwable
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        cfs.disableAutoCompaction();

        populate(rowCount, majorCompaction);

        LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);

        ShardManager boundaries = new ShardManagerNoDisks(ColumnFamilyStore.fullWeightedRange(-1, cfs.getPartitioner()));
        ShardedCompactionWriter writer = new ShardedCompactionWriter(cfs, cfs.getDirectories(), txn, txn.originals(), false, boundaries.boundaries(numShards));

        int rows = compact(cfs, txn, writer);
        assertEquals(numOutputSSTables, cfs.getLiveSSTables().size());
        assertEquals(rowCount, rows);

        long totalOnDiskLength = cfs.getLiveSSTables().stream().mapToLong(SSTableReader::onDiskLength).sum();
        long totalBFSize = cfs.getLiveSSTables().stream().mapToLong(ShardedCompactionWriterTest::getFilterSize).sum();
        assert totalBFSize > 16 * numOutputSSTables : "Bloom Filter is empty"; // 16 is the size of empty bloom filter
        for (SSTableReader rdr : cfs.getLiveSSTables())
        {
            assertEquals((double) rdr.onDiskLength() / totalOnDiskLength,
                         (double) getFilterSize(rdr) / totalBFSize, 0.1);
            assertEquals(1.0 / numOutputSSTables, rdr.tokenSpaceCoverage(), 0.05);
        }

        validateData(cfs, rowCount);
        cfs.truncateBlocking();
    }

    static long getFilterSize(SSTableReader rdr)
    {
        if (!(rdr instanceof SSTableReaderWithFilter))
            return 0;
        return ((SSTableReaderWithFilter) rdr).getFilterSerializedSize();
    }

    @Test
    public void testDiskAdvance() throws Throwable
    {
        int rowCount = 5000;
        int numDisks = 4;
        int numShards = 3;
        ColumnFamilyStore cfs = getColumnFamilyStore();
        cfs.disableAutoCompaction();

        populate(rowCount, false);

        final ColumnFamilyStore.VersionedLocalRanges localRanges = cfs.localRangesWeighted();
        final List<Token> diskBoundaries = cfs.getPartitioner().splitter().get().splitOwnedRanges(numDisks, localRanges, false);
        ShardManager shardManager = new ShardManagerDiskAware(localRanges, diskBoundaries);
        int rows = compact(1, cfs, shardManager, cfs.getLiveSSTables());

        // We must now have one sstable per disk
        assertEquals(numDisks, cfs.getLiveSSTables().size());
        assertEquals(rowCount, rows);

        for (SSTableReader rdr : cfs.getLiveSSTables())
            verifyNoSpannedBoundaries(diskBoundaries, rdr);

        Token selectionStart = diskBoundaries.get(0);
        Token selectionEnd = diskBoundaries.get(2);

        // Now compact only a section to trigger disk advance; shard needs to advance with disk, a potential problem
        // is to create on-partition sstables at the start because shard wasn't advanced at the right time.
        Set<SSTableReader> liveSSTables = cfs.getLiveSSTables();
        List<SSTableReader> selection = liveSSTables.stream()
                                                    .filter(rdr -> rdr.getFirst().getToken().compareTo(selectionStart) > 0 &&
                                                                   rdr.getLast().getToken().compareTo(selectionEnd) <= 0)
                                                    .collect(Collectors.toList());
        List<SSTableReader> remainder = liveSSTables.stream()
                                                    .filter(rdr -> !selection.contains(rdr))
                                                    .collect(Collectors.toList());

        rows = compact(numShards, cfs, shardManager, selection);

        List<SSTableReader> compactedSelection = cfs.getLiveSSTables()
                                                    .stream()
                                                    .filter(rdr -> !remainder.contains(rdr))
                                                    .collect(Collectors.toList());
        // We must now have numShards sstables per each of the two disk sections
        assertEquals(numShards * 2, compactedSelection.size());
        assertEquals(rowCount * 2.0 / numDisks, rows * 1.0, rowCount / 20.0); // should end up with roughly this many rows


        long totalOnDiskLength = compactedSelection.stream().mapToLong(SSTableReader::onDiskLength).sum();
        long totalBFSize = compactedSelection.stream().mapToLong(ShardedCompactionWriterTest::getFilterSize).sum();
        double expectedSize = totalOnDiskLength / (numShards * 2.0);
        double expectedTokenShare = 1.0 / (numDisks * numShards);

        for (SSTableReader rdr : compactedSelection)
        {
            verifyNoSpannedBoundaries(diskBoundaries, rdr);

            assertEquals((double) rdr.onDiskLength() / totalOnDiskLength,
                         (double) getFilterSize(rdr) / totalBFSize, 0.1);
            assertEquals(expectedTokenShare, rdr.tokenSpaceCoverage(), expectedTokenShare * 0.05);
            assertEquals(expectedSize, rdr.onDiskLength(), expectedSize * 0.1);
        }

        validateData(cfs, rowCount);
        cfs.truncateBlocking();
    }

    private int compact(int numShards, ColumnFamilyStore cfs, ShardManager shardManager, Collection<SSTableReader> selection)
    {
        int rows;
        LifecycleTransaction txn = cfs.getTracker().tryModify(selection, OperationType.COMPACTION);
        ShardedCompactionWriter writer = new ShardedCompactionWriter(cfs,
                                                                     cfs.getDirectories(),
                                                                     txn,
                                                                     txn.originals(),
                                                                     false,
                                                                     shardManager.boundaries(numShards));

        rows = compact(cfs, txn, writer);
        return rows;
    }

    private static void verifyNoSpannedBoundaries(List<Token> diskBoundaries, SSTableReader rdr)
    {
        for (int i = 0; i < diskBoundaries.size(); ++i)
        {
            Token boundary = diskBoundaries.get(i);
            // rdr cannot span a boundary. I.e. it must be either fully before (last <= boundary) or fully after
            // (first > boundary).
            assertTrue(rdr.getFirst().getToken().compareTo(boundary) > 0 ||
                       rdr.getLast().getToken().compareTo(boundary) <= 0);
        }
    }

    private int compact(ColumnFamilyStore cfs, LifecycleTransaction txn, CompactionAwareWriter writer)
    {
        //assert txn.originals().size() == 1;
        int rowsWritten = 0;
        long nowInSec = FBUtilities.nowInSeconds();
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(txn.originals());
             CompactionController controller = new CompactionController(cfs, txn.originals(), cfs.gcBefore(nowInSec));
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, scanners.scanners, controller, nowInSec, TimeUUID.minAtUnixMillis(System.currentTimeMillis())))
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

    private void populate(int count, boolean compact) throws Throwable
    {
        byte [] payload = new byte[5000];
        new Random(42).nextBytes(payload);
        ByteBuffer b = ByteBuffer.wrap(payload);

        ColumnFamilyStore cfs = getColumnFamilyStore();
        for (int i = 0; i < count; i++)
        {
            for (int j = 0; j < ROW_PER_PARTITION; j++)
                execute(String.format("INSERT INTO %s.%s(k, t, v) VALUES (?, ?, ?)", KEYSPACE, TABLE), i, j, b);

            if (i % (count / 4) == 0)
                cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        if (compact && cfs.getLiveSSTables().size() > 1)
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
            assert cfs.getLiveSSTables().size() == 1 : cfs.getLiveSSTables();
        }
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
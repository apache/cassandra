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

import org.junit.AfterClass;
import org.junit.Before;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.ShardManager;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ShardingTestBase extends CQLTester
{
    static final String KEYSPACE = "cawt_keyspace";
    static final String TABLE = "cawt_table";

    static final int ROW_PER_PARTITION = 10;
    static final int PARTITIONS = 50000;

    @Before
    public void before()
    {
        // Disabling durable write since we don't care
        schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes=false");
        schemaChange(String.format("CREATE TABLE IF NOT EXISTS %s.%s (k int, t int, v blob, PRIMARY KEY (k, t))", KEYSPACE, TABLE));
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

    void verifySharding(int numShards, int rowCount, int numOutputSSTables, ColumnFamilyStore cfs) throws Throwable
    {
        assertEquals(numOutputSSTables, cfs.getLiveSSTables().size());

        long totalOnDiskLength = cfs.getLiveSSTables().stream().mapToLong(SSTableReader::onDiskLength).sum();
        long totalBFSize = cfs.getLiveSSTables().stream().mapToLong(ShardingTestBase::getFilterSize).sum();
        long totalKeyCount = cfs.getLiveSSTables().stream().mapToLong(SSTableReader::estimatedKeys).sum();
        assert totalBFSize > 16 * numOutputSSTables : "Bloom Filter is empty"; // 16 is the size of empty bloom filter
        for (SSTableReader rdr : cfs.getLiveSSTables())
        {
            assertEquals((double) rdr.onDiskLength() / totalOnDiskLength,
                         (double) getFilterSize(rdr) / totalBFSize, 0.1);
            assertEquals(1.0 / numOutputSSTables, rdr.tokenSpaceCoverage(), 0.05);
        }

        System.out.println("Total on disk length: " + FBUtilities.prettyPrintMemory(totalOnDiskLength));
        System.out.println("Total BF size: " + FBUtilities.prettyPrintMemory(totalBFSize));
        System.out.println("Total key count: " + FBUtilities.prettyPrintDecimal(totalKeyCount, "", ""));
        try (var filter = FilterFactory.getFilter(totalKeyCount, 0.01))
        {
            System.out.println("Optimal total BF size: " + FBUtilities.prettyPrintMemory(filter.serializedSize(false)));
        }
        try (var filter = FilterFactory.getFilter(totalKeyCount / numShards, 0.01))
        {
            System.out.println("Sharded optimal total BF size: " + FBUtilities.prettyPrintMemory(filter.serializedSize(false) * numShards));
        }

        cfs.getLiveSSTables().forEach(s -> System.out.println("SSTable: " + s.toString() + " covers " + s.getFirst() + " to " + s.getLast()));

        validateData(cfs, rowCount);
    }

    static long getFilterSize(SSTableReader rdr)
    {
        if (!(rdr instanceof SSTableReaderWithFilter))
            return 0;
        return ((SSTableReaderWithFilter) rdr).getFilterSerializedSize();
    }

    int compact(int numShards, ColumnFamilyStore cfs, ShardManager shardManager, Collection<SSTableReader> selection)
    {
        int rows;
        LifecycleTransaction txn = cfs.getTracker().tryModify(selection, OperationType.COMPACTION);
        ShardedCompactionWriter writer = new ShardedCompactionWriter(cfs,
                                                                     cfs.getDirectories(),
                                                                     txn,
                                                                     txn.originals(),
                                                                     false,
                                                                     true,
                                                                     shardManager.boundaries(numShards));

        rows = compact(cfs, txn, writer);
        return rows;
    }

    static void verifyNoSpannedBoundaries(List<Token> diskBoundaries, SSTableReader rdr)
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

    int compact(ColumnFamilyStore cfs, LifecycleTransaction txn, CompactionAwareWriter writer)
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

    void populate(int count, boolean compact) throws Throwable
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
            // we want no overlapping sstables to avoid doing actual compaction in compact() above
            try
            {
                cfs.forceMajorCompaction();
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        }
    }

    void validateData(ColumnFamilyStore cfs, int rowCount) throws Throwable
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
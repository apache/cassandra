/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction.unified;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertEquals;

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
        double minSSTableSizeRatio = 0.5;
        testShardedCompactionWriter(numShards, rowCount, minSSTableSizeRatio, numShards, true);
    }

    @Test
    public void testOneSSTableOnly() throws Throwable
    {
        // If we set the minSSTableSize ratio to the number of shards 5, because this gets multiplied by the shard size to give
        // the min sstable size, then it should ignore all boundaries because it won't reach the minimum sstable size until the
        // end of the last shard and so we should end up with 1 sstable
        int numShards = 5;
        int rowCount = 5000;
        double minSSTableSizeRatio = 5;
        testShardedCompactionWriter(numShards, rowCount, minSSTableSizeRatio, 1, true);
    }

    @Test
    public void testThreeSSTables() throws Throwable
    {
        // If we set the minSSTableSize ratio to 2, because this gets multiplied by the shard size to give
        // the min sstable size, then it should merge 2 shards together assuming evenly distributed data
        // and so we should end up with 3 sstables (numShards / 2)
        int numShards = 6;
        int rowCount = 5000;
        double minSSTableSizeRatio = 2;
        testShardedCompactionWriter(numShards, rowCount, minSSTableSizeRatio, 3, true);
    }

    @Test
    public void testMultipleInputSSTables() throws Throwable
    {
        int numShards = 3;
        int rowCount = 5000;
        double minSSTableSizeRatio = 2;
        testShardedCompactionWriter(numShards, rowCount, minSSTableSizeRatio, numShards, false);
    }

    private void testShardedCompactionWriter(int numShards, int rowCount, double minSSTableSizeRatio, int numOutputSSTables, boolean majorCompaction) throws Throwable
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        cfs.disableAutoCompaction();

        populate(rowCount, majorCompaction);

        LifecycleTransaction txn = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);
        long inputSize = txn.originals().iterator().next().onDiskLength();
        int minSSTableSize = (int) (((double) inputSize / numShards) * minSSTableSizeRatio);

        List<PartitionPosition> boundaries = cfs.getLocalRanges().split(numShards);
        ShardedCompactionWriter writer = new ShardedCompactionWriter(cfs, cfs.getDirectories(), txn, txn.originals(), false, minSSTableSize, boundaries);

        int rows = compact(cfs, txn, writer);
        assertEquals(numOutputSSTables, cfs.getLiveSSTables().size());
        assertEquals(rowCount, rows);

        long totalOnDiskLength = cfs.getLiveSSTables().stream().map(SSTableReader::onDiskLength).mapToLong(Long::longValue).sum();
        long totalBFSize = cfs.getLiveSSTables().stream().map(SSTableReader::getBloomFilterSerializedSize).mapToLong(Long::longValue).sum();
        assert totalBFSize > 16 * numOutputSSTables : "Bloom Filter is empty"; // 16 is the size of empty bloom filter
        for (SSTableReader rdr : cfs.getLiveSSTables())
            assertEquals((double) rdr.onDiskLength() / totalOnDiskLength,
                         (double) rdr.getBloomFilterSerializedSize() / totalBFSize, 0.1);

        validateData(cfs, rowCount);
        cfs.truncateBlocking();
    }

    private int compact(ColumnFamilyStore cfs, LifecycleTransaction txn, CompactionAwareWriter writer)
    {
        //assert txn.originals().size() == 1;
        int rowsWritten = 0;
        int nowInSec = FBUtilities.nowInSeconds();
        try (ScannerList scanners = cfs.getCompactionStrategy().getScanners(txn.originals());
             CompactionController controller = new CompactionController(cfs, txn.originals(), cfs.gcBefore(nowInSec));
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, scanners.scanners, controller, nowInSec, UUIDGen.getTimeUUID()))
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
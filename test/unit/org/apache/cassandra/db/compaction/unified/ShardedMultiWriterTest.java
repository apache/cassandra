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
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;

public class ShardedMultiWriterTest extends CQLTester
{
    private static final int ROW_PER_PARTITION = 10;

    @BeforeClass
    public static void beforeClass()
    {
        CQLTester.setUpClass();
        StorageService.instance.initServer();
    }

    @Test
    public void testShardedCompactionWriter_fiveShards() throws Throwable
    {
        int numShards = 5;
        int minSSTableSizeMB = 2;
        long totSizeBytes = ((minSSTableSizeMB << 20) * numShards) * 2;

        // We have double the data required for 5 shards so we should get 5 shards
        testShardedCompactionWriter(numShards, totSizeBytes, numShards);
    }

    @Test
    public void testShardedCompactionWriter_oneShard() throws Throwable
    {
        int numShards = 1;
        int minSSTableSizeMB = 2;
        long totSizeBytes = (minSSTableSizeMB << 20);

        // there should be only 1 shard if there is <= minSSTableSize
        testShardedCompactionWriter(numShards, totSizeBytes, 1);
    }

    @Test
    public void testShardedCompactionWriter_threeShard() throws Throwable
    {
        int numShards = 3;
        int minSSTableSizeMB = 2;
        long totSizeBytes = (minSSTableSizeMB << 20) * 3;

        // there should be only 3 shards if there is minSSTableSize * 3 data
        testShardedCompactionWriter(numShards, totSizeBytes, 3);
    }

    private void testShardedCompactionWriter(int numShards, long totSizeBytes, int numOutputSSTables) throws Throwable
    {
        createTable(String.format("CREATE TABLE %%s (k int, t int, v blob, PRIMARY KEY (k, t)) with compaction = " +
                                  "{'class':'UnifiedCompactionStrategy', 'base_shard_count' : '%d'} ", numShards));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        int rowCount = insertData(totSizeBytes);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        assertEquals(numOutputSSTables, cfs.getLiveSSTables().size());
        for (SSTableReader rdr : cfs.getLiveSSTables())
        {
            assertEquals(1.0 / numOutputSSTables, rdr.tokenSpaceCoverage(), 0.05);
        }

        validateData(rowCount);
        cfs.truncateBlocking();
    }

    private int insertData(long totSizeBytes) throws Throwable
    {
        byte [] payload = new byte[5000];
        ByteBuffer b = ByteBuffer.wrap(payload);
        int rowCount = (int) Math.ceil((double) totSizeBytes / (8 + ROW_PER_PARTITION * payload.length));

        for (int i = 0; i < rowCount; i++)
        {
            for (int j = 0; j < ROW_PER_PARTITION; j++)
            {
                new Random(42 + i * ROW_PER_PARTITION + j).nextBytes(payload); // write different data each time to make non-compressible
                execute("INSERT INTO %s(k, t, v) VALUES (?, ?, ?)", i, j, b);
            }
        }

        return rowCount;
    }

    private void validateData(int rowCount) throws Throwable
    {
        for (int i = 0; i < rowCount; i++)
        {
            Object[][] expected = new Object[ROW_PER_PARTITION][];
            for (int j = 0; j < ROW_PER_PARTITION; j++)
                expected[j] = row(i, j);

            assertRows(execute("SELECT k, t FROM %s WHERE k = :i", i), expected);
        }
    }

    @Override
    public UntypedResultSet execute(String query, Object... values)
    {
        return super.executeFormattedQuery(formatQuery(KEYSPACE_PER_TEST, query), values);
    }

    @Override
    public String createTable(String query)
    {
        return super.createTable(KEYSPACE_PER_TEST, query);
    }

    @Override
    public ColumnFamilyStore getCurrentColumnFamilyStore()
    {
        return super.getCurrentColumnFamilyStore(KEYSPACE_PER_TEST);
    }
}
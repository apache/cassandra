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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.NoopCompressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRow;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.fail;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Simple read/write tests using different types of query, paticularly when the data is spread across memory and
 * multiple sstables and using different compressors. All available compressors are tested. Both ascending and
 * descending clustering orders are tested. The read queries are run using every node as a coordinator, with and without
 * paging.
 */
@RunWith(Parameterized.class)
public class SimpleReadWriteTest extends TestBaseImpl
{
    private static final int NUM_NODES = 4;
    private static final int REPLICATION_FACTOR = 3;
    private static final String CREATE_TABLE = "CREATE TABLE %s(k int, c int, v int, PRIMARY KEY (k, c)) " +
                                               "WITH CLUSTERING ORDER BY (c %s) " +
                                               "AND COMPRESSION = { 'class': '%s' } " +
                                               "AND READ_REPAIR = 'none'";
    private static final String[] COMPRESSORS = new String[]{ NoopCompressor.class.getSimpleName(),
                                                              LZ4Compressor.class.getSimpleName(),
                                                              DeflateCompressor.class.getSimpleName(),
                                                              SnappyCompressor.class.getSimpleName(),
                                                              ZstdCompressor.class.getSimpleName() };
    private static final int SECOND_SSTABLE_INTERVAL = 2;
    private static final int MEMTABLE_INTERVAL = 5;

    private static final AtomicInteger seq = new AtomicInteger();

    /**
     * The sstable compressor to be used.
     */
    @Parameterized.Parameter
    public String compressor;

    /**
     * Whether the clustering order is reverse.
     */
    @Parameterized.Parameter(1)
    public boolean reverse;

    private String tableName;

    @Parameterized.Parameters(name = "{index}: compressor={0} reverse={1}")
    public static Collection<Object[]> data()
    {
        List<Object[]> result = new ArrayList<>();
        for (String compressor : COMPRESSORS)
            for (boolean reverse : BOOLEANS)
                result.add(new Object[]{ compressor, reverse });
        return result;
    }

    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(NUM_NODES).start(), REPLICATION_FACTOR);
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Before
    public void before()
    {
        // create the table
        tableName = String.format("%s.t_%d", KEYSPACE, seq.getAndIncrement());
        cluster.schemaChange(String.format(CREATE_TABLE, tableName, reverse ? "DESC" : "ASC", compressor));
    }

    @After
    public void after()
    {
        cluster.schemaChange(withTable("DROP TABLE %s"));
    }

    /**
     * Simple put/get on a single partition with a few rows, reading with a single partition query.
     * <p>
     * Migrated from Python dtests putget_test.py:TestPutGet.test_putget[_snappy|_deflate]().
     */
    @Test
    public void testPartitionQuery()
    {
        int numRows = 10;

        writeRows(1, numRows);

        Object[][] rows = readRows("SELECT * FROM %s WHERE k=?", 0);
        Assert.assertEquals(numRows, rows.length);
        for (int c = 0; c < numRows; c++)
        {
            validateRow(rows[c], numRows, 0, c);
        }
    }

    /**
     * Simple put/get on multiple partitions with multiple rows, reading with a range query.
     * <p>
     * Migrated from Python dtests putget_test.py:TestPutGet.test_rangeputget().
     */
    @Test
    public void testRangeQuery()
    {
        int numPartitions = 10;
        int rowsPerPartition = 10;

        writeRows(numPartitions, rowsPerPartition);

        Object[][] rows = readRows("SELECT * FROM %s");
        Assert.assertEquals(numPartitions * rowsPerPartition, rows.length);
        for (int k = 0; k < numPartitions; k++)
        {
            for (int c = 0; c < rowsPerPartition; c++)
            {
                Object[] row = rows[k * rowsPerPartition + c];
                validateRow(row, rowsPerPartition, k, c);
            }
        }
    }

    /**
     * Simple put/get on a single partition with multiple rows, reading with slice queries.
     * <p>
     * Migrated from Python dtests putget_test.py:TestPutGet.test_wide_row().
     */
    @Test
    public void testSliceQuery()
    {
        int numRows = 100;

        writeRows(1, numRows);

        String query = "SELECT * FROM %s WHERE k=? AND c>=? AND c<?";
        for (int sliceSize : Arrays.asList(10, 20, 100))
        {
            for (int c = 0; c < numRows; c = c + sliceSize)
            {
                Object[][] rows = readRows(query, 0, c, c + sliceSize);
                Assert.assertEquals(sliceSize, rows.length);

                for (int i = 0; i < sliceSize; i++)
                {
                    Object[] row = rows[i];
                    validateRow(row, numRows, 0, c + i);
                }
            }
        }
    }

    /**
     * Simple put/get on multiple partitions with multiple rows, reading with IN queries.
     */
    @Test
    public void testInQuery()
    {
        int numPartitions = 10;
        int rowsPerPartition = 10;

        writeRows(numPartitions, rowsPerPartition);

        String query = "SELECT * FROM %s WHERE k IN (?, ?)";
        for (int k = 0; k < numPartitions; k += 2)
        {
            Object[][] rows = readRows(query, k, k + 1);
            Assert.assertEquals(rowsPerPartition * 2, rows.length);

            for (int i = 0; i < 2; i++)
            {
                for (int c = 0; c < rowsPerPartition; c++)
                {
                    Object[] row = rows[i * rowsPerPartition + c];
                    validateRow(row, rowsPerPartition, k + i, c);
                }
            }
        }
    }

    /**
     * Writes {@code numPartitions} with {@code rowsPerPartition} each, with overrides in different sstables and memtables.
     */
    private void writeRows(int numPartitions, int rowsPerPartition)
    {
        String update = withTable("UPDATE %s SET v=? WHERE k=? AND c=?");
        ICoordinator coordinator = cluster.coordinator(1);

        // insert all the partition rows in a single sstable
        for (int c = 0; c < rowsPerPartition; c++)
            for (int k = 0; k < numPartitions; k++)
                coordinator.execute(update, QUORUM, c, k, c);
        cluster.forEach(i -> i.flush(KEYSPACE));

        // override some rows in a second sstable
        for (int c = 0; c < rowsPerPartition; c += SECOND_SSTABLE_INTERVAL)
            for (int k = 0; k < numPartitions; k++)
                coordinator.execute(update, QUORUM, c + rowsPerPartition, k, c);
        cluster.forEach(i -> i.flush(KEYSPACE));

        // override some rows only in memtable
        for (int c = 0; c < rowsPerPartition; c += MEMTABLE_INTERVAL)
            for (int k = 0; k < numPartitions; k++)
                coordinator.execute(update, QUORUM, c + rowsPerPartition * 2, k, c);
    }

    /**
     * Runs the specified query in all coordinators, with and without paging.
     */
    private Object[][] readRows(String query, Object... boundValues)
    {
        query = withTable(query);

        // verify that all coordinators return the same results for the query, regardless of paging
        Object[][] lastRows = null;
        int lastNode = 1;
        boolean lastPaging = false;
        for (int node = 1; node <= NUM_NODES; node++)
        {
            ICoordinator coordinator = cluster.coordinator(node);

            for (boolean paging : BOOLEANS)
            {
                Object[][] rows = paging
                                  ? Iterators.toArray(coordinator.executeWithPaging(query, QUORUM, 1, boundValues),
                                                      Object[].class)
                                  : coordinator.execute(query, QUORUM, boundValues);

                if (lastRows != null)
                {
                    try
                    {
                        assertRows(lastRows, rows);
                    }
                    catch (AssertionError e)
                    {
                        fail(String.format("Node %d %s paging has returned different results " +
                                           "for the same query than node %d %s paging:\n%s",
                                           node, paging ? "with" : "without",
                                           lastNode, lastPaging ? "with" : "without",
                                           e.getMessage()));
                    }
                }

                lastRows = rows;
                lastPaging = paging;
            }

            lastNode = node;
        }
        Assert.assertNotNull(lastRows);

        // undo the clustering reverse sorting to ease validation
        if (reverse)
            ArrayUtils.reverse(lastRows);

        // sort by partition key to ease validation
        Arrays.sort(lastRows, Comparator.comparing(row -> (int) row[0]));

        return lastRows;
    }

    private void validateRow(Object[] row, int rowsPerPartition, int k, int c)
    {
        Assert.assertNotNull(row);

        if (c % MEMTABLE_INTERVAL == 0)
            assertRow(row, row(k, c, c + rowsPerPartition * 2));
        else if (c % SECOND_SSTABLE_INTERVAL == 0)
            assertRow(row, row(k, c, c + rowsPerPartition));
        else
            assertRow(row, row(k, c, c));
    }

    private String withTable(String query)
    {
        return String.format(query, tableName);
    }
}

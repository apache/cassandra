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

package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.ByteBufferUtil;

@RunWith(Parameterized.class)
public class PagingAggregationQueryTest extends CQLTester
{
    public static final int NUM_PARTITIONS = 100;
    public static final int NUM_CLUSTERINGS = 7;

    @Parameterized.Parameters(name = "aggregation_sub_page_size={0} data_size={1} flush={2}")
    public static Collection<Object[]> generateParameters()
    {
        List<Object[]> params = new ArrayList<>();
        for (PageSize pageSize : Arrays.asList(PageSize.inBytes(1024), PageSize.NONE))
            for (DataSize dataSize : DataSize.values())
                for (boolean flush : new boolean[]{ true, false })
                    params.add(new Object[]{ pageSize, dataSize, flush });
        return params;
    }

    /**
     * The regular column value to be inserted.
     * It will use different sizes to change the number of rows that fit in a page.
     */
    private final ByteBuffer value;

    /**
     * Whether to flush after each type of mutation.
     */
    private final boolean flush;

    public enum DataSize
    {
        NULL(-1),
        SMALL(10), // multiple rows per page
        LARGE(2000); // one row per page

        private final int size;

        DataSize(int size)
        {
            this.size = size;
        }

        public ByteBuffer value()
        {
            return size == -1 ? null : ByteBuffer.allocate(size);
        }
    }

    public PagingAggregationQueryTest(PageSize subPageSize, DataSize dataSize, boolean flush)
    {
        DatabaseDescriptor.setAggregationSubPageSize(subPageSize);
        this.value = dataSize.value();
        this.flush = flush;
        enableCoordinatorExecution();

        // disable read size thresholds, since we are testing with abnormally large responses
        DatabaseDescriptor.setLocalReadSizeWarnThreshold(null);
        DatabaseDescriptor.setLocalReadSizeFailThreshold(null);
        DatabaseDescriptor.setCoordinatorReadSizeWarnThreshold(null);
        DatabaseDescriptor.setCoordinatorReadSizeFailThreshold(null);
    }

    /**
     * DSP-22813, DBPE-16245, DBPE-16378 and CNDB-13978: Test that count(*) aggregation queries return the correct
     * number of rows, even if there are tombstones and paging is required.
     * </p>
     * Before the DSP-22813/DBPE-16245/DBPE-16378/CNDB-13978 fix these queries would stop counting after hitting the
     * {@code aggregation_sub_page_size} page size, returning only the count of a single page.
     */
    @Test
    public void testAggregationWithCellTomsbstones()
    {
        // we are only interested in the NULL value case, which produces cell tombstones
        Assume.assumeTrue(value == null && !flush);

        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v blob, PRIMARY KEY (k, c1, c2))");

        int ks = NUM_PARTITIONS;
        int c1s = NUM_CLUSTERINGS / 2;
        int c2s = NUM_CLUSTERINGS / 2;

        // insert some data
        for (int k = 0; k < ks; k++)
        {
            for (int c1 = 0; c1 < c1s; c1++)
            {
                for (int c2 = 0; c2 < c2s; c2++)
                {
                    execute("INSERT INTO %s (k, c1, c2, v) VALUES (?, ?, ?, null)", k, c1, c2);
                }
            }

            // test aggregation on single partition query
            assertPartitionCount(k, c1s * c2s);
        }

        // test aggregation on range query
        assertRangeCount(ks * c1s * c2s);

        // test aggregation with group by
        Assertions.assertThat(execute("SELECT COUNT(*) FROM %s GROUP BY k").size()).isEqualTo(ks);
        Assertions.assertThat(execute("SELECT COUNT(*) FROM %s GROUP BY k, c1").size()).isEqualTo(ks * c1s);
        Assertions.assertThat(execute("SELECT COUNT(*) FROM %s GROUP BY k, c1, c2").size()).isEqualTo(ks * c1s * c2s);
    }

    @Test
    public void testAggregationWithPartialRowDeletions()
    {
        createTable("CREATE TABLE %s (k bigint, c int, v blob, PRIMARY KEY(k, c))");

        // insert some clusterings, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            // insert some clusterings
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, value);
            }
        }
        maybeFlush();

        // for each partition, delete the first and last clustering
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            execute("DELETE FROM %s WHERE k = ? AND c = ?", k, 1);
            execute("DELETE FROM %s WHERE k = ? AND c = ?", k, NUM_CLUSTERINGS);

            // test aggregation on single partition query
            assertPartitionCount(k, NUM_CLUSTERINGS - 2);
            long maxK = execute("SELECT max(k) FROM %s WHERE k=?", k).one().getLong("system.max(k)");
            Assertions.assertThat(maxK).isEqualTo(k);
            int maxC = execute("SELECT max(c) FROM %s WHERE k=?", k).one().getInt("system.max(c)");
            Assertions.assertThat(maxC).isEqualTo(NUM_CLUSTERINGS - 1);
        }

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * (NUM_CLUSTERINGS - 2));
        long maxK = execute("SELECT max(k) FROM %s").one().getLong("system.max(k)");
        Assertions.assertThat(maxK).isEqualTo(NUM_PARTITIONS);
        int maxC = execute("SELECT max(c) FROM %s").one().getInt("system.max(c)");
        Assertions.assertThat(maxC).isEqualTo(NUM_CLUSTERINGS - 1);

        // test aggregation with group by
        Assertions.assertThat(execute("SELECT COUNT(*) FROM %s GROUP BY k").size()).isEqualTo(NUM_PARTITIONS);
        Assertions.assertThat(execute("SELECT COUNT(*) FROM %s GROUP BY k, c").size()).isEqualTo(NUM_PARTITIONS * (NUM_CLUSTERINGS - 2));
    }

    @Test
    public void testAggregationWithCompleteRowDeletions()
    {
        createTable("CREATE TABLE %s (k bigint, c int, v blob, PRIMARY KEY(k, c))");

        // insert some clusterings, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            // insert some clusterings
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, value);
            }
        }
        maybeFlush();

        // for each partition, delete all the clusterings
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("DELETE FROM %s WHERE k = ? AND c = ?", k, c);

                // test aggregation on single partition query
                assertPartitionCount(k, NUM_CLUSTERINGS - c);
            }

            Assertions.assertThat(execute("SELECT max(k) FROM %s WHERE k=?", k).one().getBytes("system.max(k)")).isNull();
            Assertions.assertThat(execute("SELECT max(k) FROM %s WHERE k=?", k).one().getBytes("system.max(c)")).isNull();
        }

        // test aggregation on range query
        assertRangeCount(0);
        Assertions.assertThat(execute("SELECT max(k) FROM %s").one().getBytes("system.max(k)")).isNull();
        Assertions.assertThat(execute("SELECT max(k) FROM %s").one().getBytes("system.max(c)")).isNull();

        // test aggregation with group by
        Assertions.assertThat(execute("SELECT COUNT(*) FROM %s GROUP BY k").size()).isEqualTo(0);
        Assertions.assertThat(execute("SELECT COUNT(*) FROM %s GROUP BY k, c").size()).isEqualTo(0);
    }

    @Test
    public void testAggregationWithRangeRowDeletions()
    {
        createTable("CREATE TABLE %s (k bigint, c int, v blob, PRIMARY KEY(k, c))");

        // insert some clusterings, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            // insert some clusterings
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, value);
            }
        }
        maybeFlush();

        // for each partition, delete the two first and two last clusterings
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            execute("DELETE FROM %s WHERE k = ? AND c <= ?", k, 2);
            execute("DELETE FROM %s WHERE k = ? AND c >= ?", k, NUM_CLUSTERINGS - 1);

            // test aggregation on single partition query
            assertPartitionCount(k, NUM_CLUSTERINGS - 4);
            long maxK = execute("SELECT max(k) FROM %s WHERE k=?", k).one().getLong("system.max(k)");
            Assertions.assertThat(maxK).isEqualTo(k);
            int maxC = execute("SELECT max(c) FROM %s WHERE k=?", k).one().getInt("system.max(c)");
            Assertions.assertThat(maxC).isEqualTo(NUM_CLUSTERINGS - 2);
        }

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * (NUM_CLUSTERINGS - 4));
        long maxK = execute("SELECT max(k) FROM %s").one().getLong("system.max(k)");
        Assertions.assertThat(maxK).isEqualTo(NUM_PARTITIONS);
        int maxC = execute("SELECT max(c) FROM %s").one().getInt("system.max(c)");
        Assertions.assertThat(maxC).isEqualTo(NUM_CLUSTERINGS - 2);

        // test aggregation with group by
        Assertions.assertThat(execute("SELECT COUNT(*) FROM %s GROUP BY k").size()).isEqualTo(NUM_PARTITIONS);
        Assertions.assertThat(execute("SELECT COUNT(*) FROM %s GROUP BY k, c").size()).isEqualTo(NUM_PARTITIONS * (NUM_CLUSTERINGS - 4));
    }

    @Test
    public void testAggregationWithRangeRowDeletionsComposite()
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v blob, PRIMARY KEY(k, c1, c2))");

        int c1s = 11;
        int c2s = 17;

        // insert some rows, and flush
        for (int k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c1 = 1; c1 <= c1s; c1++)
            {
                for (int c2 = 1; c2 <= c2s; c2++)
                {
                    execute("INSERT INTO %s (k, c1, c2, v) VALUES (?, ?, ?, ?)", k, c1, c2, value);
                }
            }
        }
        maybeFlush();

        // for each partition, delete the two first and two last clusterings
        for (int k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c1 = 1; c1 <= c1s; c1++)
            {
                execute("DELETE FROM %s WHERE k = ? AND c1 = ? AND c2 <= ?", k, c1, 2);
                execute("DELETE FROM %s WHERE k = ? AND c1 = ? AND c2 >= ?", k, c1, c2s - 1);
            }

            // test aggregation on single partition query
            assertPartitionCount(k, c1s * (c2s - 4));
        }

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * c1s * (c2s - 4));
    }

    @Test
    public void testAggregationWithPartitionDeletionWithoutClustering()
    {
        createTable("CREATE TABLE %s (k bigint PRIMARY KEY, v blob)");

        // insert some partitions, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", k, value);
        }
        maybeFlush();

        // delete all the partitions, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            execute("DELETE FROM %s WHERE k = ?", k);
        }
        maybeFlush();

        // re-insert part of the data, using the same partitions
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            execute("INSERT INTO %s (k) VALUES (?)", k);

            // test aggregation on single partition query
            assertPartitionCount(k, 1);
            long max = execute("SELECT max(k) FROM %s WHERE k=?", k).one().getLong("system.max(k)");
            Assertions.assertThat(max).isEqualTo(k);
        }

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS);
        long max = execute("SELECT max(k) FROM %s").one().getLong("system.max(k)");
        Assertions.assertThat(max).isEqualTo(NUM_PARTITIONS);
    }

    @Test
    public void testAggregationWithPartitionDeletionWithClustering()
    {
        createTable("CREATE TABLE %s (k bigint, c int, v blob, PRIMARY KEY(k, c))");

        int numClusteringsBeforeDeletion = 10;
        int numClusteringsAfterDeletion = 7;

        // insert some partitions, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= numClusteringsBeforeDeletion; c++)
            {
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, value);
            }
        }
        maybeFlush();

        // delete all the partitions, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            execute("DELETE FROM %s WHERE k = ?", k);
        }
        maybeFlush();

        // re-insert part of the data, using the same partitions
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= numClusteringsAfterDeletion; c++)
            {
                execute("INSERT INTO %s (k, c) VALUES (?, ?)", k, c);
            }

            // test aggregation on single partition query
            assertPartitionCount(k, numClusteringsAfterDeletion);
            long max = execute("SELECT max(k) FROM %s WHERE k=?", k).one().getLong("system.max(k)");
            Assertions.assertThat(max).isEqualTo(k);
        }

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * numClusteringsAfterDeletion);
        long max = execute("SELECT max(k) FROM %s").one().getLong("system.max(k)");
        Assertions.assertThat(max).isEqualTo(NUM_PARTITIONS);
    }

    @Test
    public void testAggregationWithLists()
    {
        // we are only interested in the not NULL value case
        Assume.assumeTrue(value != null);

        createTable("CREATE TABLE %s (k bigint, c int, v list<blob>, PRIMARY KEY(k, c))");

        // insert some clusterings, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            // insert some clusterings
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, list(value, value, value, value, value));
            }
        }
        maybeFlush();

        // for each row, delete some list elements
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("DELETE v[0] FROM %s WHERE k = ? AND c = ?", k, c);
                execute("DELETE v[1] FROM %s WHERE k = ? AND c = ?", k, c);
                execute("DELETE v[2] FROM %s WHERE k = ? AND c = ?", k, c);
            }

            // test aggregation on single partition query
            assertPartitionCount(k, NUM_CLUSTERINGS);
        }
        maybeFlush();

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * NUM_CLUSTERINGS);

        // for each row, add some list elements
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("UPDATE %s SET v = v + ? WHERE k = ? AND c = ?", list(value, value), k, c);
            }

            // test aggregation on single partition query
            assertPartitionCount(k, NUM_CLUSTERINGS);
        }
        maybeFlush();

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * NUM_CLUSTERINGS);

        // for each row, drop the entire list
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, null);
            }

            // test aggregation on single partition query
            assertPartitionCount(k, NUM_CLUSTERINGS);
        }
        maybeFlush();

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * NUM_CLUSTERINGS);
    }

    @Test
    public void testAggregationWithSets()
    {
        // we are only interested in the not NULL value case
        Assume.assumeTrue(value != null);

        createTable("CREATE TABLE %s (k bigint, c int, v set<text>, PRIMARY KEY(k, c))");

        String v1 = ByteBufferUtil.bytesToHex(value) + "_1";
        String v2 = ByteBufferUtil.bytesToHex(value) + "_2";
        String v3 = ByteBufferUtil.bytesToHex(value) + "_3";
        String v4 = ByteBufferUtil.bytesToHex(value) + "_4";
        String v5 = ByteBufferUtil.bytesToHex(value) + "_5";

        // insert some clusterings, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            // insert some clusterings
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, set(v1, v2, v3, v4));
            }
        }
        maybeFlush();

        // for each row, delete some set elements
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("UPDATE %s SET v = v - { '" + v1 + "' } WHERE k = ? AND c = ?", k, c);
                execute("UPDATE %s SET v = v - ? WHERE k = ? AND c = ?", set(v3, v5), k, c);
            }

            // test aggregation on single partition query
            assertPartitionCount(k, NUM_CLUSTERINGS);
        }
        maybeFlush();

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * NUM_CLUSTERINGS);

        // for each row, add some set elements
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("UPDATE %s SET v = v + ? WHERE k = ? AND c = ?", set(v1, v3), k, c);
            }

            // test aggregation on single partition query
            assertPartitionCount(k, NUM_CLUSTERINGS);
        }
        maybeFlush();

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * NUM_CLUSTERINGS);

        // for each row, drop the entire set
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, null);
            }

            // test aggregation on single partition query
            assertPartitionCount(k, NUM_CLUSTERINGS);
        }
        maybeFlush();

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * NUM_CLUSTERINGS);
    }

    @Test
    public void testAggregationWithMaps()
    {
        // we are only interested in the not NULL value case
        Assume.assumeTrue(value != null);

        createTable("CREATE TABLE %s (k bigint, c int, v map<text, text>, PRIMARY KEY(k, c))");

        String stringValue = ByteBufferUtil.bytesToHex(value);
        String k1 = stringValue + "_k_1";
        String k2 = stringValue + "_k_2";
        String k3 = stringValue + "_k_3";
        String k4 = stringValue + "_k_4";
        String k5 = stringValue + "_k_5";
        String v1 = stringValue + "_v_1";
        String v2 = stringValue + "_v_2";
        String v3 = stringValue + "_v_3";
        String v4 = stringValue + "_v_4";
        String v5 = stringValue + "_v_5";

        // insert some clusterings, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            // insert some clusterings
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c,
                        map(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5));
            }
        }
        maybeFlush();

        // for each row, delete some map elements
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("UPDATE %s SET v = v - { '" + k1 + "' } WHERE k = ? AND c = ?", k, c);
                execute("UPDATE %s SET v = v - ? WHERE k = ? AND c = ?", set(k3, k5), k, c);
            }

            // test aggregation on single partition query
            assertPartitionCount(k, NUM_CLUSTERINGS);
        }
        maybeFlush();

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * NUM_CLUSTERINGS);

        // for each row, add some map elements
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("UPDATE %s SET v = v + ? WHERE k = ? AND c = ?", map(k1, v1, k3, v3), k, c);
            }

            // test aggregation on single partition query
            assertPartitionCount(k, NUM_CLUSTERINGS);
        }
        maybeFlush();

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * NUM_CLUSTERINGS);

        // for each row, drop the entire map
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, null);
            }

            // test aggregation on single partition query
            assertPartitionCount(k, NUM_CLUSTERINGS);
        }
        maybeFlush();

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * NUM_CLUSTERINGS);
    }

    @Test
    public void testAggregationWithUDTs()
    {
        // we are only interested in the not NULL value case
        Assume.assumeTrue(value != null);

        String type = createType("CREATE TYPE %s (x blob, y blob)");
        createTable("CREATE TABLE %s (k bigint, c int, v " + type + ", PRIMARY KEY(k, c))");

        // insert some rows, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            // insert some clusterings
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, tuple(value, value));
            }
        }
        maybeFlush();

        // for each row, delete a tuple element
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("UPDATE %s SET v.x = null WHERE k = ? AND c = ?", k, c);
            }

            // test aggregation on single partition query
            assertPartitionCount(k, NUM_CLUSTERINGS);
        }
        maybeFlush();

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * NUM_CLUSTERINGS);

        // for each row, update a tuple element
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("UPDATE %s SET v.x = ? WHERE k = ? AND c = ?", value, k, c);
            }

            // test aggregation on single partition query
            assertPartitionCount(k, NUM_CLUSTERINGS);
        }
        maybeFlush();

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * NUM_CLUSTERINGS);

        // for each row, delete the tuple
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("DELETE v FROM %s WHERE k = ? AND c = ?", k, c);
            }

            // test aggregation on single partition query
            assertPartitionCount(k, NUM_CLUSTERINGS);
        }
        maybeFlush();

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * NUM_CLUSTERINGS);
    }

    @Test
    public void testTTLsWithSkinnyTable()
    {
        // we are only interested in the not NULL value case
        Assume.assumeTrue(value != null);

        createTable("CREATE TABLE %s (k int PRIMARY KEY, c int, v text)");

        String stringValue = ByteBufferUtil.bytesToHex(value);
        String v1 = stringValue + "_1";
        String v2 = stringValue + "_2";

        // insert some rows, and flush
        for (int k = 1; k <= NUM_PARTITIONS; k++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", k, v1);
        }
        maybeFlush();

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS);

        // Give a TTL the two first and two last partitions
        execute("UPDATE %s USING TTL 1 SET v = ? WHERE k = ? ", v2, 1);
        execute("UPDATE %s USING TTL 1 SET v = ? WHERE k = ? ", v2, NUM_PARTITIONS - 1);
        maybeFlush();

        // wait for the TTL to expire
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS);
    }

    @Test
    public void testTTLsWithWideTable()
    {
        // we are only interested in the not NULL value case
        Assume.assumeTrue(value != null);

        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY(k, c))");

        String stringValue = ByteBufferUtil.bytesToHex(value);
        String v1 = stringValue + "_1";
        String v2 = stringValue + "_2";

        // insert some rows, and flush
        for (int k = 1; k <= NUM_PARTITIONS; k++)
        {
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", k, c, v1);
            }
        }
        maybeFlush();

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * NUM_CLUSTERINGS);

        // for each partition, give a TTL the two first and two last clusterings
        for (int k = 1; k <= NUM_PARTITIONS; k++)
        {
            execute("UPDATE %s USING TTL 1 SET v = ? WHERE k = ? AND c = ?", v2, k, 1);
            execute("UPDATE %s USING TTL 1 SET v = ? WHERE k = ? AND c = ?", v2, k, NUM_CLUSTERINGS - 1);
        }
        maybeFlush();

        // wait for the TTL to expire
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * NUM_CLUSTERINGS);
    }

    @Test
    public void testAggregationWithPurgeableTombstones()
    {
        // create the table with a very short gc_grace_seconds, so there are tombstones to be purged on the replicas
        createTable("CREATE TABLE %s (k bigint, c int, v blob, PRIMARY KEY(k, c)) WITH gc_grace_seconds = 0");

        // insert some clusterings, and flush
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            // insert some clusterings
            for (int c = 1; c <= NUM_CLUSTERINGS; c++)
            {
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1", k, c, value);
            }
        }
        maybeFlush();

        // for each partition, update the first and last clustering
        for (long k = 1; k <= NUM_PARTITIONS; k++)
        {
            execute("DELETE v FROM %s USING TIMESTAMP 2 WHERE k = ? AND c = ?", k, 1);
            execute("DELETE v FROM %s USING TIMESTAMP 2 WHERE k = ? AND c = ?", k, NUM_CLUSTERINGS);

            // test aggregation on single partition query
            assertPartitionCount(k, NUM_CLUSTERINGS);
        }
        maybeFlush();

        // wait for the tombstones to be purgeable
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        // test aggregation on range query
        assertRangeCount(NUM_PARTITIONS * NUM_CLUSTERINGS);
    }

    private void assertPartitionCount(Object k, int expectedCount)
    {
        assertCount("SELECT * FROM %s WHERE k=?", "SELECT COUNT(*) FROM %s WHERE k=?", expectedCount, k);
    }

    private void assertRangeCount(int expectedCount)
    {
        assertCount("SELECT * FROM %s", "SELECT COUNT(*) FROM %s", expectedCount);
    }

    private void assertCount(String selectQuery, String countQuery, int expectedCount, Object... args)
    {
        int selectRows = execute(selectQuery, args).size();
        long selectCountRows = execute(countQuery, args).one().getLong("count");
        Assertions.assertThat(selectRows).isEqualTo(expectedCount); // both are consistent
        Assertions.assertThat(selectRows).isEqualTo(selectCountRows); // both are correct
    }

    private void maybeFlush()
    {
        if (flush)
            flush();
    }
}

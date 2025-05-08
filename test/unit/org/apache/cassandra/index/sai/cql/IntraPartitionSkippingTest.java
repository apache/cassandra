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

package org.apache.cassandra.index.sai.cql;

import org.junit.Ignore;
import org.junit.Test;

import org.HdrHistogram.Histogram;
import org.apache.cassandra.index.sai.SAITester;

/**
 * Tests for verifying intra-partition and partition-level skipping optimizations
 * introduced in CASSANDRA-20191 for SAI.
 * <p>
 * These tests validate that Cassandra can efficiently skip over rows
 * within a partition using clustering filters (name and slice), paging, reversed order,
 * and sparse matches.
 * <p>
 * Each test documents a scenario where skipping logic is expected to apply along with few where it doesn't skip.
 */
public class IntraPartitionSkippingTest extends SAITester
{
    @Test
    public void testNameFilterExactMatch() throws Throwable
    {
        createTable("CREATE TABLE %S (pk int, ck int, val text, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(val) USING 'sai'");

        for (int ck = 0; ck < 10; ck++)
        {
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", 1, ck, "val" + ck);
        }

        beforeAndAfterFlush(() -> assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND ck = 5 AND val = 'val5' ALLOW FILTERING"),
                                             row(1, 5,"val5")));
    }

    @Test
    public void testSliceFilterRangeMatch() throws Throwable
    {
        createTable("CREATE TABLE %S (pk int, ck int, val text, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(val) USING 'sai'");

        for (int ck = 0; ck < 100; ck++)
        {
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", 1, ck, "val" + ck);
        }

        beforeAndAfterFlush(() -> assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND ck > 90 AND val = 'val99' ALLOW FILTERING"),
                                             row(1, 99,"val99")));
    }

    @Test
    public void testReversedClustering() throws Throwable
    {
        createTable("CREATE TABLE %S (pk int, ck int, val text, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)");
        createIndex("CREATE INDEX ON %s(val) USING 'sai'");

        for (int ck = 0; ck < 20; ck++)
        {
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", 1, ck, "val" + ck);
        }

        beforeAndAfterFlush(() -> assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND ck < 10 AND val = 'val5'  ALLOW FILTERING"),
                                             row(1,5,"val5")));
    }

    @Test
    public void testSkippingWithPaging() throws Throwable
    {
        createTable("CREATE TABLE %S (pk int, ck int, val int, PRIMARY KEY (pk, ck))");

        createIndex("CREATE INDEX ON %s(val) USING 'sai'");

        for (int ck = 0; ck < 100; ck++)
        {
            int val = 1000 + ck;
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", 1, ck, val);
        }

        beforeAndAfterFlush(() -> assertRowsNet(executeNetWithPaging("SELECT * FROM %s WHERE pk = 1 AND ck > 90 AND val > 1090 ALLOW FILTERING", 5),
                      row(1, 91, 1091),
                      row(1, 92, 1092),
                      row(1, 93, 1093),
                      row(1, 94, 1094),
                      row(1, 95, 1095),
                      row(1, 96, 1096),
                      row(1, 97, 1097),
                      row(1, 98, 1098),
                      row(1, 99, 1099)));
    }

    @Test
    public void testCompositeClusteringKeySkipping() throws Throwable
    {
        createTable("CREATE TABLE %S (pk int, ck1 int, ck2 int, val text, PRIMARY KEY (pk, ck1, ck2))");
        createIndex("CREATE INDEX ON %s(val) USING 'sai'");

        for (int ck1 = 0; ck1 < 10; ck1++)
            for (int ck2 = 0; ck2 < 10; ck2++)
                execute("INSERT INTO %s (pk, ck1, ck2, val) VALUES (?, ?, ?, ?)", 1, ck1, ck2, "v" + (ck1*10+ck2));


        beforeAndAfterFlush(() -> assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND ck1 = 9 AND ck2 = 9 AND val = 'v99' ALLOW FILTERING"),
                                             row(1,9,9,"v99")));

    }

    @Test
    public void testSparseMatch() throws Throwable
    {
        createTable("CREATE TABLE %S (pk int, ck int, val text, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(val) USING 'sai'");

        for (int ck = 0; ck < 1000; ck++)
        {
            String value = (ck % 450 == 0) ? "insert" : "skip";
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", 1, ck, value);
        }

        beforeAndAfterFlush(() -> assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND ck > 899 AND val = 'insert' ALLOW FILTERING"),
                                             row(1,900,"insert")));

    }

    @Test
    public void testMultipleNameFilters() throws Throwable
    {
        createTable("CREATE TABLE %S (pk int, ck int, val text, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(val) USING 'sai'");

        for (int i = 0; i < 20; i++)
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", 1, i, "v5");

        beforeAndAfterFlush(() -> assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND ck IN (5, 10, 15) AND val = 'v5' ALLOW FILTERING"),
                                             row(1,5,"v5"), row(1,10,"v5"), row(1,15,"v5")));

    }

    // Multiple partition range scans won't skip
    @Test
    public void testPartitionRangeSkipping() throws Throwable
    {
        createTable("CREATE TABLE %S (pk int, ck int, val text, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(val) USING 'sai'");

        for (int pk = 0; pk < 10; pk++)
            for (int ck = 0; ck < 5; ck++)
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", pk, ck, "value" + pk);

        beforeAndAfterFlush(() -> assertRows(execute("SELECT * FROM %s WHERE val = 'value9' AND ck > 2 ALLOW FILTERING"),
                                             row(9,3,"value9"), row(9,4,"value9")));

    }

    @Test
    public void testStaticColumns() throws Throwable
    {
        createTable("CREATE TABLE %S (pk int, ck int, s text static, val text, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(val) USING 'sai'");

        execute("INSERT INTO %s (pk, s) VALUES (?, ?)", 1, "static1");

        for (int ck = 0; ck < 200; ck++)
        {
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", 1, ck, "val" + ck);
        }


        // We will not skip
        beforeAndAfterFlush(() -> assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND ck > 100 AND s = 'static1' AND val = 'val101' ALLOW FILTERING"),
                                             row(1,101,"static1","val101")));

        // we will skip
        beforeAndAfterFlush(() -> assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND ck > 100  AND val = 'val101' ALLOW FILTERING"),
                                             row(1,101,"static1","val101")));
    }

    @Test
    public void testNextKeyClusteringIndexNamesFilter() throws Throwable
    {
        createTable("CREATE TABLE %S (" +
                    "pk int," +
                    "ck int," +
                    "v int," +
                    "PRIMARY KEY (pk, ck))");

        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        int pk = 1;
        for (int ck = 0; ck < 10; ck++)
        {
            int v = ck + 1000;
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, v);
        }

        int pk1 = 2;
        for (int ck = 0; ck < 100; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk1, ck, ck);
        }

        beforeAndAfterFlush(() -> {
                                assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND ck = 5 AND v > 1004 ALLOW FILTERING"),
                                           row(1, 5, 1005));

                                assertRows(execute("SELECT * FROM %s WHERE pk = 1 AND ck = 5 AND v > 1004 AND v < 20000 ALLOW FILTERING"),
                                           row(1, 5, 1005));
                            });


    }

    // Performance testing test-cases and can be ingnored.
    @Ignore ("performance test case for Index Slice filter.")
    @Test
    public void testNextKeyPerfClusteringIndexSliceFilter()
    {
        createTable("CREATE TABLE %S (" +
                    "pk int, " +
                    "ck int, " +
                    "val text, " +
                    "PRIMARY KEY (pk, ck))");

        createIndex("CREATE INDEX ON %s(val) USING 'sai'");

        int pk = 1;
        for (int ck = 0; ck < 10000; ck++)
        {
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", pk, ck, "hello1");
        }

        int pk1 = 2;
        for (int ck = 0; ck < 100; ck++)
        {
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", pk1, ck, "hello2");
        }

        Histogram histogram = new Histogram(4);


        for (int i = 0; i < 10000; i++)
        {
            long start = System.nanoTime();
            execute("SELECT * FROM %s WHERE pk = 1 AND ck > 9000 AND val = 'hello1' ALLOW FILTERING");
            histogram.recordValue(System.nanoTime() - start);

            if (i % 1000 == 0)
            {
                System.out.println("50th: " + histogram.getValueAtPercentile(0.5));
                System.out.println("95th: " + histogram.getValueAtPercentile(0.95));
                System.out.println("99th: " + histogram.getValueAtPercentile(0.99));
            }
        }

    }


    @Ignore ("performance test case for Index Names filter.")
    @Test
    public void testNextKeyPerfClusteringIndexNamesFilter()
    {
        createTable("CREATE TABLE %S (" +
                    "pk int," +
                    "ck int," +
                    "v int," +
                    "PRIMARY KEY (pk, ck))");

        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        int pk = 1;
        for (int ck = 0; ck < 20000; ck++)
        {
            int v = ck + 10;
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, v);
        }

        int pk1 = 2;
        for (int ck = 0; ck < 100; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk1, ck, ck);
        }

        Histogram histogram = new Histogram(4);

        for (int i = 0; i < 10000; i++)
        {
            long start = System.nanoTime();
            execute("SELECT * FROM %s WHERE pk = 1 AND ck = 15000 AND v > 9000 ALLOW FILTERING");
            histogram.recordValue(System.nanoTime() - start);

            if (i % 1000 == 0)
            {
                System.out.println("50th: " + histogram.getValueAtPercentile(0.5));
                System.out.println("95th: " + histogram.getValueAtPercentile(0.95));
                System.out.println("99th: " + histogram.getValueAtPercentile(0.99));
            }
        }

    }

}

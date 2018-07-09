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

package org.apache.cassandra.tools.nodetool.stats;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

public class TableStatsPrinterTest extends TableStatsTestBase
{

    public static final String expectedDefaultTable1Output =
        "\tTable: %s\n" +
        "\tSSTable count: 60000\n" +
        "\tOld SSTable count: 0\n" +
        "\tSpace used (live): 0\n" +
        "\tSpace used (total): 9001\n" +
        "\tSpace used by snapshots (total): 1111\n" +
        "\tSSTable Compression Ratio: 0.68\n" +
        "\tNumber of partitions (estimate): 111111\n" +
        "\tMemtable cell count: 111\n" +
        "\tMemtable data size: 0\n" +
        "\tMemtable switch count: 1\n" +
        "\tLocal read count: 0\n" +
        "\tLocal read latency: 2.000 ms\n" +
        "\tLocal write count: 5\n" +
        "\tLocal write latency: 0.050 ms\n" +
        "\tPending flushes: 11111\n" +
        "\tPercent repaired: 100.0\n" +
        "\tBloom filter false positives: 30\n" +
        "\tBloom filter false ratio: 0.40000\n" +
        "\tBloom filter space used: 789\n" +
        "\tCompacted partition minimum bytes: 2\n" +
        "\tCompacted partition maximum bytes: 60\n" +
        "\tCompacted partition mean bytes: 6\n" +
        "\tAverage live cells per slice (last five minutes): 6.0\n" +
        "\tMaximum live cells per slice (last five minutes): 6\n" +
        "\tAverage tombstones per slice (last five minutes): 5.0\n" +
        "\tMaximum tombstones per slice (last five minutes): 1\n" +
        "\tDropped Mutations: 0\n" +
        "\n";

    public static final String expectedDefaultTable2Output =
        "\tTable: %s\n" +
        "\tSSTable count: 3000\n" +
        "\tOld SSTable count: 0\n" +
        "\tSpace used (live): 22\n" +
        "\tSpace used (total): 1024\n" +
        "\tSpace used by snapshots (total): 222\n" +
        "\tOff heap memory used (total): 314159367\n" +
        "\tSSTable Compression Ratio: 0.68\n" +
        "\tNumber of partitions (estimate): 22222\n" +
        "\tMemtable cell count: 22\n" +
        "\tMemtable data size: 900\n" +
        "\tMemtable off heap memory used: 314159265\n" +
        "\tMemtable switch count: 22222\n" +
        "\tLocal read count: 1\n" +
        "\tLocal read latency: 3.000 ms\n" +
        "\tLocal write count: 4\n" +
        "\tLocal write latency: 0.000 ms\n" +
        "\tPending flushes: 222222\n" +
        "\tPercent repaired: 99.9\n" +
        "\tBloom filter false positives: 600\n" +
        "\tBloom filter false ratio: 0.01000\n" +
        "\tBloom filter space used: 161718\n" +
        "\tBloom filter off heap memory used: 98\n" +
        "\tIndex summary off heap memory used: 1\n" +
        "\tCompression metadata off heap memory used: 3\n" +
        "\tCompacted partition minimum bytes: 4\n" +
        "\tCompacted partition maximum bytes: 30\n" +
        "\tCompacted partition mean bytes: 4\n" +
        "\tAverage live cells per slice (last five minutes): 4.01\n" +
        "\tMaximum live cells per slice (last five minutes): 5\n" + 
        "\tAverage tombstones per slice (last five minutes): 4.001\n" +
        "\tMaximum tombstones per slice (last five minutes): 2\n" +
        "\tDropped Mutations: 222\n" +
        "\n";

    public static final String expectedDefaultTable3Output =
        "\tTable: %s\n" +
        "\tSSTable count: 50000\n" +
        "\tOld SSTable count: 0\n" +
        "\tSpace used (live): 0\n" +
        "\tSpace used (total): 512\n" +
        "\tSpace used by snapshots (total): 0\n" +
        "\tSSTable Compression Ratio: 0.32\n" +
        "\tNumber of partitions (estimate): 3333\n" +
        "\tMemtable cell count: 333333\n" +
        "\tMemtable data size: 1999\n" +
        "\tMemtable switch count: 3333\n" +
        "\tLocal read count: 2\n" +
        "\tLocal read latency: 4.000 ms\n" +
        "\tLocal write count: 3\n" +
        "\tLocal write latency: NaN ms\n" +
        "\tPending flushes: 333\n" +
        "\tPercent repaired: 99.8\n" +
        "\tBloom filter false positives: 20\n" +
        "\tBloom filter false ratio: 0.50000\n" +
        "\tBloom filter space used: 456\n" +
        "\tCompacted partition minimum bytes: 2\n" +
        "\tCompacted partition maximum bytes: 50\n" +
        "\tCompacted partition mean bytes: 5\n" +
        "\tAverage live cells per slice (last five minutes): 0.0\n" +
        "\tMaximum live cells per slice (last five minutes): 5\n" +
        "\tAverage tombstones per slice (last five minutes): NaN\n" +
        "\tMaximum tombstones per slice (last five minutes): 3\n" +
        "\tDropped Mutations: 33333\n" +
        "\n";

    public static final String expectedDefaultTable4Output =
        "\tTable: %s\n" +
        "\tSSTable count: 2000\n" +
        "\tOld SSTable count: 0\n" +
        "\tSpace used (live): 4444\n" +
        "\tSpace used (total): 256\n" +
        "\tSpace used by snapshots (total): 44\n" +
        "\tOff heap memory used (total): 441213818\n" +
        "\tSSTable Compression Ratio: 0.95\n" +
        "\tNumber of partitions (estimate): 444\n" +
        "\tMemtable cell count: 4\n" +
        "\tMemtable data size: 3000\n" +
        "\tMemtable off heap memory used: 141421356\n" +
        "\tMemtable switch count: 444444\n" +
        "\tLocal read count: 3\n" +
        "\tLocal read latency: NaN ms\n" +
        "\tLocal write count: 2\n" +
        "\tLocal write latency: 2.000 ms\n" +
        "\tPending flushes: 4444\n" +
        "\tPercent repaired: 50.0\n" +
        "\tBloom filter false positives: 500\n" +
        "\tBloom filter false ratio: 0.02000\n" +
        "\tBloom filter space used: 131415\n" +
        "\tBloom filter off heap memory used: 299792458\n" +
        "\tIndex summary off heap memory used: 2\n" +
        "\tCompression metadata off heap memory used: 2\n" +
        "\tCompacted partition minimum bytes: 5\n" +
        "\tCompacted partition maximum bytes: 20\n" +
        "\tCompacted partition mean bytes: 4\n" +
        "\tAverage live cells per slice (last five minutes): NaN\n" +
        "\tMaximum live cells per slice (last five minutes): 3\n" +
        "\tAverage tombstones per slice (last five minutes): 0.0\n" +
        "\tMaximum tombstones per slice (last five minutes): 3\n" +
        "\tDropped Mutations: 4444\n" +
        "\n";

    public static final String expectedDefaultTable5Output =
        "\tTable: %s\n" +
        "\tSSTable count: 40000\n" +
        "\tOld SSTable count: 0\n" +
        "\tSpace used (live): 55555\n" +
        "\tSpace used (total): 64\n" +
        "\tSpace used by snapshots (total): 55555\n" +
        "\tSSTable Compression Ratio: 0.99\n" +
        "\tNumber of partitions (estimate): 55\n" +
        "\tMemtable cell count: 55555\n" +
        "\tMemtable data size: 20000\n" +
        "\tMemtable switch count: 5\n" +
        "\tLocal read count: 4\n" +
        "\tLocal read latency: 0.000 ms\n" +
        "\tLocal write count: 1\n" +
        "\tLocal write latency: 1.000 ms\n" +
        "\tPending flushes: 5\n" +
        "\tPercent repaired: 93.0\n" +
        "\tBloom filter false positives: 10\n" +
        "\tBloom filter false ratio: 0.60000\n" +
        "\tBloom filter space used: 123\n" +
        "\tCompacted partition minimum bytes: 3\n" +
        "\tCompacted partition maximum bytes: 40\n" +
        "\tCompacted partition mean bytes: 4\n" +
        "\tAverage live cells per slice (last five minutes): 4.0\n" +
        "\tMaximum live cells per slice (last five minutes): 3\n" +
        "\tAverage tombstones per slice (last five minutes): 4.01\n" +
        "\tMaximum tombstones per slice (last five minutes): 5\n" +
        "\tDropped Mutations: 0\n" +
        "\n";

    public static final String expectedDefaultTable6Output =
        "\tTable: %s\n" +
        "\tSSTable count: 1000\n" +
        "\tOld SSTable count: 0\n" +
        "\tSpace used (live): 666666\n" +
        "\tSpace used (total): 0\n" +
        "\tSpace used by snapshots (total): 0\n" +
        "\tOff heap memory used (total): 162470810\n" +
        "\tSSTable Compression Ratio: 0.68\n" +
        "\tNumber of partitions (estimate): 6\n" +
        "\tMemtable cell count: 6666\n" +
        "\tMemtable data size: 1000000\n" +
        "\tMemtable off heap memory used: 161803398\n" +
        "\tMemtable switch count: 6\n" +
        "\tLocal read count: 5\n" +
        "\tLocal read latency: 1.000 ms\n" +
        "\tLocal write count: 0\n" +
        "\tLocal write latency: 0.500 ms\n" +
        "\tPending flushes: 66\n" +
        "\tPercent repaired: 0.0\n" +
        "\tBloom filter false positives: 400\n" +
        "\tBloom filter false ratio: 0.03000\n" +
        "\tBloom filter space used: 101112\n" +
        "\tBloom filter off heap memory used: 667408\n" +
        "\tIndex summary off heap memory used: 3\n" +
        "\tCompression metadata off heap memory used: 1\n" +
        "\tCompacted partition minimum bytes: 6\n" +
        "\tCompacted partition maximum bytes: 20\n" +
        "\tCompacted partition mean bytes: 3\n" +
        "\tAverage live cells per slice (last five minutes): 5.0\n" +
        "\tMaximum live cells per slice (last five minutes): 2\n" +
        "\tAverage tombstones per slice (last five minutes): 6.0\n" +
        "\tMaximum tombstones per slice (last five minutes): 6\n" +
        "\tDropped Mutations: 666666\n" +
        "\n";

    /**
     * Expected output of TableStatsPrinter DefaultPrinter for this dataset.
     * Total number of tables is zero because it's non-trivial to simulate that metric
     * without leaking test implementation into the TableStatsHolder implementation.
     */
    public static final String expectedDefaultPrinterOutput =
        "Total number of tables: 0\n" +
        "----------------\n" +
        "Keyspace : keyspace1\n" +
        "\tRead Count: 3\n" +
        "\tRead Latency: 0.0 ms\n" +
        "\tWrite Count: 12\n" +
        "\tWrite Latency: 0.0 ms\n" +
        "\tPending Flushes: 233666\n" +
        String.format(expectedDefaultTable1Output, "table1").replace("\t", "\t\t") +
        String.format(expectedDefaultTable2Output, "table2").replace("\t", "\t\t") +
        String.format(expectedDefaultTable3Output, "table3").replace("\t", "\t\t") +
        "----------------\n" +
        "Keyspace : keyspace2\n" +
        "\tRead Count: 7\n" +
        "\tRead Latency: 0.0 ms\n" +
        "\tWrite Count: 3\n" +
        "\tWrite Latency: 0.0 ms\n" +
        "\tPending Flushes: 4449\n" +
        String.format(expectedDefaultTable4Output, "table4").replace("\t", "\t\t") +
        String.format(expectedDefaultTable5Output, "table5").replace("\t", "\t\t") +
        "----------------\n" +
        "Keyspace : keyspace3\n" +
        "\tRead Count: 5\n" +
        "\tRead Latency: 0.0 ms\n" +
        "\tWrite Count: 0\n" +
        "\tWrite Latency: NaN ms\n" +
        "\tPending Flushes: 66\n" +
        String.format(expectedDefaultTable6Output, "table6").replace("\t", "\t\t") +
        "----------------\n";

    /**
     * Expected output from SortedDefaultPrinter for data sorted by reads in this test.
     */
    private static final String expectedSortedDefaultPrinterOutput =
        "Total number of tables: 0\n" +
        "----------------\n" +
        String.format(expectedDefaultTable6Output, "keyspace3.table6") +
        String.format(expectedDefaultTable5Output, "keyspace2.table5") +
        String.format(expectedDefaultTable4Output, "keyspace2.table4") +
        String.format(expectedDefaultTable3Output, "keyspace1.table3") +
        String.format(expectedDefaultTable2Output, "keyspace1.table2") +
        String.format(expectedDefaultTable1Output, "keyspace1.table1") +
        "----------------\n";

    /**
     * Expected output from SortedDefaultPrinter for data sorted by reads and limited to the top 4 tables.
     */
    private static final String expectedSortedDefaultPrinterTopOutput =
        "Total number of tables: 0 (showing top 0 by %s)\n" +
        "----------------\n" +
        String.format(expectedDefaultTable6Output, "keyspace3.table6") +
        String.format(expectedDefaultTable5Output, "keyspace2.table5") +
        String.format(expectedDefaultTable4Output, "keyspace2.table4") +
        String.format(expectedDefaultTable3Output, "keyspace1.table3") +
        "----------------\n";

    /**
     * Expected output from SortedDefaultPrinter for data sorted by reads and limited to the top 10 tables.
     */
    private static final String expectedSortedDefaultPrinterLargeTopOutput =
        "Total number of tables: 0 (showing top 0 by %s)\n" +
        "----------------\n" +
        String.format(expectedDefaultTable6Output, "keyspace3.table6") +
        String.format(expectedDefaultTable5Output, "keyspace2.table5") +
        String.format(expectedDefaultTable4Output, "keyspace2.table4") +
        String.format(expectedDefaultTable3Output, "keyspace1.table3") +
        String.format(expectedDefaultTable2Output, "keyspace1.table2") +
        String.format(expectedDefaultTable1Output, "keyspace1.table1") +
        "----------------\n";

    @Test
    public void testDefaultPrinter() throws Exception
    {
        StatsHolder holder = new TestTableStatsHolder(testKeyspaces, "", 0);
        StatsPrinter printer = TableStatsPrinter.from("", false);
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        printer.print(holder, new PrintStream(byteStream));
        assertEquals("StatsTablePrinter.DefaultPrinter does not print test vector as expected", expectedDefaultPrinterOutput, byteStream.toString());
    }

    @Test
    public void testSortedDefaultPrinter() throws Exception
    {
        // test sorting
        StatsHolder holder = new TestTableStatsHolder(testKeyspaces, "reads", 0);
        StatsPrinter printer = TableStatsPrinter.from("reads", true);
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        printer.print(holder, new PrintStream(byteStream));
        assertEquals("StatsTablePrinter.SortedDefaultPrinter does not print sorted tables as expected",
                     expectedSortedDefaultPrinterOutput, byteStream.toString());
        byteStream.reset();
        // test sorting and filtering top k, where k < total number of tables
        String sortKey = "reads";
        int top = 4;
        holder = new TestTableStatsHolder(testKeyspaces, sortKey, top);
        printer = TableStatsPrinter.from(sortKey, true);
        printer.print(holder, new PrintStream(byteStream));
        assertEquals("StatsTablePrinter.SortedDefaultPrinter does not print top K sorted tables as expected",
                     String.format(expectedSortedDefaultPrinterTopOutput, sortKey), byteStream.toString());
        byteStream.reset();
        // test sorting and filtering top k, where k >= total number of tables
        sortKey = "reads";
        top = 10;
        holder = new TestTableStatsHolder(testKeyspaces, sortKey, top);
        printer = TableStatsPrinter.from(sortKey, true);
        printer.print(holder, new PrintStream(byteStream));
        assertEquals("StatsTablePrinter.SortedDefaultPrinter does not print top K sorted tables as expected for large values of K",
                     String.format(expectedSortedDefaultPrinterLargeTopOutput, sortKey), byteStream.toString());
    }

    /**
     * A test version of TableStatsHolder to hold a test vector instead of gathering stats from a live cluster.
     */
    private static class TestTableStatsHolder extends TableStatsHolder
    {

        public TestTableStatsHolder(List<StatsKeyspace> testKeyspaces, String sortKey, int top)
        {
            super(null, false, false, new ArrayList<>(), sortKey, top);
            this.keyspaces.clear();
            this.keyspaces.addAll(testKeyspaces);
        }

        @Override
        protected boolean isTestTableStatsHolder()
        {
            return true;
        }
    }

}

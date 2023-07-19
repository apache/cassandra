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

import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StatsTableComparatorTest extends TableStatsTestBase
{

    /**
     * Builds a string of the format "table1 > table2 > ... > tableN-1 > tableN"
     * to show the order of StatsTables in a sorted list.
     * @return String a string showing the relative position in the list of its StatsTables
     */
    private String buildSortOrderString(List<StatsTable> sorted) {
        if (sorted == null)
            return null;
        if (sorted.size() == 0)
            return "";
        StringBuilder names = new StringBuilder(sorted.get(0).tableName);
        for (int i = 1; i < sorted.size(); i++)
            names.append(" > ").append(sorted.get(i).tableName);
        return names.toString();
    }

    /**
     * Reusable runner to test whether StatsTableComparator achieves the expected order for the given parameters.
     */
    private void runCompareTest(List<StatsTable> vector, String sortKey, String expectedOrder,
                                boolean humanReadable, boolean ascending)
    {
        vector.sort(new StatsTableComparator(sortKey, humanReadable, ascending));
        String failureMessage = String.format("StatsTableComparator failed to sort by %s", sortKey);
        assertEquals(failureMessage, expectedOrder, buildSortOrderString(vector));
    }

    @Test
    public void testCompareDoubles()
    {
        boolean humanReadable = false;
        boolean ascending = false;
        // average live cells: 1 > 6 > 2 > 5 > 3 > 4
        runCompareTest(testTables,
                       "average_live_cells_per_slice_last_five_minutes",
                       "table1 > table6 > table2 > table5 > table3 > table4",
                       humanReadable,
                       ascending);
        // average tombstones: 6 > 1 > 5 > 2 > 3 > 4
        runCompareTest(testTables,
                       "average_tombstones_per_slice_last_five_minutes",
                       "table6 > table1 > table5 > table2 > table3 > table4",
                       humanReadable,
                       ascending);
        // read latency: 3 > 2 > 1 > 6 > 4 > 5
        runCompareTest(testTables,
                       "read_latency",
                       "table3 > table2 > table1 > table6 > table4 > table5",
                       humanReadable,
                       ascending);
        // write latency: 4 > 5 > 6 > 1 > 2 > 3
        runCompareTest(testTables,
                       "write_latency",
                       "table4 > table5 > table6 > table1 > table2 > table3",
                       humanReadable,
                       ascending);
        // percent repaired
        runCompareTest(testTables,
                       "percent_repaired",
                       "table1 > table2 > table3 > table5 > table4 > table6",
                       humanReadable,
                       ascending);
    }

    @Test
    public void testCompareLongs()
    {
        boolean humanReadable = false;
        boolean ascending = false;
        // reads: 6 > 5 > 4 > 3 > 2 > 1
        runCompareTest(testTables,
                       "reads",
                       "table6 > table5 > table4 > table3 > table2 > table1",
                       humanReadable,
                       ascending);
        // writes: 1 > 2 > 3 > 4 > 5 > 6
        runCompareTest(testTables,
                       "writes",
                       "table1 > table2 > table3 > table4 > table5 > table6",
                       humanReadable,
                       ascending);
        // compacted partition maximum bytes: 1 > 3 > 5 > 2 > 4 = 6 
        runCompareTest(testTables,
                       "compacted_partition_maximum_bytes",
                       "table1 > table3 > table5 > table2 > table4 > table6",
                       humanReadable,
                       ascending);
        // compacted partition mean bytes: 1 > 3 > 2 = 4 = 5 > 6
        runCompareTest(testTables,
                       "compacted_partition_mean_bytes",
                       "table1 > table3 > table2 > table4 > table5 > table6",
                       humanReadable,
                       ascending);
        // compacted partition minimum bytes: 6 > 4 > 2 > 5 > 1 = 3
        runCompareTest(testTables,
                       "compacted_partition_minimum_bytes",
                       "table6 > table4 > table2 > table5 > table1 > table3",
                       humanReadable,
                       ascending);
        // maximum live cells last five minutes: 1 > 2 = 3 > 4 = 5 > 6
        runCompareTest(testTables,
                       "maximum_live_cells_per_slice_last_five_minutes",
                       "table1 > table2 > table3 > table4 > table5 > table6",
                       humanReadable,
                       ascending);
        // maximum tombstones last five minutes: 6 > 5 > 3 = 4 > 2 > 1
        runCompareTest(testTables,
                       "maximum_tombstones_per_slice_last_five_minutes",
                       "table6 > table5 > table3 > table4 > table2 > table1",
                       humanReadable,
                       ascending);
    }

    @Test
    public void testCompareHumanReadable()
    {
        boolean humanReadable = true;
        boolean ascending = false;
        // human readable space used total: 6 > 5 > 4 > 3 > 2 > 1
        runCompareTest(humanReadableTables,
                       "space_used_total",
                       "table6 > table5 > table4 > table3 > table2 > table1",
                       humanReadable,
                       ascending);
        // human readable memtable data size: 1 > 3 > 5 > 2 > 4 > 6
        runCompareTest(humanReadableTables,
                       "memtable_data_size",
                       "table1 > table3 > table5 > table2 > table4 > table6",
                       humanReadable,
                       ascending);
    }

    @Test
    public void testCompareObjects()
    {
        boolean humanReadable = false;
        boolean ascending = false;
        // bloom filter false positives: 2 > 4 > 6 > 1 > 3 > 5
        runCompareTest(testTables,
                       "bloom_filter_false_positives",
                       "table2 > table4 > table6 > table1 > table3 > table5",
                       humanReadable,
                       ascending);
        // bloom filter false positive ratio: 5 > 3 > 1 > 6 > 4 > 2
        runCompareTest(testTables,
                       "bloom_filter_false_ratio",
                       "table5 > table3 > table1 > table6 > table4 > table2",
                       humanReadable,
                       ascending);
        // memtable cell count: 3 > 5 > 6 > 1 > 2 > 4
        runCompareTest(testTables,
                       "memtable_cell_count",
                       "table3 > table5 > table6 > table1 > table2 > table4",
                       humanReadable,
                       ascending);
        // memtable switch count: 4 > 2 > 3 > 6 > 5 > 1
        runCompareTest(testTables,
                       "memtable_switch_count",
                       "table4 > table2 > table3 > table6 > table5 > table1",
                       humanReadable,
                       ascending);
        // number of partitions estimate: 1 > 2 > 3 > 4 > 5 > 6
        runCompareTest(testTables,
                       "number_of_partitions_estimate",
                       "table1 > table2 > table3 > table4 > table5 > table6",
                       humanReadable,
                       ascending);
        // pending flushes: 2 > 1 > 4 > 3 > 6 > 5
        runCompareTest(testTables,
                       "pending_flushes",
                       "table2 > table1 > table4 > table3 > table6 > table5",
                       humanReadable,
                       ascending);
        // sstable compression ratio: 5 > 4 > 1 = 2 = 6 > 3
        runCompareTest(testTables,
                       "sstable_compression_ratio",
                       "table5 > table4 > table1 > table2 > table6 > table3",
                       humanReadable,
                       ascending);
        // sstable count: 1 > 3 > 5 > 2 > 4 > 6
        runCompareTest(testTables,
                       "sstable_count",
                       "table1 > table3 > table5 > table2 > table4 > table6",
                       humanReadable,
                       ascending);
        runCompareTest(testTables,
                       "twcs_max_duration",
                       "table2 > table4 > table1 > table3 > table6 > table5",
                       humanReadable,
                       ascending);
    }

    @Test
    public void testCompareOffHeap()
    {
        boolean humanReadable = false;
        boolean ascending = false;
        // offheap memory total: 4 > 2 > 6 > 1 = 3 = 5 
        runCompareTest(testTables,
                       "off_heap_memory_used_total",
                       "table4 > table2 > table6 > table1 > table3 > table5",
                       humanReadable,
                       ascending);
        // bloom filter offheap: 4 > 6 > 2 > 1 > 3 > 5
        runCompareTest(testTables,
                       "bloom_filter_off_heap_memory_used",
                       "table4 > table6 > table2 > table1 > table3 > table5",
                       humanReadable,
                       ascending);
        // compression metadata offheap: 2 > 4 > 6 > 1 = 3 = 5
        runCompareTest(testTables,
                       "compression_metadata_off_heap_memory_used",
                       "table2 > table4 > table6 > table1 > table3 > table5",
                       humanReadable,
                       ascending);
        // index summary offheap: 6 > 4 > 2 > 1 = 3 = 5
        runCompareTest(testTables,
                       "index_summary_off_heap_memory_used",
                       "table6 > table4 > table2 > table1 > table3 > table5",
                       humanReadable,
                       ascending);
        // memtable offheap: 2 > 6 > 4 > 1 = 3 = 5
        runCompareTest(testTables,
                       "memtable_off_heap_memory_used",
                       "table2 > table6 > table4 > table1 > table3 > table5",
                       humanReadable,
                       ascending);
    }

    @Test
    public void testCompareStrings()
    {
        boolean humanReadable = false;
        boolean ascending = false;
        // full name (keyspace.table) ascending: 1 > 2 > 3 > 4 > 5 > 6
        runCompareTest(testTables,
                       "full_name",
                       "table1 > table2 > table3 > table4 > table5 > table6",
                       humanReadable,
                       true);
        // table name ascending: 6 > 5 > 4 > 3 > 2 > 1
        runCompareTest(testTables,
                       "table_name",
                       "table1 > table2 > table3 > table4 > table5 > table6",
                       humanReadable,
                       true);
        // bloom filter space used: 2 > 4 > 6 > 1 > 3 > 5
        runCompareTest(testTables,
                       "bloom_filter_space_used",
                       "table2 > table4 > table6 > table1 > table3 > table5",
                       humanReadable,
                       ascending);
        // space used by snapshots: 5 > 1 > 2 > 4 > 3 = 6
        runCompareTest(testTables,
                       "space_used_by_snapshots_total",
                       "table5 > table1 > table2 > table4 > table3 > table6",
                       humanReadable,
                       ascending);
        // space used live: 6 > 5 > 4 > 2 > 1 = 3
        runCompareTest(testTables,
                       "space_used_live",
                       "table6 > table5 > table4 > table2 > table1 > table3",
                       humanReadable,
                       ascending);
        // space used total: 1 > 2 > 3 > 4 > 5 > 6
        runCompareTest(testTables,
                       "space_used_total",
                       "table1 > table2 > table3 > table4 > table5 > table6",
                       humanReadable,
                       ascending);
        // memtable data size: 6 > 5 > 4 > 3 > 2 > 1
        runCompareTest(testTables,
                       "memtable_data_size",
                       "table6 > table5 > table4 > table3 > table2 > table1",
                       humanReadable,
                       ascending);
    }

}

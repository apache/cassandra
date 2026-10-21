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

package org.apache.cassandra.db.compaction.simple;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter;

import static org.apache.cassandra.utils.TestHelper.verifyAndPrint;
import static org.junit.Assert.assertEquals;

/**
 * Compaction of a table with bloom filters disabled. {@code bloom_filter_fp_chance = 1.0} is the
 * documented way to switch them off: {@code FilterFactory} then returns its {@code AlwaysPresent}
 * singleton, which is an {@code IFilter} but NOT a {@code BloomFilter}. The BIG cursor index writer
 * must not assume the concrete class — the iterator path goes through the {@code IFilter} interface,
 * where {@code add()} is a no-op — and a writer that casts unconditionally throws
 * {@code ClassCastException}, so the table cannot be cursor-compacted at all.
 * <p>
 * This scenario belongs here rather than in the differential suite because the defect it guards is
 * caught by the compaction *completing*, not by comparing the two paths' output bytes: a
 * {@code ClassCastException} fails any test that runs the cursor path over such a table. No
 * differential scenario disables the filter, so nothing byte-compares this shape — and it does not
 * need to: a compaction that throws is not a divergence, and the filter both paths write is the
 * same empty one. Here the merged rows are asserted through CQL and extended verification runs over
 * the output.
 */
public class CompactionDisabledBloomFilterTest extends SimpleCompactionTest
{
    @Test
    public void testCompactionWithBloomFilterDisabled() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : " +
                                         "'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( pk bigint, ck bigint, v1 bigint, " +
                                             "PRIMARY KEY(pk, ck)) WITH bloom_filter_fp_chance = 1.0");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // two overlapping generations: pk 5..9 exist in both, so the output is a genuine merge
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO " + table + " (pk, ck, v1) VALUES (?, ?, ?)", pk, ck, ck);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        for (long pk = 5; pk < 15; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO " + table + " (pk, ck, v1) VALUES (?, ?, ?)", pk, ck, ck + 100);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        assertEquals("the scenario needs two input sstables so that compaction merges rather than " +
                     "skipping", 2, cfs.getLiveSSTables().size());
        for (SSTableReader input : cfs.getLiveSSTables())
            assertFilterDisabled(input);
        assertCursorPathWillRun(cfs);

        majorCompact(cfs);

        assertEquals("expected a single compaction output", 1, cfs.getLiveSSTables().size());
        SSTableReader output = cfs.getLiveSSTables().iterator().next();
        assertFilterDisabled(output);

        // 15 partitions of 10 rows; pk 5..14 carry second-generation values, pk 5..9 by winning the merge
        List<Object[]> expected = new ArrayList<>();
        for (long pk = 0; pk < 15; pk++)
            for (long ck = 0; ck < 10; ck++)
                expected.add(new Object[]{ pk, ck, pk >= 5 ? ck + 100 : ck });
        assertRowsIgnoringOrder(execute("SELECT pk, ck, v1 FROM " + table), expected.toArray(new Object[0][]));

        verifyAndPrint(cfs, output);
    }

    /**
     * The sstable carries no filter data: {@code FilterFactory}'s always-present singleton serializes
     * to zero bytes, where any real bloom filter over a non-empty key set would not.
     * <p>
     * This is a precondition guard, and that is its whole job: it establishes that
     * {@code bloom_filter_fp_chance = 1.0} really did take effect, so the compaction really does reach
     * the {@code AlwaysPresent} branch the cursor index writer has to tolerate. Without it a later
     * schema-default change could leave a real filter in place and the test would pass while
     * exercising nothing. It is not a claim about the writer's own behaviour — the {@code IFilter}
     * instance comes from {@code SortedTableWriter} and is shared by both compaction paths, so no
     * cursor-path change could substitute a real filter here.
     */
    private static void assertFilterDisabled(SSTableReader sstable)
    {
        assertEquals("bloom_filter_fp_chance = 1.0 must leave no filter data in " + sstable.descriptor,
                     0L, ((SSTableReaderWithFilter) sstable).getFilterSerializedSize());
    }
}

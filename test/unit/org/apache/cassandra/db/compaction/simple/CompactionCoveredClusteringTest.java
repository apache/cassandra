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
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;

import static org.apache.cassandra.utils.TestHelper.verifyAndPrint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The clustering that reaches the metadata collector when a partition closes must be the last
 * unfiltered actually <em>written</em>, not the last one read.
 * <p>
 * A cursor holds a single {@code UnfilteredDescriptor} and overwrites it in place on every read,
 * including reads of unfiltereds that then merge away and never reach the output. So the value fed
 * to {@code MetadataCollector.updateClusteringValues} at partition close has to be a snapshot taken
 * at write time; a live reference to the cursor's descriptor describes whatever was read last, which
 * for a partition whose tail is shadowed away is a clustering the output does not contain. The
 * covered-clustering slice in {@code Statistics.db} then claims a range wider than the sstable holds.
 * <p>
 * The scenario below builds exactly that shape and asserts the resulting bound absolutely, on the
 * committed output. Absolutely rather than comparatively, because what the collector was told is
 * invisible to everything a compaction returns: the finalized covered-clustering slice is the only
 * place it surfaces, and the differential suite — which does cover the divergence, through
 * {@code Statistics.db} byte identity — can only say the two paths agree, never which of them is
 * right. All four parameter rows assert the same constants, so the two iterator rows are the oracle
 * for the two cursor rows.
 */
public class CompactionCoveredClusteringTest extends SimpleCompactionTest
{
    /** Rows at this timestamp outlive the delete and are written. */
    private static final long SURVIVING_TIMESTAMP = 2000;
    /** The partition delete: newer than the tail, older than the surviving head. */
    private static final long DELETE_TIMESTAMP = 1000;
    /** Rows at this timestamp are shadowed by the delete and written by neither path. */
    private static final long SHADOWED_TIMESTAMP = 500;

    /** Clusterings 0..4 survive; 5..9 are shadowed. The INPUT sstable's largest clustering is 9. */
    private static final int LAST_SURVIVING_CK = 4;
    private static final int LAST_SHADOWED_CK = 9;

    @Test
    public void testCoveredClusteringStopsAtLastWrittenRow() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : " +
                                         "'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // One partition, ten rows in clustering order: the head outlives the delete, the tail does not.
        for (int ck = 0; ck <= LAST_SHADOWED_CK; ck++)
            execute("INSERT INTO " + table + " (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
                    1, ck, ck, ck <= LAST_SURVIVING_CK ? SURVIVING_TIMESTAMP : SHADOWED_TIMESTAMP);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

        // The rows the merge must read and drop really are on disk, out to the largest clustering.
        // Without this the scenario could stop covering its subject silently: if the tail never
        // reached an sstable there would be nothing for a stale descriptor to describe.
        assertEquals("the scenario needs one input sstable holding the whole partition",
                     1, cfs.getLiveSSTables().size());
        SSTableReader input = cfs.getLiveSSTables().iterator().next();
        assertEquals("all ten rows must reach that one sstable", 10, input.getSSTableMetadata().totalRows);
        assertCoveredClustering("input", input, 0, LAST_SHADOWED_CK);

        // The delete has to land in its OWN sstable. Issued into the same memtable as the rows it
        // shadows, it would reconcile them away before anything was written and the compaction would
        // never see the tail at all.
        execute("DELETE FROM " + table + " USING TIMESTAMP ? WHERE pk = ?", DELETE_TIMESTAMP, 1);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        assertEquals("the delete must be a second sstable, not a memtable reconciliation",
                     2, cfs.getLiveSSTables().size());

        assertCursorPathWillRun(cfs);
        majorCompact(cfs);

        assertEquals("expected a single compaction output", 1, cfs.getLiveSSTables().size());
        SSTableReader output = cfs.getLiveSSTables().iterator().next();

        // ck 5..9 were read by the merge and written by neither path, so the covered clustering must
        // end at ck 4 — the last row written — and not at ck 9, the last row read.
        assertCoveredClustering("output", output, 0, LAST_SURVIVING_CK);

        assertOutputContents(output);
        assertRowsIgnoringOrder(execute("SELECT pk, ck, v FROM " + table),
                                row(1, 0, 0), row(1, 1, 1), row(1, 2, 2), row(1, 3, 3), row(1, 4, 4));

        // For suite consistency only: extended verification never looks at the covered clustering,
        // and its one comparator check is gated on a promoted index, which a five-row partition does
        // not have. It adds no coverage of the bound asserted above.
        verifyAndPrint(cfs, output);
    }

    /**
     * Asserts the covered-clustering slice the writer finalized for this sstable spans exactly the
     * given single-component bounds. {@code getSSTableMetadata()} hands back that finalized
     * {@code StatsMetadata} rather than re-reading it, and the same slice is what gets serialized
     * into {@code Statistics.db}.
     */
    private static void assertCoveredClustering(String role, SSTableReader sstable, int expectedMin, int expectedMax)
    {
        StatsMetadata stats = sstable.getSSTableMetadata();
        assertEquals(role + " covered clustering start must have the table's one clustering component",
                     1, stats.coveredClustering.start().size());
        assertEquals(role + " covered clustering end must have the table's one clustering component",
                     1, stats.coveredClustering.end().size());
        assertEquals(role + " covered clustering must start at the smallest clustering written",
                     expectedMin, (int) Int32Type.instance.compose(stats.coveredClustering.start().bufferAt(0)));
        assertEquals(role + " covered clustering must end at the largest clustering WRITTEN, not the " +
                     "largest one read",
                     expectedMax, (int) Int32Type.instance.compose(stats.coveredClustering.end().bufferAt(0)));
    }

    /**
     * Corroborates that ck 4 really is the last clustering in the output and ck 9 really is absent:
     * the shadowing delete survived the merge, and the rows it shadows did not. Without this the
     * covered-clustering assertion above could be satisfied by an output that simply dropped the tail
     * for some other reason — a purge, say — in which case nothing was ever "read but not written".
     */
    private static void assertOutputContents(SSTableReader output)
    {
        List<Integer> clusterings = new ArrayList<>();
        try (ISSTableScanner scanner = output.getScanner())
        {
            assertTrue("expected the compacted partition in the output", scanner.hasNext());
            UnfilteredRowIterator partition = scanner.next();
            assertFalse("the shadowing partition delete must survive the merge; if it purged, the " +
                        "tail was not dropped by shadowing", partition.partitionLevelDeletion().isLive());
            assertEquals("the surviving partition delete must be the one this scenario issued",
                         DELETE_TIMESTAMP, partition.partitionLevelDeletion().markedForDeleteAt());
            while (partition.hasNext())
            {
                Unfiltered unfiltered = partition.next();
                assertTrue("expected only rows in the output partition, got " + unfiltered.kind(),
                           unfiltered.isRow());
                clusterings.add(Int32Type.instance.compose(unfiltered.clustering().bufferAt(0)));
            }
            assertFalse("expected exactly one partition in the output", scanner.hasNext());
        }
        assertEquals("the shadowed tail must be absent and the head present, in clustering order",
                     List.of(0, 1, 2, 3, 4), clusterings);
    }
}

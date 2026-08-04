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

package org.apache.cassandra.db.partitions;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.utils.btree.BTree;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Row-level analogue of {@link SetCellAccountingTest}. Where that test grows/resets a single {@code set<text>}
 * column on one row, this test grows/resets the <em>rows of a single partition</em>:
 * <ul>
 *   <li>adding a row is the analogue of adding a set element</li>
 *   <li>a whole-partition tombstone is the analogue of a {@code set = {...}} override: it shadows every previously
 *       written row, just as a collection assignment shadows every previously written element.</li>
 * </ul>
 * It runs a grow/reset op mix on one partition with strictly increasing timestamps and asserts the memtable's
 * on/off-heap ownership never goes negative -- i.e. the update accounting never releases more than it allocated.
 */
public class PartitionRowAccountingTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionRowAccountingTest.class);

    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();
        try
        {
            Field confField = DatabaseDescriptor.class.getDeclaredField("conf");
            confField.setAccessible(true);
            Config conf = (Config) confField.get(null);
            conf.memtable_allocation_type = Config.MemtableAllocationType.offheap_objects;
            conf.memtable_cleanup_threshold = 0.8f; // to reduce risk of memtable switch during a test
        }
        catch (ReflectiveOperationException e)
        {
            throw new RuntimeException(e);
        }
        CQLTester.prepareServer();
    }

    private static void flushAllToBaseline()
    {
        for (Keyspace ks : Keyspace.all())
            for (ColumnFamilyStore c : ks.getColumnFamilyStores())
                c.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    private static final int MAX_ROUND_ATTEMPTS = 10;

    /**
     * Runs one round of a test -- the writes plus the ownership measurement taken at their end -- and returns that
     * measurement, guaranteeing it was read from the very memtables the round started writing into.
     * <p>
     * Ownership is only comparable while everything the round wrote is still held by the memtable it is measured from,
     * but a flush can roll a memtable at any moment (memtable pressure, the commitlog, another test's flush). So
     * instead of failing when that happens we discard the round and repeat it: every attempt starts from a freshly
     * flushed baseline, and replaying the identical writes (identical timestamps) rebuilds the identical state in the
     * new memtables. The measurement is taken inside {@code round}, before the memtables are re-checked, so a roll
     * between the writes and the measurement is caught too.
     */
    private <T> T onStableMemtables(Supplier<T> round, ColumnFamilyStore... cfss)
    {
        for (int attempt = 1; attempt <= MAX_ROUND_ATTEMPTS; attempt++)
        {
            flushAllToBaseline();
            Memtable[] started = currentMemtables(cfss);

            T measurement = round.get();

            if (stillCurrent(started, cfss))
                return measurement;

            logger.info("attempt " + attempt + '/' + MAX_ROUND_ATTEMPTS +
                        " discarded: a memtable rolled (flushed) mid-round, repeating the round");
        }
        throw new AssertionError("a memtable rolled (flushed) mid-round in each of the " + MAX_ROUND_ATTEMPTS +
                                 " attempts -- ownership measurement never valid");
    }

    private static Memtable[] currentMemtables(ColumnFamilyStore... cfss)
    {
        Memtable[] memtables = new Memtable[cfss.length];
        for (int i = 0; i < cfss.length; i++)
            memtables[i] = cfss[i].getCurrentMemtable();
        return memtables;
    }

    private static boolean stillCurrent(Memtable[] started, ColumnFamilyStore... cfss)
    {
        for (int i = 0; i < cfss.length; i++)
            if (started[i] != cfss[i].getCurrentMemtable())
                return false;
        return true;
    }

    /**
     * The on/off-heap ownership of the retain- and removeShadowed-path memtables, captured in one measurement.
     */
    private static class Owns
    {
        final long onHeapRetain, offHeapRetain, onHeapShadow, offHeapShadow;

        Owns(ColumnFamilyStore cfsRetain, ColumnFamilyStore cfsShadow)
        {
            onHeapRetain = ownsOnHeapNow(cfsRetain);
            offHeapRetain = ownsOffHeapNow(cfsRetain);
            onHeapShadow = ownsOnHeapNow(cfsShadow);
            offHeapShadow = ownsOffHeapNow(cfsShadow);
        }
    }

    private ColumnFamilyStore createTestTable()
    {
        createTable("CREATE TABLE %s (" +
                    "    pk text," +
                    "    ck int," +
                    "    last_contact timestamp," +
                    "    namespace text," +
                    "    properties text," +
                    "    state text," +
                    "    v text," +
                    "    PRIMARY KEY (pk, ck))");
        return getCurrentColumnFamilyStore();
    }

    @Test
    public void largePartitionGrowShrinkKeepOwnsNonNegative()
    {
        ColumnFamilyStore cfs = createTestTable();
        String tbl = KEYSPACE + '.' + currentTable();

        final int rowCount = BTree.MAX_KEYS + 1;
        final int ops = 2_000;
        final AtomicLong ts = new AtomicLong(1_000_000L);

        for (int op = 0; op < ops; op++)
        {
            String cql;
            int expectedLiveRows;
            if (op % 2 == 0)
            {
                // grow: add the full set of rows (each row ~ a set element)
                cql = growBatch(tbl, 0, rowCount, ts.incrementAndGet());
                expectedLiveRows = rowCount;
            }
            else
            {
                // reset: a whole-partition tombstone (~ "set = {...}") followed by a smaller set of rows.
                // the tombstone is at an earlier timestamp than the re-inserted rows so the latter survive,
                // while every previously written row (at an earlier timestamp) is shadowed by the tombstone.
                long deleteTs = ts.incrementAndGet();
                long insertTs = ts.incrementAndGet();
                cql = resetBatch(tbl, 0, rowCount / 2, deleteTs, insertTs);
                expectedLiveRows = rowCount / 2;
            }

            QueryProcessor.executeInternal(cql);

            UntypedResultSet rs = QueryProcessor.executeInternal("SELECT ck FROM " + tbl + " WHERE pk = 'test'");
            int liveRows = 0;
            if (rs != null)
                for (UntypedResultSet.Row ignored : rs)
                    liveRows++;

            if (op % 100 == 0)
                logger.info("== op=" + op +
                            ", liveRows= " + liveRows +
                            ", heapSize= " + ownsOnHeapNow(cfs) +
                            ", offheapSize= " + ownsOffHeapNow(cfs));

            // the grow/reset analogue must actually oscillate the visible row count, proving the partition
            // tombstone shadows the prior rows just as a set override shadows prior elements
            assertThat(liveRows).as("visible rows after op=" + op).isEqualTo(expectedLiveRows);
            assertOwnsNonNegative(cfs, "after op=" + op);
        }

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        assertOwnsNonNegative(cfs, "after flush");
    }

    /**
     * Builds two logically identical partitions -- a deleted wide row -- via two merge paths and requires their
     * on-heap ownership to be exactly equal:
     * <ul>
     *   <li>retain path: insert a row's cells, then delete the row (the tombstone shadows the existing, owned cells);</li>
     *   <li>removeShadowed path: delete the row first, then insert cells the tombstone already shadows.</li>
     * </ul>
     * Both end with the same columns, row tombstone and empty cell tree, so correct accounting leaves them owning the
     * same on-heap. Only the retain path can leak: if {@code BTreeRow.merge} fails to release the shrunk column tree or
     * the row's liveness/deletion delta, the retain partition owns more -- an inflation a non-negative-ownership check
     * cannot catch. Off-heap is not compared: under {@code offheap_objects} the retain path has written the cells into
     * the off-heap slab, only reclaimed at flush, so it legitimately owns more off-heap.
     */
    @Test
    public void rowTombstoneOverExistingRowDoesNotInflateOwnership()
    {
        ColumnFamilyStore cfsRetain = createWideTable();
        String tblRetain = KEYSPACE + '.' + currentTable();
        ColumnFamilyStore cfsShadow = createWideTable();
        String tblShadow = KEYSPACE + '.' + currentTable();

        final int rows = 50;
        final long insertTs = 1000L;
        final long deleteTs = 2000L; // newer than insertTs, so the tombstone shadows the cells

        Owns owns = onStableMemtables(() -> {
            for (int ck = 0; ck < rows; ck++)
            {
                // retain path: write the row, then delete it -> the tombstone shadows existing (owned) cells
                QueryProcessor.executeInternal(wideInsert(tblRetain, ck, insertTs));
                QueryProcessor.executeInternal(rowDelete(tblRetain, ck, deleteTs));

                // removeShadowed path: delete first, then write cells the tombstone already shadows
                QueryProcessor.executeInternal(rowDelete(tblShadow, ck, deleteTs));
                QueryProcessor.executeInternal(wideInsert(tblShadow, ck, insertTs));
            }
            return new Owns(cfsRetain, cfsShadow);
        }, cfsRetain, cfsShadow);

        long onHeapRetain = owns.onHeapRetain, onHeapShadow = owns.onHeapShadow;
        logger.info("retain-path onHeap=" + onHeapRetain + " offHeap=" + owns.offHeapRetain +
                    "; removeShadowed-path onHeap=" + onHeapShadow + " offHeap=" + owns.offHeapShadow);

        assertOwnsNonNegative(cfsRetain, "retain path");
        assertOwnsNonNegative(cfsShadow, "removeShadowed path");

        assertThat(onHeapRetain)
        .as("writing then deleting a row must leave the memtable owning exactly the same on-heap (%d bytes) as never " +
            "effectively writing it (%d bytes) -- the shrunk column tree and the row's liveness/deletion delta must be " +
            "accounted", onHeapRetain, onHeapShadow)
        .isEqualTo(onHeapShadow);
    }

    /**
     * Same as {@link #rowTombstoneOverExistingRowDoesNotInflateOwnership} but the shadowed data lives in a
     * <em>complex</em> (collection) column whose internal cell tree spans multiple BTree nodes (branch + leaves).
     * <p>
     * The retain path ({@code BTreeRow.merge} → {@code reconciler::retain} → {@code ColumnData.removeShadowed} for the
     * {@code set} column) drops every collection cell when the row tombstone supersedes them, but must also release the
     * collection's internal tree node structure -- which was counted as memtable-owned when the row was first inserted
     * ({@code ComplexColumnData.unsharedHeapSizeExcludingData} includes {@code sizeOnHeapOf(tree)}). If that internal
     * structure is not released, the retain partition owns more on-heap than the logically identical removeShadowed
     * partition.
     */
    @Test
    public void rowTombstoneOverExistingCollectionDoesNotInflateOwnership()
    {
        ColumnFamilyStore cfsRetain = createCollectionTable();
        String tblRetain = KEYSPACE + '.' + currentTable();
        ColumnFamilyStore cfsShadow = createCollectionTable();
        String tblShadow = KEYSPACE + '.' + currentTable();

        final int rows = 50;
        final long insertTs = 1000L;
        final long deleteTs = 2000L; // newer than insertTs, so the tombstone shadows the collection cells

        Owns owns = onStableMemtables(() -> {
            for (int ck = 0; ck < rows; ck++)
            {
                // retain path: write the row (large collection), then delete it -> the tombstone shadows existing
                // (owned) cells and the collection's multi-node internal tree
                QueryProcessor.executeInternal(setInsert(tblRetain, ck, insertTs));
                QueryProcessor.executeInternal(rowDelete(tblRetain, ck, deleteTs));

                // removeShadowed path: delete first, then write cells the tombstone already shadows
                QueryProcessor.executeInternal(rowDelete(tblShadow, ck, deleteTs));
                QueryProcessor.executeInternal(setInsert(tblShadow, ck, insertTs));
            }
            return new Owns(cfsRetain, cfsShadow);
        }, cfsRetain, cfsShadow);

        long onHeapRetain = owns.onHeapRetain, onHeapShadow = owns.onHeapShadow;
        logger.info("collection retain-path onHeap=" + onHeapRetain + " offHeap=" + owns.offHeapRetain +
                    "; removeShadowed-path onHeap=" + onHeapShadow + " offHeap=" + owns.offHeapShadow);

        assertOwnsNonNegative(cfsRetain, "retain path");
        assertOwnsNonNegative(cfsShadow, "removeShadowed path");

        assertThat(onHeapRetain)
        .as("writing then deleting a row with a multi-node collection must leave the memtable owning exactly the same " +
            "on-heap (%d bytes) as never effectively writing it (%d bytes) -- the collection's freed internal tree " +
            "structure must be accounted", onHeapRetain, onHeapShadow)
        .isEqualTo(onHeapShadow);
    }

    /**
     * Isolates the complex column's own tombstone heap, which the broader
     * {@link #rowTombstoneOverExistingCollectionDoesNotInflateOwnership} masks behind a large surviving cell tree.
     * <p>
     * A whole-collection delete ({@code DELETE s ...}) writes a {@link org.apache.cassandra.db.rows.ComplexColumnData}
     * carrying only a <em>non-LIVE</em> complex deletion and an <em>empty</em> cell tree, so its memtable-owned heap
     * ({@code unsharedHeapSizeExcludingData}) reduces to {@code EMPTY_SIZE + complexDeletion.unsharedHeapSize()} -- the
     * cell tree contributes nothing. When a newer row tombstone supersedes it, {@code ColumnData.removeShadowed} must
     * release that complex deletion's heap ({@code DeletionTime.EMPTY_SIZE}) and not just the wrapper. If that term is
     * dropped, the retain path strands exactly one {@code DeletionTime.EMPTY_SIZE} per row and owns more on-heap than
     * the logically identical removeShadowed path -- a gap the large-tree test cannot attribute and a non-negative
     * check cannot see. This is the drop-side half of the complex-deletion heap accounting; the swap-side (merge) half
     * is exercised exhaustively by {@code AtomicBTreePartitionMemtableAccountingTest}.
     */
    @Test
    public void rowTombstoneOverCollectionDeletionReleasesComplexDeletionHeap()
    {
        ColumnFamilyStore cfsRetain = createCollectionTable();
        String tblRetain = KEYSPACE + '.' + currentTable();
        ColumnFamilyStore cfsShadow = createCollectionTable();
        String tblShadow = KEYSPACE + '.' + currentTable();

        final int rows = 200;
        final long collectionDeleteTs = 1000L;
        final long rowDeleteTs = 2000L; // newer, so the row tombstone supersedes the collection tombstone

        Owns owns = onStableMemtables(() -> {
            for (int ck = 0; ck < rows; ck++)
            {
                // retain path: write a collection tombstone (an owned complex deletion over an empty cell tree),
                // then delete the row -> the row tombstone supersedes and releases it via reconciler::retain
                QueryProcessor.executeInternal(collectionDelete(tblRetain, ck, collectionDeleteTs));
                QueryProcessor.executeInternal(rowDelete(tblRetain, ck, rowDeleteTs));

                // removeShadowed path: delete the row first, then write a collection tombstone it already shadows
                QueryProcessor.executeInternal(rowDelete(tblShadow, ck, rowDeleteTs));
                QueryProcessor.executeInternal(collectionDelete(tblShadow, ck, collectionDeleteTs));
            }
            return new Owns(cfsRetain, cfsShadow);
        }, cfsRetain, cfsShadow);

        long onHeapRetain = owns.onHeapRetain, onHeapShadow = owns.onHeapShadow;
        logger.info("collection-tombstone retain-path onHeap=" + onHeapRetain +
                    "; removeShadowed-path onHeap=" + onHeapShadow);

        assertOwnsNonNegative(cfsRetain, "retain path");
        assertOwnsNonNegative(cfsShadow, "removeShadowed path");

        assertThat(onHeapRetain)
        .as("a row tombstone superseding a collection tombstone must release the complex deletion's own heap " +
            "(DeletionTime.EMPTY_SIZE per row): retain owns %d, removeShadowed owns %d", onHeapRetain, onHeapShadow)
        .isEqualTo(onHeapShadow);
    }

    private static String collectionDelete(String tbl, int ck, long timestamp)
    {
        return "DELETE s FROM " + tbl + " USING TIMESTAMP " + timestamp + " WHERE pk = 'test' AND ck = " + ck;
    }

    private static long ownsOnHeapNow(ColumnFamilyStore cfs)
    {
        return Memtable.getMemoryUsage(cfs.getTracker().getView().getCurrentMemtable()).ownsOnHeap;
    }

    private static long ownsOffHeapNow(ColumnFamilyStore cfs)
    {
        return Memtable.getMemoryUsage(cfs.getTracker().getView().getCurrentMemtable()).ownsOffHeap;
    }

    /**
     * A batch that inserts rows with clustering keys in [from,to), all at {@code timestamp}.
     */
    private static String growBatch(String tbl, int from, int to, long timestamp)
    {
        StringBuilder sb = new StringBuilder("BEGIN UNLOGGED BATCH\n");
        for (int ck = from; ck < to; ck++)
            appendInsert(sb, tbl, ck, timestamp);
        return sb.append("APPLY BATCH;").toString();
    }

    /**
     * A batch that first deletes the whole partition at {@code deleteTs} (a partition tombstone, the analogue of a
     * {@code set = {...}} override), then inserts rows with clustering keys in [from,to) at {@code insertTs > deleteTs}.
     */
    private static String resetBatch(String tbl, int from, int to, long deleteTs, long insertTs)
    {
        StringBuilder sb = new StringBuilder("BEGIN UNLOGGED BATCH\n");
        sb.append("DELETE FROM ").append(tbl).append(" USING TIMESTAMP ").append(deleteTs)
          .append(" WHERE pk = 'test';\n");
        for (int ck = from; ck < to; ck++)
            appendInsert(sb, tbl, ck, insertTs);
        return sb.append("APPLY BATCH;").toString();
    }

    private static void appendInsert(StringBuilder sb, String tbl, int ck, long timestamp)
    {
        sb.append("INSERT INTO ").append(tbl)
          .append(" (pk, ck, namespace, properties, state, v) VALUES ('test', ").append(ck)
          .append(", '").append(elemName(ck)).append('\'')
          .append(", '").append(elemName(ck)).append('\'')
          .append(", '").append(elemName(ck)).append('\'')
          .append(", '").append(elemName(ck)).append('\'')
          .append(") USING TIMESTAMP ").append(timestamp).append(";\n");
    }

    private static String elemName(int i)
    {
        return 'e' + String.format("%05d", i);
    }

    private static final int WIDE_COLS = BTree.MAX_KEYS + 1; // one more than fits in a leaf, so each row's column tree spans multiple nodes

    private ColumnFamilyStore createWideTable()
    {
        StringBuilder sb = new StringBuilder("CREATE TABLE %s (pk text, ck int");
        for (int i = 0; i < WIDE_COLS; i++)
            sb.append(", ").append(colName(i)).append(" int");
        sb.append(", PRIMARY KEY (pk, ck))");
        createTable(sb.toString());
        return getCurrentColumnFamilyStore();
    }

    private static String wideInsert(String tbl, int ck, long timestamp)
    {
        StringBuilder names = new StringBuilder("pk, ck");
        StringBuilder vals = new StringBuilder("'test', ").append(ck);
        for (int i = 0; i < WIDE_COLS; i++)
        {
            names.append(", ").append(colName(i));
            vals.append(", ").append(i);
        }
        return "INSERT INTO " + tbl + " (" + names + ") VALUES (" + vals + ") USING TIMESTAMP " + timestamp;
    }

    private static String rowDelete(String tbl, int ck, long timestamp)
    {
        return "DELETE FROM " + tbl + " USING TIMESTAMP " + timestamp + " WHERE pk = 'test' AND ck = " + ck;
    }

    // enough elements that each row's collection cell tree spans multiple nodes (branches + leaves), so the freed
    // internal structure is non-trivial
    private static final int SET_ELEMENTS = 4 * (BTree.MAX_KEYS + 1);

    private ColumnFamilyStore createCollectionTable()
    {
        createTable("CREATE TABLE %s (pk text, ck int, s set<text>, PRIMARY KEY (pk, ck))");
        return getCurrentColumnFamilyStore();
    }

    private static String setInsert(String tbl, int ck, long timestamp)
    {
        StringBuilder set = new StringBuilder("{");
        for (int i = 0; i < SET_ELEMENTS; i++)
        {
            if (i > 0)
                set.append(", ");
            set.append('\'').append(elemName(i)).append('\'');
        }
        set.append('}');
        return "INSERT INTO " + tbl + " (pk, ck, s) VALUES ('test', " + ck + ", " + set + ") USING TIMESTAMP " + timestamp;
    }

    private static String colName(int i)
    {
        return "c" + String.format("%03d", i);
    }

    private static void assertOwnsNonNegative(ColumnFamilyStore cfs, String step)
    {
        for (Memtable mt : cfs.getTracker().getView().getAllMemtables())
        {
            assertThat(Memtable.getMemoryUsage(mt).ownsOnHeap)
            .as("ON-heap owns went NEGATIVE [" + step + "]")
            .isGreaterThanOrEqualTo(0L);
            assertThat(Memtable.getMemoryUsage(mt).ownsOffHeap)
            .as("OFF-heap owns went NEGATIVE [" + step + "]")
            .isGreaterThanOrEqualTo(0L);
        }
    }
}

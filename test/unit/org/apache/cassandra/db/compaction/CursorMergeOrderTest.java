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

package org.apache.cassandra.db.compaction;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The checks that keep {@link CursorMergeOrder} in step with the cursor sort must fail with an
 * exception when they do not hold, also with assertions off, as in production.  A mismatch the
 * checks miss lets the cursor path write wrong data.
 */
public class CursorMergeOrderTest extends CQLTester
{
    static
    {
        // Production runs without -ea, so the checks must not depend on assertions.
        CursorMergeOrderTest.class.getClassLoader().setClassAssertionStatus(CursorMergeOrder.class.getName(), false);
    }

    private SSTableFormat<?, ?> originalFormat;
    private boolean originalCursorCompactionEnabled;

    @After
    public void restore()
    {
        if (originalFormat != null)
        {
            DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
            DatabaseDescriptor.setCursorCompactionEnabled(originalCursorCompactionEnabled);
        }
    }

    private void selectFormat(String format)
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        originalCursorCompactionEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
        DatabaseDescriptor.setCursorCompactionEnabled(true);
    }

    @Test
    public void assertionsAreOffForCursorMergeOrder()
    {
        assertThat(CursorMergeOrder.class.desiredAssertionStatus()).isFalse();
    }

    // END TO END: overlapping sstables inside one LeveledScanner, as a damaged manifest gives

    @Test
    public void overlappingSSTablesInOneLeveledScannerFailBig() throws Throwable
    {
        assertOverlappingSSTablesInOneLeveledScannerFail("big", false);
    }

    @Test
    public void overlappingSSTablesInOneLeveledScannerFailBti() throws Throwable
    {
        assertOverlappingSSTablesInOneLeveledScannerFail("bti", false);
    }

    /** The other scanner holds other partitions, so each merge group is no larger than the scanner count. */
    @Test
    public void overlappingSSTablesInOneLeveledScannerBesideOtherPartitionsFailBig() throws Throwable
    {
        assertOverlappingSSTablesInOneLeveledScannerFail("big", true);
    }

    @Test
    public void overlappingSSTablesInOneLeveledScannerBesideOtherPartitionsFailBti() throws Throwable
    {
        assertOverlappingSSTablesInOneLeveledScannerFail("bti", true);
    }

    /**
     * Two sstables that hold the same partitions reach the merge through one LeveledScanner, as if
     * the manifest put both in the same level.  The iterator path merges the scanner as one source
     * and fails on the keys out of order.  The cursor sort groups both cursors, but the partition
     * merge order counts the scanner once, so the cursor path must fail too.
     */
    private void assertOverlappingSSTablesInOneLeveledScannerFail(String format, boolean otherPartitions) throws Throwable
    {
        selectFormat(format);
        // decimal clustering values that compare equal can differ in bytes, so the table gets a CursorMergeOrder
        createTable("CREATE TABLE %s (pk int, ck decimal, v int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        List<SSTableReader> flushed = new ArrayList<>();
        for (int sstable = 0; sstable < 3; sstable++)
        {
            int first = otherPartitions && sstable == 2 ? 100 : 0;
            for (int pk = first; pk < first + 10; pk++)
                for (int ck = 0; ck < 3; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, new BigDecimal(ck + ".0"), sstable);
            flush();
            for (SSTableReader live : cfs.getLiveSSTables())
                if (!flushed.contains(live))
                    flushed.add(live);
        }
        assertThat(flushed).hasSize(3);
        Set<SSTableReader> inputs = cfs.getLiveSSTables();

        Throwable iterator = compact(cfs, inputs, flushed, false);
        assertThat(iterator).as("the iterator path").isNotNull();
        assertThat(iterator).hasMessageContaining("Last written key");

        Throwable cursor = compact(cfs, inputs, flushed, true);
        assertThat(cursor).as("the cursor path").isInstanceOf(IllegalStateException.class);
        assertThat(cursor).hasMessageMatching(otherPartitions ? "partition merge order found 1 sources, the cursor sort 2"
                                                              : "partition merge order found 2 sources, the cursor sort 3");
    }

    /** @return what the compaction threw, or null if it finished */
    private static Throwable compact(ColumnFamilyStore cfs, Set<SSTableReader> inputs, List<SSTableReader> flushed, boolean cursor)
    {
        long nowInSec = FBUtilities.nowInSeconds();
        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        assertThat(txn).isNotNull();
        List<ISSTableScanner> scanners = new ArrayList<>();
        scanners.add(new LeveledCompactionStrategy.LeveledScanner(cfs.metadata(), Arrays.asList(flushed.get(0), flushed.get(1)), null));
        scanners.add(flushed.get(2).getScanner());
        try (AbstractCompactionStrategy.ScannerList scannerList = new AbstractCompactionStrategy.ScannerList(scanners);
             CompactionController controller = new CompactionController(cfs, inputs, cfs.getDefaultGcBefore(nowInSec));
             DefaultCompactionWriter writer = new DefaultCompactionWriter(cfs, cfs.getDirectories(), txn, inputs))
        {
            if (cursor)
            {
                assertThat(CursorCompactor.isSupported(scannerList, controller)).as("the cursor path takes this input").isTrue();
                CursorCompactor compactor = new CursorCompactor(OperationType.COMPACTION, scanners, controller, nowInSec, TimeUUID.Generator.nextTimeUUID());
                try
                {
                    int written = 0;
                    while (compactor.writeNextPartition(writer))
                        written++;
                    throw new AssertionError("the cursor path wrote " + written + " partitions and did not fail");
                }
                finally
                {
                    compactor.close();
                }
            }
            try (CompactionIterator iter = new CompactionIterator(OperationType.COMPACTION, scanners, controller, nowInSec, TimeUUID.Generator.nextTimeUUID()))
            {
                while (iter.hasNext())
                {
                    try (UnfilteredRowIterator partition = iter.next())
                    {
                        writer.append(partition);
                    }
                }
            }
            return null;
        }
        catch (AssertionError e)
        {
            throw e;
        }
        catch (Throwable t)
        {
            return t;
        }
        finally
        {
            try
            {
                txn.abort();
            }
            catch (Throwable ignored)
            {
                // the writer's abort may already have aborted the transaction
            }
        }
    }

    // CONSTRUCTOR

    @Test
    public void moreCursorsThanScannerSSTablesFails() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck decimal, v int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 1.0, 0)");
        flush();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        try (ISSTableScanner scanner = sstable.getScanner();
             StatefulCursor a = new StatefulCursor(sstable, DiskAccessMode.standard);
             StatefulCursor b = new StatefulCursor(sstable, DiskAccessMode.standard))
        {
            assertThatThrownBy(() -> CursorMergeOrder.create(cfs.metadata(), Collections.singletonList(scanner),
                                                             Collections.singletonList(sstable), new StatefulCursor[]{ a, b }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("scanners hold 1 sstables, but there are 2 cursors");
        }
    }

    // CHECK HELPERS, called directly with a group that does not match the cursor sort

    /**
     * Two sstables that each hold {@code statement}'s partition 0, one cursor and one scanner for
     * each, and the CursorMergeOrder of the two.  The table has a decimal clustering and a set of
     * decimals, so the order handles both rows and cells.
     */
    private final class TwoSources implements AutoCloseable
    {
        final List<ISSTableScanner> scanners = new ArrayList<>();
        final StatefulCursor a;
        final StatefulCursor b;
        final CursorMergeOrder order;

        TwoSources(String statement) throws Throwable
        {
            createTable("CREATE TABLE %s (pk int, ck decimal, s set<decimal>, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            List<SSTableReader> sstables = new ArrayList<>();
            for (int i = 0; i < 2; i++)
            {
                execute(statement);
                flush();
                for (SSTableReader live : cfs.getLiveSSTables())
                    if (!sstables.contains(live))
                        sstables.add(live);
            }
            for (SSTableReader sstable : sstables)
                scanners.add(sstable.getScanner());
            a = new StatefulCursor(sstables.get(0), DiskAccessMode.standard);
            b = new StatefulCursor(sstables.get(1), DiskAccessMode.standard);
            order = CursorMergeOrder.create(cfs.metadata(), scanners, sstables, new StatefulCursor[]{ a, b });
            assertThat(order).isNotNull();
            a.readPartitionHeader();
            b.readPartitionHeader();
        }

        StatefulCursor[] both()
        {
            return new StatefulCursor[]{ a, b };
        }

        void partitionGroup(int partitionMergeLimit)
        {
            order.partitionGroup(partitionMergeLimit);
        }

        /** Moves both cursors to the first cell of the row. */
        void toFirstCell()
        {
            for (StatefulCursor cursor : both())
            {
                cursor.readRowHeader();
                cursor.readCellHeader();
                assertThat(cursor.cellCursor().producedCell).isTrue();
            }
        }

        public void close() throws Exception
        {
            a.close();
            b.close();
            for (ISSTableScanner scanner : scanners)
                scanner.close();
        }
    }

    private static final String ROW = "UPDATE %s SET s = s + {1.0} WHERE pk = 0 AND ck = 1.0";
    private static final boolean[] EQUAL = { true, false };

    @Test
    public void partitionGroupCountMismatchFails() throws Throwable
    {
        try (TwoSources sources = new TwoSources(ROW))
        {
            assertThatThrownBy(() -> sources.partitionGroup(1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("partition merge order found 2 sources, the cursor sort 1");
        }
    }

    @Test
    public void rowGroupCountMismatchFails() throws Throwable
    {
        try (TwoSources sources = new TwoSources(ROW))
        {
            sources.partitionGroup(2);
            sources.a.readRowHeader();
            sources.b.readRowHeader();
            assertThatThrownBy(() -> sources.order.orderUnfilteredGroup(sources.both(), EQUAL, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("row merge order found 2 sources, the cursor sort 1");
        }
    }

    /** A partition deletion alone: the partitions have no rows, so the row merge order finds no source. */
    @Test
    public void emptyRowGroupFails() throws Throwable
    {
        try (TwoSources sources = new TwoSources("DELETE FROM %s WHERE pk = 0"))
        {
            sources.partitionGroup(2);
            assertThatThrownBy(() -> sources.order.orderUnfilteredGroup(sources.both(), EQUAL, 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("row merge order found no source");
        }
    }

    @Test
    public void partitionMergeIndexOutOfRangeFails() throws Throwable
    {
        try (TwoSources sources = new TwoSources(ROW))
        {
            sources.partitionGroup(2);
            sources.b.partitionMergeIndex = 7;
            assertThatThrownBy(() -> sources.order.startRowCells(sources.both(), 2, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is not in the partition merge group");
        }
    }

    @Test
    public void partitionMergeIndexOfAnotherSourceFails() throws Throwable
    {
        try (TwoSources sources = new TwoSources(ROW))
        {
            sources.partitionGroup(2);
            sources.b.partitionMergeIndex = sources.a.partitionMergeIndex;
            assertThatThrownBy(() -> sources.order.startRowCells(sources.both(), 2, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is not in the partition merge group");
        }
    }

    /** The row merge order gives the column merge no source, but the cursor sort is at a cell. */
    @Test
    public void emptyColumnGroupFails() throws Throwable
    {
        try (TwoSources sources = new TwoSources(ROW))
        {
            sources.partitionGroup(2);
            sources.toFirstCell();
            sources.order.startRowCells(sources.both(), 0, false);
            assertThatThrownBy(() -> sources.order.orderCellGroup(sources.both(), EQUAL, 2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("column merge order is not at column");
        }
    }

    @Test
    public void cellGroupCountMismatchFails() throws Throwable
    {
        try (TwoSources sources = new TwoSources(ROW))
        {
            sources.partitionGroup(2);
            sources.toFirstCell();
            sources.order.startRowCells(sources.both(), 2, false);
            assertThatThrownBy(() -> sources.order.orderCellGroup(sources.both(), EQUAL, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("cell merge order found 2 sources, the cursor sort 1");
        }
    }

    @Test
    public void cellGroupMemberMismatchFails() throws Throwable
    {
        try (TwoSources sources = new TwoSources(ROW))
        {
            sources.partitionGroup(2);
            sources.toFirstCell();
            sources.order.startRowCells(sources.both(), 2, false);
            StatefulCursor[] sstableCursors = { sources.a, sources.a };
            assertThatThrownBy(() -> sources.order.orderCellGroup(sstableCursors, EQUAL, 2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("the cell merge order and the cursor sort hold different sources");
            // the group is not copied over the cursor sort's order
            assertThat(sstableCursors).containsExactly(sources.a, sources.a);
        }
    }

    /** Groups that match pass every check, and the cell group is copied in cell merge order. */
    @Test
    public void matchingGroupsPass() throws Throwable
    {
        try (TwoSources sources = new TwoSources(ROW))
        {
            StatefulCursor[] sstableCursors = sources.both();
            sources.partitionGroup(2);
            sources.a.readRowHeader();
            sources.b.readRowHeader();
            sources.order.orderUnfilteredGroup(sstableCursors, EQUAL, 2);
            assertThat(sstableCursors).containsExactlyInAnyOrder(sources.a, sources.b);
            sources.order.startRowCells(sstableCursors, 2, false);
            for (StatefulCursor cursor : sstableCursors)
                cursor.readCellHeader();
            sources.order.orderCellGroup(sstableCursors, EQUAL, 2);
            assertThat(sstableCursors).containsExactlyInAnyOrder(sources.a, sources.b);
        }
    }
}

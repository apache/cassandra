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

package org.apache.cassandra.db.rows;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.PartitionStatisticsCollector;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class RowsTest
{
    private static final String KEYSPACE = "rows_test";
    private static final String KCVM_TABLE = "kcvm";
    private static final TableMetadata kcvm;
    private static final ColumnMetadata v;
    private static final ColumnMetadata m;
    private static final Clustering<?> c1;

    static
    {
        DatabaseDescriptor.daemonInitialization();
        kcvm =
            TableMetadata.builder(KEYSPACE, KCVM_TABLE)
                         .addPartitionKeyColumn("k", IntegerType.instance)
                         .addClusteringColumn("c", IntegerType.instance)
                         .addRegularColumn("v", IntegerType.instance)
                         .addRegularColumn("m", MapType.getInstance(IntegerType.instance, IntegerType.instance, true))
                         .build();

        v = kcvm.getColumn(new ColumnIdentifier("v", false));
        m = kcvm.getColumn(new ColumnIdentifier("m", false));
        c1 = kcvm.comparator.make(BigInteger.valueOf(1));
    }

    private static final ByteBuffer BB1 = ByteBufferUtil.bytes(1);
    private static final ByteBuffer BB2 = ByteBufferUtil.bytes(2);
    private static final ByteBuffer BB3 = ByteBufferUtil.bytes(3);
    private static final ByteBuffer BB4 = ByteBufferUtil.bytes(4);

    private static class MergedPair<T>
    {
        public final int idx;
        public final T merged;
        public final T original;

        private MergedPair(int idx, T merged, T original)
        {
            this.idx = idx;
            this.merged = merged;
            this.original = original;
        }

        static <T> MergedPair<T> create(int i, T m, T o)
        {
            return new MergedPair<>(i, m, o);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MergedPair<?> that = (MergedPair<?>) o;

            if (idx != that.idx) return false;
            if (merged != null ? !merged.equals(that.merged) : that.merged != null) return false;
            return !(original != null ? !original.equals(that.original) : that.original != null);
        }

        public int hashCode()
        {
            int result = idx;
            result = 31 * result + (merged != null ? merged.hashCode() : 0);
            result = 31 * result + (original != null ? original.hashCode() : 0);
            return result;
        }

        public String toString()
        {
            return "MergedPair{" +
                   "idx=" + idx +
                   ", merged=" + merged +
                   ", original=" + original +
                   '}';
        }
    }

    private static class DiffListener implements RowDiffListener
    {
        int updates = 0;
        Clustering<?> clustering = null;

        private void updateClustering(Clustering<?> c)
        {
            assert clustering == null || clustering == c;
            clustering = c;
        }

        List<MergedPair<Cell<?>>> cells = new LinkedList<>();
        public void onCell(int i, Clustering<?> clustering, Cell<?> merged, Cell<?> original)
        {
            updateClustering(clustering);
            cells.add(MergedPair.create(i, merged, original));
            updates++;
        }

        List<MergedPair<LivenessInfo>> liveness = new LinkedList<>();
        public void onPrimaryKeyLivenessInfo(int i, Clustering<?> clustering, LivenessInfo merged, LivenessInfo original)
        {
            updateClustering(clustering);
            liveness.add(MergedPair.create(i, merged, original));
            updates++;
        }

        List<MergedPair<Row.Deletion>> deletions = new LinkedList<>();
        public void onDeletion(int i, Clustering<?> clustering, Row.Deletion merged, Row.Deletion original)
        {
            updateClustering(clustering);
            deletions.add(MergedPair.create(i, merged, original));
            updates++;
        }

        Map<ColumnMetadata, List<MergedPair<DeletionTime>>> complexDeletions = new HashMap<>();
        public void onComplexDeletion(int i, Clustering<?> clustering, ColumnMetadata column, DeletionTime merged, DeletionTime original)
        {
            updateClustering(clustering);
            if (!complexDeletions.containsKey(column)) complexDeletions.put(column, new LinkedList<>());
            complexDeletions.get(column).add(MergedPair.create(i, merged, original));
            updates++;
        }
    }

    public static class StatsCollector implements PartitionStatisticsCollector
    {
        List<Cell<?>> cells = new LinkedList<>();
        public void update(Cell<?> cell)
        {
            cells.add(cell);
        }

        List<LivenessInfo> liveness = new LinkedList<>();
        public void update(LivenessInfo info)
        {
            liveness.add(info);
        }

        List<DeletionTime> deletions = new LinkedList<>();
        public void update(DeletionTime deletion)
        {
            deletions.add(deletion);
        }

        long columnCount = -1;
        public void updateColumnSetPerRow(long columnSetInRow)
        {
            assert columnCount < 0;
            this.columnCount = columnSetInRow;
        }

        boolean hasLegacyCounterShards = false;
        public void updateHasLegacyCounterShards(boolean hasLegacyCounterShards)
        {
            this.hasLegacyCounterShards |= hasLegacyCounterShards;
        }

        @Override
        public void updatePartitionDeletion(DeletionTime dt)
        {
            update(dt);
        }
    }

    private static long secondToTs(long now)
    {
        return now * 1000000L;
    }

    private static Row.Builder createBuilder(Clustering<?> c)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(c);
        return builder;
    }

    private static Row.Builder createBuilder(Clustering<?> c, long now, ByteBuffer vVal, ByteBuffer mKey, ByteBuffer mVal)
    {
        long ts = secondToTs(now);
        Row.Builder builder = createBuilder(c);
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(ts, now));
        if (vVal != null)
        {
            builder.addCell(BufferCell.live(v, ts, vVal));
        }
        if (mKey != null && mVal != null)
        {
            builder.addComplexDeletion(m, DeletionTime.build(ts - 1, now));
            builder.addCell(BufferCell.live(m, ts, mVal, CellPath.create(mKey)));
        }

        return builder;
    }

    @Test
    public void collectStats()
    {
        long now = FBUtilities.nowInSeconds();
        long ts = secondToTs(now);
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(c1);
        LivenessInfo liveness = LivenessInfo.create(ts, now);
        builder.addPrimaryKeyLivenessInfo(liveness);
        DeletionTime complexDeletion = DeletionTime.build(ts-1, now);
        builder.addComplexDeletion(m, complexDeletion);
        List<Cell<?>> expectedCells = Lists.newArrayList(BufferCell.live(v, ts, BB1),
                                                      BufferCell.live(m, ts, BB1, CellPath.create(BB1)),
                                                      BufferCell.live(m, ts, BB2, CellPath.create(BB2)));
        expectedCells.forEach(builder::addCell);
        // We need to use ts-1 so the deletion doesn't shadow what we've created
        Row.Deletion rowDeletion = new Row.Deletion(DeletionTime.build(ts-1, now), false);
        builder.addRowDeletion(rowDeletion);

        StatsCollector collector = new StatsCollector();
        Rows.collectStats(builder.build(), collector);

        Assert.assertEquals(Lists.newArrayList(liveness), collector.liveness);
        Assert.assertEquals(Sets.newHashSet(rowDeletion.time(), complexDeletion), Sets.newHashSet(collector.deletions));
        Assert.assertEquals(Sets.newHashSet(expectedCells), Sets.newHashSet(collector.cells));
        Assert.assertEquals(2, collector.columnCount);
        Assert.assertFalse(collector.hasLegacyCounterShards);
    }


    public static void addExpectedCells(Set<MergedPair<Cell<?>>> dst, Cell<?> merged, Cell<?>... inputs)
    {
        for (int i=0; i<inputs.length; i++)
        {
            dst.add(MergedPair.create(i, merged, inputs[i]));
        }
    }

    @Test
    public void diff()
    {
        long now1 = FBUtilities.nowInSeconds();
        long ts1 = secondToTs(now1);
        Row.Builder r1Builder = BTreeRow.unsortedBuilder();
        r1Builder.newRow(c1);
        LivenessInfo r1Liveness = LivenessInfo.create(ts1, now1);
        r1Builder.addPrimaryKeyLivenessInfo(r1Liveness);
        DeletionTime r1ComplexDeletion = DeletionTime.build(ts1-1, now1);
        r1Builder.addComplexDeletion(m, r1ComplexDeletion);

        Cell<?> r1v = BufferCell.live(v, ts1, BB1);
        Cell<?> r1m1 = BufferCell.live(m, ts1, BB1, CellPath.create(BB1));
        Cell<?> r1m2 = BufferCell.live(m, ts1, BB2, CellPath.create(BB2));
        List<Cell<?>> r1ExpectedCells = Lists.newArrayList(r1v, r1m1, r1m2);

        r1ExpectedCells.forEach(r1Builder::addCell);

        long now2 = now1 + 1;
        long ts2 = secondToTs(now2);
        Row.Builder r2Builder = BTreeRow.unsortedBuilder();
        r2Builder.newRow(c1);
        LivenessInfo r2Liveness = LivenessInfo.create(ts2, now2);
        r2Builder.addPrimaryKeyLivenessInfo(r2Liveness);
        Cell<?> r2v = BufferCell.live(v, ts2, BB2);
        Cell<?> r2m2 = BufferCell.live(m, ts2, BB1, CellPath.create(BB2));
        Cell<?> r2m3 = BufferCell.live(m, ts2, BB2, CellPath.create(BB3));
        Cell<?> r2m4 = BufferCell.live(m, ts2, BB3, CellPath.create(BB4));
        List<Cell<?>> r2ExpectedCells = Lists.newArrayList(r2v, r2m2, r2m3, r2m4);

        r2ExpectedCells.forEach(r2Builder::addCell);
        Row.Deletion r2RowDeletion = new Row.Deletion(DeletionTime.build(ts1 - 2, now2), false);
        r2Builder.addRowDeletion(r2RowDeletion);

        Row r1 = r1Builder.build();
        Row r2 = r2Builder.build();
        Row merged = Rows.merge(r1, r2);

        Assert.assertEquals(r1ComplexDeletion, merged.getComplexColumnData(m).complexDeletion());

        DiffListener listener = new DiffListener();
        Rows.diff(listener, merged, r1, r2);

        Assert.assertEquals(c1, listener.clustering);

        // check cells
        Set<MergedPair<Cell<?>>> expectedCells = Sets.newHashSet();
        addExpectedCells(expectedCells, r2v,  r1v,  r2v);     // v
        addExpectedCells(expectedCells, r1m1, r1m1, null);   // m[1]
        addExpectedCells(expectedCells, r2m2, r1m2, r2m2);   // m[2]
        addExpectedCells(expectedCells, r2m3, null, r2m3);   // m[3]
        addExpectedCells(expectedCells, r2m4, null, r2m4);   // m[4]

        Assert.assertEquals(expectedCells.size(), listener.cells.size());
        Assert.assertEquals(expectedCells, Sets.newHashSet(listener.cells));

        // liveness
        List<MergedPair<LivenessInfo>> expectedLiveness = Lists.newArrayList(MergedPair.create(0, r2Liveness, r1Liveness),
                                                                             MergedPair.create(1, r2Liveness, r2Liveness));
        Assert.assertEquals(expectedLiveness, listener.liveness);

        // deletions
        List<MergedPair<Row.Deletion>> expectedDeletions = Lists.newArrayList(MergedPair.create(0, r2RowDeletion, null),
                                                                              MergedPair.create(1, r2RowDeletion, r2RowDeletion));
        Assert.assertEquals(expectedDeletions, listener.deletions);

        // complex deletions
        List<MergedPair<DeletionTime>> expectedCmplxDeletions = Lists.newArrayList(MergedPair.create(0, r1ComplexDeletion, r1ComplexDeletion),
                                                                                   MergedPair.create(1, r1ComplexDeletion, DeletionTime.LIVE));
        Assert.assertEquals(ImmutableMap.builder().put(m, expectedCmplxDeletions).build(), listener.complexDeletions);
    }

    /**
     * merged row has no column data
     */
    @Test
    public void diffEmptyMerged()
    {
        long now1 = FBUtilities.nowInSeconds();
        long ts1 = secondToTs(now1);
        Row.Builder r1Builder = BTreeRow.unsortedBuilder();
        r1Builder.newRow(c1);
        LivenessInfo r1Liveness = LivenessInfo.create(ts1, now1);
        r1Builder.addPrimaryKeyLivenessInfo(r1Liveness);

        // mergedData == null
        long now2 = now1 + 1L;
        long ts2 = secondToTs(now2);
        Row.Builder r2Builder = BTreeRow.unsortedBuilder();
        r2Builder.newRow(c1);
        LivenessInfo r2Liveness = LivenessInfo.create(ts2, now2);
        r2Builder.addPrimaryKeyLivenessInfo(r2Liveness);
        DeletionTime r2ComplexDeletion = DeletionTime.build(ts2-1, now2);
        r2Builder.addComplexDeletion(m, r2ComplexDeletion);
        Cell<?> r2v = BufferCell.live(v, ts2, BB2);
        Cell<?> r2m2 = BufferCell.live(m, ts2, BB1, CellPath.create(BB2));
        Cell<?> r2m3 = BufferCell.live(m, ts2, BB2, CellPath.create(BB3));
        Cell<?> r2m4 = BufferCell.live(m, ts2, BB3, CellPath.create(BB4));
        List<Cell<?>> r2ExpectedCells = Lists.newArrayList(r2v, r2m2, r2m3, r2m4);

        r2ExpectedCells.forEach(r2Builder::addCell);
        Row.Deletion r2RowDeletion = new Row.Deletion(DeletionTime.build(ts1 - 1, now2), false);
        r2Builder.addRowDeletion(r2RowDeletion);

        Row r1 = r1Builder.build();
        Row r2 = r2Builder.build();

        DiffListener listener = new DiffListener();
        Rows.diff(listener, r1, r2);

        Assert.assertEquals(c1, listener.clustering);

        // check cells
        Set<MergedPair<Cell<?>>> expectedCells = Sets.newHashSet(MergedPair.create(0, null, r2v),   // v
                                                              MergedPair.create(0, null, r2m2),  // m[2]
                                                              MergedPair.create(0, null, r2m3),  // m[3]
                                                              MergedPair.create(0, null, r2m4)); // m[4]

        Assert.assertEquals(expectedCells.size(), listener.cells.size());
        Assert.assertEquals(expectedCells, Sets.newHashSet(listener.cells));

        // complex deletions
        List<MergedPair<DeletionTime>> expectedCmplxDeletions = Lists.newArrayList(MergedPair.create(0, null, r2ComplexDeletion));
        Assert.assertEquals(ImmutableMap.builder().put(m, expectedCmplxDeletions).build(), listener.complexDeletions);
    }

    /**
     * input row has no column data
     */
    @Test
    public void diffEmptyInput()
    {
        long now1 = FBUtilities.nowInSeconds();
        long ts1 = secondToTs(now1);
        Row.Builder r1Builder = BTreeRow.unsortedBuilder();
        r1Builder.newRow(c1);
        LivenessInfo r1Liveness = LivenessInfo.create(ts1, now1);
        r1Builder.addPrimaryKeyLivenessInfo(r1Liveness);

        // mergedData == null
        long now2 = now1 + 1L;
        long ts2 = secondToTs(now2);
        Row.Builder r2Builder = BTreeRow.unsortedBuilder();
        r2Builder.newRow(c1);
        LivenessInfo r2Liveness = LivenessInfo.create(ts2, now2);
        r2Builder.addPrimaryKeyLivenessInfo(r2Liveness);
        DeletionTime r2ComplexDeletion = DeletionTime.build(ts2-1, now2);
        r2Builder.addComplexDeletion(m, r2ComplexDeletion);
        Cell<?> r2v = BufferCell.live(v, ts2, BB2);
        Cell<?> r2m2 = BufferCell.live(m, ts2, BB1, CellPath.create(BB2));
        Cell<?> r2m3 = BufferCell.live(m, ts2, BB2, CellPath.create(BB3));
        Cell<?> r2m4 = BufferCell.live(m, ts2, BB3, CellPath.create(BB4));
        List<Cell<?>> r2ExpectedCells = Lists.newArrayList(r2v, r2m2, r2m3, r2m4);

        r2ExpectedCells.forEach(r2Builder::addCell);
        Row.Deletion r2RowDeletion = new Row.Deletion(DeletionTime.build(ts1 - 1, now2), false);
        r2Builder.addRowDeletion(r2RowDeletion);

        Row r1 = r1Builder.build();
        Row r2 = r2Builder.build();

        DiffListener listener = new DiffListener();
        Rows.diff(listener, r2, r1);

        Assert.assertEquals(c1, listener.clustering);

        // check cells
        Set<MergedPair<Cell<?>>> expectedCells = Sets.newHashSet(MergedPair.create(0, r2v, null),   // v
                                                              MergedPair.create(0, r2m2, null),  // m[2]
                                                              MergedPair.create(0, r2m3, null),  // m[3]
                                                              MergedPair.create(0, r2m4, null)); // m[4]

        Assert.assertEquals(expectedCells.size(), listener.cells.size());
        Assert.assertEquals(expectedCells, Sets.newHashSet(listener.cells));

        // complex deletions
        List<MergedPair<DeletionTime>> expectedCmplxDeletions = Lists.newArrayList(MergedPair.create(0, r2ComplexDeletion, null));
        Assert.assertEquals(ImmutableMap.builder().put(m, expectedCmplxDeletions).build(), listener.complexDeletions);
    }

    @Test
    public void merge()
    {
        long now1 = FBUtilities.nowInSeconds();
        Row.Builder existingBuilder = createBuilder(c1, now1, BB1, BB1, BB1);

        long now2 = now1 + 1L;
        long ts2 = secondToTs(now2);

        Cell<?> expectedVCell = BufferCell.live(v, ts2, BB2);
        Cell<?> expectedMCell = BufferCell.live(m, ts2, BB2, CellPath.create(BB1));
        DeletionTime expectedComplexDeletionTime = DeletionTime.build(ts2 - 1, now2);

        Row.Builder updateBuilder = createBuilder(c1, now2, null, null, null);
        updateBuilder.addCell(expectedVCell);
        updateBuilder.addComplexDeletion(m, expectedComplexDeletionTime);
        updateBuilder.addCell(expectedMCell);

        Row merged = Rows.merge(existingBuilder.build(), updateBuilder.build());

        Assert.assertEquals(c1, merged.clustering());
        Assert.assertEquals(LivenessInfo.create(ts2, now2), merged.primaryKeyLivenessInfo());

        Iterator<Cell<?>> iter = merged.cells().iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(expectedVCell, iter.next());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(expectedMCell, iter.next());
        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void mergeComplexDeletionSupersededByRowDeletion()
    {
        long now1 = FBUtilities.nowInSeconds();
        Row.Builder existingBuilder = createBuilder(c1, now1, null, BB2, BB2);

        long now2 = now1 + 1L;
        Row.Builder updateBuilder = createBuilder(c1);
        long now3 = now2 + 1L;
        Row.Deletion expectedDeletion = new Row.Deletion(DeletionTime.build(secondToTs(now3), now3), false);
        updateBuilder.addRowDeletion(expectedDeletion);

        Row merged = Rows.merge(existingBuilder.build(), updateBuilder.build());

        Assert.assertEquals(expectedDeletion, merged.deletion());
        Assert.assertFalse(merged.hasComplexDeletion());
        Assert.assertFalse(merged.cells().iterator().hasNext());
    }

    @Test
    public void mergeRowDeletionSupercedesLiveness()
    {
        long now1 = FBUtilities.nowInSeconds();
        Row.Builder existingBuilder = createBuilder(c1, now1, BB1, BB1, BB1);

        long now2 = now1 + 1L;
        Row.Builder updateBuilder = createBuilder(c1);
        long now3 = now2 + 1L;
        Row.Deletion expectedDeletion = new Row.Deletion(DeletionTime.build(secondToTs(now3), now3), false);
        updateBuilder.addRowDeletion(expectedDeletion);

        Row merged = Rows.merge(existingBuilder.build(), updateBuilder.build());

        Assert.assertEquals(expectedDeletion, merged.deletion());
        Assert.assertEquals(LivenessInfo.EMPTY, merged.primaryKeyLivenessInfo());
        Assert.assertEquals(0, merged.columns().size());
    }

    // Creates a dummy cell for a (regular) column for the provided name and without a cellPath.
    private static Cell<?> liveCell(ColumnMetadata name)
    {
        return liveCell(name, -1);
    }

    // Creates a dummy cell for a (regular) column for the provided name.
    // If path >= 0, the cell will have a CellPath containing path as an Int32Type.
    private static Cell<?> liveCell(ColumnMetadata name, int path)
    {
        CellPath cp = path < 0 ? null : CellPath.create(ByteBufferUtil.bytes(path));
        return new BufferCell(name, 0L, Cell.NO_TTL, Cell.NO_DELETION_TIME, ByteBuffer.allocate(1), cp);
    }

    // Assert that the cells generated by iterating iterable are the cell of cells (in the same order
    // and with neither more nor less cells).
    private static void assertCellOrder(Iterable<Cell<?>> iterable, Cell<?>... cells)
    {
        int i = 0;
        for (Cell<?> actual : iterable)
        {
            Assert.assertFalse(String.format("Got more rows than expected (expecting %d). First unexpected cell is %s", cells.length, actual), i >= cells.length);
            Assert.assertEquals(cells[i++], actual);
        }
        Assert.assertFalse(String.format("Got less rows than expected (got %d while expecting %d).", i, cells.length), i < cells.length);
    }

    // Make a dummy row (empty clustering) with the provided cells, that are assumed to be in order
    private static Row makeDummyRow(Cell<?> ... cells)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        for (Cell<?> cell : cells)
            builder.addCell(cell);

        return builder.build();
    }

    @Test
    public void testLegacyCellIterator()
    {
        // Creates a table with
        //   - 3 Simple columns: a, c and e
        //   - 2 Complex columns: b and d
        TableMetadata metadata =
            TableMetadata.builder("dummy_ks", "dummy_tbl")
                         .addPartitionKeyColumn("k", BytesType.instance)
                         .addRegularColumn("a", BytesType.instance)
                         .addRegularColumn("b", MapType.getInstance(Int32Type.instance, BytesType.instance, true))
                         .addRegularColumn("c", BytesType.instance)
                         .addRegularColumn("d", MapType.getInstance(Int32Type.instance, BytesType.instance, true))
                         .addRegularColumn("e", BytesType.instance)
                         .build();

        ColumnMetadata a = metadata.getColumn(new ColumnIdentifier("a", false));
        ColumnMetadata b = metadata.getColumn(new ColumnIdentifier("b", false));
        ColumnMetadata c = metadata.getColumn(new ColumnIdentifier("c", false));
        ColumnMetadata d = metadata.getColumn(new ColumnIdentifier("d", false));
        ColumnMetadata e = metadata.getColumn(new ColumnIdentifier("e", false));

        Row row;

        // Row with only simple columns

        row = makeDummyRow(liveCell(a),
                           liveCell(c),
                           liveCell(e));


        assertCellOrder(row.cellsInLegacyOrder(metadata, false),
                        liveCell(a),
                        liveCell(c),
                        liveCell(e));

        assertCellOrder(row.cellsInLegacyOrder(metadata, true),
                        liveCell(e),
                        liveCell(c),
                        liveCell(a));

        // Row with only complex columns

        row = makeDummyRow(liveCell(b, 1),
                           liveCell(b, 2),
                           liveCell(d, 3),
                           liveCell(d, 4));


        assertCellOrder(row.cellsInLegacyOrder(metadata, false),
                        liveCell(b, 1),
                        liveCell(b, 2),
                        liveCell(d, 3),
                        liveCell(d, 4));

        assertCellOrder(row.cellsInLegacyOrder(metadata, true),
                        liveCell(d, 4),
                        liveCell(d, 3),
                        liveCell(b, 2),
                        liveCell(b, 1));

        // Row with mixed simple and complex columns

        row = makeDummyRow(liveCell(a),
                           liveCell(c),
                           liveCell(e),
                           liveCell(b, 1),
                           liveCell(b, 2),
                           liveCell(d, 3),
                           liveCell(d, 4));


        assertCellOrder(row.cellsInLegacyOrder(metadata, false),
                        liveCell(a),
                        liveCell(b, 1),
                        liveCell(b, 2),
                        liveCell(c),
                        liveCell(d, 3),
                        liveCell(d, 4),
                        liveCell(e));

        assertCellOrder(row.cellsInLegacyOrder(metadata, true),
                        liveCell(e),
                        liveCell(d, 4),
                        liveCell(d, 3),
                        liveCell(c),
                        liveCell(b, 2),
                        liveCell(b, 1),
                        liveCell(a));
    }
}
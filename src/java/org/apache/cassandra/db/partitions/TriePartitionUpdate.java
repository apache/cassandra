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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * A trie-backed PartitionUpdate. Immutable.
 * <p>
 * Provides factories for simple variations (e.g. singleRowUpdate) and a mutable builder for constructing one.
 * The builder holds a mutable trie to which content may be added in any order, also taking care of
 * merging any duplicate rows, and keeping track of statistics and column coverage.
 */
public class TriePartitionUpdate extends TrieBackedPartition implements PartitionUpdate
{
    protected static final Logger logger = LoggerFactory.getLogger(TriePartitionUpdate.class);

    public static final Factory FACTORY = new TrieFactory();

    private static EnumSet<TrieTombstoneMarker.Kind> ROW_DELETION_KINDS =
        EnumSet.of(TrieTombstoneMarker.Kind.ROW, TrieTombstoneMarker.Kind.RANGE, TrieTombstoneMarker.Kind.PARTITION);

    final int dataSize;

    /// The schema epoch at which this update was serialized, if it was received from another node.
    private final Epoch serializedAtEpoch;

    private TriePartitionUpdate(TableMetadata metadata,
                                DecoratedKey key,
                                RegularAndStaticColumns columns,
                                EncodingStats stats,
                                int rowCountIncludingStatic,
                                int tombstoneCount,
                                int dataSize,
                                InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie)
    {
        this(metadata, metadata.epoch, key, columns, stats, rowCountIncludingStatic, tombstoneCount, dataSize, trie);
    }

    private TriePartitionUpdate(TableMetadata metadata,
                                Epoch serializedAtEpoch,
                                DecoratedKey key,
                                RegularAndStaticColumns columns,
                                EncodingStats stats,
                                int rowCountIncludingStatic,
                                int tombstoneCount,
                                int dataSize,
                                InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie)
    {
        super(key, columns, stats, rowCountIncludingStatic, tombstoneCount, trie, metadata);
        this.dataSize = dataSize;
        this.serializedAtEpoch = serializedAtEpoch;
    }

    /// This is extremely inefficient and is to be used by tests only.
    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;
        if (!(obj instanceof TriePartitionUpdate))
            return false;

        TriePartitionUpdate that = (TriePartitionUpdate) obj;
        return partitionKey.equals(that.partitionKey)
               && metadata().id.equals(that.metadata().id)
               && deletionInfo().equals(that.deletionInfo())
               && staticRow().equals(that.staticRow())
               && Iterators.elementsEqual(rowIterator(), that.rowIterator());
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    /** @see PartitionUpdate.Factory#emptyUpdate  */
    public static TriePartitionUpdate emptyUpdate(TableMetadata metadata, DecoratedKey key)
    {
        return new TriePartitionUpdate(metadata,
                                       key,
                                       RegularAndStaticColumns.NONE,
                                       EncodingStats.NO_STATS,
                                       0,
                                       0,
                                       0,
                                       newTrieWithPartitionMarker());
    }

    /** @see PartitionUpdate.Factory#fullPartitionDelete */
    public static TriePartitionUpdate fullPartitionDelete(TableMetadata metadata, DecoratedKey key, long timestamp, long nowInSec)
    {
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie = newTrieWithPartitionMarker();
        DeletionTime deletionTime = DeletionTime.build(timestamp, nowInSec);
        putPartitionDeletionInTrie(makeNoConflictMutator(trie), deletionTime);
        return new TriePartitionUpdate(metadata,
                                       key,
                                       RegularAndStaticColumns.NONE,
                                       new EncodingStats(timestamp, nowInSec, LivenessInfo.NO_TTL),
                                       0,
                                       1,
                                       deletionTime.dataSize(),
                                       trie);
    }

    /** @see PartitionUpdate.Factory#singleRowUpdate */
    public static TriePartitionUpdate singleRowUpdate(TableMetadata metadata, DecoratedKey key, Row row)
    {
        EncodingStats stats = row.isEmpty() ? EncodingStats.NO_STATS : EncodingStats.Collector.forRow(row);
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie = newTrieWithPartitionMarker();

        RegularAndStaticColumns columns;
        if (row.isStatic())
            columns = new RegularAndStaticColumns(Columns.from(row.columns()), Columns.NONE);
        else
            columns = new RegularAndStaticColumns(Columns.NONE, Columns.from(row.columns()));

        putInTrie(makeNoConflictMutator(trie), metadata, metadata.comparator, row);

        return new TriePartitionUpdate(metadata, key, columns, stats, 1, row.deletion().isLive() ? 0 : 1, row.dataSize(), trie);
    }

    /** @see PartitionUpdate.Factory#fromIterator(UnfilteredRowIterator)  */
    public static TriePartitionUpdate fromIterator(UnfilteredRowIterator iterator)
    {
        return fromDeserialized(iterator, iterator.metadata().epoch);
    }

    /** @see PartitionUpdate.Factory#fromDeserialized */
    @SuppressWarnings("resource")
    public static TriePartitionUpdate fromDeserialized(UnfilteredRowIterator iterator, Epoch serializedAtEpoch)
    {
        ContentBuilder builder = build(iterator, true);

        return new TriePartitionUpdate(iterator.metadata(),
                                       serializedAtEpoch,
                                       iterator.partitionKey(),
                                       iterator.columns(),
                                       iterator.stats(),
                                       builder.rowCountIncludingStatic(),
                                       builder.tombstoneCount(),
                                       builder.dataSize(),
                                       builder.trie());
    }

    /**
     * Convert the given update (with unknown implementation type) to a TriePartitionUpdate for insertion in a trie.
     */
    public static TriePartitionUpdate asTrieUpdate(PartitionUpdate update)
    {
        if (update instanceof TriePartitionUpdate)
            return (TriePartitionUpdate) update;

        try (UnfilteredRowIterator iterator = update.unfilteredIterator())
        {
            return fromIterator(iterator);
        }
    }

    /**
     * Convert the given update (with unknown implementation type) to its trie representation, including the partition
     * key prefix.
     */
    public static DeletionAwareTrie<Object, TrieTombstoneMarker> asMergableTrie(PartitionUpdate update)
    {
        return asTrieUpdate(update).trie.prefixedBy(update.partitionKey());
    }

    /**
     * Return the given update's trie representation without the partition prefix. Used for testing.
     */
    public static DeletionAwareTrie<Object, TrieTombstoneMarker> asMergableTrieWithoutPartitionPrefix(PartitionUpdate update)
    {
        return asTrieUpdate(update).trie;
    }

    @Override
    public Epoch serializedAtEpoch()
    {
        return serializedAtEpoch;
    }

    @Override
    public boolean canHaveShadowedData()
    {
        // Data is merged in the trie as it is added, applying any deletions to it.
        return false;
    }

    @Override
    public TriePartitionUpdate withUpdatedTimestamps(long newTimestamp)
    {

        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> t = TrieBackedRow.newTrie();
        try
        {
            t.apply(trie,
                    (shouldBeNull, o) ->
                    {
                        assert shouldBeNull == null;
                        if (o instanceof Cell<?>)
                            return ((Cell<?>) o).updateAllTimestamp(newTimestamp);

                        if (o == TrieBackedRow.COMPLEX_COLUMN_MARKER)
                            return o;

                        if (o == LivenessInfo.EMPTY)
                            return o;   // Empty liveness should remain unchanged.

                        if (o instanceof LivenessInfo)
                            return ((LivenessInfo) o).withUpdatedTimestamp(newTimestamp);

                        if (o instanceof PartitionMarker)
                            return o;

                        throw new AssertionError("Unexpected data in trie: " + o);
                    },
                    (shouldBeNull, o) ->
                    {
                        assert shouldBeNull == null;
                        return o.withUpdatedTimestamp(newTimestamp - 1);
                    },
                    noIncomingSelfDeletion(),
                    noExistingSelfDeletion(),
                    true,
                    x -> false);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return new TriePartitionUpdate(metadata, partitionKey, columns, stats, rowCountIncludingStatic, tombstoneCount, dataSize, t);
    }

    @Override
    public DeletionInfo deletionInfo()
    {
        // Collect deletion info from the trie.
        DeletionTime partitionLevelDeletion = partitionLevelDeletion();
        MutableDeletionInfo.Builder builder = MutableDeletionInfo.builder(partitionLevelDeletion, metadata.comparator, false);
        for (Map.Entry<ByteComparable.Preencoded, TrieTombstoneMarker> entry : trie.deletionBranchAtRoot().entrySet())
        {
            RangeTombstoneMarker marker = entry.getValue().toRangeTombstoneMarker(entry.getKey(), BYTE_COMPARABLE_VERSION, metadata.comparator);
            if (marker != null)
                builder.add(marker);
        }
        return builder.build();
    }

    /// @inheritDoc
    /// Note: This will not count rows that contain only column-level deletions, as these are not represented in either
    /// the live row count or the tombstone count. As this method is meant to get an approximation, we would rather
    /// spare the cost of correcting this.
    @Override
    public int operationCount()
    {
        return rowCountIncludingStatic + tombstoneCount;
    }


    /// @inheritDoc
    /// Note: This will not count rows that contain only column-level deletions, as these are not represented in either
    /// the live row count or the tombstone count. As this method is meant to get an approximation, we would rather
    /// spare the cost of correcting this.
    @Override
    public int affectedRowCount()
    {
        // If there is a partition-level deletion, we intend to delete at least the columns of one row.
        if (!partitionLevelDeletion().isLive())
            return 1;

        return rowCountIncludingStatic + tombstoneCount;
    }

    @Override
    public int affectedColumnCount()
    {
        // If there is a partition-level deletion, we intend to delete at least the columns of one row.
        if (!partitionLevelDeletion().isLive())
            return metadata().regularAndStaticColumns().size();

        return TrieBackedRow.countColumns(trie) +
               // Each range delete should correspond to at least one intended row deletion, and with it, its regular columns.
               tombstoneCount * metadata().regularColumns().size();
    }

    @Override
    public int dataSize()
    {
        return dataSize;
    }

    @Override
    public long unsharedHeapSize()
    {
        assert trie instanceof InMemoryDeletionAwareTrie;
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> inMemoryTrie = (InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>) trie;
        class Collector implements DeletionAwareTrie.ValueConsumer<Object, TrieTombstoneMarker>
        {
            long heapSize = inMemoryTrie.usedSizeOnHeap();

            @Override
            public void deletionMarker(TrieTombstoneMarker marker)
            {
                heapSize += marker.unsharedHeapSize();
            }

            @Override
            public void content(Object o)
            {
                if (o instanceof Cell)
                    heapSize += ((Cell<?>) o).unsharedHeapSize();
                else if (o instanceof LivenessInfo)
                    heapSize += ((LivenessInfo) o).unsharedHeapSize();
            }
        }
        Collector collector = new Collector();
        inMemoryTrie.process(Direction.FORWARD, collector);
        return collector.heapSize;
    }

    @Override
    public void validate()
    {
        for (Iterator<Row> it = rowsIncludingStatic(); it.hasNext();)
        {
            Row row = it.next();
            metadata().comparator.validate(row.clustering());

            for (Cell<?> cell : row.cells())
                cell.validate();
        }
    }

    @Override
    public long maxTimestamp()
    {
        long maxTimestamp = LivenessInfo.NO_TIMESTAMP;
        for (Iterator<TrieTombstoneMarker> it = trie.deletionBranchAtRoot().valueIterator(); it.hasNext();)
        {
            TrieTombstoneMarker next = it.next();
            DeletionTime pointDeletion = next.pointDeletion();
            if (pointDeletion != null)
                maxTimestamp = Math.max(maxTimestamp, pointDeletion.markedForDeleteAt());
            DeletionTime rightDeletion = next.rightDeletion(); // we can ignore left side as it has appeared on the right first
            if (rightDeletion != null)
                maxTimestamp = Math.max(maxTimestamp, rightDeletion.markedForDeleteAt());
        }
        for (Iterator<Row> it = rowsIncludingStatic(); it.hasNext();)
            maxTimestamp = Math.max(maxTimestamp, Rows.collectMaxTimestamp(it.next()));

        return maxTimestamp;
    }

    @Override
    public List<CounterMark> collectCounterMarks()
    {
        assert metadata().isCounter();
        // We will take aliases on the rows of this update, and update them in-place. So we should be sure the
        // update is now immutable for all intent and purposes.
        List<CounterMark> marks = new ArrayList<>();
        for (Iterator<Row> it = rowsIncludingStatic(); it.hasNext();)
        {
            Row row = it.next();
            addMarksForRow(row, marks);
        }
        return marks;
    }

    private void addMarksForRow(Row row, List<CounterMark> marks)
    {
        for (Cell<?> cell : row.cells())
        {
            if (cell.isCounterCell())
                marks.add(new CounterMark(this, row, cell.column(), cell.path()));
        }
    }

    @Override
    public void setCounterMarkValue(CounterMark mark, ByteBuffer value)
    {
        Row row = mark.row();
        ColumnMetadata column = mark.column();
        CellPath path = mark.path();
        ByteComparable key = v ->
            ByteSource.concat(metadata.comparator.asByteComparable(row.clustering()).asComparableBytes(v),
                              TrieBackedRow.columnKey(columns.columns(row.isStatic()), column),
                              path != null ? TrieBackedRow.cellPathKey(column, path, v) : ByteSource.EMPTY);
        try
        {
            ((InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>) trie).apply(
                DeletionAwareTrie.<ByteBuffer, TrieTombstoneMarker>singleton(key, BYTE_COMPARABLE_VERSION, value),
                (c, v) -> ((Cell) c).withUpdatedValue(v),
                (x, y) -> x,
                (x, y) -> x,
                (x, y) -> y,
                true,
                Predicates.alwaysFalse()
            );
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    @Override
    public PartitionUpdate withOnlyPresentColumns()
    {
        Set<ColumnMetadata> columnSet = new HashSet<>();

        for (Row row : rows())
            for (ColumnData column : row)
                columnSet.add(column.column());

        RegularAndStaticColumns columns = RegularAndStaticColumns.builder().addAll(columnSet).build();
        return new TriePartitionUpdate(metadata,
                                       partitionKey,
                                       columns,
                                       stats,
                                       rowCountIncludingStatic,
                                       tombstoneCount,
                                       dataSize,
                                       (InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>) trie);
    }

    /// Returns true iff the given marker has a row-level deletion on its right side.
    static boolean startsRowDeletion(TrieTombstoneMarker update)
    {
        TrieTombstoneMarker.Covering side = update.rightDeletion();
        if (side == null)
            return false;

        return ROW_DELETION_KINDS.contains(side.deletionKind());
    }

    /**
     * Builder for PartitionUpdates
     *
     * This class is not thread safe, but the PartitionUpdate it produces is (since it is immutable).
     */
    public static class Builder implements PartitionUpdate.Builder
    {
        private final TableMetadata metadata;
        private final DecoratedKey key;
        private final RegularAndStaticColumns columns;
        private final InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie = newTrieWithPartitionMarker();
        private final InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker> mutator;
        private final EncodingStats.Collector statsCollector = new EncodingStats.Collector();
        /// Counts live rows (i.e. instances of LivenessInfo including EMPTY)
        private int rowCountIncludingStatic;
        /// Counts tombstone ranges and row deletions
        private int tombstoneCount;
        private long dataSize;

        private boolean isBuilt;

        public Builder(TableMetadata metadata,
                       DecoratedKey key,
                       RegularAndStaticColumns columns)
        {
            this.metadata = metadata;
            this.key = key;
            this.columns = columns;
            isBuilt = false;
            rowCountIncludingStatic = 0;
            tombstoneCount = 0;
            dataSize = 0;
            mutator = trie.mutator(this::mergeIncomingData,
                                   this::mergeTombstones,
                                   this::applyIncomingTombstone,
                                   this::applyExistingTombstoneToIncomingRow,
                                   true,
                                   Predicates.alwaysFalse(),
                                   Predicates.alwaysFalse(),
                                   this::dropLevelMarker);
        }

        void assertNotBuilt()
        {
            assert !isBuilt : "A PartitionUpdate.Builder cannot be used after build()";
        }

        @Override
        public void add(Row row)
        {
            assertNotBuilt();
            if (row.isEmpty())
                return;

            putInTrie(mutator, metadata, metadata.comparator, row);
        }

        @Override
        public void addPartitionDeletion(DeletionTime deletionTime)
        {
            assertNotBuilt();
            putPartitionDeletionInTrie(mutator, deletionTime);
        }

        @Override
        public void add(RangeTombstone range)
        {
            assertNotBuilt();
            putRangeDeletionInTrie(mutator,
                                   metadata.comparator.asByteComparable(range.deletedSlice().start()),
                                   metadata.comparator.asByteComparable(range.deletedSlice().end()),
                                   range.deletionTime());
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return key;
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public TriePartitionUpdate build()
        {
            assertNotBuilt();
            isBuilt = true;
            return new TriePartitionUpdate(metadata,
                                           partitionKey(),
                                           columns,
                                           statsCollector.get(),
                                           rowCountIncludingStatic,
                                           tombstoneCount,
                                           Ints.saturatedCast(dataSize),
                                           trie);
        }

        /** Merge in live data from the update trie. This can be various markers, liveness info or cells. */
        private Object mergeIncomingData(Object existing, Object update)
        {
            if (update instanceof Cell)
            {
                Cell<?> updateCell = (Cell<?>) update;
                Cell<?> existingCell = (Cell<?>) existing; // existing must be either null or a cell
                Cells.collectStats(updateCell, statsCollector);
                Cell<?> reconciled;
                if (existingCell == null)
                {
                    reconciled = updateCell;
                    dataSize += reconciled.dataSizeWithoutPath();
                }
                else
                {
                    reconciled = Cells.reconcile(existingCell, updateCell);
                    if (reconciled != existingCell)
                        dataSize += reconciled.dataSizeWithoutPath() - existingCell.dataSizeWithoutPath();
                }
                return reconciled;
            }
            else if (update == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            {
                assert existing == null || existing == TrieBackedRow.COMPLEX_COLUMN_MARKER;
                return update;
            }
            else if (update instanceof LivenessInfo)
            {
                LivenessInfo rowUpdate = (LivenessInfo) update;
                LivenessInfo existingRow = (LivenessInfo) existing; // existing must be either null or LivenessIndo
                statsCollector.update(rowUpdate);
                LivenessInfo reconciled;

                if (existingRow == null)
                {
                    ++rowCountIncludingStatic;
                    dataSize += rowUpdate.dataSize();
                    reconciled = rowUpdate;
                }
                else
                {
                    reconciled = LivenessInfo.merge(existingRow, rowUpdate);
                    dataSize = reconciled.dataSize() - existingRow.dataSize();
                }
                return reconciled;
            }
            else if (update instanceof PartitionMarker)
            {
                assert update == PARTITION_MARKER;
                assert existing == null || existing == PARTITION_MARKER;
                return PARTITION_MARKER;
            }

            throw new AssertionError("Unknown data in trie: " + update);
        }

        /** Apply an existing tombstone to incoming data before merging that data in the trie. */
        private Object applyExistingTombstoneToIncomingRow(TrieTombstoneMarker marker, Object o)
        {
            // This is done before merging the data; we will reflect size changes when the data is merged if it survives.
            return applyTombstone(marker, o, false);
        }

        /** Apply an incoming tombstone to existing data, possibly removing it from the trie. */
        private Object applyIncomingTombstone(Object o, TrieTombstoneMarker marker)
        {
            return applyTombstone(marker, o, true);
        }

        private Object applyTombstone(TrieTombstoneMarker marker, Object o, boolean updateDataSize)
        {
            DeletionTime deletion = marker.applicableToPointForward();
            if (deletion == null)
                return o;

            if (o instanceof Cell)
            {
                Cell<?> cell = (Cell<?>) o;
                if (!deletion.deletes(cell))
                    return o;
                if (updateDataSize)
                    dataSize -= cell.dataSizeWithoutPath();
                return null;
            }
            else if (o == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            {
                return o;
            }
            else if (o instanceof LivenessInfo)
            {
                LivenessInfo info = (LivenessInfo) o;
                if (!deletion.deletes(info))
                    return o;

                if (updateDataSize)
                    dataSize -= info.dataSize();
                return LivenessInfo.EMPTY;
            }
            else if (o instanceof PartitionMarker)
            {
                return o;
            }
            throw new AssertionError("Unknown data in trie: " + o);
        }

        /**
         * Merge an incoming tombstone with existing deletions.
         * This will be called for all boundary tombstones in the update, but also for all existing boundaries that are
         * covered by an incoming range.
         */
        private TrieTombstoneMarker mergeTombstones(TrieTombstoneMarker existing, @Nonnull TrieTombstoneMarker update)
        {
            updateStats(update);
            if (existing == null)
            {
                // We are adding a new tombstone. We are counting tombstones on the row level, so ones that introduce
                // or close column deletions should not count.
                // We will only count one of the sides as we want to increase the count by one for each pair.
                if (startsRowDeletion(update))
                    ++tombstoneCount;
                return update;
            }
            else
            {
                TrieTombstoneMarker merged = update.mergeWith(existing);
                int hadTombstone = existing.isBoundary() && startsRowDeletion(existing) ? 1 : 0;
                int hasTombstone = merged != null && merged.isBoundary() && startsRowDeletion(merged) ? 1 : 0;
                tombstoneCount += hasTombstone - hadTombstone;
                return merged;
            }
        }

        private void updateStats(TrieTombstoneMarker update)
        {
            // Only process the right sides of a boundary as every newly-introduced deletion will appear in one.
            if (!update.isBoundary())
                return;

            TrieTombstoneMarker.Covering rightSide = update.rightDeletion();
            if (rightSide != null)
                statsCollector.update(rightSide);
        }

        private void dropLevelMarker(Object o)
        {
            if (o == LivenessInfo.EMPTY)
                --rowCountIncludingStatic;
            if (o instanceof TrieTombstoneMarker && ((TrieTombstoneMarker) o).rightDeletion() != null)
                --tombstoneCount;
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return columns;
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return TrieTombstoneMarker.applicableDeletionOrLiveAtRoot(trie);
        }

        @Override
        public String toString()
        {
            return "Builder{" +
                   "metadata=" + metadata +
                   ", key=" + key +
                   ", columns=" + columns +
                   '}';
        }
    }

    public static class TrieFactory implements PartitionUpdate.Factory
    {
        @Override
        public Row.Builder rowBuilder(Columns columns, boolean dataIsSorted)
        {
            return TrieBackedRow.builder(columns);
        }

        @Override
        public PartitionUpdate.Builder builder(TableMetadata metadata, DecoratedKey partitionKey, RegularAndStaticColumns columns, int initialRowCapacity)
        {
            return new TriePartitionUpdate.Builder(metadata, partitionKey, columns);
        }

        @Override
        public PartitionUpdate emptyUpdate(TableMetadata metadata, DecoratedKey partitionKey)
        {
            return TriePartitionUpdate.emptyUpdate(metadata, partitionKey);
        }

        @Override
        public PartitionUpdate singleRowUpdate(TableMetadata metadata, DecoratedKey valueKey, Row row)
        {
            return TriePartitionUpdate.singleRowUpdate(metadata, valueKey, row);
        }

        @Override
        public PartitionUpdate fullPartitionDelete(TableMetadata metadata, DecoratedKey key, long timestamp, long nowInSec)
        {
            return TriePartitionUpdate.fullPartitionDelete(metadata, key, timestamp, nowInSec);
        }

        @Override
        public PartitionUpdate fromIterator(UnfilteredRowIterator iterator)
        {
            return TriePartitionUpdate.fromIterator(iterator);
        }

        @Override
        public PartitionUpdate fromIterator(UnfilteredRowIterator iterator, ColumnFilter filter)
        {
            return TriePartitionUpdate.fromIterator(UnfilteredRowIterators.withOnlyQueriedData(iterator, filter));
        }

        @Override
        public PartitionUpdate fromDeserialized(UnfilteredRowIterator iterator, Epoch serializedAtEpoch)
        {
            return TriePartitionUpdate.fromDeserialized(iterator, serializedAtEpoch);
        }
    }
}

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

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import com.google.common.base.Predicates;
import com.google.common.primitives.Ints;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryBaseTrie;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.RangeTrie;
import org.apache.cassandra.db.tries.TrieSet;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.db.tries.TrieTailsIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.memory.HeapCloner;

/// In-memory partition backed by a deletion-aware trie. The rows of the partition are values in the leaves of the trie,
/// where the key to the row is only stored as the path to reach that leaf; static rows are also treated as a row with
/// `STATIC_CLUSTERING` path; the deletion information is placed in a deletion branch of the trie which starts at the
/// root of the partition. This matches how `TrieMemtable` stores partitions within the larger map, so that
/// `TrieBackedPartition` objects can be created directly from `TrieMemtable` tail tries.
///
/// This object also holds the partition key, as well as some metadata (columns and statistics).
/// Currently, all descendants and instances of this class are immutable (even tail tries from mutable memtables are
/// guaranteed to not change as we use forced copying below the partition level), though this may change in the future.
public class TrieBackedPartition implements Partition
{
    public static final ByteComparable.Version BYTE_COMPARABLE_VERSION = ByteComparable.Version.OSS50;

    /** Pre-made path for STATIC_CLUSTERING, to avoid creating path object when querying static path. */
    public static final ByteComparable STATIC_CLUSTERING_PATH = v -> ByteSource.oneByte(ClusteringPrefix.Kind.STATIC_CLUSTERING.asByteComparableValue(v));
    /** Pre-made path for BOTTOM, to avoid creating path object when iterating rows. */
    public static final ByteComparable BOTTOM_PATH = v -> ByteSource.oneByte(ClusteringPrefix.Kind.INCL_START_BOUND.asByteComparableValue(v));

    /// Interface implemented by partition markers, both the singleton below used for standalone [TrieBackedPartition],
    /// and the marker used in tail tries in `TrieMemtable`s.
    public interface PartitionMarker {}

    /// Singleton partition marker used for standalone [TrieBackedPartition] and [TriePartitionUpdate] objects.
    public static final PartitionMarker PARTITION_MARKER = new PartitionMarker()
    {
        public String toString()
        {
            return "PARTITION_MARKER";
        }
    };

    /// Predicate to identify partition boundaries in tries. This accepts any [PartitionMarker], not just the
    /// [#PARTITION_MARKER] used for standalone trie-backed partitions.
    public static final Predicate<Object> IS_PARTITION_BOUNDARY = TrieBackedPartition::isPartitionBoundary;

    /// Returns true if the given content is a partition marker.
    public static boolean isPartitionBoundary(Object content)
    {
        return content instanceof TrieBackedPartition.PartitionMarker;
    }

    protected final DeletionAwareTrie<Object, TrieTombstoneMarker> trie;
    protected final DecoratedKey partitionKey;
    protected final TableMetadata metadata;
    protected final RegularAndStaticColumns columns;
    protected final EncodingStats stats;
    /// Number of rows in the partition. This will count only the rows that have live data.
    protected final int rowCountIncludingStatic;
    protected final DeletionTime partitionLevelDeletion;
    /// Number of tombstone boundary pairs on the row-level or above: partition, range and row tombstones.
    protected final int tombstoneCount;
    boolean hasStaticRow;

    public TrieBackedPartition(DecoratedKey partitionKey,
                               RegularAndStaticColumns columns,
                               EncodingStats stats,
                               int rowCountIncludingStatic,
                               int tombstoneCount,
                               DeletionAwareTrie<Object, TrieTombstoneMarker> trie,
                               TableMetadata metadata)
    {
        this.partitionKey = partitionKey;
        this.trie = trie;
        this.metadata = metadata;
        this.columns = columns;
        this.stats = stats;
        this.rowCountIncludingStatic = rowCountIncludingStatic;
        this.tombstoneCount = tombstoneCount;
        this.hasStaticRow = trie.get(STATIC_CLUSTERING_PATH) != null;
        this.partitionLevelDeletion = TrieTombstoneMarker.applicableDeletionOrLiveAtRoot(trie);
        // There must always be a partition marker.
        assert trie.get(ByteComparable.EMPTY) != null;
        assert stats != null;
    }

    public static TrieBackedPartition fromIterator(UnfilteredRowIterator iterator)
    {
        ContentBuilder builder = build(iterator, false);
        return new TrieBackedPartition(iterator.partitionKey(),
                                       iterator.columns(),
                                       iterator.stats(),
                                       builder.rowCountIncludingStatic(),
                                       builder.tombstoneCount(),
                                       builder.trie(),
                                       iterator.metadata());
    }

    protected static ContentBuilder build(UnfilteredRowIterator iterator, boolean collectDataSize)
    {
        ContentBuilder builder = new ContentBuilder(iterator.metadata(), iterator.partitionLevelDeletion(), iterator.isReverseOrder(), collectDataSize);

        builder.addStatic(iterator.staticRow());

        while (iterator.hasNext())
            builder.addUnfiltered(iterator.next());

        return builder.complete();
    }

    /// Create a row with the given properties and content, making sure to copy all off-heap data to keep it alive when
    /// the given access mode requires it.
    public static TrieBackedPartition create(DecoratedKey partitionKey,
                                             RegularAndStaticColumns columnMetadata,
                                             EncodingStats encodingStats,
                                             int rowCountIncludingStatic,
                                             int tombstoneCount,
                                             DeletionAwareTrie<Object, TrieTombstoneMarker> trie,
                                             TableMetadata metadata,
                                             TableMetadata droppedColumnsSource,
                                             boolean copyToHeap)
    {
        return copyToHeap
               ? new WithCopyingToHeap(partitionKey, columnMetadata, encodingStats, rowCountIncludingStatic, tombstoneCount, trie, metadata, droppedColumnsSource)
               : new WithDroppedColumnsSource(partitionKey, columnMetadata, encodingStats, rowCountIncludingStatic, tombstoneCount, trie, metadata, droppedColumnsSource);
    }

    /// Implementation of an iterator over rows. Note that because the legacy containers store deleted rows as [Row]s
    /// with a row deletion time and empty content and present them here, we must also present fully deleted rows
    /// (but not range deletions).
    class RowIterator extends TrieTailsIterator.DeletionAware<Object, TrieTombstoneMarker, Object, Row>
    {
        public RowIterator(DeletionAwareTrie<Object, TrieTombstoneMarker> trie, Direction direction)
        {
            super(trie,
                  direction,
                  (live, marker) ->
                      live instanceof LivenessInfo ? live
                                                   : marker != null && marker.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW) ? marker
                                                                                                                                  : null,
                  false);
        }

        @Override
        protected Row mapContent(Object content, DeletionAwareTrie<Object, TrieTombstoneMarker> tailTrie, byte[] bytes, int byteLength)
        {
            return toRow(tailTrie, getClustering(bytes, byteLength));
        }
    }

    private Iterator<Row> rowIterator(DeletionAwareTrie<Object, TrieTombstoneMarker> trie, Direction direction)
    {
        return new RowIterator(trie, direction);
    }

    /// Conversion from row branch to [Row]. [WithCopyingToHeap] overrides this to do the necessary copying
    /// (hence the non-static method).
    TrieBackedRow toRow(DeletionAwareTrie<Object, TrieTombstoneMarker> rowContent, Clustering<?> clustering)
    {
        return rowContent != null ? TrieBackedRow.create(metadata, clustering, rowContent) : null;
    }

    /// Put the given row in the trie, used by methods to build stand-alone partitions.
    ///
    /// @param mutator    destination trie's mutator
    /// @param comparator for converting key to byte-comparable
    /// @param untypedRow content to put
    protected static void putInTrie(InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker> mutator,
                                    TableMetadata metadata,
                                    ClusteringComparator comparator,
                                    Row untypedRow)
    {
        TrieBackedRow row;
        if (untypedRow instanceof TrieBackedRow)
            row = (TrieBackedRow) untypedRow;
        else
            row = TrieBackedRow.from(metadata, untypedRow);

        ByteComparable comparableClustering = comparator.asByteComparable(row.clustering());

        try
        {
            mutator.apply(row.trie().prefixedBySeparately(comparableClustering, true));
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    protected static
    InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker>
    makeNoConflictMutator(InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie)
    {
        return trie.mutator(noConflictInData(),
                            mergeTombstoneRanges(),
                            noIncomingSelfDeletion(),
                            noExistingSelfDeletion(),
                            true,
                            Predicates.alwaysFalse());
    }

    protected static void putRangeDeletionInTrie(InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker> mutator,
                                                 ByteComparable start, ByteComparable end,
                                                 DeletionTime deletionTime)
    {
        assert !deletionTime.isLive();
        try
        {
            mutator.delete(RangeTrie.range(start, true,
                                           end, false, // must be false to ensure intersecting intervals can be properly treated
                                           BYTE_COMPARABLE_VERSION,
                                           TrieTombstoneMarker.covering(deletionTime, TrieTombstoneMarker.Kind.RANGE)));
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    protected static void putRangeDeletionInTrie(InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker> mutator,
                                                 ClusteringComparator comparator,
                                                 RangeTombstoneMarker openMarker,
                                                 RangeTombstoneMarker closeMarker)
    {
        DeletionTime deletionTime = openMarker.openDeletionTime(false);
        assert deletionTime.equals(closeMarker.closeDeletionTime(false));
        ByteComparable start = comparator.asByteComparable(openMarker.clustering());
        ByteComparable end = comparator.asByteComparable(closeMarker.clustering());
        putRangeDeletionInTrie(mutator, start, end, deletionTime);
    }

    protected static void putPartitionDeletionInTrie(InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker> mutator,
                                                     DeletionTime deletionTime)
    {
        if (deletionTime.isLive())
            return;

        try
        {
            mutator.delete(RangeTrie.branch(ByteComparable.EMPTY,
                                            BYTE_COMPARABLE_VERSION,
                                            TrieTombstoneMarker.covering(deletionTime, TrieTombstoneMarker.Kind.PARTITION)));
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    protected static void putPartitionMarkerInTrie(InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie)
    {
        try
        {
            trie.putRecursive(ByteComparable.EMPTY, PARTITION_MARKER, noConflictInData());
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }


    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    @Override
    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    @Override
    public RegularAndStaticColumns columns()
    {
        return columns;
    }

    @Override
    public EncodingStats stats()
    {
        return stats;
    }

    /// @inheritDoc
    /// Trie-backed partitions only count rows with live data. If a row only contains a row deletion, or a column-level
    /// deletion with no overriding live cells, it will not be counted in this number. Though this is somewhat wrong,
    /// the uses of the row count do not require the number to be precise, and getting the right count can be expensive.
    @Override
    public int rowCount()
    {
        return rowCountIncludingStatic - (hasStaticRow() ? 1 : 0);
    }

    public ByteComparable path(ClusteringPrefix<?> clustering)
    {
        return metadata.comparator.asByteComparable(clustering);
    }

    @Override
    public Row staticRow()
    {
        if (!hasStaticRow)
            return Rows.EMPTY_STATIC_ROW;

        // Unlike getRow, this method does not apply any covering deletion to the returned row.
        DeletionAwareTrie<Object, TrieTombstoneMarker> staticRow = trie.tailTrie(STATIC_CLUSTERING_PATH, false);
        assert staticRow != null; // hasStaticRow would be false if there's no static row
        return toRow(staticRow, Clustering.STATIC_CLUSTERING);
    }

    @Override
    public boolean isEmpty()
    {
        return rowCountIncludingStatic + tombstoneCount == 0;
    }

    private boolean hasStaticRow()
    {
        return hasStaticRow;
    }

    @Override
    public boolean hasRows()
    {
        return rowCountIncludingStatic > 1 || rowCountIncludingStatic > 0 && !hasStaticRow();
    }

    /// Provides read access to the trie for users that can take advantage of it directly (e.g. `TrieMemtable`).
    public DeletionAwareTrie<Object, TrieTombstoneMarker> trie()
    {
        return trie;
    }

    private DeletionAwareTrie<Object, TrieTombstoneMarker> nonStaticSubtrie()
    {
        // skip static row if present - the static clustering sorts before BOTTOM so that it's never included in
        // any slices (we achieve this by using the byte ByteSource.EXCLUDED for its representation, which is lower
        // than BOTTOM's ByteSource.LT_NEXT_COMPONENT).
        return trie.subtrie(BOTTOM_PATH, null);
    }

    @Override
    public Iterator<Row> rowIterator()
    {
        return rowIterator(nonStaticSubtrie(), Direction.FORWARD);
    }

    /// Iterator over all rows of the partition including the static one.
    public Iterator<Row> rowsIncludingStatic()
    {
        return rowIterator(trie, Direction.FORWARD);
    }

    @Override
    public Row lastRow()
    {
        Iterator<Row> reverseIterator = rowIterator(nonStaticSubtrie(), Direction.REVERSE);
        return reverseIterator.hasNext() ? reverseIterator.next() : null;
    }

    @Override
    public Row getRow(Clustering<?> clustering)
    {
        // getRow must return range and partition deletion applicable to the row
        return toRow(trie.tailTrie(path(clustering), true), clustering);
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator()
    {
        return new UnfilteredIterator(null, nonStaticSubtrie(), false);
    }

    private Clustering<?> getClustering(byte[] bytes, int byteLength)
    {
        return metadata.comparator.clusteringFromByteComparable(ByteBufferAccessor.instance,
                                                                ByteComparable.preencoded(BYTE_COMPARABLE_VERSION,
                                                                                          bytes, 0, byteLength));
    }

    /// Combine live and deletion branch markers to identify row roots in the trie. Must return non-null when a row
    /// is identified; the actual value is passed on to [UnfilteredIterator#mapContent] below.
    private static Object combineDataAndDeletionForUnfilteredIterator(Object data, TrieTombstoneMarker deletion)
    {
        if (data instanceof LivenessInfo)
            return data; // We don't need to return the deletion marker as it will be included in the tail trie.

        if (deletion != null)
        {
            // There are several ways we can end up here:
            // - The partition deletion starts or ends. This is treated like a range deletion here but thrown away in
            //   mapContent.
            // - A range deletion starts or ends. In this case we don't care about the tail trie as it only contains
            //   the marker we return.
            // - We have a row level marker in the deletion path for a row that has no live data but row, column or cell
            //   deletion. We will return this branch as a deleted row; since we skip the branch, we will also skip the
            //   return path marker.

            if (deletion.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW))
                return LivenessInfo.EMPTY; // Treat this branch as a row.
            else
                return deletion; // Range or partition deletion marker.
        }

        return null;
    }

    /// Source of dropped columns, if they could be different from the metadata in use.
    /// To be overridden by memtable partitions.
    protected TableMetadata droppedColumnsSource()
    {
        return metadata;
    }

    /// Implementation of [UnfilteredRowIterator] for this partition.
    ///
    /// Looks for row and tombstone markers in the trie and presents each branch as a [TrieBackedRow] or
    /// [RangeTombstoneMarker]. Because the legacy convention is that the static row and partition-level deletion are
    /// given separately by methods of the iterator, they are filtered out from the returned content -- the former by
    /// excluding the static key from the translation of [Slice#ALL], and the latter by excluding covering deletions
    /// in the returned trie and filtering out the non-range sides of all tombstone boundaries.
    class UnfilteredIterator
    extends TrieTailsIterator.DeletionAware<Object, TrieTombstoneMarker, Object, Unfiltered>
    implements UnfilteredRowIterator
    {
        final boolean reversed;
        final ColumnFilter selection;
        final Row staticRow;
        final boolean mayFilterColumns;

        protected UnfilteredIterator(ColumnFilter selection, DeletionAwareTrie<Object, TrieTombstoneMarker> trie, boolean reversed)
        {
            super(trie, Direction.fromBoolean(reversed), TrieBackedPartition::combineDataAndDeletionForUnfilteredIterator, false);
            this.selection = selection;
            this.reversed = reversed;
            this.mayFilterColumns = !droppedColumnsSource().droppedColumns.isEmpty() ||
                                    selection != null && (!selection.fetchesAllColumns(false) ||
                                                          !selection.fetchesAllColumns(true) ||
                                                          !selection.allFetchedColumnsAreQueried());
            Row staticRow = filter(TrieBackedPartition.this.staticRow());
            this.staticRow = staticRow != null ? staticRow : Rows.EMPTY_STATIC_ROW;
        }

        Row filter(Row row)
        {
            if (!mayFilterColumns || row == null)
                return row;
            return row.filter(selection, droppedColumnsSource());
        }

        @Override
        protected Unfiltered mapContent(Object content, DeletionAwareTrie<Object, TrieTombstoneMarker> tailTrie, byte[] bytes, int byteLength)
        {
            // content is as prepared by combineDataAndDeletionForUnfilteredIterator above.
            if (content instanceof LivenessInfo)
            {
                // Row.
                return filter(toRow(tailTrie, getClustering(bytes, byteLength)));
            }
            else
            {
                // Range or partition deletion marker.
                // The method below will present only range tombstones, identified by their marker kind.
                return ((TrieTombstoneMarker) content).toRangeTombstoneMarker(
                    ByteComparable.preencoded(BYTE_COMPARABLE_VERSION, bytes, 0, byteLength),
                    BYTE_COMPARABLE_VERSION,
                    metadata.comparator);
            }
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return TrieBackedPartition.this.partitionLevelDeletion;
        }

        @Override
        public EncodingStats stats()
        {
            return stats;
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public boolean isReverseOrder()
        {
            return reversed;
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return selection != null ? selection.fetchedColumns() : TrieBackedPartition.this.columns;
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return partitionKey;
        }

        @Override
        public Row staticRow()
        {
            return staticRow;
        }

        @Override
        public void close()
        {
            // nothing to close
        }

        @Override
        public boolean stopIssuingTombstones()
        {
            stopIssuingDeletions(current -> !current.isRow() || ((Row) current).isEmptyAfterDeletion());
            return true;
        }
    }

    private UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, ByteComparable[] bounds, boolean reversed)
    {
        if (bounds.length == 0)
            return UnfilteredRowIterators.noRowsIterator(metadata, partitionKey, staticRow(), partitionLevelDeletion(), reversed);

        DeletionAwareTrie<Object, TrieTombstoneMarker> slicedTrie =
            trie.intersect(TrieSet.ranges(BYTE_COMPARABLE_VERSION, bounds));
        return new UnfilteredIterator(selection, slicedTrie, reversed);
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
    {
        ByteComparable[] bounds = new ByteComparable[slices.size() * 2];
        int index = 0;
        for (Slice slice : slices)
        {
            bounds[index++] = metadata.comparator.asByteComparable(slice.start());
            bounds[index++] = metadata.comparator.asByteComparable(slice.end());
        }
        return unfilteredIterator(selection, bounds, reversed);
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, NavigableSet<Clustering<?>> clusteringsInQueryOrder, boolean reversed)
    {
        ByteComparable[] bounds = new ByteComparable[clusteringsInQueryOrder.size() * 2];
        // Trie intersection requires the boundaries to be given in forward order. Our clusterings are given in query
        // order, which is why we have to reverse them if we are making a reversed iterator.
        int index = reversed ? (clusteringsInQueryOrder.size() - 1) * 2 : 0;
        int indexInc = reversed ? -2 : +2;
        for (Clustering<?> clustering : clusteringsInQueryOrder)
        {
            bounds[index + 0] = metadata.comparator.asByteComparable(clustering.asStartBound());
            bounds[index + 1] = metadata.comparator.asByteComparable(clustering.asEndBound());
            index += indexInc;
        }
        return unfilteredIterator(selection, bounds, reversed);
    }

    @Override
    public String toString()
    {
        return Partition.toString(this);
    }

    private static class WithDroppedColumnsSource extends TrieBackedPartition
    {
        final TableMetadata droppedColumnsSource;

        public WithDroppedColumnsSource(DecoratedKey partitionKey, RegularAndStaticColumns columns, EncodingStats stats, int rowCountIncludingStatic, int tombstoneCount, DeletionAwareTrie<Object, TrieTombstoneMarker> trie, TableMetadata metadata, TableMetadata droppedColumnsSource)
        {
            super(partitionKey, columns, stats, rowCountIncludingStatic, tombstoneCount, trie, metadata);
            this.droppedColumnsSource = droppedColumnsSource;
        }

        @Override
        protected TableMetadata droppedColumnsSource()
        {
            return droppedColumnsSource;
        }
    }

    /// A snapshot of the current [TrieBackedPartition] data, copied on heap when retrieved.
    private static final class WithCopyingToHeap extends WithDroppedColumnsSource
    {
        public WithCopyingToHeap(DecoratedKey partitionKey,
                                 RegularAndStaticColumns columns,
                                 EncodingStats stats,
                                 int rowCountIncludingStatic,
                                 int tombstoneCount,
                                 DeletionAwareTrie<Object, TrieTombstoneMarker> trie,
                                 TableMetadata metadata,
                                 TableMetadata droppedColumnsSource)
        {
            super(partitionKey, columns, stats, rowCountIncludingStatic, tombstoneCount, trie, metadata, droppedColumnsSource);
        }

        @Override
        public TrieBackedRow toRow(DeletionAwareTrie<Object, TrieTombstoneMarker> data, Clustering<?> clustering)
        {
            TrieBackedRow row = super.toRow(data, clustering);
            if (row == null || row == Rows.EMPTY_STATIC_ROW)
                return row;
            return row.clone(HeapCloner.instance);
        }
    }

    /// Resolver for operations with trie-backed partitions. We don't permit any overwrites/merges.
    @SuppressWarnings("rawtypes")
    private static final InMemoryBaseTrie.UpsertTransformer NO_CONFLICT_RESOLVER =
            (existing, update) ->
            {
                if (existing != null)
                    throw new AssertionError("Unique rows expected.");
                return update;
            };

    /// Resolver for data in trie-backed partitions. We don't permit any overwrites/merges.
    @SuppressWarnings("unchecked")
    public static InMemoryBaseTrie.UpsertTransformer<Object, Object> noConflictInData()
    {
        return NO_CONFLICT_RESOLVER;
    }

    /// Tombstone merging resolver. Even though we don't support overwrites, we get requests to add the two sides
    /// of a boundary separately and must join them.
    private static final InMemoryBaseTrie.UpsertTransformer<TrieTombstoneMarker, TrieTombstoneMarker> MERGE_TOMBSTONE_RANGES =
        (existing, update) -> existing != null ? existing.mergeWith(update) : update;

    /// Tombstone merging resolver. Even though we don't support overwrites, we get requests to add the two sides
    /// of a boundary separately and must join them.
    public static InMemoryBaseTrie.UpsertTransformer<TrieTombstoneMarker, TrieTombstoneMarker> mergeTombstoneRanges()
    {
        return MERGE_TOMBSTONE_RANGES;
    }

    private static final InMemoryBaseTrie.UpsertTransformer<Object, TrieTombstoneMarker> IGNORE_UPDATE = (left, right) -> left;

    /// Resolver for applying incoming deletions to existing data in trie-backed partitions. We assume that the data is
    /// not affected by the deletion.
    public static InMemoryBaseTrie.UpsertTransformer<Object, TrieTombstoneMarker> noIncomingSelfDeletion()
    {
        return IGNORE_UPDATE;
    }

    private static final BiFunction<TrieTombstoneMarker, Object, Object> IGNORE_EXISTING = (left, right) -> right;

    /// Resolver for applying existing deletions to incoming data in trie-backed partitions. We assume that the data is
    /// not affected by the deletion.
    public static BiFunction<TrieTombstoneMarker, Object, Object> noExistingSelfDeletion()
    {
        return IGNORE_EXISTING;
    }

    protected static InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> newTrieWithPartitionMarker()
    {
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie = TrieBackedRow.newTrie();
        putPartitionMarkerInTrie(trie);
        return trie;
    }

    /// Helper class for constructing tries and deletion info from an iterator.
    public static class ContentBuilder
    {
        final TableMetadata metadata;
        final ClusteringComparator comparator;

        private final InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie;
        private final InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker> mutator;

        private final boolean collectDataSize;

        private int rowCountIncludingStatic;
        private int tombstoneCount;
        private long dataSize;

        private RangeTombstoneMarker openMarker = null;
        private final boolean isReverseOrder;

        public ContentBuilder(TableMetadata metadata, DeletionTime partitionLevelDeletion, boolean isReverseOrder, boolean collectDataSize)
        {
            this.metadata = metadata;
            this.comparator = metadata.comparator;

            this.trie = newTrieWithPartitionMarker();
            this.mutator = makeNoConflictMutator(this.trie);

            this.collectDataSize = collectDataSize;

            rowCountIncludingStatic = 0;
            tombstoneCount = 0;
            dataSize = 0;
            this.isReverseOrder = isReverseOrder;

            if (!partitionLevelDeletion.isLive())
            {
                putPartitionDeletionInTrie(mutator, partitionLevelDeletion);
                ++tombstoneCount;
            }
        }

        public ContentBuilder addStatic(Row staticRow)
        {
            if (!staticRow.isEmpty())
                return addRow(staticRow);
            else
                return this;
        }

        public ContentBuilder addRow(Row row)
        {
            putInTrie(mutator, metadata, comparator, row);
            ++rowCountIncludingStatic;
            if (collectDataSize)
                dataSize += row.dataSize();
            if (!row.deletion().isLive())
                ++tombstoneCount;
            return this;
        }

        public ContentBuilder addRangeTombstoneMarker(RangeTombstoneMarker unfiltered)
        {
            if (openMarker != null)
            {
                // This will check that unfiltered closes openMarker
                putRangeDeletionInTrie(mutator,
                                       comparator,
                                       isReverseOrder ? unfiltered : openMarker,
                                       isReverseOrder ? openMarker : unfiltered);
                ++tombstoneCount; // we only count one side of a range, to match DeletionInfo.rangeCount
                if (unfiltered.isOpen(isReverseOrder))
                    openMarker = unfiltered;
                else
                    openMarker = null;
            }
            else
            {
                assert unfiltered.isOpen(isReverseOrder);
                openMarker = unfiltered;
            }
            return this;
        }

        public ContentBuilder addUnfiltered(Unfiltered unfiltered)
        {
            if (unfiltered.kind() == Unfiltered.Kind.ROW)
                return addRow((Row) unfiltered);
            else
                return addRangeTombstoneMarker((RangeTombstoneMarker) unfiltered);
        }

        public ContentBuilder complete()
        {
            assert openMarker == null;
            return this;
        }

        public InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie()
        {
            return trie;
        }

        public int rowCountIncludingStatic()
        {
            return rowCountIncludingStatic;
        }

        public int tombstoneCount()
        {
            return tombstoneCount;
        }

        public int dataSize()
        {
            assert collectDataSize;
            return Ints.saturatedCast(dataSize);
        }
    }
}

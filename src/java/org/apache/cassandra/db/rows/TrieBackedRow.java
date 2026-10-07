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

import java.nio.ByteBuffer;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.MapMaker;
import com.google.common.primitives.Ints;

import org.agrona.collections.Object2IntHashMap;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.IDataSize;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.tries.BaseTrie;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.RangeTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieEntriesIterator;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.db.tries.TrieTailsIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.BiLongAccumulator;
import org.apache.cassandra.utils.LongAccumulator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.cassandra.utils.memory.Cloner;

import static org.apache.cassandra.db.partitions.TrieBackedPartition.BYTE_COMPARABLE_VERSION;
import static org.apache.cassandra.db.partitions.TrieBackedPartition.mergeTombstoneRanges;
import static org.apache.cassandra.db.partitions.TrieBackedPartition.noConflictInData;
import static org.apache.cassandra.db.partitions.TrieBackedPartition.noExistingSelfDeletion;
import static org.apache.cassandra.db.partitions.TrieBackedPartition.noIncomingSelfDeletion;

/// Immutable implementation of a [Row] object, where the data structure is represented by a trie.
/// Stores either [Cell], or [TrieCellData] (if the row comes from a memtable trie).
///
/// Trie-backed rows using a mapping of column ids to numbers that is to be obtained from the table metadata. Currently,
/// we don't support mixing different column sets in trie rows, thus all rows in use in one object (partition, memtable)
/// must share the same column set.
public class TrieBackedRow extends AbstractRow
{
    public static final Object COMPLEX_COLUMN_MARKER = new Object()
    {
        @Override
        public String toString()
        {
            return "COMPLEX_COLUMN_MARKER";
        }
    };

    static final DeletionAwareTrie<Object, TrieTombstoneMarker> EMPTY_ROW = DeletionAwareTrie.singleton(ByteComparable.EMPTY,
                                                                                                        BYTE_COMPARABLE_VERSION,
                                                                                                        LivenessInfo.EMPTY);
    static final Object2IntHashMap<ColumnIdentifier> EMPTY_COLUMN_IDS = makeColumnIdsMap(Columns.NONE);

    private static final long EMPTY_SIZE = ObjectSizes.measure(new TrieBackedRow(Columns.NONE, EMPTY_COLUMN_IDS, Clustering.EMPTY, LivenessInfo.EMPTY, Row.Deletion.LIVE, 0, EMPTY_ROW));

    static final int COLUMN_NOT_PRESENT = -1;

    // A column with this ID cannot exist. Used to make sure we don't find anything when we are passed an unknown column definition.
    public static final ByteComparable MISSING_COLUMN_KEY = encodeUnsignedInt(Long.MAX_VALUE);

    public static final int MAX_RECURSIVE_LENGTH = 128;

    /// The row's clustering key.
    private final Clustering<?> clustering;

    /// Mapping from column id to its numeric index, used to reach the column in the trie.
    private final Object2IntHashMap<ColumnIdentifier> columnIds;
    /// List of columns in this row, also used a map from column index to column id/definition.
    private final Columns columns;

    /// Pre-computed column maps for each `Columns` instance.
    private static final Map<Columns, Object2IntHashMap<ColumnIdentifier>> columnsMapCache = new MapMaker().weakKeys().makeMap();

    // We need to filter the tombstones of a row on every read (twice in fact: first to remove purgeable tombstone, and then after reconciliation to remove
    // all tombstone since we don't return them to the client) as well as on compaction. But it's likely that many rows won't have any tombstone at all, so
    // we want to speed up that case by not having to iterate/copy the row in this case. We could keep a single boolean telling us if we have tombstones,
    // but that doesn't work for expiring columns. So instead we keep the deletion time for the first thing in the row to be deleted. This allow at any given
    // time to know if we have any deleted information or not. If we any "true" tombstone (i.e. not an expiring cell), this value will be forced to
    // Long.MIN_VALUE, but if we don't and have expiring cells, this will the time at which the first expiring cell expires. If we have no tombstones and
    // no expiring cells, this will be CellData.MAX_DELETION_TIME;
    private final long minLocalDeletionTime;

    ///  Data trie contains:
    ///  - LivenessInfo header at the root
    ///  - A Cell for each (simple or complex) cell of the row
    ///    - Cells may be expiring or even expired (not really expected for memtables but possible)
    ///  - Deletion branch with tombstones
    private final DeletionAwareTrie<Object, TrieTombstoneMarker> data;

    /// Copy of the row's liveness info (also stored at the root of the trie).
    private final LivenessInfo livenessInfo;
    /// Copy of the row-level deletion time as [Row.Deletion] (also stored at the root of the deletion branch).
    private final Deletion deletion;

    /// Create a trie-backed version of a row, using the column definitions of the given table metadata.
    public static TrieBackedRow from(TableMetadata metadata, Row row)
    {
        Builder builder = builder(metadata, row.clustering());
        builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo());
        builder.addRowDeletion(row.deletion());
        for (ColumnData cd : row)
            builder.addColumn(cd);
        return builder.build();
    }

    /// Returns false if the given object is a level marker with no meaning of its own. Used to drop unproductive
    /// markers that can remain after deletions.
    public static boolean shouldPreserveContentWithoutChildren(Object o)
    {
        if (o == LivenessInfo.EMPTY || o == COMPLEX_COLUMN_MARKER || o == TrieTombstoneMarker.LevelMarker.ROW)
            return false;
        if (!(o instanceof TrieTombstoneMarker))
            return true;
        TrieTombstoneMarker m = (TrieTombstoneMarker) o;
        if (!m.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW))
            return true;
        return !Objects.equals(m.leftDeletion(), m.rightDeletion());
    }

    public static TrieBackedRow create(TableMetadata tableMetadata, Clustering<?> clustering, DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        return create(tableMetadata.regularAndStaticColumns().columns(clustering == Clustering.STATIC_CLUSTERING), clustering, data);
    }

    static TrieBackedRow create(Columns columns, Clustering<?> clustering, DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        return create(columns,
                      columnsMapCache.computeIfAbsent(columns, TrieBackedRow::makeColumnIdsMap),
                      clustering,
                      data);
    }

    static TrieBackedRow create(Columns columns,
                                Object2IntHashMap<ColumnIdentifier> columnIds,
                                Clustering<?> clustering,
                                DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        TrieTombstoneMarker deletionRoot = data.deletionAtRoot();
        Deletion deletion = Deletion.LIVE;
        long minLocalDeletionTime = CellData.MAX_DELETION_TIME;
        if (deletionRoot != null)
        {
            TrieTombstoneMarker.Covering rowDeletionTime = deletionRoot.applicableToPointForward();
            if (rowDeletionTime != null)
                deletion = Deletion.regular(rowDeletionTime);
            if (data instanceof InMemoryDeletionAwareTrie || rowDeletionTime != null)
            {
                // We only have a deletion row marker if we have a real deletion.
                minLocalDeletionTime = Long.MIN_VALUE;
            }
            else
                minLocalDeletionTime = accumulate(minLocalDeletionTime, TrieBackedRow::mergeMinLocalDeletionTime, TrieBackedRow::mergeMinLocalDeletionTime, TrieBackedRow::mergeMinLocalDeletionTime, data);
        }
        else
            minLocalDeletionTime = accumulate(minLocalDeletionTime, TrieBackedRow::mergeMinLocalDeletionTime, TrieBackedRow::mergeMinLocalDeletionTime, data.contentOnlyTrie());

        return new TrieBackedRow(columns,
                                 columnIds,
                                 clustering,
                                 getLivenessInfo(data),
                                 deletion,
                                 minLocalDeletionTime,
                                 data);
    }

    private TrieBackedRow(Columns columns,
                          Object2IntHashMap<ColumnIdentifier> columnIds,
                          Clustering<?> clustering,
                          LivenessInfo livenessInfo,
                          Deletion deletion,
                          long minLocalDeletionTime,
                          DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        this.deletion = deletion;
        this.livenessInfo = livenessInfo;
        this.minLocalDeletionTime = minLocalDeletionTime;
        assert data != null;
        this.columns = columns;
        this.columnIds = columnIds;
        this.clustering = clustering;
        this.data = data;
    }

    private static Object2IntHashMap<ColumnIdentifier> makeColumnIdsMap(Columns columns)
    {
        Object2IntHashMap<ColumnIdentifier> columnIds = new Object2IntHashMap<>(COLUMN_NOT_PRESENT);
        for (int i = 0; i < columns.size(); i++)
            columnIds.put(columns.getSimple(i).name, i);
        return columnIds;
    }

    private static RangeTrie<TrieTombstoneMarker> rowDeletionTrie(DeletionTime deletion)
    {
        return deletionTrie(ByteComparable.EMPTY, deletion, TrieTombstoneMarker.Kind.ROW);
    }

    private static RangeTrie<TrieTombstoneMarker> deletionTrie(ByteComparable prefix, DeletionTime deletion, TrieTombstoneMarker.Kind kind)
    {
        return withDeletionRoot(RangeTrie.branch(prefix,
                                                 BYTE_COMPARABLE_VERSION,
                                                 TrieTombstoneMarker.covering(deletion, kind)));
    }

    public static TrieBackedRow emptyDeletedRow(Clustering<?> clustering, DeletionTime deletion)
    {
        assert !deletion.isLive();
        RangeTrie<TrieTombstoneMarker> deletionTrie = rowDeletionTrie(deletion);
        return new TrieBackedRow(Columns.NONE, EMPTY_COLUMN_IDS, clustering, LivenessInfo.EMPTY, Deletion.regular(deletion), Long.MIN_VALUE,
                                 DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY, BYTE_COMPARABLE_VERSION, deletionTrie));
    }

    /// Adjust the given deletion trie to include a row-level marker.
    private static RangeTrie<TrieTombstoneMarker> withDeletionRoot(RangeTrie<TrieTombstoneMarker> trie)
    {
        // Range tries present separate content in the two directions. We need to add a marker in both.
        return trie.mergeWith(RangeTrie.point(ByteComparable.EMPTY,
                                              BYTE_COMPARABLE_VERSION,
                                              true,
                                              TrieTombstoneMarker.LevelMarker.ROW),
                              TrieTombstoneMarker::mergeUpdate)
                   .mergeWith(RangeTrie.point(ByteComparable.EMPTY,
                                              BYTE_COMPARABLE_VERSION,
                                              false,
                                              TrieTombstoneMarker.LevelMarker.ROW),
                              TrieTombstoneMarker::mergeUpdate);
    }

    @Override
    public long accumulate(LongAccumulator<ColumnData> accumulator, long initialValue)
    {
        // TODO: this isn't efficient at all
        long v = initialValue;
        for (ColumnData c : this)
            v = accumulator.apply(c, v);
        return v;
    }

    @Override
    public <A> long accumulate(BiLongAccumulator<A, ColumnData> accumulator, A arg, long initialValue)
    {
        // TODO: this isn't efficient at all
        long v = initialValue;
        for (ColumnData c : this)
            v = accumulator.apply(arg, c, v);
        return v;
    }

    private static class Accumulator implements DeletionAwareTrie.ValueConsumer<Object, TrieTombstoneMarker>
    {
        final LongAccumulator<? super CellData<?, ?>> cellAccumulator;
        final LongAccumulator<? super LivenessInfo> livenessAccumulator;
        final LongAccumulator<? super DeletionTime> markerAccumulator;
        long value;

        Accumulator(long initialValue,
                    LongAccumulator<? super CellData<?, ?>> cellAccumulator,
                    LongAccumulator<? super LivenessInfo> livenessAccumulator,
                    LongAccumulator<? super DeletionTime> markerAccumulator)
        {
            this.cellAccumulator = cellAccumulator;
            this.livenessAccumulator = livenessAccumulator;
            this.markerAccumulator = markerAccumulator;
            this.value = initialValue;
        }

        @Override
        public void content(Object content)
        {
            if (content instanceof LivenessInfo)
                value = livenessAccumulator.apply((LivenessInfo) content, value);
            else if (content instanceof CellData)
                value = cellAccumulator.apply((CellData<?, ?>) content, value);
            else if (content != COMPLEX_COLUMN_MARKER)
                throw new AssertionError("Unexpected content type: " + content);
        }

        @Override
        public void deletionMarker(TrieTombstoneMarker marker)
        {
            if (marker.isBoundary())
            {
                // We only apply the function to one side of the marker; the other has to be already seen as a
                // succeeding side of a different marker.
                TrieTombstoneMarker.Covering applicableState = marker.applicableToPointForward();
                if (applicableState != null)
                    value = markerAccumulator.apply(applicableState, value);
            }
        }
    }

    private static long accumulate(long initialValue,
                                   LongAccumulator<? super LivenessInfo> livenessAccumulator,
                                   LongAccumulator<? super CellData<?, ?>> cellAccumulator,
                                   Trie<Object> data)
    {
        Accumulator accumulator = new Accumulator(initialValue, cellAccumulator, livenessAccumulator,
                                                  (d, l) -> {
                                                      throw new AssertionError();
                                                  });
        data.process(Direction.FORWARD, accumulator);
        return accumulator.value;
    }

    private static long accumulate(long initialValue,
                                   LongAccumulator<? super LivenessInfo> livenessAccumulator,
                                   LongAccumulator<? super CellData<?, ?>> cellAccumulator,
                                   LongAccumulator<? super DeletionTime> markerAccumulator,
                                   DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        Accumulator accumulator = new Accumulator(initialValue, cellAccumulator, livenessAccumulator, markerAccumulator);
        data.process(Direction.FORWARD, accumulator);
        return accumulator.value;
    }

    /// Accumulate a long value, using the given cell-level functions.
    ///
    /// Note: For efficiency, the cell accumulator is given cells without path. If the path is needed, use a different
    /// `accumulate` method.
    long accumulate(long initialValue,
                    LongAccumulator<? super LivenessInfo> livenessAccumulator,
                    LongAccumulator<? super CellData<?, ?>> cellAccumulator,
                    LongAccumulator<? super DeletionTime> markerAccumulator)
    {
        return accumulate(initialValue, livenessAccumulator, cellAccumulator, markerAccumulator, data);
    }

    @Override
    public Clustering<?> clustering()
    {
        return clustering;
    }

    @Override
    public LivenessInfo primaryKeyLivenessInfo()
    {
        return livenessInfo;
    }

    public static LivenessInfo getLivenessInfo(DeletionAwareTrie<Object, TrieTombstoneMarker> trie)
    {
        LivenessInfo info = (LivenessInfo) trie.get(ByteComparable.EMPTY);
        return info != null ? info : LivenessInfo.EMPTY;
    }

    @Override
    public boolean isEmpty()
    {
        // Empty has no live or deletion branch but may have an empty row marker.
        return isEmpty(data);
    }

    public static boolean isEmpty(DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        if (data == null)
            return true;

        if (!isEmptyAfterDeletion(data))
            return false;

        if (data instanceof InMemoryDeletionAwareTrie)
        {
            // the row deletion marker will only be present if there is a deletion present
            return data.deletionAtRoot() == null;
        }
        else
        {
            // The row deletion marker may remain even if the data is deleted/filtered out.
            // Check for the existence of a deletion boundary
            return !data.deletionBranchAtRoot().filteredValuesIterator(Direction.FORWARD, TrieTombstoneMarker.Boundary.class).hasNext();
        }
    }

    @Override
    public boolean isEmptyAfterDeletion()
    {
        return isEmptyAfterDeletion(data);
    }

    public static boolean isEmptyAfterDeletion(DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        if (data instanceof InMemoryDeletionAwareTrie)
        {
            // the liveness marker will be dropped if there are no cells
            return data.get(ByteComparable.EMPTY) == null;
        }
        else
        {
            // The liveness marker may remain even if the data is deleted/filtered out.
            // Check for the existence of:
            // - non-empty liveness
            LivenessInfo info = (LivenessInfo) data.get(ByteComparable.EMPTY);
            if (info != null && info != LivenessInfo.EMPTY)
                return false;

            // - a cell
            return !data.contentOnlyTrie().filteredValuesIterator(Direction.FORWARD, CellData.class).hasNext();
        }
    }

    @Override
    public Deletion deletion()
    {
        return deletion;
    }

    static Deletion getDeletion(DeletionAwareTrie<Object, TrieTombstoneMarker> trie)
    {
        DeletionTime delTime = TrieTombstoneMarker.applicableDeletionAtRoot(trie);
        if (delTime == null)
            return Deletion.LIVE;
        else
            return Deletion.regular(delTime);
    }

    public static ByteSource columnKey(Columns columns, ColumnMetadata column)
    {
        return ByteSource.variableLengthUnsignedInteger(columns.simpleIdx(column));
    }

    public static ByteSource cellPathKey(ColumnMetadata column, CellPath path, ByteComparable.Version version)
    {
        return ByteSource.withTerminator(ByteSource.TERMINATOR,
                                         getCellPathType(column).asComparableBytes(path.get(0), version));
    }

    static int columnId(Object2IntHashMap<ColumnIdentifier> columnIds, ColumnMetadata column)
    {
        return columnIds.getValue(column.name);
    }

    /// Return a cell key (i.e. path in the trie) for the given (simple or complex) column and cell path (which must be
    /// non-null if the column is complex).
    private static ByteComparable cellKey(Object2IntHashMap<ColumnIdentifier> columnIds, ColumnMetadata column, CellPath path)
    {
        int id = columnId(columnIds, column);
        if (id == COLUMN_NOT_PRESENT)
            return MISSING_COLUMN_KEY; // SAI can call cellKey for static columns on regular rows and vice versa
        if (!column.isComplex())
            return encodeUnsignedInt(id);
        else
            return cellKey(id, column, path);
    }

    private ByteComparable cellKey(ColumnMetadata column, CellPath path)
    {
        return cellKey(columnIds, column, path);
    }

    private static ByteSource columnIdPrefix(int columnId)
    {
        if (columnId < 0)
            return ByteSource.EMPTY;
        else
            return ByteSource.variableLengthUnsignedInteger(columnId);
    }

    /// Return a cell key (i.e. path in the trie) for a complex cell with the given column index and cell path.
    ///
    /// Note: this method is also used by [TrieBackedComplexColumn] where the column index is in the path leading to the
    /// complex column trie. To support this, a `columnId` of -1 is used to skip the column index.
    static ByteComparable cellKey(int columnId, ColumnMetadata column, @Nonnull CellPath path)
    {
        if (path == CellPath.BOTTOM)
            return v -> ByteSource.concat(columnIdPrefix(columnId),
                                          ByteSource.oneByte(ByteSource.LT_NEXT_COMPONENT));
        else if (path == CellPath.TOP)
            return v -> ByteSource.concat(columnIdPrefix(columnId),
                                          ByteSource.oneByte(ByteSource.GT_NEXT_COMPONENT));
        else
            return v -> ByteSource.concat(columnIdPrefix(columnId),
                                          cellPathKey(column, path, v));
        // TODO: figure out a better way to do path slices and remove the leading path byte as
//        return v -> ByteSource.concat(columnIdPrefix(columnId),
//                                      getCellPathType(column).asComparableBytes(path.get(0), v),
//                                      ByteSource.oneByte(ByteSource.TERMINATOR));
    }

    private static AbstractType<?> getCellPathType(ColumnMetadata column)
    {
        assert column.isComplex();
        return ColumnMetadata.pathNameComparator(column.type);
    }

    /// Returns the column key, i.e. the column index path in the trie without the part corresponding to the cell path.
    static ByteComparable columnKey(Object2IntHashMap<ColumnIdentifier> columnIds, ColumnMetadata column)
    {
        int id = columnId(columnIds, column);
        if (id == COLUMN_NOT_PRESENT)
            return MISSING_COLUMN_KEY;
        return encodeUnsignedInt(id);
    }

    /// Convert the cell-path part of the trie path into a cell path to use in a [Cell].
    static CellPath cellPath(ColumnMetadata column, ByteSource.Peekable src)
    {
        int next = src.next();
        if (next == ByteSource.LT_NEXT_COMPONENT)
            return CellPath.BOTTOM;
        else if (next == ByteSource.GT_NEXT_COMPONENT)
            return CellPath.TOP;

        ByteSource.Peekable componentSource = ByteSourceInverse.nextComponentSource(src, next);
        ByteBuffer path = getCellPathType(column).fromComparableBytes(componentSource, BYTE_COMPARABLE_VERSION);
        return CellPath.create(path);
        // TODO: figure out a better way to do path slices and remove the leading path byte as
//        return CellPath.create(getCellPathType(column).fromComparableBytes(src, BYTE_COMPARABLE_VERSION));
    }

    @Override
    public Cell<?> getCell(ColumnMetadata c)
    {
        assert !c.isComplex();
        return getCellInternal(c, null);
    }

    @Override
    public Cell<?> getCell(ColumnMetadata c, CellPath path)
    {
        assert c.isComplex();
        return getCellInternal(c, path);
    }

    private Cell<?> getCellInternal(ColumnMetadata c, CellPath path)
    {
        Object o = data.get(cellKey(c, path));
        if (o == null || o instanceof Cell)
            return (Cell<?>) o;
        CellData<?, ?> cellData = (CellData<?, ?>) o;
        return cellData.toCell(c, path);
    }

    @Override
    public ComplexColumnData getComplexColumnData(ColumnMetadata c)
    {
        assert c.isComplex();
        DeletionAwareTrie<Object, TrieTombstoneMarker> tail = data.tailTrie(columnKey(columnIds, c));
        if (!isColumnDataTrieEmpty(tail))
            return new TrieBackedComplexColumn(c, tail);
        else
            return null;
    }

    private static boolean isColumnDataTrieEmpty(DeletionAwareTrie<Object, TrieTombstoneMarker> tail)
    {
        if (tail == null)
            return true;
        // We may be left with only a COMPLEX_COLUMN_MARKER after some transformation.
        if (tail instanceof InMemoryDeletionAwareTrie)
            return false; // in-memory trie will drop the marker
        if (TrieTombstoneMarker.applicableDeletionAtRoot(tail) != null)
            return false;
        // otherwise it's empty if it has no cells
        return !tail.filteredValuesIterator(Direction.FORWARD, CellData.class).hasNext();
    }

    @Override
    public ColumnData getColumnData(ColumnMetadata c)
    {
        return c.isComplex() ? getComplexColumnData(c) : getCell(c);
    }

    @Override
    public Collection<ColumnMetadata> columns()
    {
        return new AbstractCollection<>()
        {
            @Override public Iterator<ColumnMetadata> iterator()
            {
                return Iterators.transform(TrieBackedRow.this.iterator(), ColumnData::column);
            }
            @Override public int size()
            {
                return columnCount();
            }
        };
    }

    /// Combine data in the live and deletion branches to identify column roots.
    private static Object combineDataAndDeletionForColumnIterator(Object content, TrieTombstoneMarker marker, Direction direction)
    {
        if (content instanceof CellData)
            return content;
        if (content == COMPLEX_COLUMN_MARKER)
            return content;
        // We may also have a complex column in the deletion trie, introducing a COLUMN-level deletion.
        if (marker == null)
            return null; // any other content in the live part of the trie is not a column root
        TrieTombstoneMarker.Covering introducedDeletion = marker.succedingState(direction);
        if (introducedDeletion != null && introducedDeletion.deletionKind() == TrieTombstoneMarker.Kind.COLUMN)
            return COMPLEX_COLUMN_MARKER;
        // This is a complex column deletion marker. Return it as a complex column, which will also result in skipping
        // the return path marker.
        return null;
    }

    private static Object combineDataAndDeletionForColumnIteratorForward(Object content, TrieTombstoneMarker marker)
    {
        return combineDataAndDeletionForColumnIterator(content, marker, Direction.FORWARD);
    }

    private static Object combineDataAndDeletionForColumnIteratorReverse(Object content, TrieTombstoneMarker marker)
    {
        return combineDataAndDeletionForColumnIterator(content, marker, Direction.REVERSE);
    }

    static class ColumnDataIterator extends TrieTailsIterator.DeletionAware<Object, TrieTombstoneMarker, Object, ColumnData>
    {
        private final Columns columns;

        ColumnDataIterator(Columns columns, DeletionAwareTrie<Object, TrieTombstoneMarker> trie, Direction direction)
        {
            super(trie,
                  direction,
                  direction.select(TrieBackedRow::combineDataAndDeletionForColumnIteratorForward,
                                   TrieBackedRow::combineDataAndDeletionForColumnIteratorReverse),
                  false);
            this.columns = columns;
        }

        @Override
        protected ColumnData mapContent(Object value, DeletionAwareTrie<Object, TrieTombstoneMarker> tailTrie, byte[] bytes, int byteLength)
        {
            // value is given by combineDataAndDeletionForColumnIterator above
            if (value != COMPLEX_COLUMN_MARKER)
                return cellFromCellData((CellData<?, ?>) value, bytes, byteLength, columns);

            return new TrieBackedComplexColumn(columnMetadataFromPath(bytes, byteLength, columns), tailTrie);
        }

    }

    public static ColumnMetadata columnMetadataFromPath(byte[] bytes, int byteLength, Columns columns)
    {
        long columnIndex = ByteSourceInverse.getVariableLengthUnsignedInteger(ByteSource.preencoded(bytes, 0, byteLength));
        assert ((int) columnIndex) == columnIndex;
        return columns.getSimple((int) columnIndex);
    }

    public static Cell<?> cellFromCellData(CellData<?, ?> value, byte[] bytes, int byteLength, Columns columns)
    {
        if (value instanceof Cell)
            return (Cell<?>) value;
        ByteSource.Peekable pathBytes = ByteSource.preencoded(bytes, 0, byteLength);
        long columnIdx = ByteSourceInverse.getVariableLengthUnsignedInteger(pathBytes);
        ColumnMetadata column = columns.getSimple((int) columnIdx);
        return value.toCell(column, column.isComplex() ? cellPath(column, pathBytes) : null);
    }

    /// Count the columns in the provided data trie (a row or a partition trie). This includes simple and complex
    /// columns, including fully deleted ones.
    public static int countColumns(DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        class Counter implements BaseTrie.ValueConsumer<Object>
        {
            int count = 0;

            @Override
            public void content(Object content)
            {
                ++count;
            }
        }

        Counter counter = new Counter();
        data.mergedTrie(TrieBackedRow::combineDataAndDeletionForColumnIteratorForward)
            .processSkippingBranches(Direction.FORWARD, counter);
        return counter.count;
    }

    @Override
    public int columnCount()
    {
        return countColumns(data);
    }

    @Override
    public Iterator<ColumnData> iterator()
    {
        return new ColumnDataIterator(columns, data, Direction.FORWARD);
    }

    @Override
    public Iterable<Cell<?>> cells()
    {
        return () -> new CellsWithPath(data.contentOnlyTrie(), Direction.FORWARD);
    }

    @Override
    public Row filter(ColumnFilter filter, TableMetadata droppedColumnsSource)
    {
        return filter(filter, DeletionTime.LIVE, false, droppedColumnsSource);
    }

    @Override
    public Row filter(ColumnFilter filter, DeletionTime activeDeletion, boolean setActiveDeletionToRow, TableMetadata droppedColumnsSource)
    {
        Map<ByteBuffer, DroppedColumn> droppedColumns = droppedColumnsSource.droppedColumns;

        boolean mayFilterColumns = filter != null && (!filter.fetchesAllColumns(isStatic()) || !filter.allFetchedColumnsAreQueried());
        // When merging sstable data in Row.Merger#merge(), rowDeletion is removed if it doesn't supersede activeDeletion.
        boolean mayHaveDeleted = activeDeletion.supersedes(deletion.time());
        if (!mayFilterColumns && !mayHaveDeleted && droppedColumns.isEmpty())
            return this;

        if (!mayFilterColumns && droppedColumns.isEmpty() && setActiveDeletionToRow)
        {
            assert mayHaveDeleted;
            return new TrieBackedRow(columns, columnIds,
                                     clustering,
                                     activeDeletion.deletes(livenessInfo) ? LivenessInfo.EMPTY : livenessInfo,
                                     Deletion.regular(activeDeletion),
                                     Long.MIN_VALUE,
                                     applyAndSetActiveDeletion(data, activeDeletion));
        }

        try
        {
            TrieColumnFilter tcf = new TrieColumnFilter(columns, columnIds, filter, isStatic(), mayFilterColumns);

            if (!droppedColumns.isEmpty())
            {
                // If we are filtering, we only need to drop columns we have selected. If not, a dropped column may be
                // in the columns set but not filtered out.
                tcf.applyDroppedColumns(droppedColumns, mayFilterColumns ? filter.fetchedColumns().columns(isStatic()) : columns);
            }

            if (mayHaveDeleted && !setActiveDeletionToRow)
                tcf.applyDeletion(activeDeletion);

            DeletionAwareTrie<Object, TrieTombstoneMarker> filteredData = tcf.apply(data);
            if (mayHaveDeleted && setActiveDeletionToRow)
                filteredData = applyAndSetActiveDeletion(filteredData, activeDeletion);

            if (isEmpty(filteredData))
                return null;

            return TrieBackedRow.create(columns, columnIds, clustering, filteredData);
        }
        catch (TrieSpaceExhaustedException e)
        {
            // This cannot happen
            throw new AssertionError(e);
        }
    }

    private static DeletionAwareTrie<Object, TrieTombstoneMarker>
    applyAndSetActiveDeletion(DeletionAwareTrie<Object, TrieTombstoneMarker> data,
                              DeletionTime activeDeletion)
    {
        return data.mergeWithDeletion(RangeTrie.branch(ByteComparable.EMPTY,
                                                       BYTE_COMPARABLE_VERSION,
                                                       TrieTombstoneMarker.covering(activeDeletion, TrieTombstoneMarker.Kind.ROW)),
                                      TrieBackedRow::deleteData,
                                      TrieTombstoneMarker::mergeUpdate,
                                      true);
    }

    private static Object deleteData(Object existing, TrieTombstoneMarker marker)
    {
        return deleteData(marker, existing);
    }

    private static Object deleteData(TrieTombstoneMarker marker, Object existing)
    {
        DeletionTime deletion = marker.applicableToPointForward();
        if (deletion == null)
            return existing;
        if (existing == COMPLEX_COLUMN_MARKER)
            return existing;
        if (existing instanceof LivenessInfo)
        {
            if (deletion.deletes(((LivenessInfo) existing).timestamp()))
                return LivenessInfo.EMPTY;
            else
                return existing;
        }
        if (existing instanceof CellData)
        {
            if (deletion.deletes((CellData<?, ?>) existing))
                return null;
            else
                return existing;
        }
        throw new AssertionError("Unknown content type: " + existing);
    }

    static ByteComparable encodeUnsignedInt(long id)
    {
        return v -> ByteSource.variableLengthUnsignedInteger(id);
    }

    @Override
    public Row withOnlyQueriedData(ColumnFilter filter)
    {
        if (filter.allFetchedColumnsAreQueried())
            return this;

        Columns queried = filter.queriedColumns().columns(isStatic());
        try
        {
            TrieColumnFilter tcf = new TrieColumnFilter(columns, columnIds, queried);
            return create(columns, columnIds, clustering, tcf.apply(data));
        }
        catch (TrieSpaceExhaustedException e)
        {
            // can't happen
            throw new AssertionError(e);
        }
    }

    @Override
    public boolean hasComplexDeletion()
    {
        for (Iterator<TrieTombstoneMarker> it = data.deletionBranchAtRoot().valueIterator(); it.hasNext();)
        {
            TrieTombstoneMarker marker = it.next();
            TrieTombstoneMarker.Covering introducedDeletion = marker.rightDeletion();
            if (introducedDeletion != null && introducedDeletion.deletionKind() == TrieTombstoneMarker.Kind.COLUMN)
                return true;
        }
        return false;
    }

    @Override
    public Row markCounterLocalToBeCleared()
    {
        return transformAndFilter(x -> x, CellData::markCounterLocalToBeCleared);
    }

    @Override
    public boolean hasLiveData(long nowInSec, boolean enforceStrictLiveness)
    {
        if (primaryKeyLivenessInfo().isLive(nowInSec))
            return true;
        else if (enforceStrictLiveness)
            return false;
        return Iterables.any(cells(), cell -> cell.isLive(nowInSec));
    }

    @Override
    public boolean hasDeletion(long nowInSec)
    {
        return nowInSec >= minLocalDeletionTime;
    }

    @Override
    public long minLocalDeletionTime()
    {
        return minLocalDeletionTime;
    }

    @Override
    public boolean hasInvalidDeletions()
    {
        return accumulate(0,
                          (liveness, v) -> (liveness.isExpiring() && (liveness.ttl() < 0 || liveness.localExpirationTime() < 0)) ? 1 : v,
                          (cell, v) -> cell.hasInvalidDeletions() ? 1 : v,
                          (marker, v) -> !marker.validate() ? 1 : v)
               != 0;
    }

    /// Return the backing trie for merging into a partition or memtable.
    public DeletionAwareTrie<Object, TrieTombstoneMarker> trie()
    {
        return data;
    }

    @Override
    public Row updateAllTimestamp(long newTimestamp)
    {
        return transformAndFilter(liveness -> liveness.withUpdatedTimestamp(newTimestamp),
                                  cell -> cell.updateAllTimestamp(newTimestamp),
                                  dt -> dt.isLive() ? dt : DeletionTime.build(newTimestamp - 1, dt.localDeletionTime()));
    }

    @Override
    public Row withRowDeletion(DeletionTime newDeletion)
    {
        // Applies the deletion to the branch, removing any shadowed data (caller should ensure there isn't any, but
        // we do this properly for safety).
        if (newDeletion.isLive())
            return this;

        DeletionAwareTrie<Object, TrieTombstoneMarker> newData = data.mergeWithDeletion(rowDeletionTrie(newDeletion),
                                                                                        TrieBackedRow::deleteData,
                                                                                        TrieTombstoneMarker::mergeUpdate,
                                                                                        true);
        return new TrieBackedRow(columns, columnIds, clustering, getLivenessInfo(newData), getDeletion(newData), Long.MIN_VALUE, newData);
    }

    @Override
    public Row purge(DeletionPurger purger, long nowInSec, boolean enforceStrictLiveness)
    {
        // TODO: evaluate need/performance effect
        if (!hasDeletion(nowInSec))
            return this;

        if (enforceStrictLiveness)
        {
            // when enforceStrictLiveness is set, a row is considered dead when it's PK liveness info is not present
            LivenessInfo primaryLiveness = primaryKeyLivenessInfo();
            primaryLiveness = purger.shouldPurge(primaryLiveness, nowInSec) ? LivenessInfo.EMPTY : primaryLiveness;
            DeletionTime rowDeletion = TrieTombstoneMarker.applicableDeletionAtRoot(data);
            rowDeletion = rowDeletion != null && !purger.shouldPurge(rowDeletion) ? rowDeletion : null;
            if (primaryLiveness.isEmpty() && rowDeletion == null)
                return null;
        }

        return transformAndFilter(primaryKeyLivenessInfo -> purger.shouldPurge(primaryKeyLivenessInfo, nowInSec) ? LivenessInfo.EMPTY : primaryKeyLivenessInfo,
                                  cell -> cell.purge(purger, nowInSec),
                                  deletion -> purger.shouldPurge(deletion) ? null : deletion);
    }

    public Row purgeDataOlderThan(long timestamp, boolean enforceStrictLiveness)
    {
        if (enforceStrictLiveness)
        {
            // when enforceStrictLiveness is set, a row is considered dead when it's PK liveness info is not present
            DeletionTime rowDeletion = TrieTombstoneMarker.applicableDeletionAtRoot(data);
            if (primaryKeyLivenessInfo().timestamp() < timestamp && (rowDeletion == null || rowDeletion.markedForDeleteAt() < timestamp))
                return null;
        }

        return transformAndFilter(primaryKeyLivenessInfo -> primaryKeyLivenessInfo.timestamp() < timestamp ? LivenessInfo.EMPTY : primaryKeyLivenessInfo,
                                  c -> c.purgeDataOlderThan(timestamp),
                                  deletion -> deletion.markedForDeleteAt() < timestamp ? null : deletion);
    }


    @Override
    public Row transformAndFilter(UnaryOperator<LivenessInfo> livenessInfoFunction,
                                  CellTransformer cellFunction)
    {
        return create(columns, columnIds, clustering, data.mapValues(
            (Object x) ->
            {
                if (x instanceof LivenessInfo)
                {
                    return livenessInfoFunction.apply((LivenessInfo) x);
                }
                else if (x instanceof CellData)
                {
                    //noinspection rawtypes
                    return cellFunction.apply((CellData) x);
                }
                else
                    return x;   // complex column marker
            }));
    }

    Row transformAndFilter(Function<LivenessInfo, LivenessInfo> livenessInfoFunction,
                           Function<CellData<?, ?>, CellData<?, ?>> cellFunction,
                           Function<DeletionTime, DeletionTime> markerFunction)
    {
        DeletionAwareTrie<Object, TrieTombstoneMarker> mappedData = data.mapValuesAndDeletions(
            (Object x) ->
            {
                if (x instanceof LivenessInfo)
                {
                    return (livenessInfoFunction.apply((LivenessInfo) x));
                }
                else if (x instanceof CellData)
                {
                    return cellFunction.apply((CellData<?, ?>) x);
                }
                else
                    return x;   // complex column marker
            },
            t -> t.map(markerFunction));
        if (isEmpty(mappedData))
            return null;

        return create(columns,
                      columnIds,
                      clustering,
                      mappedData);
    }

    @Override
    public TrieBackedRow clone(Cloner cloner)
    {
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> newTrie = newTrie();
        try
        {
            newTrie.mutator(((ex, toClone) -> toClone instanceof CellData ? ((CellData<?, ?>) toClone).clone(cloner) : toClone),
                            mergeTombstoneRanges(),
                            noIncomingSelfDeletion(),
                            noExistingSelfDeletion(),
                            true,
                            Predicates.alwaysFalse())
            .apply(data);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return new TrieBackedRow(columns, columnIds, cloner.clone(clustering), livenessInfo, deletion, minLocalDeletionTime, newTrie);
    }

    // TODO: Redo size collection to be more direct.

    @Override
    public int dataSize()
    {
        // TrieMemtable and its trie-backed elements only count data and deletions, ignoring key sizes as they are part
        // of the trie (and very costly to measure on insert/delete).
        LongAccumulator<IDataSize> accumulator = (o, v) -> v + o.dataSize();
        return Ints.checkedCast(accumulate(0L,
                                           accumulator,
                                           accumulator,
                                           accumulator));
    }

    @Override
    public int liveDataSize(long nowInSec)
    {
        // This is used for paging by bytes and must match BTreeRow.liveDataSize. Unlike dataSize(), it includes the
        // size of the clustering.
        LivenessInfo liveLivenessInfo = livenessInfo.isLive(nowInSec) ? livenessInfo : LivenessInfo.EMPTY;
        int dataSize = clustering.dataSize()
                       + liveLivenessInfo.dataSize()
                       + deletion.dataSize();

        return Ints.checkedCast(accumulate((cd, v) -> v + cd.liveDataSize(nowInSec), dataSize));
    }

    @Override
    public long unsharedHeapSize()
    {
        long heapSize = EMPTY_SIZE + clustering.unsharedHeapSizeExcludingData();
        // For this calculation we assume the source of the row trie is a memtable and don't count its on-heap size.

        return accumulate(heapSize,
                          (liveness, v) -> v + liveness.unsharedHeapSize(),
                          (cell, v) -> v + cell.unsharedHeapSize(),
                          (marker, v) -> v + marker.unsharedHeapSize());
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        long heapSize = EMPTY_SIZE + clustering.unsharedHeapSizeExcludingData();
        if (data instanceof InMemoryDeletionAwareTrie)
            heapSize += ((InMemoryDeletionAwareTrie<?, ?>) data).usedSizeOnHeap();

        return accumulate(heapSize,
                          (liveness, v) -> v + liveness.unsharedHeapSize(),
                          (cell, v) -> v + cell.unsharedHeapSizeExcludingData(),
                          (marker, v) -> v + marker.unsharedHeapSize());
    }

    @Override
    public void apply(Consumer<ColumnData> function)
    {
        for (ColumnData cd : this)
            function.accept(cd);
    }

    @Override
    public <A> void apply(BiConsumer<A, ColumnData> function, A arg)
    {
        for (ColumnData cd : this)
            function.accept(arg, cd);
    }

    private static Builder builder(TableMetadata metadata, Clustering<?> clustering)
    {
        Builder builder = new Builder(clustering == Clustering.STATIC_CLUSTERING ? metadata.staticColumns() : metadata.regularColumns());
        builder.newRow(clustering);
        return builder;
    }

    public static Row.Builder builder(Columns columns)
    {
        return new Builder(columns);
    }

    private static Object mergeData(Object existing, Object update)
    {
        if (update instanceof LivenessInfo)
            return LivenessInfo.merge((LivenessInfo) existing, (LivenessInfo) update);
        else if (update instanceof CellData)
        {
            CellData<?, ?> existingCell = (CellData<?, ?>) existing;
            CellData<?, ?> updateCell = (CellData<?, ?>) update;
            //noinspection unchecked
            return Cells.<CellData>reconcile(existingCell, updateCell);
        }
        else
        {
            assert existing == COMPLEX_COLUMN_MARKER;
            return existing;
        }
    }

    public static InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> newTrie()
    {
        return InMemoryDeletionAwareTrie.shortLived(BYTE_COMPARABLE_VERSION, TrieBackedRow::shouldPreserveContentWithoutChildren);
    }

    public Row mergeWith(Row updateAsRow)
    {
        if (!(updateAsRow instanceof TrieBackedRow))
            throw new IllegalArgumentException("Merging different row types.");
        TrieBackedRow update = (TrieBackedRow) updateAsRow;
        if (!this.columns.containsAll(update.columns))
            throw new IllegalArgumentException("Can't handle varying column lists.");

        try
        {
            InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> mergedData = newTrie();
            makeMutator(mergedData)
                .apply(this.data.mergeWith(update.data,
                                           TrieBackedRow::mergeData,
                                           TrieTombstoneMarker::mergeUpdate,
                                           TrieBackedRow::deleteData,
                                           true
                ));
            return create(this.columns,
                          this.columnIds,
                          this.clustering,
                          mergedData);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    private static long mergeMinLocalDeletionTime(CellData<?, ?> cell, long prev)
    {
        return cell.isTombstone() ? Long.MIN_VALUE : Math.min(prev, cell.localDeletionTime());
    }

    private static long mergeMinLocalDeletionTime(LivenessInfo info, long prev)
    {
        return info.isExpiring() ? Math.min(info.localExpirationTime(), prev) : prev;
    }

    private static long mergeMinLocalDeletionTime(DeletionTime dt, long prev)
    {
        return Long.MIN_VALUE; // we only have a marker when its non-live
    }

    class CellsWithPath extends TrieEntriesIterator.WithNullFiltering<Object, Cell<?>>
    {
        protected CellsWithPath(Trie<Object> trie, Direction direction)
        {
            super(trie, direction);
        }

        @Override
        protected Cell<?> mapContent(Object content, byte[] bytes, int byteLength)
        {
            if (!(content instanceof CellData))
                return null;
            return cellFromCellData((CellData<?, ?>) content, bytes, byteLength, columns);
        }
    }

    static InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker>
    makeMutator(InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        return data.mutator(noConflictInData(),
                            TrieBackedPartition.mergeTombstoneRanges(),
                            TrieBackedRow::deleteData,
                            TrieBackedRow::deleteData,
                            true,
                            Predicates.alwaysFalse(),
                            Predicates.alwaysFalse());
    }

    public static class Builder implements Row.Builder
    {
        protected final Columns columns;
        protected final Object2IntHashMap<ColumnIdentifier> columnIds;
        protected Clustering<?> clustering;
        private InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data;
        private InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker> mutator;

        protected Builder(Columns columns)
        {
            this.columns = columns;
            columnIds = columnsMapCache.computeIfAbsent(columns, TrieBackedRow::makeColumnIdsMap);
        }

        protected Builder(Builder builder)
        {
            this.columns = builder.columns;
            this.clustering = builder.clustering;
            this.columnIds = builder.columnIds;
            data = newTrie();
            mutator = makeMutator(data);
            try
            {
                mutator.apply(builder.data);
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e); // cannot happen
            }
        }

        @Override
        public Builder copy()
        {
            return new Builder(this);
        }

        @Override
        public boolean isSorted()
        {
            return true;
        }

        @Override
        public void newRow(Clustering<?> clustering)
        {
            assert this.clustering == null; // Ensures we've properly called build() if we've use this builder before
            this.clustering = clustering;
            data = newTrie();
            mutator = makeMutator(data);
        }

        @Override
        public Clustering<?> clustering()
        {
            return clustering;
        }

        protected void reset()
        {
            this.clustering = null;
            data = null;
            mutator = null;
        }

        @Override
        public void addPrimaryKeyLivenessInfo(LivenessInfo info)
        {
            if (info.isEmpty())
                return;

            DeletionTime rowDeletion = TrieTombstoneMarker.applicableDeletionAtRoot(data);
            if (rowDeletion != null && rowDeletion.deletes(info))
                return;

            try
            {
                data.putRecursive(ByteComparable.EMPTY, (info), (x, y) -> y);
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
        }

        @Override
        public void addRowDeletion(Deletion deletion)
        {
            if (deletion.isLive())
                return;

            try
            {
                mutator.delete(rowDeletionTrie(deletion.time()));
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
        }

        private static Object addIfNotPresent(Object existing, Object update)
        {
            return existing != null ? existing : update;
        }

        @Override
        public void addCell(Cell<?> cell)
        {
            assert cell.column().isStatic() == (clustering == Clustering.STATIC_CLUSTERING) : "Column is " + cell.column() + ", clustering = " + clustering;
            CellPath path = cell.path();
            ByteComparable key = cellKey(columnIds, cell.column, path);
            Preconditions.checkArgument(key != MISSING_COLUMN_KEY, "Column %s not present in row columns set.", cell.column);

            try
            {
                if (path == null || path.dataSize() <= MAX_RECURSIVE_LENGTH)
                {
                    DeletionTime cellDeletion =  TrieTombstoneMarker.applicableDeletion(data, true, key);
                    if (cellDeletion != null && cellDeletion.deletes(cell))
                        return;

                    data.putRecursive(key, cell, (x, y) -> Cells.reconcile((Cell<?>) x, y));
                }
                else // long path, avoid stack overflow by using the apply path; this will also take into account any existing deletion
                    mutator.apply(DeletionAwareTrie.singleton(key, BYTE_COMPARABLE_VERSION, cell));

                if (path != null)
                    data.putRecursive(columnKey(columnIds, cell.column), COMPLEX_COLUMN_MARKER, Builder::addIfNotPresent);

                data.putRecursive(ByteComparable.EMPTY, LivenessInfo.EMPTY, Builder::addIfNotPresent);
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
        }

        @Override
        public void addComplexDeletion(ColumnMetadata column, DeletionTime deletion)
        {
            if (deletion.isLive())
                return;

            ByteComparable key = columnKey(columnIds, column);
            try
            {
                mutator.delete(deletionTrie(key, deletion, TrieTombstoneMarker.Kind.COLUMN));
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
        }

        @Override
        public TrieBackedRow build()
        {
            TrieBackedRow row = create(columns,
                                       columnIds,
                                       clustering,
                                       data);
            reset();
            return row;
        }
    }
}

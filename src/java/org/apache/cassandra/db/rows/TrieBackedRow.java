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
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;

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
import org.apache.cassandra.db.tries.TrieEntriesWalker;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
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
/// Columns are identified in the trie by their unique id ([ColumnMetadata#uniqueId]), which is stable across schema
/// changes. Because unique ids are not ordered like the columns, the row's column set is used to list the columns in
/// the correct order, looking each of them up in the trie.
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

    private static final long EMPTY_SIZE = ObjectSizes.measure(new TrieBackedRow(Columns.NONE, Clustering.EMPTY, LivenessInfo.EMPTY, Row.Deletion.LIVE, 0, EMPTY_ROW));

    // A column with this ID cannot exist. Used to make sure we don't find anything when we are passed a column
    // definition that we cannot identify.
    public static final ByteComparable MISSING_COLUMN_KEY = encodeUnsignedInt(Long.MAX_VALUE);

    public static final int MAX_RECURSIVE_LENGTH = 128;

    /// The row's clustering key.
    private final Clustering<?> clustering;

    /// The columns that this row can contain, used to list its columns in order.
    private final Columns columns;

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

    /// Create a row over the given data trie. The row can list all columns of the table, including dropped ones, so
    /// that the data of dropped columns is presented in the same way as by other row implementations (and can be
    /// filtered by [#filter]).
    public static TrieBackedRow create(TableMetadata tableMetadata, Clustering<?> clustering, DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        return create(tableColumns(tableMetadata, clustering), clustering, data);
    }

    private static Columns tableColumns(TableMetadata tableMetadata, Clustering<?> clustering)
    {
        return tableMetadata.regularAndStaticAndDroppedColumns().columns(clustering == Clustering.STATIC_CLUSTERING);
    }

    static TrieBackedRow create(Columns columns,
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
                                 clustering,
                                 getLivenessInfo(data),
                                 deletion,
                                 minLocalDeletionTime,
                                 data);
    }

    private TrieBackedRow(Columns columns,
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
        this.clustering = clustering;
        this.data = data;
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
        return new TrieBackedRow(Columns.NONE, clustering, LivenessInfo.EMPTY, Deletion.regular(deletion), Long.MIN_VALUE,
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

    /// Returns the column key, i.e. the path in the trie leading to the column's data, as a byte source. The column
    /// must be identifiable using the given column set, see [#columnId].
    public static ByteSource columnKey(Columns columns, ColumnMetadata column)
    {
        int id = columnId(columns, column);
        Preconditions.checkArgument(id >= 0, "Column %s not present in columns set %s", column, columns);
        return ByteSource.variableLengthUnsignedInteger(id);
    }

    public static ByteSource cellPathKey(ColumnMetadata column, CellPath path, ByteComparable.Version version)
    {
        return ByteSource.withTerminator(ByteSource.TERMINATOR,
                                         getCellPathType(column).asComparableBytes(path.get(0), version));
    }

    /// Returns the id with which the column is identified in the trie, or -1 if the column cannot be identified.
    ///
    /// Columns are identified by their unique id. Column definitions that are not taken from the table's metadata
    /// (e.g. ones made to access the rows of system tables) may not have one; for these we use the id of the matching
    /// column in the given column set, so that rows can be accessed using such definitions as with other row
    /// implementations.
    static int columnId(Columns columns, ColumnMetadata column)
    {
        if (column.uniqueId >= 0)
            return column.uniqueId;
        int index = columns.simpleIdx(column); // index in the whole set, valid for complex columns too
        if (index < 0)
            return -1;
        return Math.max(columns.getSimple(index).uniqueId, -1);
    }

    /// Return a cell key (i.e. path in the trie) for the given (simple or complex) column and cell path (which must be
    /// non-null if the column is complex).
    private static ByteComparable cellKey(Columns columns, ColumnMetadata column, CellPath path)
    {
        int id = columnId(columns, column);
        if (id < 0)
            return MISSING_COLUMN_KEY;
        if (!column.isComplex())
            return encodeUnsignedInt(id);
        else
            return cellKey(id, column, path);
    }

    private static ByteSource columnIdPrefix(int columnId)
    {
        if (columnId < 0)
            return ByteSource.EMPTY;
        else
            return ByteSource.variableLengthUnsignedInteger(columnId);
    }

    /// Return a cell key (i.e. path in the trie) for a complex cell with the given column id and cell path.
    ///
    /// Note: this method is also used by [TrieBackedComplexColumn] where the column id is in the path leading to the
    /// complex column trie. To support this, a `columnId` of -1 is used to skip the column id.
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

    /// Returns the column key, i.e. the column id path in the trie without the part corresponding to the cell path.
    static ByteComparable columnKeyComparable(Columns columns, ColumnMetadata column)
    {
        int id = columnId(columns, column);
        if (id < 0)
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
        Object o = data.get(cellKey(columns, c, path));
        if (o == null || o instanceof Cell)
            return (Cell<?>) o;
        CellData<?, ?> cellData = (CellData<?, ?>) o;
        return cellData.toCell(c, path);
    }

    @Override
    public ComplexColumnData getComplexColumnData(ColumnMetadata c)
    {
        assert c.isComplex();
        DeletionAwareTrie<Object, TrieTombstoneMarker> tail = complexColumnTrie(c);
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

    /// Returns the column for the column key at the start of the given path in the trie.
    public static ColumnMetadata columnMetadataFromPath(byte[] bytes, int byteLength, TableMetadata metadata)
    {
        return columnFromKey(ByteSource.preencoded(bytes, 0, byteLength), metadata);
    }

    /// Converts the given cell data, found at the given path in a row trie, to a [Cell].
    public static Cell<?> cellFromCellData(CellData<?, ?> value, byte[] bytes, int byteLength, TableMetadata metadata)
    {
        if (value instanceof Cell)
            return (Cell<?>) value;
        ByteSource.Peekable pathBytes = ByteSource.preencoded(bytes, 0, byteLength);
        ColumnMetadata column = columnFromKey(pathBytes, metadata);
        return value.toCell(column, column.isComplex() ? cellPath(column, pathBytes) : null);
    }

    private static ColumnMetadata columnFromKey(ByteSource.Peekable keyBytes, TableMetadata metadata)
    {
        long columnId = ByteSourceInverse.getVariableLengthUnsignedInteger(keyBytes);
        assert ((int) columnId) == columnId;
        ColumnMetadata column = metadata.getColumnById((int) columnId);
        assert column != null : "Unknown column id " + columnId + " in " + metadata;
        return column;
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
        // Count only the columns in the row's column set, to be consistent with the iterator.
        int count = 0;
        for (ColumnMetadata column : columns)
            if (hasColumnData(column))
                ++count;
        return count;
    }

    private boolean hasColumnData(ColumnMetadata column)
    {
        if (column.isSimple())
            return data.get(cellKey(columns, column, null)) instanceof CellData;
        else
            return !isColumnDataTrieEmpty(complexColumnTrie(column));
    }

    /// Returns the branch of the given complex column. Deletions that cover the column (e.g. the row deletion) are not
    /// included, so that the complex deletion of the column is the one that is specified for it.
    private DeletionAwareTrie<Object, TrieTombstoneMarker> complexColumnTrie(ColumnMetadata column)
    {
        return data.tailTrie(columnKeyComparable(columns, column), false);
    }

    /// Lists the column data in the row. Because the order of column ids in the trie does not match the order of
    /// columns, we do this by looking up each of the row's columns in order.
    @Override
    public Iterator<ColumnData> iterator()
    {
        return new AbstractIterator<>()
        {
            final Iterator<ColumnMetadata> columnIterator = columns.iterator();

            @Override
            protected ColumnData computeNext()
            {
                while (columnIterator.hasNext())
                {
                    ColumnData columnData = getColumnData(columnIterator.next());
                    if (columnData != null)
                        return columnData;
                }
                return endOfData();
            }
        };
    }

    @Override
    public Iterable<Cell<?>> cells()
    {
        return () -> Iterators.concat(Iterators.transform(iterator(),
                                                          cd -> cd.column().isSimple()
                                                                ? Iterators.singletonIterator((Cell<?>) cd)
                                                                : ((ComplexColumnData) cd).iterator()));
    }

    /// Lists the cell data directly from the trie, in the order of the column ids, without the lookups that are needed
    /// to present the cells in column order.
    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Iterable<? extends CellData<?, ?>> cellsData()
    {
        return (Iterable<CellData<?, ?>>) (Iterable) data.contentOnlyTrie().filteredValues(CellData.class);
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
            return new TrieBackedRow(columns,
                                     clustering,
                                     activeDeletion.deletes(livenessInfo) ? LivenessInfo.EMPTY : livenessInfo,
                                     Deletion.regular(activeDeletion),
                                     Long.MIN_VALUE,
                                     applyAndSetActiveDeletion(data, activeDeletion));
        }

        try
        {
            // As in BTreeRow.filter, the cells of fetched but not queried columns that are older than the row's
            // liveness are skipped, using the liveness left after applying an active deletion that shadows the row's.
            boolean livenessShadowed = !activeDeletion.isLive() && !deletion.time().supersedes(activeDeletion) && activeDeletion.deletes(livenessInfo.timestamp());
            long rowLivenessTimestamp = livenessShadowed ? LivenessInfo.NO_TIMESTAMP : livenessInfo.timestamp();
            TrieColumnFilter tcf = new TrieColumnFilter(columns, filter, isStatic(), mayFilterColumns, rowLivenessTimestamp);

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

            return TrieBackedRow.create(columns, clustering, filteredData);
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
            TrieColumnFilter tcf = new TrieColumnFilter(columns, queried);
            return create(columns, clustering, tcf.apply(data));
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
        return Iterables.any(cellsData(), cell -> cell.isLive(nowInSec));
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
        return new TrieBackedRow(columns, clustering, getLivenessInfo(newData), getDeletion(newData), Long.MIN_VALUE, newData);
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
        return create(columns, clustering, data.mapValues(
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
        return new TrieBackedRow(columns, cloner.clone(clustering), livenessInfo, deletion, minLocalDeletionTime, newTrie);
    }

    /// Copies the given row branch of a trie (e.g. a memtable row) directly to a [BTreeRow], cloning every cell with
    /// the given cloner.
    ///
    /// The result has the same content as `BTreeRow.asBTreeRow(TrieBackedRow.create(metadata, clustering, data).clone(cloner))`,
    /// but is made with a single walk over the cells and complex column deletions of the trie, without building an
    /// intermediate trie. It is meant for the memtable read path, where the returned row must not reference memtable
    /// memory and is usually converted to a [BTreeRow] anyway. Unlike `clone`, the clustering is used as given: it
    /// must not reference memory that may be recycled.
    ///
    /// The walk lists the columns in the order of their unique ids, which is not the order in which rows present
    /// them, thus the row is put together by an unsorted builder.
    public static BTreeRow toBTreeRow(TableMetadata metadata,
                                      Clustering<?> clustering,
                                      DeletionAwareTrie<Object, TrieTombstoneMarker> data,
                                      Cloner cloner)
    {
        BTreeRow.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering);
        builder.addPrimaryKeyLivenessInfo(getLivenessInfo(data));
        // Same row deletion as computed by create(), as a plain DeletionTime rather than the trie's marker state.
        TrieTombstoneMarker deletionRoot = data.deletionAtRoot();
        TrieTombstoneMarker.Covering rowDeletionTime = deletionRoot != null ? deletionRoot.applicableToPointForward() : null;
        if (rowDeletionTime != null)
            builder.addRowDeletion(Deletion.regular(DeletionTime.build(rowDeletionTime.markedForDeleteAt(), rowDeletionTime.localDeletionTime())));

        return data.mergedTrie(TrieBackedRow::cellOrComplexDeletion)
                   .process(Direction.FORWARD, new BTreeRowCopier(metadata, builder, cloner));
    }

    /// Combine data in the live and deletion branches to find the cells and the complex column deletions of a row.
    private static Object cellOrComplexDeletion(Object content, TrieTombstoneMarker marker)
    {
        if (content instanceof CellData)
            return content;
        if (marker == null)
            return null;
        // A complex column deletion is introduced by a marker at the column's root. Other markers (row deletion at
        // the row root, ends of deletion ranges) are not needed: the row deletion is given separately.
        TrieTombstoneMarker.Covering introducedDeletion = marker.succedingState(Direction.FORWARD);
        if (introducedDeletion != null && introducedDeletion.deletionKind() == TrieTombstoneMarker.Kind.COLUMN)
            return introducedDeletion;
        return null;
    }

    /// Walker over the result of [#cellOrComplexDeletion], adding every cell (cloned) and complex column deletion to a
    /// [BTreeRow.Builder].
    private static class BTreeRowCopier extends TrieEntriesWalker<Object, BTreeRow>
    {
        private final TableMetadata metadata;
        private final BTreeRow.Builder builder;
        private final Cloner cloner;

        BTreeRowCopier(TableMetadata metadata, BTreeRow.Builder builder, Cloner cloner)
        {
            this.metadata = metadata;
            this.builder = builder;
            this.cloner = cloner;
        }

        @Override
        protected void content(Object content, byte[] bytes, int byteLength)
        {
            if (content instanceof CellData)
                builder.addCell(cellFromCellData((CellData<?, ?>) content, bytes, byteLength, metadata).clone(cloner));
            else
                builder.addComplexDeletion(columnMetadataFromPath(bytes, byteLength, metadata), (DeletionTime) content);
        }

        @Override
        public BTreeRow complete()
        {
            return builder.build();
        }
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
        Builder builder = new Builder(tableColumns(metadata, clustering));
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
        // Columns are identified by their unique id, thus rows with different column sets can be merged.
        Columns mergedColumns = this.columns.mergeTo(update.columns);

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
            return create(mergedColumns,
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
        protected Clustering<?> clustering;
        private InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data;
        private InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker> mutator;

        protected Builder(Columns columns)
        {
            this.columns = columns;
        }

        protected Builder(Builder builder)
        {
            this.columns = builder.columns;
            this.clustering = builder.clustering;
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
            ByteComparable key = cellKey(columns, cell.column, path);
            Preconditions.checkArgument(key != MISSING_COLUMN_KEY, "Column %s not present in row columns set %s", cell.column, columns);
            assert columns.contains(cell.column) : "Column " + cell.column + " not present in row columns set " + columns;

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
                    data.putRecursive(columnKeyComparable(columns, cell.column), COMPLEX_COLUMN_MARKER, Builder::addIfNotPresent);

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

            ByteComparable key = columnKeyComparable(columns, column);
            Preconditions.checkArgument(key != MISSING_COLUMN_KEY, "Column %s not present in row columns set %s", column, columns);
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
                                       clustering,
                                       data);
            reset();
            return row;
        }
    }
}

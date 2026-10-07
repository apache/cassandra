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
import java.util.Map;
import java.util.SortedSet;
import java.util.function.Predicate;

import com.google.common.base.Predicates;
import com.google.common.collect.SortedSetMultimap;

import org.agrona.collections.Object2IntHashMap;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.ColumnSubselection;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryRangeTrie;
import org.apache.cassandra.db.tries.RangeState;
import org.apache.cassandra.db.tries.RangeTrie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// Translation of [ColumnFilter] to a format that can be applied to a trie representing a row.
///
/// Builds an in-memory range trie that covers the queried and fetched columns or subselections, presents the dropped
/// columns with their drop time as a deletion and makes it possible to also apply additional active deletion to the row.
///
/// A key complication for this to work is that we do not want to modify the location of row and complex column
/// deletions. If we use normal intersection to restrict the columns, all deletions are restricted to the span of the
/// intersecting set, moving them to the individual columns/cells instead of the level on which they are defined.
///
/// To avoid this, we use a generalized range intersection where we mark the positions where a deletion may occur with
/// a [#MARKER] marker to be able to retain them in the result. Note that this can only work because we know exactly
/// where these deletions can be and by using the marker on both the opening and closing position, we are certain that
/// both boundaries will be presented in the result (missing one or the other would create an invalid range trie).
public class TrieColumnFilter
{
    final InMemoryRangeTrie<FilterState> trie = InMemoryRangeTrie.shortLived(TrieBackedPartition.BYTE_COMPARABLE_VERSION);
    final InMemoryRangeTrie<FilterState>.Mutator<FilterState> mutator = trie.mutator(TrieColumnFilter::mergeFilterState, Predicates.alwaysFalse());
    final Columns columns;
    final Object2IntHashMap<ColumnIdentifier> columnIds;

    /// Create a filter trie that applies the given [ColumnFilter]:
    /// - the trie is restricted to only the fetched columns
    /// - columns that are fetched but not queried have their cell values dropped
    /// - complex columns are restricted to their subselections, if any are specified, preserving the complex deletion
    /// - the row deletion is preserved
    ///
    /// @param isStatic true if the static part of the filter should be used (i.e. we are processing a static row),
    ///                 false if the regular part
    /// @param mayFilterColumns must be set to `!filter.fetchesAllColumns(isStatic()) || !filter.allFetchedColumnsAreQueried()`
    public TrieColumnFilter(Columns columns, Object2IntHashMap<ColumnIdentifier> columnIds, ColumnFilter filter, boolean isStatic, boolean mayFilterColumns)
    throws TrieSpaceExhaustedException
    {
        this.columns = columns;
        this.columnIds = columnIds;

        if (mayFilterColumns)
        {
            applyColumnFilter(filter, isStatic);

            // Add row-level before and after markers to preserve row-level liveness and deletion.
            trie.putRecursive(ByteComparable.EMPTY, MARKER, false, TrieColumnFilter::addNew);
            trie.putRecursive(ByteComparable.EMPTY, MARKER, true, TrieColumnFilter::addNew);
        }
        else
        {
            // Include the whole source trie.
            trie.putRecursive(ByteComparable.EMPTY, INCLUDED_START, false, TrieColumnFilter::addNew);
            trie.putRecursive(ByteComparable.EMPTY, INCLUDED_END, true, TrieColumnFilter::addNew);
        }
    }

    /// Create a filter trie that restricts a trie to only the specified set of queried columns.
    public TrieColumnFilter(Columns columns, Object2IntHashMap<ColumnIdentifier> columnIds, Columns queriedColumns)
    throws TrieSpaceExhaustedException
    {
        this.columns = columns;
        this.columnIds = columnIds;

        for (ColumnMetadata column : queriedColumns)
        {
            int id = columnIds.get(column.name);
            if (id == TrieBackedRow.COLUMN_NOT_PRESENT)
                continue;
            ByteComparable columnKey = TrieBackedRow.encodeUnsignedInt(id);

            trie.putRecursive(columnKey, INCLUDED_START, false, TrieColumnFilter::addNew);
            trie.putRecursive(columnKey, INCLUDED_END, true, TrieColumnFilter::addNew);
        }

        // Add row-level before and after markers to preserve row-level deletion and row markers.
        trie.putRecursive(ByteComparable.EMPTY, MARKER, false, TrieColumnFilter::addNew);
        trie.putRecursive(ByteComparable.EMPTY, MARKER, true, TrieColumnFilter::addNew);
    }

    private void applyColumnFilter(ColumnFilter filter, boolean isStatic) throws TrieSpaceExhaustedException
    {
        // Note: this method uses `putRecursive`, assuming the trie starts empty and that no range presented covers
        // another. It must be called before any deletion (dropped column or active deletion).
        Columns columns = filter.fetchedColumns().columns(isStatic);
        Predicate<ColumnMetadata> queriedByUserTester = filter.queriedColumns().columns(isStatic).inOrderInclusionTester();
        SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subselections = filter.subSelections();

        for (ColumnMetadata column : columns)
        {
            int id = TrieBackedRow.columnId(columnIds, column);
            if (id == TrieBackedRow.COLUMN_NOT_PRESENT)
                continue; // unknown columns can't be present in the row
            ByteComparable columnKey = TrieBackedRow.encodeUnsignedInt(id);

            if (queriedByUserTester.test(column))
            {
                // column is queried
                SortedSet<ColumnSubselection> subselection = subselections != null ? subselections.get(column.name) : null;
                if (subselection != null && !subselection.isEmpty())
                {
                    // include only the subselection
                    for (ColumnSubselection ss : subselection)
                    {
                        trie.putRecursive(TrieBackedRow.cellKey(id, column, ss.startInclusive()), INCLUDED_START, false, TrieColumnFilter::addNew);
                        trie.putRecursive(TrieBackedRow.cellKey(id, column, ss.endInclusive()), INCLUDED_END, true, TrieColumnFilter::addNew);
                    }

                    // preserve column marker and deletion
                    trie.putRecursive(columnKey, MARKER, false, TrieColumnFilter::addNew);
                    trie.putRecursive(columnKey, MARKER, true, TrieColumnFilter::addNew);
                }
                else
                {
                    // include the column with all cells and complex deletion
                    trie.putRecursive(columnKey, INCLUDED_START, false, TrieColumnFilter::addNew);
                    trie.putRecursive(columnKey, INCLUDED_END, true, TrieColumnFilter::addNew);
                }
            }
            else
            {
                // cover the column with all cells and complex deletion, but drop the cell values of any covered cells
                trie.putRecursive(columnKey, DROP_VALUE_START, false, TrieColumnFilter::addNew);
                trie.putRecursive(columnKey, DROP_VALUE_END, true, TrieColumnFilter::addNew);
            }
        }
    }

    /// Apply dropping columns by applying a deletion over them with the drop time. Anything older will be removed, and
    /// anything with a newer timestamp (i.e. if the column was then re-added and data inserted) will be preserved.
    ///
    /// @param columns The set of columns that can be in the filtered trie. If we didn't do any filtering on
    /// construction (i.e. all cells are queried), this must include the full column set, otherwise it can be
    /// the filter's set of fetched columns.
    public void applyDroppedColumns(Map<ByteBuffer, DroppedColumn> droppedColumns, Columns columns)
    throws TrieSpaceExhaustedException
    {
        for (ColumnMetadata c : columns)
        {
            DroppedColumn dropped = droppedColumns.get(c.name.bytes);
            if (dropped != null)
            {
                ByteComparable columnKey = TrieBackedRow.columnKey(columnIds, c);
                Covering deletion = deletion(dropped.droppedTime, 0);
                // applying the deletion will merge INCLUDED markers with the deletion time
                mutator.apply(RangeTrie.branch(columnKey, TrieBackedPartition.BYTE_COMPARABLE_VERSION, deletion));
            }
        }
    }

    /// Apply an additional deletion to the row, removing all shadowed content.
    public void applyDeletion(DeletionTime deletionTime) throws TrieSpaceExhaustedException
    {
        Covering deletion = deletion(deletionTime.markedForDeleteAt(), deletionTime.localDeletionTime());
        mutator.apply(RangeTrie.branch(ByteComparable.EMPTY, TrieBackedPartition.BYTE_COMPARABLE_VERSION, deletion));
    }

    /// Apply the constructed filter to the given row trie.
    DeletionAwareTrie<Object, TrieTombstoneMarker> apply(DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        return data.intersectWith(trie,
                                  TrieColumnFilter::included,
                                  TrieColumnFilter::included,
                                  TrieColumnFilter::resolve,
                                  TrieColumnFilter::resolve);
    }

    private static final Covering INCLUDED = new Covering(true, false, null);
    private static final Boundary INCLUDED_START = new Boundary(null, INCLUDED, INCLUDED);
    private static final Boundary INCLUDED_END = new Boundary(INCLUDED, null, INCLUDED);
    private static final Covering DROP_VALUE = new Covering(true, true, null);
    private static final Boundary DROP_VALUE_START = new Boundary(null, DROP_VALUE, DROP_VALUE);
    private static final Boundary DROP_VALUE_END = new Boundary(DROP_VALUE, null, DROP_VALUE);

    static final Marker MARKER = new Marker();

    static Covering deletion(long timestamp, long localDeletionTime)
    {
        return new Covering(false, false, TrieTombstoneMarker.covering(timestamp, localDeletionTime, TrieTombstoneMarker.Kind.COLUMN));
    }

    static FilterState mergeFilterState(FilterState existing, FilterState update)
    {
        return update.mergeWith(existing);
    }

    static FilterState addNew(FilterState existing, FilterState update)
    {
        assert existing == null;
        return update;
    }

    /// Predicate to skip over parts of the source trie this filter has not selected.
    static boolean included(FilterState state)
    {
        return state != null && state.included();
    }

    /// Predicate to skip over parts of the source deletion branch that have no content.
    static boolean included(TrieTombstoneMarker marker)
    {
        return marker != null;
    }

    /// Apply the selection to deletion markers.
    static TrieTombstoneMarker resolve(TrieTombstoneMarker marker, FilterState state)
    {
        if (marker == null || state == null)
            return null;

        // If a deletion is in force, drop anything shadowed; otherwise, leave markers we happen upon unchanged
        TrieTombstoneMarker.Covering deletion = state.applicableDeletion();
        if (deletion != null)
            return marker.dropShadowed(deletion);
        else
            return marker;
    }

    /// Apply the selection to data.
    static Object resolve(Object data, FilterState state)
    {
        // We are processing either the prefix of a column, where we meet markers we want to preserve, or are in a
        // covered region of the trie. In any case, return the data (applying any deletion in force and dropping cell
        // value if requested).
        if (data == null || data == TrieBackedRow.COMPLEX_COLUMN_MARKER || data == LivenessInfo.EMPTY)
            return data;

        TrieTombstoneMarker.Covering deletion = state.applicableDeletion();
        if (data instanceof CellData)
        {
            assert state != null && state.included();
            CellData<?, ?> cell = (CellData<?, ?>) data;
            if (deletion != null && deletion.deletes(cell))
                return null;
            else if (state.dropsValue())
                return cell.withSkippedValue();
            else
                return cell;
        }
        else if (deletion == null)
            return data;
        else if (data instanceof LivenessInfo)
            return deletion.deletes((LivenessInfo) data) ? LivenessInfo.EMPTY : data;
        else
            throw new AssertionError("Unknown data in trie: " + data);
    }

    interface FilterState extends RangeState<FilterState>
    {
        boolean included();

        boolean dropsValue();

        TrieTombstoneMarker.Covering applicableDeletion();

        FilterState mergeWith(FilterState existing);
    }

    static class Covering implements FilterState
    {
        private final boolean included;
        private final boolean dropsValue;
        private final TrieTombstoneMarker.Covering applicableDeletion;

        Covering(boolean included, boolean dropsValue, TrieTombstoneMarker.Covering applicableDeletion)
        {
            this.included = included;
            this.dropsValue = dropsValue;
            this.applicableDeletion = applicableDeletion;
        }

        @Override
        public boolean included()
        {
            return included;
        }

        @Override
        public boolean dropsValue()
        {
            return dropsValue;
        }

        @Override
        public TrieTombstoneMarker.Covering applicableDeletion()
        {
            return applicableDeletion;
        }

        @Override
        public FilterState mergeWith(FilterState existing)
        {
            if (existing == null)
                return this;
            if (existing instanceof Marker)
                return new Boundary((Marker) existing, this, this, this);
            if (existing instanceof Boundary)
                return existing.mergeWith(this);
            return mergeWithCovering((Covering) existing);
        }

        private Covering mergeWithCovering(Covering other)
        {
            if (other == null)
                return this;
            TrieTombstoneMarker.Covering newDeletion = TrieTombstoneMarker.combine(applicableDeletion, other.applicableDeletion);
            return new Covering(included | other.included, dropsValue | other.dropsValue, newDeletion);
        }

        @Override
        public boolean isBoundary()
        {
            return false;
        }

        @Override
        public Covering precedingState(Direction direction)
        {
            return this;
        }

        @Override
        public Covering succedingState(Direction direction)
        {
            return this;
        }

        @Override
        public FilterState restrict(boolean applicableBefore, boolean applicableAfter)
        {
            throw new AssertionError();
        }

        @Override
        public FilterState asBoundary(Direction direction)
        {
            return direction.isForward() ? new Boundary(null, this, this) : new Boundary(this, null, this);
        }

        @Override
        public String toString()
        {
            if (applicableDeletion != null)
                return (dropsValue ? "D " : (included ? "I " : "")) + applicableDeletion;
            if (dropsValue)
                return "DROPPED_VALUE";
            if (included)
                return "INCLUDED";
            return "UNEFFECTIVE";
        }
    }

    static class Marker implements FilterState
    {
        @Override
        public boolean included()
        {
            return false;
        }

        @Override
        public boolean dropsValue()
        {
            return false;
        }

        @Override
        public TrieTombstoneMarker.Covering applicableDeletion()
        {
            return null;
        }

        @Override
        public FilterState mergeWith(FilterState existing)
        {
            if (existing == null)
                return this;
            return existing.mergeWith(this);
        }

        @Override
        public boolean isBoundary()
        {
            return true;
        }

        @Override
        public FilterState precedingState(Direction direction)
        {
            return null;
        }

        @Override
        public FilterState succedingState(Direction direction)
        {
            return null;
        }

        @Override
        public FilterState restrict(boolean applicableBefore, boolean applicableAfter)
        {
            return this;
        }

        @Override
        public FilterState asBoundary(Direction direction)
        {
            throw new AssertionError();
        }

        @Override
        public String toString()
        {
            return "Marker";
        }
    }

    static class Boundary implements FilterState
    {
        final Marker marker;
        final Covering left;
        final Covering right;
        final Covering appliesToPoint;

        Boundary(Covering left, Covering right, Covering appliesToPoint)
        {
            this(null, left, right, appliesToPoint);
        }

        Boundary(Marker marker, Covering left, Covering right, Covering appliesToPoint)
        {
            this.marker = marker;
            this.left = left;
            this.right = right;
            this.appliesToPoint = appliesToPoint;
            assert appliesToPoint != null;
            assert appliesToPoint == left || appliesToPoint == right;
        }

        @Override
        public boolean included()
        {
            return appliesToPoint.included();
        }

        @Override
        public boolean dropsValue()
        {
            return appliesToPoint.dropsValue();
        }

        @Override
        public TrieTombstoneMarker.Covering applicableDeletion()
        {
            return appliesToPoint.applicableDeletion();
        }

        @Override
        public FilterState mergeWith(FilterState existing)
        {
            if (existing == null)
                return this;
            else if (existing instanceof Marker)
            {
                Marker other = (Marker) existing;
                if (this.marker == other)
                    return this;
                else
                    return new Boundary(other, left, right, appliesToPoint);
            }
            else if (existing instanceof Covering)
            {
                Covering other = (Covering) existing;
                Covering l = other.mergeWithCovering(left);
                Covering r = other.mergeWithCovering(right);
                if (l == left && r == right)
                    return this;
                if (l == r)
                    return marker;
                return new Boundary(marker, l, r, left == appliesToPoint ? l : r);
            }
            else
            {
                Boundary other = (Boundary) existing;
                Covering l = left != null ? left.mergeWithCovering(other.left) : other.left;
                Covering r = right != null ? right.mergeWithCovering(other.right) : other.right;
                Marker h = marker != null ? marker : other.marker;
                if (l == r)
                    return h;
                if (l == left && r == right && h == marker)
                    return this;
                return new Boundary(marker, l, r, left == appliesToPoint ? l : r);
            }
        }

        @Override
        public boolean isBoundary()
        {
            return true;
        }

        @Override
        public FilterState precedingState(Direction direction)
        {
            return direction.select(left, right);
        }

        @Override
        public FilterState succedingState(Direction direction)
        {
            return direction.select(right, left);
        }

        @Override
        public FilterState restrict(boolean applicableBefore, boolean applicableAfter)
        {
            Covering l = applicableBefore ? left : null;
            Covering r = applicableAfter ? right : null;
            if (l == null && r == null)
                return marker;
            if (l == left && r == right)
                return this;
            return new Boundary(marker, l, r, left == appliesToPoint ? l : r);
        }

        @Override
        public FilterState asBoundary(Direction direction)
        {
            throw new AssertionError();
        }

        @Override
        public String toString()
        {
            return "Boundary{" +
                   (marker != null ? "Marker + " : "") +
                   left + "->" + right + '}';
        }
    }
}

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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.SortedSet;

import com.google.common.base.Function;

import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.IIterableColumns;
import org.apache.cassandra.utils.Allocator;

/**
 * A sorted map of columns.
 * This represents the backing map of a colum family.
 *
 * Whether the implementation is thread safe or not is left to the
 * implementing classes.
 */
public interface ISortedColumns extends IIterableColumns
{
    /**
     * Shallow cloning of the column map.
     */
    public ISortedColumns cloneMe();

    /**
     * Returns the factory used for this ISortedColumns implementation.
     */
    public Factory getFactory();

    public DeletionInfo getDeletionInfo();
    public void setDeletionInfo(DeletionInfo info);

    public void delete(DeletionInfo info);
    public void maybeResetDeletionTimes(int gcBefore);
    public void retainAll(ISortedColumns columns);

    /**
     * Adds a column to this column map.
     * If a column with the same name is already present in the map, it will
     * be replaced by the newly added column.
     */
    public void addColumn(IColumn column, Allocator allocator);

    /**
     * Adds all the columns of a given column map to this column map.
     * This is equivalent to:
     *   <code>
     *   for (Column c : cm)
     *      add(c);
     *   </code>
     *  but is potentially faster.
     *
     *  @return the difference in size seen after merging the given columns
     */
    public long addAllWithSizeDelta(ISortedColumns cm, Allocator allocator, Function<IColumn, IColumn> transformation, SecondaryIndexManager.Updater indexer);

    /**
     * Adds the columns without necessarily computing the size delta
     */
    public void addAll(ISortedColumns cm, Allocator allocator, Function<IColumn, IColumn> transformation);

    /**
     * Replace oldColumn if present by newColumn.
     * Returns true if oldColumn was present and thus replaced.
     * oldColumn and newColumn should have the same name.
     */
    public boolean replace(IColumn oldColumn, IColumn newColumn);

    /**
     * Remove if present a column by name.
     */
    public void removeColumn(ByteBuffer name);

    /**
     * Clear this column map, removing all columns.
     */
    public void clear();

    /**
     * Get a column given its name, returning null if the column is not
     * present.
     */
    public IColumn getColumn(ByteBuffer name);

    /**
     * Returns a set with the names of columns in this column map.
     * The resulting set is sorted and the order is the one of the columns in
     * this column map.
     */
    public SortedSet<ByteBuffer> getColumnNames();

    /**
     * Returns the columns of this column map as a collection.
     * The columns in the returned collection should be sorted as the columns
     * in this map.
     */
    public Collection<IColumn> getSortedColumns();

    /**
     * Returns the columns of this column map as a collection.
     * The columns in the returned collection should be sorted in reverse
     * order of the columns in this map.
     */
    public Collection<IColumn> getReverseSortedColumns();

    /**
     * Returns the number of columns in this map.
     */
    public int size();

    /**
     * Returns true if this map is empty, false otherwise.
     */
    public boolean isEmpty();

    /**
     * Returns an iterator over the columns of this map that returns only the matching @param slices.
     * The provided slices must be in order and must be non-overlapping.
     */
    public Iterator<IColumn> iterator(ColumnSlice[] slices);

    /**
     * Returns a reversed iterator over the columns of this map that returns only the matching @param slices.
     * The provided slices must be in reversed order and must be non-overlapping.
     */
    public Iterator<IColumn> reverseIterator(ColumnSlice[] slices);

    /**
     * Returns if this map only support inserts in reverse order.
     */
    public boolean isInsertReversed();

    public interface Factory
    {
        /**
         * Returns a (initially empty) column map whose columns are sorted
         * according to the provided comparator.
         * The {@code insertReversed} flag is an hint on how we expect insertion to be perfomed,
         * either in sorted or reverse sorted order. This is used by ArrayBackedSortedColumns to
         * allow optimizing for both forward and reversed slices. This does not matter for ThreadSafeSortedColumns.
         * Note that this is only an hint on how we expect to do insertion, this does not change the map sorting.
         */
        public ISortedColumns create(AbstractType<?> comparator, boolean insertReversed);

        /**
         * Returns a column map whose columns are sorted according to the comparator of the provided sorted
         * map (which thus, is assumed to _not_ be sorted by natural order) and that initially contains the
         * columns in the provided sorted map.
         * See {@code create} for the description of {@code insertReversed}
         */
        public ISortedColumns fromSorted(SortedMap<ByteBuffer, IColumn> sm, boolean insertReversed);
    }
}

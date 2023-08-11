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
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.*;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Represents which (non-PK) columns (and optionally which sub-part of a column for complex columns) are selected
 * by a query.
 *
 * We distinguish 2 sets of columns in practice: the _fetched_ columns, which are the columns that we (may, see
 * below) need to fetch internally, and the _queried_ columns, which are the columns that the user has selected
 * in its request.
 *
 * The reason for distinguishing those 2 sets is that due to the CQL semantic (see #6588 for more details), we
 * often need to internally fetch all regular columns or all columns for the queried table, but can still do some
 * optimizations for those columns that are not directly queried by the user (see #10657 for more details).
 *
 * Note that in practice:
 *   - the _queried_ columns set is always included in the _fetched_ one.
 *   - whenever those sets are different, the _fetched_ columns can contain either all the regular columns and
 *     the static columns queried by the user or all the regular and static columns. If the query is a partition level
 *     query (no restrictions on clustering or regular columns) all the static columns will need to be fetched as
 *     some data will need to be returned to the user if the partition has no row but some static data. For all the
 *     other scenarios only the regular columns are required.
 *   - in the special case of a {@code SELECT *} query, we want to query all columns, and _fetched_ == _queried.
 *     As this is a common case, we special case it by using a specific subclass for it.
 *
 * For complex columns, this class optionally allows to specify a subset of the cells to query for each column.
 * We can either select individual cells by path name, or a slice of them. Note that this is a sub-selection of
 * _queried_ cells, so if _fetched_ != _queried_, then the cell selected by this sub-selection are considered
 * queried and the other ones are considered fetched (and if a column has some sub-selection, it must be a queried
 * column, which is actually enforced by the Builder below).
 */
public abstract class ColumnFilter
{

    public static final ColumnFilter NONE = selection(RegularAndStaticColumns.NONE);

    public static final Serializer serializer = new Serializer();

    /**
     * The fetching strategy for the different queries.
     */
    private enum FetchingStrategy
    {
        /**
         * This strategy will fetch all the regular and static columns.
         *
         * <p>According to the CQL semantic a partition exists if it has at least one row or one of its static columns is not null.
         * For queries that have no restrictions on the clustering or regular columns, C* will return some data for
         * the partition even if it does not contains any row as long as one of the static columns contains data.
         * To be able to ensure those queries all columns need to be fetched.</p>
         */
        ALL_COLUMNS
        {
            @Override
            boolean fetchesAllColumns(boolean isStatic)
            {
                return true;
            }

            @Override
            RegularAndStaticColumns getFetchedColumns(TableMetadata metadata, RegularAndStaticColumns queried)
            {
                return metadata.regularAndStaticColumns();
            }
        },

        /**
         * This strategy will fetch all the regular and selected static columns.
         *
         * <p>According to the CQL semantic a row exists if at least one of its columns is not null.
         * To ensure that we need to fetch all regular columns.</p>
         */
        ALL_REGULARS_AND_QUERIED_STATICS_COLUMNS
        {
            @Override
            boolean fetchesAllColumns(boolean isStatic)
            {
                return !isStatic;
            }

            @Override
            RegularAndStaticColumns getFetchedColumns(TableMetadata metadata, RegularAndStaticColumns queried)
            {
                return new RegularAndStaticColumns(queried.statics, metadata.regularColumns());
            }
        },

        /**
         * Fetch only the columns that have been selected.
         *
         * <p>With this strategy _queried_ == _fetched_. This strategy is only used for internal queries.</p>
         */
        ONLY_QUERIED_COLUMNS
        {
            @Override
            boolean fetchesAllColumns(boolean isStatic)
            {
                return false;
            }

            @Override
            boolean areAllFetchedColumnsQueried()
            {
                return true;
            }

            @Override
            RegularAndStaticColumns getFetchedColumns(TableMetadata metadata, RegularAndStaticColumns queried)
            {
                return queried;
            }
        };

        /**
         * Checks if the strategy fetch all the specified columns
         *
         * @param isStatic {@code true} is the check is for static columns, {@code false} otherwise
         * @return {@code true} if the strategy fetch all the static columns, {@code false} otherwise.
         */
        abstract boolean fetchesAllColumns(boolean isStatic);

        /**
         * Checks if all the fetched columns are guaranteed to be queried
         *
         * @return {@code true} if all the fetched columns are guaranteed to be queried, {@code false} otherwise.
         */
        boolean areAllFetchedColumnsQueried()
        {
            return false;
        }

        /**
         * Returns the columns that must be fetched to answer the query.
         *
         * @param metadata the table metadata
         * @param queried the queried columns
         * @return the columns that must be fetched
         */
        abstract RegularAndStaticColumns getFetchedColumns(TableMetadata metadata, RegularAndStaticColumns queried);
    }

    /**
     * A filter that includes all columns for the provided table.
     */
    public static ColumnFilter all(TableMetadata metadata)
    {
        return new WildCardColumnFilter(metadata.regularAndStaticColumns());
    }

    /**
     * A filter that only fetches/queries the provided columns.
     * <p>
     * Note that this shouldn't be used for CQL queries in general as all columns should be queried to
     * preserve CQL semantic (see class javadoc). This is ok for some internal queries however (and
     * for #6588 if/when we implement it).
     */
    public static ColumnFilter selection(RegularAndStaticColumns columns)
    {
        return SelectionColumnFilter.newInstance(FetchingStrategy.ONLY_QUERIED_COLUMNS, null, columns, null);
    }

    /**
     * A filter that fetches all columns for the provided table, but returns
     * only the queried ones.
     */
    @VisibleForTesting
    public static ColumnFilter selection(TableMetadata metadata,
                                         RegularAndStaticColumns queried,
                                         boolean returnStaticContentOnPartitionWithNoRows)
    {
        if (!returnStaticContentOnPartitionWithNoRows)
            return SelectionColumnFilter.newInstance(FetchingStrategy.ALL_REGULARS_AND_QUERIED_STATICS_COLUMNS, metadata, queried, null);

        return SelectionColumnFilter.newInstance(FetchingStrategy.ALL_COLUMNS, metadata, queried, null);
    }

    /**
     * The columns that needs to be fetched internally for this filter.
     *
     * @return the columns to fetch for this filter.
     */
    public abstract RegularAndStaticColumns fetchedColumns();

    /**
     * The columns actually queried by the user.
     * <p>
     * Note that this is in general not all the columns that are fetched internally (see {@link #fetchedColumns}).
     */
    public abstract RegularAndStaticColumns queriedColumns();

    /**
     * Whether all the (regular or static) columns are fetched by this filter.
     * <p>
     * Note that this method is meant as an optimization but a negative return
     * shouldn't be relied upon strongly: this can return {@code false} but
     * still have all the columns fetches if those were manually selected by the
     * user. The goal here is to cheaply avoid filtering things on wildcard
     * queries, as those are common.
     *
     * @param isStatic whether to check for static columns or not. If {@code true},
     * the method returns if all static columns are fetched, otherwise it checks
     * regular columns.
     */
    public abstract boolean fetchesAllColumns(boolean isStatic);

    /**
     * Whether _fetched_ == _queried_ for this filter, and so if the {@code isQueried()} methods
     * can return {@code false} for some column/cell.
     */
    public abstract boolean allFetchedColumnsAreQueried();

    /**
     * Whether the provided column is fetched by this filter.
     */
    public abstract boolean fetches(ColumnMetadata column);

    /**
     * Whether the provided column, which is assumed to be _fetched_ by this filter (so the caller must guarantee
     * that {@code fetches(column) == true}, is also _queried_ by the user.
     *
     * !WARNING! please be sure to understand the difference between _fetched_ and _queried_
     * columns that this class made before using this method. If unsure, you probably want
     * to use the {@link #fetches} method.
     */
    public abstract boolean fetchedColumnIsQueried(ColumnMetadata column);

    /**
     * Whether the provided complex cell (identified by its column and path), which is assumed to be _fetched_ by
     * this filter, is also _queried_ by the user.
     *
     * !WARNING! please be sure to understand the difference between _fetched_ and _queried_
     * columns that this class made before using this method. If unsure, you probably want
     * to use the {@link #fetches} method.
     */
    public abstract boolean fetchedCellIsQueried(ColumnMetadata column, CellPath path);

    /**
     * Creates a new {@code Tester} to efficiently test the inclusion of cells of complex column
     * {@code column}.
     *
     * @param column for complex column for which to create a tester.
     * @return the created tester or {@code null} if all the cells from the provided column
     * are queried.
     */
    @Nullable
    public abstract Tester newTester(ColumnMetadata column);

    /**
     * Checks if this {@code ColumnFilter} is for a wildcard query.
     *
     * @return {@code true} if this {@code ColumnFilter} is for a wildcard query, {@code false} otherwise.
     */
    public boolean isWildcard()
    {
        return false;
    }

    /**
     * Returns the CQL string corresponding to this {@code ColumnFilter}.
     *
     * @return the CQL string corresponding to this {@code ColumnFilter}.
     */
    public abstract String toCQLString();

    /**
     * Returns the sub-selections or {@code null} if there are none.
     *
     * @return the sub-selections or {@code null} if there are none
     */
    protected abstract SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections();

    /**
     * Returns a {@code ColumnFilter} builder that fetches all regular columns or all columns (and queries the columns
     * added to the builder, or everything if no column is added).
     *
     * @param metadata the table metadata
     * @param returnStaticContentOnPartitionWithNoRows {@code true} if the query must return static contents if the partition has no row,
     * {@code false} otherwise.
     */
    public static Builder allRegularColumnsBuilder(TableMetadata metadata, boolean returnStaticContentOnPartitionWithNoRows)
    {
        return new Builder(metadata, returnStaticContentOnPartitionWithNoRows);
    }

    /**
     * Returns a {@code ColumnFilter} builder that only fetches the columns/cells added to the builder.
     */
    public static Builder selectionBuilder()
    {
        return new Builder(null, false);
    }

    public static class Tester
    {
        private final boolean isFetched;
        private ColumnSubselection current;
        private final Iterator<ColumnSubselection> iterator;

        private Tester(boolean isFetched, Iterator<ColumnSubselection> iterator)
        {
            this.isFetched = isFetched;
            this.iterator = iterator;
        }

        public boolean fetches(CellPath path)
        {
            return isFetched || hasSubselection(path);
        }

        /**
         * Must only be called if {@code fetches(path) == true}.
         */
        public boolean fetchedCellIsQueried(CellPath path)
        {
            return !isFetched || hasSubselection(path);
        }

        private boolean hasSubselection(CellPath path)
        {
            while (current != null || iterator.hasNext())
            {
                if (current == null)
                    current = iterator.next();

                int cmp = current.compareInclusionOf(path);
                if (cmp == 0) // The path is included
                    return true;
                else if (cmp < 0) // The path is before this sub-selection, it's not included by any
                    return false;

                // the path is after this sub-selection, we need to check the next one.
                current = null;
            }
            return false;
        }
    }

    /**
     * A builder for a {@code ColumnFilter} object.
     *
     * Note that the columns added to this build are the _queried_ column. Whether or not all columns
     * are _fetched_ depends on which constructor you've used to obtained this builder, allRegularColumnsBuilder (all
     * columns are fetched) or selectionBuilder (only the queried columns are fetched).
     *
     * Note that for a allRegularColumnsBuilder, if no queried columns are added, this is interpreted as querying
     * all columns, not querying none (but if you know you want to query all columns, prefer
     * {@link ColumnFilter#all(TableMetadata)}. For selectionBuilder, adding no queried columns means no column will be
     * fetched (so the builder will return {@code PartitionColumns.NONE}).
     *
     * Also, if only a sub-selection of a complex column should be queried, then only the corresponding
     * sub-selection method of the builder ({@link #slice} or {@link #select}) should be called for the
     * column, but {@link #add} shouldn't. if {@link #add} is also called, the whole column will be
     * queried and the sub-selection(s) will be ignored. This is done for correctness of CQL where
     * if you do "SELECT m, m[2..5]", you are really querying the whole collection.
     */
    public static class Builder
    {
        private final TableMetadata metadata; // null if we don't fetch all columns

        /**
         * {@code true} if the query must return static contents if the partition has no row, {@code false} otherwise.
         */
        private final boolean returnStaticContentOnPartitionWithNoRows;

        private RegularAndStaticColumns.Builder queriedBuilder;

        private List<ColumnSubselection> subSelections;

        private Set<ColumnMetadata> fullySelectedComplexColumns;

        private Builder(TableMetadata metadata, boolean returnStaticContentOnPartitionWithNoRows)
        {
            this.metadata = metadata;
            this.returnStaticContentOnPartitionWithNoRows = returnStaticContentOnPartitionWithNoRows;
        }

        public Builder add(ColumnMetadata c)
        {
            if (c.isComplex() && c.type.isMultiCell())
            {
                if (fullySelectedComplexColumns == null)
                    fullySelectedComplexColumns = new HashSet<>();
                fullySelectedComplexColumns.add(c);
            }
            return addInternal(c);
        }

        public Builder addAll(Iterable<ColumnMetadata> columns)
        {
            for (ColumnMetadata column : columns)
                add(column);
            return this;
        }

        private Builder addInternal(ColumnMetadata c)
        {
            if (c.isPrimaryKeyColumn())
                return this;

            if (queriedBuilder == null)
                queriedBuilder = RegularAndStaticColumns.builder();
            queriedBuilder.add(c);
            return this;
        }

        private Builder addSubSelection(ColumnSubselection subSelection)
        {
            ColumnMetadata column = subSelection.column();
            assert column.isComplex() && column.type.isMultiCell();
            addInternal(column);
            if (subSelections == null)
                subSelections = new ArrayList<>();
            subSelections.add(subSelection);
            return this;
        }

        public Builder slice(ColumnMetadata c, CellPath from, CellPath to)
        {
            return addSubSelection(ColumnSubselection.slice(c, from, to));
        }

        public Builder select(ColumnMetadata c, CellPath elt)
        {
            return addSubSelection(ColumnSubselection.element(c, elt));
        }

        public ColumnFilter build()
        {
            boolean isFetchAllRegulars = metadata != null;

            RegularAndStaticColumns queried = queriedBuilder == null ? null : queriedBuilder.build();

            // It's only ok to have queried == null in ColumnFilter if isFetchAllRegulars. So deal with the case of a selectionBuilder
            // with nothing selected (we can at least happen on some backward compatible queries - CASSANDRA-10471).
            if (!isFetchAllRegulars && queried == null)
                queried = RegularAndStaticColumns.NONE;

            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> s = buildSubSelections();

            if (isFetchAllRegulars)
            {
                // there is no way to convert the filter with fetchAll and queried != null so all columns are queried
                //   see CASSANDRA-10657, CASSANDRA-15833, CASSANDRA-16415
                if (queried == null)
                    return new WildCardColumnFilter(metadata.regularAndStaticColumns());

                if (!returnStaticContentOnPartitionWithNoRows)
                    return SelectionColumnFilter.newInstance(FetchingStrategy.ALL_REGULARS_AND_QUERIED_STATICS_COLUMNS, metadata, queried, s);

                return SelectionColumnFilter.newInstance(FetchingStrategy.ALL_COLUMNS, metadata, queried, s);
            }

            return SelectionColumnFilter.newInstance(FetchingStrategy.ONLY_QUERIED_COLUMNS, (TableMetadata) null, queried, s);
        }

        private SortedSetMultimap<ColumnIdentifier, ColumnSubselection> buildSubSelections()
        {
            if (subSelections == null)
                return null;

            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> s = TreeMultimap.create(Comparator.naturalOrder(), Comparator.naturalOrder());
            for (ColumnSubselection subSelection : subSelections)
            {
                if (fullySelectedComplexColumns == null || !fullySelectedComplexColumns.contains(subSelection.column()))
                    s.put(subSelection.column().name, subSelection);
            }

            return s;
        }
    }

    /**
     * {@code ColumnFilter} sub-class for wildcard queries.
     *
     * <p>The class does not rely on TableMetadata and expects a fix set of columns to prevent issues
     * with Schema race propagation. See CASSANDRA-15899.</p>
     */
    public static class WildCardColumnFilter extends ColumnFilter
    {
        /**
         * The queried and fetched columns.
         */
        private final RegularAndStaticColumns fetchedAndQueried;

        /**
         * Creates a {@code ColumnFilter} for wildcard queries.
         *
         * <p>The class does not rely on TableMetadata and expects a fix set of columns to prevent issues
         * with Schema race propagation. See CASSANDRA-15899.</p>
         *
         * @param fetchedAndQueried the fetched and queried columns
         */
        private WildCardColumnFilter(RegularAndStaticColumns fetchedAndQueried)
        {
            this.fetchedAndQueried = fetchedAndQueried;
        }

        @Override
        public RegularAndStaticColumns fetchedColumns()
        {
            return fetchedAndQueried;
        }

        @Override
        public RegularAndStaticColumns queriedColumns()
        {
            return fetchedAndQueried;
        }

        @Override
        public boolean fetchesAllColumns(boolean isStatic)
        {
            return true;
        }

        @Override
        public boolean allFetchedColumnsAreQueried()
        {
            return true;
        }

        @Override
        public boolean fetches(ColumnMetadata column)
        {
            return true;
        }

        @Override
        public boolean fetchedColumnIsQueried(ColumnMetadata column)
        {
            return true;
        }

        @Override
        public boolean fetchedCellIsQueried(ColumnMetadata column, CellPath path)
        {
            return true;
        }

        @Override
        public Tester newTester(ColumnMetadata column)
        {
            return null;
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == this)
                return true;

            if (!(other instanceof WildCardColumnFilter))
                return false;

            WildCardColumnFilter w = (WildCardColumnFilter) other;

            return fetchedAndQueried.equals(w.fetchedAndQueried);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(fetchedAndQueried);
        }

        @Override
        public String toString()
        {
            return "*/*";
        }

        public String toCQLString()
        {
            return "*";
        }

        @Override
        public boolean isWildcard()
        {
            return true;
        }

        @Override
        protected SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections()
        {
            return null;
        }
    }

    /**
     * {@code ColumnFilter} sub-class for queries with selected columns.
     *
     * <p>The class  does not rely on TableMetadata and expect a fix set of fetched columns to prevent issues
     * with Schema race propagation. See CASSANDRA-15899.</p>
     */
    public static class SelectionColumnFilter extends ColumnFilter
    {
        public final FetchingStrategy fetchingStrategy;

        /**
         * The selected columns
         */
        private final RegularAndStaticColumns queried;

        /**
         * The columns that need to be fetched to be able
         */
        private final RegularAndStaticColumns fetched;

        private final SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections; // can be null

        public static SelectionColumnFilter newInstance(FetchingStrategy fetchingStrategy,
                                                        TableMetadata metadata,
                                                        RegularAndStaticColumns queried,
                                                        SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections)
        {
            assert fetchingStrategy != FetchingStrategy.ONLY_QUERIED_COLUMNS || metadata == null;
            assert queried != null;

            return new SelectionColumnFilter(fetchingStrategy,
                                             queried,
                                             fetchingStrategy.getFetchedColumns(metadata, queried),
                                             subSelections);
        }

        /**
         * Creates a {@code ColumnFilter} for queries with selected columns.
         *
         * <p>The class  does not rely on TableMetadata and expect a fix set of columns to prevent issues
         * with Schema race propagation. See CASSANDRA-15899.</p>
         *
         * @param fetchingStrategy the strategy used to select the fetched columns
         * @param fetched the columns that must be fetched
         * @param queried the queried columns
         * @param subSelections the columns sub-selections
         */
        public SelectionColumnFilter(FetchingStrategy fetchingStrategy,
                                     RegularAndStaticColumns queried,
                                     RegularAndStaticColumns fetched,
                                     SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections)
        {
            assert queried != null;
            assert fetched.includes(queried);

            this.fetchingStrategy = fetchingStrategy;
            this.queried = queried;
            this.fetched = fetched;
            this.subSelections = subSelections;
        }

        @Override
        public RegularAndStaticColumns fetchedColumns()
        {
            return fetched;
        }

        @Override
        public RegularAndStaticColumns queriedColumns()
        {
            return queried;
        }

        @Override
        public boolean fetchesAllColumns(boolean isStatic)
        {
            return fetchingStrategy.fetchesAllColumns(isStatic);
        }

        @Override
        public boolean allFetchedColumnsAreQueried()
        {
            return fetchingStrategy.areAllFetchedColumnsQueried();
        }

        @Override
        public boolean fetches(ColumnMetadata column)
        {
            return fetchingStrategy.fetchesAllColumns(column.isStatic()) || fetched.contains(column);
        }

        /**
         * Whether the provided complex cell (identified by its column and path), which is assumed to be _fetched_ by
         * this filter, is also _queried_ by the user.
         *
         * !WARNING! please be sure to understand the difference between _fetched_ and _queried_
         * columns that this class made before using this method. If unsure, you probably want
         * to use the {@link #fetches} method.
         */
        @Override
        public boolean fetchedColumnIsQueried(ColumnMetadata column)
        {
            return fetchingStrategy.areAllFetchedColumnsQueried() || queried.contains(column);
        }

        @Override
        public boolean fetchedCellIsQueried(ColumnMetadata column, CellPath path)
        {
            assert path != null;

            // first verify that the column to which the cell belongs is queried
            if (!fetchedColumnIsQueried(column))
                return false;

            if (subSelections == null)
                return true;

            SortedSet<ColumnSubselection> s = subSelections.get(column.name);
            // No subsection for this column means everything is queried
            if (s.isEmpty())
                return true;

            for (ColumnSubselection subSel : s)
                if (subSel.compareInclusionOf(path) == 0)
                    return true;

            return false;
        }

        @Override
        public Tester newTester(ColumnMetadata column)
        {
            if (subSelections == null || !column.isComplex())
                return null;

            SortedSet<ColumnSubselection> s = subSelections.get(column.name);
            if (s.isEmpty())
                return null;

            return new Tester(fetchingStrategy.fetchesAllColumns(column.isStatic()), s.iterator());
        }

        @Override
        protected SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections()
        {
            return subSelections;
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == this)
                return true;

            if (!(other instanceof SelectionColumnFilter))
                return false;

            SelectionColumnFilter otherCf = (SelectionColumnFilter) other;

            return otherCf.fetchingStrategy == this.fetchingStrategy &&
                   Objects.equals(otherCf.queried, this.queried) &&
                   Objects.equals(otherCf.fetched, this.fetched) &&
                   Objects.equals(otherCf.subSelections, this.subSelections);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(fetchingStrategy, queried, fetched, subSelections);
        }

        @Override
        public String toString()
        {
            String prefix = "";

            if (fetchingStrategy.fetchesAllColumns(true))
                prefix = "*/";

            if (fetchingStrategy == FetchingStrategy.ALL_REGULARS_AND_QUERIED_STATICS_COLUMNS)
            {
                prefix = queried.statics.isEmpty()
                       ? "<all regulars>/"
                       : String.format("<all regulars>+%s/", toString(queried.statics.selectOrderIterator(), false));
            }

            return prefix + toString(queried.selectOrderIterator(), false);
        }

        @Override
        public String toCQLString()
        {
            return queried.isEmpty() ? "*" : toString(queried.selectOrderIterator(), true);
        }

        private String toString(Iterator<ColumnMetadata> columns, boolean cql)
        {
            StringJoiner joiner = cql ? new StringJoiner(", ") : new StringJoiner(", ", "[", "]");

            while (columns.hasNext())
            {
                ColumnMetadata column = columns.next();
                String columnName = cql ? column.name.toCQLString() : String.valueOf(column.name);

                SortedSet<ColumnSubselection> s = subSelections != null
                                                ? subSelections.get(column.name)
                                                : Collections.emptySortedSet();

                if (s.isEmpty())
                    joiner.add(columnName);
                else
                    s.forEach(subSel -> joiner.add(String.format("%s%s", columnName, subSel.toString(cql))));
            }
            return joiner.toString();
        }
    }

    public static class Serializer
    {
        // fetch all regular columns and queried static columns
        private static final int FETCH_ALL_REGULARS_MASK = 0x01;
        private static final int HAS_QUERIED_MASK = 0x02;
        private static final int HAS_SUB_SELECTIONS_MASK = 0x04;
        // The FETCH_ALL_STATICS flag was added in CASSANDRA-16686 to allow 4.0 to handle queries that required
        // to return static data for empty partitions
        private static final int FETCH_ALL_STATICS_MASK = 0x08;

        private static int makeHeaderByte(ColumnFilter selection)
        {
            return (selection.fetchesAllColumns(false) ? FETCH_ALL_REGULARS_MASK : 0)
                   | (!selection.isWildcard() ? HAS_QUERIED_MASK : 0)
                   | (selection.subSelections() != null ? HAS_SUB_SELECTIONS_MASK : 0)
                   | (selection.fetchesAllColumns(true) ? FETCH_ALL_STATICS_MASK : 0);
        }

        public void serialize(ColumnFilter selection, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(makeHeaderByte(selection));

            if (selection.fetchesAllColumns(false))
            {
                serializeRegularAndStaticColumns(selection.fetchedColumns(), out);
            }

            if (!selection.isWildcard())
            {
                serializeRegularAndStaticColumns(selection.queriedColumns(), out);
            }

            serializeSubSelections(selection.subSelections(), out, version);
        }

        private void serializeSubSelections(SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections,
                                            DataOutputPlus out,
                                            int version) throws IOException
        {
            if (subSelections != null)
            {
                out.writeUnsignedVInt32(subSelections.size());
                for (ColumnSubselection subSel : subSelections.values())
                    ColumnSubselection.serializer.serialize(subSel, out, version);
            }
        }

        private void serializeRegularAndStaticColumns(RegularAndStaticColumns regularAndStaticColumns,
                                                      DataOutputPlus out) throws IOException
        {
            Columns.serializer.serialize(regularAndStaticColumns.statics, out);
            Columns.serializer.serialize(regularAndStaticColumns.regulars, out);
        }

        public ColumnFilter deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            int header = in.readUnsignedByte();
            boolean isFetchAllRegulars = (header & FETCH_ALL_REGULARS_MASK) != 0;
            boolean hasQueried = (header & HAS_QUERIED_MASK) != 0;
            boolean hasSubSelections = (header & HAS_SUB_SELECTIONS_MASK) != 0;
            boolean isFetchAllStatics = (header & FETCH_ALL_STATICS_MASK) != 0;

            RegularAndStaticColumns fetched = null;
            RegularAndStaticColumns queried = null;

            if (isFetchAllRegulars)
                fetched = deserializeRegularAndStaticColumns(in, metadata);

            if (hasQueried)
                queried = deserializeRegularAndStaticColumns(in, metadata);

            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections = null;
            if (hasSubSelections)
                subSelections = deserializeSubSelection(in, version, metadata);

            if (isFetchAllRegulars)
            {
                if (!hasQueried)
                    return new WildCardColumnFilter(fetched);

                if (!isFetchAllStatics)
                    return new SelectionColumnFilter(FetchingStrategy.ALL_REGULARS_AND_QUERIED_STATICS_COLUMNS, queried, fetched, subSelections);

                return new SelectionColumnFilter(FetchingStrategy.ALL_COLUMNS, queried, fetched, subSelections);
            }

            return new SelectionColumnFilter(FetchingStrategy.ONLY_QUERIED_COLUMNS, queried, queried, subSelections);
        }

        private RegularAndStaticColumns deserializeRegularAndStaticColumns(DataInputPlus in,
                                                                           TableMetadata metadata) throws IOException
        {
            Columns statics = Columns.serializer.deserialize(in, metadata);
            Columns regulars = Columns.serializer.deserialize(in, metadata);
            return new RegularAndStaticColumns(statics, regulars);
        }

        private SortedSetMultimap<ColumnIdentifier, ColumnSubselection> deserializeSubSelection(DataInputPlus in,
                                                                                                int version,
                                                                                                TableMetadata metadata) throws IOException
        {
            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections = TreeMultimap.create(Comparator.naturalOrder(), Comparator.naturalOrder());
            int size = in.readUnsignedVInt32();
            for (int i = 0; i < size; i++)
            {
                ColumnSubselection subSel = ColumnSubselection.serializer.deserialize(in, version, metadata);
                subSelections.put(subSel.column().name, subSel);
            }
            return subSelections;
        }

        public long serializedSize(ColumnFilter selection, int version)
        {
            long size = 1; // header byte

            if (selection.fetchesAllColumns(false))
            {
                size += regularAndStaticColumnsSerializedSize(selection.fetchedColumns());
            }

            if (!selection.isWildcard())
            {
                size += regularAndStaticColumnsSerializedSize(selection.queriedColumns());
            }

            size += subSelectionsSerializedSize(selection.subSelections(), version);

            return size;
        }

        private long regularAndStaticColumnsSerializedSize(RegularAndStaticColumns columns)
        {
            return Columns.serializer.serializedSize(columns.statics)
                    + Columns.serializer.serializedSize(columns.regulars);
        }

        private long subSelectionsSerializedSize(SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections,
                                                 int version)
        {
            if (subSelections == null)
                return 0;

            int size = TypeSizes.sizeofUnsignedVInt(subSelections.size());
            for (ColumnSubselection subSel : subSelections.values())
                size += ColumnSubselection.serializer.serializedSize(subSel, version);

            return size;
        }
    }
}

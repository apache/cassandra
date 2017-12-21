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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
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
 * often need to internally fetch all regular columns for the queried table, but can still do some optimizations for
 * those columns that are not directly queried by the user (see #10657 for more details).
 *
 * Note that in practice:
 *   - the _queried_ columns set is always included in the _fetched_ one.
 *   - whenever those sets are different, we know 1) the _fetched_ set contains all regular columns for the table and 2)
 *     _fetched_ == _queried_ for static columns, so we don't have to record this set, we just keep a pointer to the
 *     table metadata. The only set we concretely store is thus the _queried_ one.
 *   - in the special case of a {@code SELECT *} query, we want to query all columns, and _fetched_ == _queried.
 *     As this is a common case, we special case it by keeping the _queried_ set {@code null} (and we retrieve
 *     the columns through the metadata pointer).
 *
 * For complex columns, this class optionally allows to specify a subset of the cells to query for each column.
 * We can either select individual cells by path name, or a slice of them. Note that this is a sub-selection of
 * _queried_ cells, so if _fetched_ != _queried_, then the cell selected by this sub-selection are considered
 * queried and the other ones are considered fetched (and if a column has some sub-selection, it must be a queried
 * column, which is actually enforced by the Builder below).
 */
public class ColumnFilter
{
    public static final Serializer serializer = new Serializer();

    // True if _fetched_ includes all regular columns (and any static in _queried_), in which case metadata must not be
    // null. If false, then _fetched_ == _queried_ and we only store _queried_.
    final boolean fetchAllRegulars;

    final RegularAndStaticColumns fetched;
    final RegularAndStaticColumns queried; // can be null if fetchAllRegulars, to represent a wildcard query (all
                                           // static and regular columns are both _fetched_ and _queried_).
    private final SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections; // can be null

    private ColumnFilter(boolean fetchAllRegulars,
                         TableMetadata metadata,
                         RegularAndStaticColumns queried,
                         SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections)
    {
        assert !fetchAllRegulars || metadata != null;
        assert fetchAllRegulars || queried != null;
        this.fetchAllRegulars = fetchAllRegulars;

        if (fetchAllRegulars)
        {
            RegularAndStaticColumns all = metadata.regularAndStaticColumns();

            this.fetched = (all.statics.isEmpty() || queried == null)
                           ? all
                           : new RegularAndStaticColumns(queried.statics, all.regulars);
        }
        else
        {
            this.fetched = queried;
        }

        this.queried = queried;
        this.subSelections = subSelections;
    }

    /**
     * Used on replica for deserialisation
     */
    private ColumnFilter(boolean fetchAllRegulars,
                         RegularAndStaticColumns fetched,
                         RegularAndStaticColumns queried,
                         SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections)
    {
        assert !fetchAllRegulars || fetched != null;
        assert fetchAllRegulars || queried != null;
        this.fetchAllRegulars = fetchAllRegulars;
        this.fetched = fetchAllRegulars ? fetched : queried;
        this.queried = queried;
        this.subSelections = subSelections;
    }

    /**
     * A filter that includes all columns for the provided table.
     */
    public static ColumnFilter all(TableMetadata metadata)
    {
        return new ColumnFilter(true, metadata, null, null);
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
        return new ColumnFilter(false, (TableMetadata) null, columns, null);
    }

	/**
     * A filter that fetches all columns for the provided table, but returns
     * only the queried ones.
     */
    public static ColumnFilter selection(TableMetadata metadata, RegularAndStaticColumns queried)
    {
        return new ColumnFilter(true, metadata, queried, null);
    }

    /**
     * The columns that needs to be fetched internally for this filter.
     *
     * @return the columns to fetch for this filter.
     */
    public RegularAndStaticColumns fetchedColumns()
    {
        return fetched;
    }

    /**
     * The columns actually queried by the user.
     * <p>
     * Note that this is in general not all the columns that are fetched internally (see {@link #fetchedColumns}).
     */
    public RegularAndStaticColumns queriedColumns()
    {
        return queried == null ? fetched : queried;
    }

    /**
     * Wether all the (regular or static) columns are fetched by this filter.
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
    public boolean fetchesAllColumns(boolean isStatic)
    {
        return isStatic ? queried == null : fetchAllRegulars;
    }

    /**
     * Whether _fetched_ == _queried_ for this filter, and so if the {@code isQueried()} methods
     * can return {@code false} for some column/cell.
     */
    public boolean allFetchedColumnsAreQueried()
    {
        return !fetchAllRegulars || queried == null;
    }

    /**
     * Whether the provided column is fetched by this filter.
     */
    public boolean fetches(ColumnMetadata column)
    {
        // For statics, it is included only if it's part of _queried_, or if _queried_ is null (wildcard query).
        if (column.isStatic())
            return queried == null || queried.contains(column);

        // For regulars, if 'fetchAllRegulars', then it's included automatically. Otherwise, it depends on _queried_.
        return fetchAllRegulars || queried.contains(column);
    }

    /**
     * Whether the provided column, which is assumed to be _fetched_ by this filter (so the caller must guarantee
     * that {@code fetches(column) == true}, is also _queried_ by the user.
     *
     * !WARNING! please be sure to understand the difference between _fetched_ and _queried_
     * columns that this class made before using this method. If unsure, you probably want
     * to use the {@link #fetches} method.
     */
    public boolean fetchedColumnIsQueried(ColumnMetadata column)
    {
        return !fetchAllRegulars || queried == null || queried.contains(column);
    }

    /**
     * Whether the provided complex cell (identified by its column and path), which is assumed to be _fetched_ by
     * this filter, is also _queried_ by the user.
     *
     * !WARNING! please be sure to understand the difference between _fetched_ and _queried_
     * columns that this class made before using this method. If unsure, you probably want
     * to use the {@link #fetches} method.
     */
    public boolean fetchedCellIsQueried(ColumnMetadata column, CellPath path)
    {
        assert path != null;
        if (!fetchAllRegulars || subSelections == null)
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

    /**
     * Creates a new {@code Tester} to efficiently test the inclusion of cells of complex column
     * {@code column}.
     *
     * @param column for complex column for which to create a tester.
     * @return the created tester or {@code null} if all the cells from the provided column
     * are queried.
     */
    public Tester newTester(ColumnMetadata column)
    {
        if (subSelections == null || !column.isComplex())
            return null;

        SortedSet<ColumnSubselection> s = subSelections.get(column.name);
        if (s.isEmpty())
            return null;

        return new Tester(!column.isStatic() && fetchAllRegulars, s.iterator());
    }

    /**
     * Given an iterator on the cell of a complex column, returns an iterator that only include the cells selected by
     * this filter.
     *
     * @param column the (complex) column for which the cells are.
     * @param cells the cells to filter.
     * @return a filtered iterator that only include the cells from {@code cells} that are included by this filter.
     */
    public Iterator<Cell> filterComplexCells(ColumnMetadata column, Iterator<Cell> cells)
    {
        Tester tester = newTester(column);
        if (tester == null)
            return cells;

        return Iterators.filter(cells, cell -> tester.fetchedCellIsQueried(cell.path()));
    }

    /**
     * Returns a {@code ColumnFilter}} builder that fetches all regular columns (and queries the columns
     * added to the builder, or everything if no column is added).
     */
    public static Builder allRegularColumnsBuilder(TableMetadata metadata)
    {
        return new Builder(metadata);
    }

    /**
     * Returns a {@code ColumnFilter} builder that only fetches the columns/cells added to the builder.
     */
    public static Builder selectionBuilder()
    {
        return new Builder(null);
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
     * are _fetched_ depends on which constructor you've used to obtained this builder, allColumnsBuilder (all
     * columns are fetched) or selectionBuilder (only the queried columns are fetched).
     *
     * Note that for a allColumnsBuilder, if no queried columns are added, this is interpreted as querying
     * all columns, not querying none (but if you know you want to query all columns, prefer
     * {@link ColumnFilter#all(TableMetadata)}. For selectionBuilder, adding no queried columns means no column will be
     * fetched (so the builder will return {@code PartitionColumns.NONE}).
     *
     * Also, if only a subselection of a complex column should be queried, then only the corresponding
     * subselection method of the builder ({@link #slice} or {@link #select}) should be called for the
     * column, but {@link #add} shouldn't. if {@link #add} is also called, the whole column will be
     * queried and the subselection(s) will be ignored. This is done for correctness of CQL where
     * if you do "SELECT m, m[2..5]", you are really querying the whole collection.
     */
    public static class Builder
    {
        private final TableMetadata metadata; // null if we don't fetch all columns
        private RegularAndStaticColumns.Builder queriedBuilder;
        private List<ColumnSubselection> subSelections;

        private Set<ColumnMetadata> fullySelectedComplexColumns;

        private Builder(TableMetadata metadata)
        {
            this.metadata = metadata;
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
            boolean isFetchAll = metadata != null;

            RegularAndStaticColumns queried = queriedBuilder == null ? null : queriedBuilder.build();
            // It's only ok to have queried == null in ColumnFilter if isFetchAll. So deal with the case of a selectionBuilder
            // with nothing selected (we can at least happen on some backward compatible queries - CASSANDRA-10471).
            if (!isFetchAll && queried == null)
                queried = RegularAndStaticColumns.NONE;

            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> s = null;
            if (subSelections != null)
            {
                s = TreeMultimap.create(Comparator.<ColumnIdentifier>naturalOrder(), Comparator.<ColumnSubselection>naturalOrder());
                for (ColumnSubselection subSelection : subSelections)
                {
                    if (fullySelectedComplexColumns == null || !fullySelectedComplexColumns.contains(subSelection.column()))
                        s.put(subSelection.column().name, subSelection);
                }
            }

            return new ColumnFilter(isFetchAll, metadata, queried, s);
        }
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this)
            return true;

        if (!(other instanceof ColumnFilter))
            return false;

        ColumnFilter otherCf = (ColumnFilter) other;

        return otherCf.fetchAllRegulars == this.fetchAllRegulars &&
               Objects.equals(otherCf.fetched, this.fetched) &&
               Objects.equals(otherCf.queried, this.queried) &&
               Objects.equals(otherCf.subSelections, this.subSelections);
    }

    @Override
    public String toString()
    {
        if (fetchAllRegulars && queried == null)
            return "*";

        if (queried.isEmpty())
            return "";

        Iterator<ColumnMetadata> defs = queried.selectOrderIterator();
        if (!defs.hasNext())
            return "<none>";

        StringBuilder sb = new StringBuilder();
        while (defs.hasNext())
        {
            appendColumnDef(sb, defs.next());
            if (defs.hasNext())
                sb.append(", ");
        }
        return sb.toString();
    }

    private void appendColumnDef(StringBuilder sb, ColumnMetadata column)
    {
        if (subSelections == null)
        {
            sb.append(column.name);
            return;
        }

        SortedSet<ColumnSubselection> s = subSelections.get(column.name);
        if (s.isEmpty())
        {
            sb.append(column.name);
            return;
        }

        int i = 0;
        for (ColumnSubselection subSel : s)
            sb.append(i++ == 0 ? "" : ", ").append(column.name).append(subSel);
    }

    public static class Serializer
    {
        private static final int FETCH_ALL_MASK          = 0x01;
        private static final int HAS_QUERIED_MASK        = 0x02;
        private static final int HAS_SUB_SELECTIONS_MASK = 0x04;

        private static int makeHeaderByte(ColumnFilter selection)
        {
            return (selection.fetchAllRegulars ? FETCH_ALL_MASK : 0)
                 | (selection.queried != null ? HAS_QUERIED_MASK : 0)
                 | (selection.subSelections != null ? HAS_SUB_SELECTIONS_MASK : 0);
        }

        @VisibleForTesting
        public static ColumnFilter maybeUpdateForBackwardCompatility(ColumnFilter selection, int version)
        {
            if (version > MessagingService.VERSION_3014 || !selection.fetchAllRegulars || selection.queried == null)
                return selection;

            // The meaning of fetchAllRegulars changed (at least when queried != null) due to CASSANDRA-12768: in
            // pre-4.0 it means that *all* columns are fetched, not just the regular ones, and so 3.0/3.X nodes
            // would send us more than we'd like. So instead recreating a filter that correspond to what we
            // actually want (it's a tiny bit less efficient as we include all columns manually and will mark as
            // queried some columns that are actually only fetched, but it's fine during upgrade).
            // More concretely, we replace our filter by a non-fetch-all one that queries every columns that our
            // current filter fetches.
            Set<ColumnMetadata> queriedStatic = new HashSet<>();
            Iterables.addAll(queriedStatic, Iterables.filter(selection.queried, ColumnMetadata::isStatic));
            return new ColumnFilter(false,
                                    (TableMetadata) null,
                                    new RegularAndStaticColumns(Columns.from(queriedStatic), selection.fetched.regulars),
                                    selection.subSelections);
        }

        public void serialize(ColumnFilter selection, DataOutputPlus out, int version) throws IOException
        {
            selection = maybeUpdateForBackwardCompatility(selection, version);

            out.writeByte(makeHeaderByte(selection));

            if (version >= MessagingService.VERSION_3014 && selection.fetchAllRegulars)
            {
                Columns.serializer.serialize(selection.fetched.statics, out);
                Columns.serializer.serialize(selection.fetched.regulars, out);
            }

            if (selection.queried != null)
            {
                Columns.serializer.serialize(selection.queried.statics, out);
                Columns.serializer.serialize(selection.queried.regulars, out);
            }

            if (selection.subSelections != null)
            {
                out.writeUnsignedVInt(selection.subSelections.size());
                for (ColumnSubselection subSel : selection.subSelections.values())
                    ColumnSubselection.serializer.serialize(subSel, out, version);
            }
        }

        public ColumnFilter deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            int header = in.readUnsignedByte();
            boolean isFetchAll = (header & FETCH_ALL_MASK) != 0;
            boolean hasQueried = (header & HAS_QUERIED_MASK) != 0;
            boolean hasSubSelections = (header & HAS_SUB_SELECTIONS_MASK) != 0;

            RegularAndStaticColumns fetched = null;
            RegularAndStaticColumns queried = null;

            if (isFetchAll)
            {
                if (version >= MessagingService.VERSION_3014)
                {
                    Columns statics = Columns.serializer.deserialize(in, metadata);
                    Columns regulars = Columns.serializer.deserialize(in, metadata);
                    fetched = new RegularAndStaticColumns(statics, regulars);
                }
                else
                {
                    fetched = metadata.regularAndStaticColumns();
                }
            }

            if (hasQueried)
            {
                Columns statics = Columns.serializer.deserialize(in, metadata);
                Columns regulars = Columns.serializer.deserialize(in, metadata);
                queried = new RegularAndStaticColumns(statics, regulars);
            }

            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections = null;
            if (hasSubSelections)
            {
                subSelections = TreeMultimap.create(Comparator.<ColumnIdentifier>naturalOrder(), Comparator.<ColumnSubselection>naturalOrder());
                int size = (int)in.readUnsignedVInt();
                for (int i = 0; i < size; i++)
                {
                    ColumnSubselection subSel = ColumnSubselection.serializer.deserialize(in, version, metadata);
                    subSelections.put(subSel.column().name, subSel);
                }
            }

            // Same concern than in serialize/serializedSize: we should be wary of the change in meaning for isFetchAll.
            // If we get a filter with isFetchAll from 3.0/3.x, it actually expects all static columns to be fetched,
            // make sure we do that (note that if queried == null, that's already what we do).
            // Note that here again this will make us do a bit more work that necessary, namely we'll _query_ all
            // statics even though we only care about _fetching_ them all, but that's a minor inefficiency, so fine
            // during upgrade.
            if (version <= MessagingService.VERSION_30 && isFetchAll && queried != null)
                queried = new RegularAndStaticColumns(metadata.staticColumns(), queried.regulars);

            return new ColumnFilter(isFetchAll, fetched, queried, subSelections);
        }

        public long serializedSize(ColumnFilter selection, int version)
        {
            selection = maybeUpdateForBackwardCompatility(selection, version);

            long size = 1; // header byte

            if (version >= MessagingService.VERSION_3014 && selection.fetchAllRegulars)
            {
                size += Columns.serializer.serializedSize(selection.fetched.statics);
                size += Columns.serializer.serializedSize(selection.fetched.regulars);
            }

            if (selection.queried != null)
            {
                size += Columns.serializer.serializedSize(selection.queried.statics);
                size += Columns.serializer.serializedSize(selection.queried.regulars);
            }

            if (selection.subSelections != null)
            {

                size += TypeSizes.sizeofUnsignedVInt(selection.subSelections.size());
                for (ColumnSubselection subSel : selection.subSelections.values())
                    size += ColumnSubselection.serializer.serializedSize(subSel, version);
            }

            return size;
        }
    }
}
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
import com.google.common.collect.Iterators;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.CassandraVersion;

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
    private final static Logger logger = LoggerFactory.getLogger(ColumnFilter.class);

    public static final ColumnFilter NONE = selection(RegularAndStaticColumns.NONE);

    public static final Serializer serializer = new Serializer();

    // True if _fetched_ includes all regular columns (and any static in _queried_), in which case metadata must not be
    // null. If false, then _fetched_ == _queried_ and we only store _queried_.
    @VisibleForTesting
    final boolean fetchAllRegulars;

    // This flag can be only set when fetchAllRegulars is set. When fetchAllRegulars is set and queried==null then
    // it is implied to be true. The flag when set allows for interpreting the column filter in the same way as it was
    // interpreted by pre 4.0 Cassandra versions (3.4 ~ 4.0), that is, we fetch all columns (both regulars and static)
    // but we query only some of them. This allows for proper behaviour during upgrades.
    private final boolean fetchAllStatics;

    @VisibleForTesting
    final RegularAndStaticColumns fetched;

    private final RegularAndStaticColumns queried; // can be null if fetchAllRegulars, to represent a wildcard query (all

    // static and regular columns are both _fetched_ and _queried_).
    private final SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections; // can be null

    private ColumnFilter(boolean fetchAllRegulars,
                         boolean fetchAllStatics,
                         TableMetadata metadata,
                         RegularAndStaticColumns queried,
                         SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections)
    {
        assert !fetchAllRegulars || metadata != null;
        assert fetchAllRegulars || queried != null;
        assert !fetchAllStatics || fetchAllRegulars;
        this.fetchAllRegulars = fetchAllRegulars;
        this.fetchAllStatics = fetchAllStatics || fetchAllRegulars && queried == null;

        if (fetchAllRegulars)
        {
            RegularAndStaticColumns all = metadata.regularAndStaticColumns();

            this.fetched = (all.statics.isEmpty() || queried == null || fetchAllStatics)
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
                         boolean fetchAllStatics,
                         RegularAndStaticColumns fetched,
                         RegularAndStaticColumns queried,
                         SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections)
    {
        assert !fetchAllRegulars || fetched != null;
        assert fetchAllRegulars || queried != null;
        assert !fetchAllStatics || fetchAllRegulars;
        this.fetchAllRegulars = fetchAllRegulars;
        this.fetchAllStatics = fetchAllStatics || fetchAllRegulars && queried == null;
        this.fetched = fetchAllRegulars ? fetched : queried;
        this.queried = queried;
        this.subSelections = subSelections;
    }

    /**
     * Returns true if all static columns should be fetched along with all regular columns (it only makes sense to call
     * this method if fetchAllRegulars is going to be true and queried != null).
     *
     * We have to apply this conversion when there are pre-4.0 nodes in the cluster because they interpret
     * the ColumnFilter with fetchAllRegulars (translated to fetchAll in pre 4.0) and queried != null so that all
     * the columns are fetched (both regular and static) and just some of them are queried. In 4.0+ with the same
     * scenario, all regulars are fetched and only those statics which are queried. We need to apply the conversion
     * so that the retrieved data is the same (note that non-queried columns may have skipped values or may not be
     * included at all).
     */
    private static boolean shouldFetchAllStatics()
    {
        if (Gossiper.instance.isUpgradingFromVersionLowerThan(CassandraVersion.CASSANDRA_4_0))
        {
            logger.trace("ColumnFilter conversion has been applied so that all static columns will be fetched because there are pre 4.0 nodes in the cluster");
            return true;
        }
        return false;
    }

    /**
     * Returns true if we want to consider all fetched columns as queried as well (it only makes sense to call
     * this method if fetchAllRegulars is going to be true).
     *
     * We have to apply this conversion when there are pre-3.4 (in particular, pre CASSANDRA-10657) nodes in the cluster
     * because they interpret the ColumnFilter with fetchAllRegulars (translated to fetchAll in pre 4.0) so that all
     * fetched columns are queried. In 3.4+ with the same scenario, all the columns are fetched
     * (though see {@link #shouldFetchAllStatics()}) but queried columns are taken into account in the way that we may
     * skip values or whole cells when reading data. We need to apply the conversion so that the retrieved data is
     * the same.
     */
    private static boolean shouldQueriedBeNull()
    {
        if (Gossiper.instance.isUpgradingFromVersionLowerThan(CassandraVersion.CASSANDRA_3_4))
        {
            logger.trace("ColumnFilter conversion has been applied so that all columns will be queried because there are pre 3.4 nodes in the cluster");
            return true;
        }
        return false;
    }

    /**
     * A filter that includes all columns for the provided table.
     */
    public static ColumnFilter all(TableMetadata metadata)
    {
        return new ColumnFilter(true, true, metadata, null, null);
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
        return new ColumnFilter(false, false, (TableMetadata) null, columns, null);
    }

    /**
     * A filter that fetches all columns for the provided table, but returns
     * only the queried ones.
     */
    public static ColumnFilter selection(TableMetadata metadata, RegularAndStaticColumns queried)
    {
        return new ColumnFilter(true, shouldFetchAllStatics(), metadata, shouldQueriedBeNull() ? null : queried, null);
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
        return isStatic ? queried == null || fetchAllStatics : fetchAllRegulars;
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
            return fetchAllStatics || queried == null || queried.contains(column);

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

        return new Tester(!column.isStatic() && fetchAllRegulars || column.isStatic() && fetchAllStatics, s.iterator());
    }

    /**
     * Given an iterator on the cell of a complex column, returns an iterator that only include the cells selected by
     * this filter.
     *
     * @param column the (complex) column for which the cells are.
     * @param cells the cells to filter.
     * @return a filtered iterator that only include the cells from {@code cells} that are included by this filter.
     */
    public Iterator<Cell<?>> filterComplexCells(ColumnMetadata column, Iterator<Cell<?>> cells)
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
                s = TreeMultimap.create(Comparator.naturalOrder(), Comparator.naturalOrder());
                for (ColumnSubselection subSelection : subSelections)
                {
                    if (fullySelectedComplexColumns == null || !fullySelectedComplexColumns.contains(subSelection.column()))
                        s.put(subSelection.column().name, subSelection);
                }
            }

            // When fetchAll is enabled on pre CASSANDRA-10657 (3.4-), queried columns are not considered at all, and it
            // is assumed that all columns are queried. CASSANDRA-10657 (3.4+) brings back skipping values of columns
            // which are not in queried set when fetchAll is enabled. That makes exactly the same filter being
            // interpreted in a different way on 3.4- and 3.4+.
            //
            // Moreover, there is no way to convert the filter with fetchAll and queried != null so that it is
            // interpreted the same way on 3.4- because that Cassandra version does not support such filtering.
            //
            // In order to avoid inconsitencies in data read by 3.4- and 3.4+ we need to avoid creation of incompatible
            // filters when the cluster contains 3.4- nodes. We do that by forcibly setting queried to null.
            //
            // see CASSANDRA-10657, CASSANDRA-15833, CASSANDRA-16415
            return new ColumnFilter(isFetchAll, isFetchAll && shouldFetchAllStatics(), metadata, isFetchAll && shouldQueriedBeNull() ? null : queried, s);
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
               otherCf.fetchAllStatics == this.fetchAllStatics &&
               Objects.equals(otherCf.fetched, this.fetched) &&
               Objects.equals(otherCf.queried, this.queried) &&
               Objects.equals(otherCf.subSelections, this.subSelections);
    }

    @Override
    public String toString()
    {
        String prefix = "";

        if (fetchAllRegulars && queried == null)
            return "*/*";

        if (fetchAllRegulars && fetchAllStatics)
            prefix = "*/";

        if (fetchAllRegulars && !fetchAllStatics)
        {
            prefix = queried.statics.isEmpty()
                   ? "<all regulars>/"
                   : String.format("<all regulars>+%s/", toString(queried.statics.selectOrderIterator(), false));
        }

        return prefix + toString(queried.selectOrderIterator(), false);
    }

    public String toCQLString()
    {
        if (queried == null || queried.isEmpty())
            return "*";

        return toString(queried.selectOrderIterator(), true);
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
                s.forEach(subSel -> joiner.add(String.format("%s%s", columnName, subSel)));
        }
        return joiner.toString();
    }

    public static class Serializer
    {
        private static final int FETCH_ALL_MASK = 0x01;
        private static final int HAS_QUERIED_MASK = 0x02;
        private static final int HAS_SUB_SELECTIONS_MASK = 0x04;

        private static int makeHeaderByte(ColumnFilter selection)
        {
            return (selection.fetchAllRegulars ? FETCH_ALL_MASK : 0)
                   | (selection.queried != null ? HAS_QUERIED_MASK : 0)
                   | (selection.subSelections != null ? HAS_SUB_SELECTIONS_MASK : 0);
        }

        public void serialize(ColumnFilter selection, DataOutputPlus out, int version) throws IOException
        {
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
                    Columns statics = Columns.serializer.deserializeStatics(in, metadata);
                    Columns regulars = Columns.serializer.deserializeRegulars(in, metadata);
                    fetched = new RegularAndStaticColumns(statics, regulars);
                }
                else
                {
                    fetched = metadata.regularAndStaticColumns();
                }
            }

            if (hasQueried)
            {
                Columns statics = Columns.serializer.deserializeStatics(in, metadata);
                Columns regulars = Columns.serializer.deserializeRegulars(in, metadata);
                queried = new RegularAndStaticColumns(statics, regulars);
            }

            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections = null;
            if (hasSubSelections)
            {
                subSelections = TreeMultimap.create(Comparator.naturalOrder(), Comparator.naturalOrder());
                int size = (int) in.readUnsignedVInt();
                for (int i = 0; i < size; i++)
                {
                    ColumnSubselection subSel = ColumnSubselection.serializer.deserialize(in, version, metadata);
                    subSelections.put(subSel.column().name, subSel);
                }
            }

            return new ColumnFilter(isFetchAll, isFetchAll && shouldFetchAllStatics(), fetched, isFetchAll && shouldQueriedBeNull() ? null : queried, subSelections);
        }

        public long serializedSize(ColumnFilter selection, int version)
        {
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

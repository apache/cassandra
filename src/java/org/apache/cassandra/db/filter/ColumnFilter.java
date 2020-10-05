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

import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

/**
 * Represents which (non-PK) columns (and optionally which sub-part of a column for complex columns) are selected
 * by a query.
 *
 * In practice, this class cover 2 main cases:
 *   1) most user queries have to internally query all columns, because the CQL semantic requires us to know if
 *      a row is live or not even if it has no values for the columns requested by the user (see #6588for more
 *      details). However, while we need to know for columns if it has live values, we can actually save from
 *      sending the values for those columns that will not be returned to the user.
 *   2) for some internal queries (and for queries using #6588 if we introduce it), we're actually fine only
 *      actually querying some of the columns.
 *
 * For complex columns, this class allows to be more fine grained than the column by only selection some of the
 * cells of the complex column (either individual cell by path name, or some slice).
 */
public class ColumnFilter
{
    public static final Serializer serializer = new Serializer();

    // Distinguish between the 2 cases described above: if 'isFetchAll' is true, then all columns will be retrieved
    // by the query, but the values for column/cells not selected by 'queried' and 'subSelections' will be skipped.
    // Otherwise, only the column/cells returned by 'queried' and 'subSelections' will be returned at all.
    private final boolean isFetchAll;

    private final PartitionColumns queried; // can be null if isFetchAll and we don't want to skip any value
    private final PartitionColumns fetched;
    private final SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections; // can be null

    /**
     * Used on replica for deserialisation
     */
    private ColumnFilter(boolean isFetchAll,
                         PartitionColumns fetched,
                         PartitionColumns queried,
                         SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subSelections)
    {
        assert !isFetchAll || fetched != null;
        assert isFetchAll || queried != null;
        this.isFetchAll = isFetchAll;
        this.fetched = isFetchAll ? fetched : queried;
        this.queried = queried;
        this.subSelections = subSelections;
    }

    /**
     * A selection that includes all columns (and their values).
     */
    public static ColumnFilter all(CFMetaData metadata)
    {
        return new ColumnFilter(true, metadata.partitionColumns(), null, null);
    }

    /**
     * A selection that only fetch the provided columns.
     * <p>
     * Note that this shouldn't be used for CQL queries in general as all columns should be queried to
     * preserve CQL semantic (see class javadoc). This is ok for some internal queries however (and
     * for #6588 if/when we implement it).
     */
    public static ColumnFilter selection(PartitionColumns columns)
    {
        return new ColumnFilter(false, null, columns, null);
    }

	/**
     * A filter that fetches all columns for the provided table, but returns
     * only the queried ones.
     */
    public static ColumnFilter selection(CFMetaData metadata, PartitionColumns queried)
    {
        return new ColumnFilter(true, metadata.partitionColumns(), queried, null);
    }

    /**
     * The columns that needs to be fetched internally for this selection.
     * <p>
     * This is the columns that must be present in the internal rows returned by queries using this selection,
     * not the columns that are actually queried by the user (see the class javadoc for details).
     *
     * @return the column to fetch for this selection.
     */
    public PartitionColumns fetchedColumns()
    {
        return fetched;
    }

    public boolean includesAllColumns()
    {
        return isFetchAll;
    }

    /**
     * Whether the provided column is selected by this selection.
     */
    public boolean includes(ColumnDefinition column)
    {
        return isFetchAll || queried.contains(column);
    }

    /**
     * Whether we can skip the value for the provided selected column.
     */
    public boolean canSkipValue(ColumnDefinition column)
    {
        // We don't use that currently, see #10655 for more details.
        return false;
    }

    /**
     * Whether the provided cell of a complex column is selected by this selection.
     */
    public boolean includes(Cell cell)
    {
        if (isFetchAll || subSelections == null || !cell.column().isComplex())
            return true;

        SortedSet<ColumnSubselection> s = subSelections.get(cell.column().name);
        if (s.isEmpty())
            return true;

        for (ColumnSubselection subSel : s)
            if (subSel.compareInclusionOf(cell.path()) == 0)
                return true;

        return false;
    }

    /**
     * Whether we can skip the value of the cell of a complex column.
     */
    public boolean canSkipValue(ColumnDefinition column, CellPath path)
    {
        if (!isFetchAll || subSelections == null || !column.isComplex())
            return false;

        SortedSet<ColumnSubselection> s = subSelections.get(column.name);
        if (s.isEmpty())
            return false;

        for (ColumnSubselection subSel : s)
            if (subSel.compareInclusionOf(path) == 0)
                return false;

        return true;
    }

    /**
     * Creates a new {@code Tester} to efficiently test the inclusion of cells of complex column
     * {@code column}.
     */
    public Tester newTester(ColumnDefinition column)
    {
        if (subSelections == null || !column.isComplex())
            return null;

        SortedSet<ColumnSubselection> s = subSelections.get(column.name);
        if (s.isEmpty())
            return null;

        return new Tester(isFetchAll, s.iterator());
    }

    /**
     * Returns a {@code ColumnFilter}} builder that includes all columns (so the selections
     * added to the builder are the columns/cells for which we shouldn't skip the values).
     */
    public static Builder allColumnsBuilder(CFMetaData metadata)
    {
        return new Builder(metadata);
    }

    /**
     * Returns a {@code ColumnFilter}} builder that includes only the columns/cells
     * added to the builder.
     */
    public static Builder selectionBuilder()
    {
        return new Builder(null);
    }

    public static class Tester
    {
        private final boolean isFetchAll;
        private ColumnSubselection current;
        private final Iterator<ColumnSubselection> iterator;

        private Tester(boolean isFetchAll, Iterator<ColumnSubselection> iterator)
        {
            this.isFetchAll = isFetchAll;
            this.iterator = iterator;
        }

        public boolean includes(CellPath path)
        {
            return isFetchAll || includedBySubselection(path);
        }

        public boolean canSkipValue(CellPath path)
        {
            return isFetchAll && !includedBySubselection(path);
        }

        private boolean includedBySubselection(CellPath path)
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

    public static class Builder
    {
        private final CFMetaData metadata;
        private PartitionColumns.Builder selection;
        private List<ColumnSubselection> subSelections;

        private Builder(CFMetaData metadata)
        {
            this.metadata = metadata;
        }

        public Builder add(ColumnDefinition c)
        {
            if (selection == null)
                selection = PartitionColumns.builder();
            selection.add(c);
            return this;
        }

        public Builder addAll(Iterable<ColumnDefinition> columns)
        {
            if (selection == null)
                selection = PartitionColumns.builder();
            selection.addAll(columns);
            return this;
        }

        private Builder addSubSelection(ColumnSubselection subSelection)
        {
            add(subSelection.column());
            if (subSelections == null)
                subSelections = new ArrayList<>();
            subSelections.add(subSelection);
            return this;
        }

        public Builder slice(ColumnDefinition c, CellPath from, CellPath to)
        {
            return addSubSelection(ColumnSubselection.slice(c, from, to));
        }

        public Builder select(ColumnDefinition c, CellPath elt)
        {
            return addSubSelection(ColumnSubselection.element(c, elt));
        }

        public ColumnFilter build()
        {
            boolean isFetchAll = metadata != null;

            PartitionColumns selectedColumns = selection == null ? null : selection.build();
            // It's only ok to have queried == null in ColumnFilter if isFetchAll. So deal with the case of a "selection" builder
            // with nothing selected (we can at least happen on some backward compatible queries - CASSANDRA-10471).
            if (!isFetchAll && selectedColumns == null)
                selectedColumns = PartitionColumns.NONE;

            SortedSetMultimap<ColumnIdentifier, ColumnSubselection> s = null;
            if (subSelections != null)
            {
                s = TreeMultimap.create(Comparator.<ColumnIdentifier>naturalOrder(), Comparator.<ColumnSubselection>naturalOrder());
                for (ColumnSubselection subSelection : subSelections)
                    s.put(subSelection.column().name, subSelection);
            }

            return new ColumnFilter(isFetchAll, isFetchAll ? metadata.partitionColumns() : selectedColumns, selectedColumns, s);
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

        return otherCf.isFetchAll == this.isFetchAll &&
               Objects.equals(otherCf.fetched, this.fetched) &&
               Objects.equals(otherCf.queried, this.queried) &&
               Objects.equals(otherCf.subSelections, this.subSelections);

    }
    @Override
    public String toString()
    {
        if (isFetchAll)
            return "*";

        if (queried.isEmpty())
            return "";

        Iterator<ColumnDefinition> defs = queried.selectOrderIterator();
        if (!defs.hasNext())
            return "<none>";

        StringBuilder sb = new StringBuilder();
        appendColumnDef(sb, defs.next());
        while (defs.hasNext())
            appendColumnDef(sb.append(", "), defs.next());
        return sb.toString();
    }

    private void appendColumnDef(StringBuilder sb, ColumnDefinition column)
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
        private static final int IS_FETCH_ALL_MASK       = 0x01;
        private static final int HAS_SELECTION_MASK      = 0x02;
        private static final int HAS_SUB_SELECTIONS_MASK = 0x04;

        private static int makeHeaderByte(ColumnFilter selection)
        {
            return (selection.isFetchAll ? IS_FETCH_ALL_MASK : 0)
                 | (selection.queried != null ? HAS_SELECTION_MASK : 0)
                 | (selection.subSelections != null ? HAS_SUB_SELECTIONS_MASK : 0);
        }

        public void serialize(ColumnFilter selection, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(makeHeaderByte(selection));

            if (version >= MessagingService.VERSION_3014 && selection.isFetchAll)
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

        public ColumnFilter deserialize(DataInputPlus in, int version, CFMetaData metadata) throws IOException
        {
            int header = in.readUnsignedByte();
            boolean isFetchAll = (header & IS_FETCH_ALL_MASK) != 0;
            boolean hasSelection = (header & HAS_SELECTION_MASK) != 0;
            boolean hasSubSelections = (header & HAS_SUB_SELECTIONS_MASK) != 0;

            PartitionColumns fetched = null;
            PartitionColumns selection = null;

            if (isFetchAll)
            {
                if (version >= MessagingService.VERSION_3014)
                {
                    Columns statics = Columns.serializer.deserializeStatics(in, metadata);
                    Columns regulars = Columns.serializer.deserializeRegulars(in, metadata);
                    fetched = new PartitionColumns(statics, regulars);
                }
                else
                {
                    fetched = metadata.partitionColumns();
                }
            }

            if (hasSelection)
            {
                Columns statics = Columns.serializer.deserializeStatics(in, metadata);
                Columns regulars = Columns.serializer.deserializeRegulars(in, metadata);
                selection = new PartitionColumns(statics, regulars);
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

            return new ColumnFilter(isFetchAll, fetched, selection, subSelections);
        }

        public long serializedSize(ColumnFilter selection, int version)
        {
            long size = 1; // header byte

            if (version >= MessagingService.VERSION_3014 && selection.isFetchAll)
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
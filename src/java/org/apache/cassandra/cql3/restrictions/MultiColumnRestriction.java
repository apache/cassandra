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
package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.MarkerOrList;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Tuples;
import org.apache.cassandra.cql3.Term.Terminal;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.ListSerializer;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public abstract class MultiColumnRestriction implements SingleRestriction
{
    /**
     * The columns to which the restriction apply.
     */
    protected final List<ColumnMetadata> columnDefs;

    public MultiColumnRestriction(List<ColumnMetadata> columnDefs)
    {
        this.columnDefs = columnDefs;
    }

    @Override
    public boolean isMultiColumn()
    {
        return true;
    }

    @Override
    public ColumnMetadata getFirstColumn()
    {
        return columnDefs.get(0);
    }

    @Override
    public ColumnMetadata getLastColumn()
    {
        return columnDefs.get(columnDefs.size() - 1);
    }

    @Override
    public List<ColumnMetadata> getColumnDefs()
    {
        return columnDefs;
    }

    @Override
    public final SingleRestriction mergeWith(SingleRestriction otherRestriction)
    {
        // We want to allow query like: (b,c) > (?, ?) AND b < ?
        if (!otherRestriction.isMultiColumn()
                && ((SingleColumnRestriction) otherRestriction).canBeConvertedToMultiColumnRestriction())
        {
            return doMergeWith(((SingleColumnRestriction) otherRestriction).toMultiColumnRestriction());
        }

        return doMergeWith(otherRestriction);
    }

    protected abstract SingleRestriction doMergeWith(SingleRestriction otherRestriction);

    /**
     * Returns the names of the columns that are specified within this <code>Restrictions</code> and the other one
     * as a comma separated <code>String</code>.
     *
     * @param otherRestriction the other restrictions
     * @return the names of the columns that are specified within this <code>Restrictions</code> and the other one
     * as a comma separated <code>String</code>.
     */
    protected final String getColumnsInCommons(Restriction otherRestriction)
    {
        Set<ColumnMetadata> commons = new HashSet<>(getColumnDefs());
        commons.retainAll(otherRestriction.getColumnDefs());
        StringBuilder builder = new StringBuilder();
        for (ColumnMetadata columnMetadata : commons)
        {
            if (builder.length() != 0)
                builder.append(" ,");
            builder.append(columnMetadata.name);
        }
        return builder.toString();
    }

    @Override
    public final boolean hasSupportingIndex(IndexRegistry indexRegistry)
    {
        for (Index index : indexRegistry.listIndexes())
           if (isSupportedBy(index))
               return true;

        return false;
    }

    @Override
    public boolean needsFiltering(Index.Group indexGroup)
    {
        for (ColumnMetadata column : columnDefs)
        {
            if (!isSupportedBy(indexGroup, column))
                return true;
        }
        return false;
    }

    private boolean isSupportedBy(Index.Group indexGroup, ColumnMetadata column)
    {
        for (Index index : indexGroup.getIndexes())
        {
            if (isSupportedBy(index, column))
                return true;
        }
        return false;
    }

    /**
     * Check if this type of restriction is supported for by the specified index.
     * @param index the secondary index
     *
     * @return <code>true</code> this type of restriction is supported by the specified index,
     * <code>false</code> otherwise.
     */
    private boolean isSupportedBy(Index index)
    {
        for (ColumnMetadata column : columnDefs)
        {
            if (isSupportedBy(index, column))
                return true;
        }
        return false;
    }

    protected abstract boolean isSupportedBy(Index index, ColumnMetadata def);

    public static class EQRestriction extends MultiColumnRestriction
    {
        protected final Term value;

        public EQRestriction(List<ColumnMetadata> columnDefs, Term value)
        {
            super(columnDefs);
            this.value = value;
        }

        @Override
        public boolean isEQ()
        {
            return true;
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            value.addFunctionsTo(functions);
        }

        @Override
        public String toString()
        {
            return String.format("EQ(%s)", value);
        }

        @Override
        public SingleRestriction doMergeWith(SingleRestriction otherRestriction)
        {
            if (otherRestriction instanceof SliceRestriction)
            {
                SingleRestriction thisAsSlice = this.toSliceRestriction();
                return thisAsSlice.mergeWith(otherRestriction);
            }
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes an Equal",
                                 getColumnsInCommons(otherRestriction));
        }

        private SingleRestriction toSliceRestriction()
        {
            SliceRestriction start = SliceRestriction.fromBound(columnDefs, Bound.START, true, this.value);
            SliceRestriction end = SliceRestriction.fromBound(columnDefs, Bound.END, true, this.value);
            return start.mergeWith(end);
        }

        @Override
        protected boolean isSupportedBy(Index index, ColumnMetadata column)
        {
            return index.supportsExpression(column, Operator.EQ);
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            Tuples.Value t = ((Tuples.Value) value.bind(options));
            List<ByteBuffer> values = t.getElements();
            for (int i = 0, m = values.size(); i < m; i++)
            {
                ColumnMetadata column = columnDefs.get(i);
                builder.extend(MultiCBuilder.Element.point(values.get(i)), Collections.singletonList(column));
                checkFalse(builder.containsNull(), "Invalid null value for column %s", column.name);
            }
            return builder;
        }

        @Override
        public final void addToRowFilter(RowFilter filter, IndexRegistry indexRegistry, QueryOptions options)
        {
            Tuples.Value t = ((Tuples.Value) value.bind(options));
            List<ByteBuffer> values = t.getElements();

            for (int i = 0, m = columnDefs.size(); i < m; i++)
            {
                ColumnMetadata columnDef = columnDefs.get(i);
                filter.add(columnDef, Operator.EQ, values.get(i));
            }
        }
    }

    public static class INRestriction extends MultiColumnRestriction
    {
        private final MarkerOrList values;
        private final Collection<ColumnIdentifier> columnIdentifiers;

        public INRestriction(List<ColumnMetadata> columnDefs, MarkerOrList values)
        {
            super(columnDefs);
            this.values = values;
            this.columnIdentifiers = ColumnMetadata.toIdentifiers(columnDefs);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            List<List<ByteBuffer>> splitInValues = values.bindAndGetTuples(options, columnIdentifiers);
            List<MultiCBuilder.Element> elements = new ArrayList<>(splitInValues.size());
            for (List<ByteBuffer> value: splitInValues)
                elements.add(MultiCBuilder.Element.point(value));

            builder.extend(elements, columnDefs);

            if (builder.containsNull())
                throw invalidRequest("Invalid null value in condition for columns: %s", columnIdentifiers);
            return builder;
        }

        @Override
        public boolean isIN()
        {
            return true;
        }

        @Override
        public SingleRestriction doMergeWith(SingleRestriction otherRestriction)
        {
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes a IN",
                                 getColumnsInCommons(otherRestriction));
        }

        @Override
        protected boolean isSupportedBy(Index index, ColumnMetadata column)
        {
            return index.supportsExpression(column, Operator.IN);
        }

        @Override
        public final void addToRowFilter(RowFilter filter,
                                         IndexRegistry indexRegistry,
                                         QueryOptions options)
        {
            // If the relation is of the type (c) IN ((x),(y),(z)) then it is equivalent to
            // c IN (x, y, z) and we can perform filtering
            if (getColumnDefs().size() == 1)
            {
                List<List<ByteBuffer>> splitValues = values.bindAndGetTuples(options, columnIdentifiers);
                List<ByteBuffer> values = new ArrayList<>(splitValues.size());
                for (List<ByteBuffer> splitValue : splitValues)
                    values.add(splitValue.get(0));

                ByteBuffer buffer = ListSerializer.pack(values, values.size());
                filter.add(getFirstColumn(), Operator.IN, buffer);
            }
            else
            {
                throw invalidRequest("Multicolumn IN filters are not supported");
            }
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            values.addFunctionsTo(functions);
        }

        @Override
        public String toString()
        {
            return String.format("IN(%s)", values);
        }
    }


    public static class SliceRestriction extends MultiColumnRestriction
    {
        private final TermSlice slice;

        private final List<MarkerOrList> skippedValues; // values passed in NOT IN


        SliceRestriction(List<ColumnMetadata> columnDefs, TermSlice slice, List<MarkerOrList> skippedValues)
        {
            super(columnDefs);
            assert slice != null;
            assert skippedValues != null;
            this.slice = slice;
            this.skippedValues = skippedValues;
        }

        public static MultiColumnRestriction.SliceRestriction fromBound(List<ColumnMetadata> columnDefs, Bound bound, boolean inclusive, Term term)
        {
            TermSlice slice = TermSlice.newInstance(bound, inclusive, term);
            return new MultiColumnRestriction.SliceRestriction(columnDefs, slice, Collections.emptyList());
        }

        public static MultiColumnRestriction.SliceRestriction fromSkippedValues(List<ColumnMetadata> columnDefs, MarkerOrList skippedValues)
        {
            return new MultiColumnRestriction.SliceRestriction(columnDefs, TermSlice.UNBOUNDED, Collections.singletonList(skippedValues));
        }

        @Override
        public boolean isSlice()
        {
            return true;
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options)
        {
            List<MultiCBuilder.Element> toAdd = new ArrayList<>();
            addSliceBounds(bound, options, toAdd);
            addSkippedValues(bound, options, toAdd);
            return builder.extend(toAdd, columnDefs);
        }

        /**
         * Generates a list of clustering bounds based on this slice bounds and adds them to the toAdd list.
         * Clustering bounds used for the table range scan might not be equal to this slice bounds.
         * This method has to generate the TOP/BOTTOM bounds if this slice is unbounded on any side.
         * It is also possible to generate multiple bounds, if clustering columns have mixed order.
         * Does not guarantee order of results, but does not generate duplicates.
         *
         * @param bound the type of bounds to generate (start or end)
         * @param options needed to get the actual values bound to markers
         * @param toAdd receiver of the result
         */
        private void addSliceBounds(Bound bound, QueryOptions options, List<MultiCBuilder.Element> toAdd)
        {
            // Stores the direction of sorting of the current processed column.
            // Used to detect when the next processed column has different direction of sorting than the last one.
            // If clustering columns are all sorted in the same direction (doesn't matter if ASC or DESC, but must be
            // the same for all), we can just need to generate only one
            boolean reversed = getFirstColumn().isReversedType();

            EnumMap<Bound, List<ByteBuffer>> componentBounds = new EnumMap<>(Bound.class);
            componentBounds.put(Bound.START, componentBounds(Bound.START, options));
            componentBounds.put(Bound.END, componentBounds(Bound.END, options));

            // We will pick a prefix of bounds from `componentBounds` into this array, either start or end bounds
            // depending on the column clustering direction.
            List<ByteBuffer> values = Collections.emptyList();

            // Tracks whether the last bound added to `values` is inclusive.
            // We must start from true, because if there are no bounds at all (unbounded slice),
            // we must not restrict the clusterings added by other restrictions.
            boolean inclusive = true;

            // Number of components in the last element added to the toAdd collection.
            // Used to avoid adding the same composite multiple times.
            int sizeOfLastElement = -1;

            for (int i = 0, m = columnDefs.size(); i < m; i++)
            {
                ColumnMetadata column = columnDefs.get(i);
                Bound b = bound.reverseIfNeeded(column);

                // For mixed order columns, we need to create additional slices when 2 columns are in reverse order
                if (reversed != column.isReversedType())
                {
                    reversed = column.isReversedType();

                    // In the following comments, assume:
                    // c1 - previous column (== columnDefs.get(i - 1))
                    // c2 - current column (== columnDefs.get(i))
                    // x1 - the last bound stored in values (values == [..., x1])
                    // x2 - the bound of c2

                    // Only try to add the current composite if we haven't done it already, to avoid duplicates.
                    if (values.size() > sizeOfLastElement)
                    {
                        sizeOfLastElement = values.size();

                        // note that b.reverse() matches the bound of the last component added to the `values`
                        if (hasComponent(b.reverse(), i, componentBounds))
                        {
                            // (c1, c2) <= (x1, x2) ----> (c1 < x1) || (c1 = x1) && (c2 <= x2)
                            // (c1, c2) >= (x1, x2) ----> (c1 > x1) || (c1 = x1) && (c2 >= x2)
                            // (c1, c2) <  (x1, x2) ----> (c1 < x1) || (c1 = x1) && (c2 < x2)
                            // (c1, c2) >  (x1, x2) ----> (c1 > x1) || (c1 = x1) && (c2 > x2)
                            //                            ^^^^^^^^^
                            toAdd.add(MultiCBuilder.Element.bound(values, bound, false));

                            // Now add the other side of the union:
                            // (c1, c2) <= (x1, x2) ----> (c1 < x1) || (c1 = x1) && (c2 < x2)
                            // (c1, c2) >= (x1, x2) ----> (c1 > x1) || (c1 = x1) && (c2 > x2)
                            //                                         ^^^^^^^^^
                            // The other side has still some components. We need to end the slice that we have just open.
                            // Note that (c2 > x2) will be added by the call to an opposite bound.
                            toAdd.add(MultiCBuilder.Element.point(values));
                        }
                        else
                        {
                            // The new bound side has no value for this component. Just add current composite as-is.
                            // No value means min or max, depending on the direction of the comparison.
                            // (c1, c2) <= (x1, no value) ----> (c1 <= x1)
                            // (c1, c2) >= (x1, no value) ----> (c1 >= x1)
                            // (c1, c2) <  (x1, no value) ----> (c1 < x1)
                            // (c1, c2) >  (x1, no value) ----> (c1 > x1)
                            toAdd.add(MultiCBuilder.Element.bound(values, bound, inclusive));
                        }
                    }
                }

                if (hasComponent(b, i, componentBounds))
                {
                    values = componentBounds.get(b).subList(0, i + 1);
                    inclusive = isInclusive(b);
                }
            }

            if (values.size() > sizeOfLastElement)
                toAdd.add(MultiCBuilder.Element.bound(values, bound, inclusive));
        }



        /**
         * Generates a list of clustering bounds that exclude the skipped values.
         * I.e. for skipped elements (s1, s2, ..., sN), generates the slices
         * (BOTTOM, s1), (s1, s2), ..., (s(N-1), sN), (sN, TOP) and returns the list of
         * their start or end bounds depending on the selected `bound` param.
         *
         * @param bound which bound of the slices we want to generate
         * @param options needed to get the actual values bound to markers
         * @param toAdd receiver of the result
         */
        private void addSkippedValues(Bound bound, QueryOptions options, List<MultiCBuilder.Element> toAdd)
        {
            for (MarkerOrList markerOrList: skippedValues)
            {
                for (List<ByteBuffer> tuple: markerOrList.bindAndGetTuples(options, ColumnMetadata.toIdentifiers(columnDefs)))
                {
                    MultiCBuilder.Element element = MultiCBuilder.Element.bound(tuple, bound, false);
                    toAdd.add(element);
                }
            }
        }

        @Override
        protected boolean isSupportedBy(Index index, ColumnMetadata column)
        {
            return slice.isSupportedBy(column, index);
        }

        @Override
        public boolean hasBound(Bound bound)
        {
            return slice.hasBound(bound);
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            slice.addFunctionsTo(functions);
        }

        @Override
        public boolean isInclusive(Bound bound)
        {
            return slice.isInclusive(bound);
        }

        @Override
        public SingleRestriction doMergeWith(SingleRestriction otherRestriction)
        {
            checkTrue(otherRestriction.isSlice(),
                      "Column \"%s\" cannot be restricted by both an equality and an inequality relation",
                      getColumnsInCommons(otherRestriction));

            if (!getFirstColumn().equals(otherRestriction.getFirstColumn()))
            {
                ColumnMetadata column = getFirstColumn().position() > otherRestriction.getFirstColumn().position()
                        ? getFirstColumn() : otherRestriction.getFirstColumn();

                throw invalidRequest("Column \"%s\" cannot be restricted by two inequalities not starting with the same column",
                                     column.name);
            }

            checkFalse(hasBound(Bound.START) && otherRestriction.hasBound(Bound.START),
                       "More than one restriction was found for the start bound on %s",
                       getColumnsInCommons(otherRestriction));
            checkFalse(hasBound(Bound.END) && otherRestriction.hasBound(Bound.END),
                       "More than one restriction was found for the end bound on %s",
                       getColumnsInCommons(otherRestriction));

            SliceRestriction otherSlice = (SliceRestriction) otherRestriction;
            List<ColumnMetadata> newColumnDefs = columnDefs.size() >= otherSlice.columnDefs.size() ? columnDefs : otherSlice.columnDefs;

            int sizeHint = skippedValues.size() + otherSlice.skippedValues.size();
            List<MarkerOrList> newSkippedValues = new ArrayList<>(sizeHint);
            newSkippedValues.addAll(skippedValues);
            newSkippedValues.addAll(otherSlice.skippedValues);
            TermSlice newSlice = slice.merge(otherSlice.slice);
            return new SliceRestriction(newColumnDefs, newSlice, newSkippedValues);
        }

        @Override
        public final void addToRowFilter(RowFilter filter,
                                         IndexRegistry indexRegistry,
                                         QueryOptions options)
        {
            throw invalidRequest("Multi-column slice restrictions cannot be used for filtering.");
        }

        @Override
        public String toString()
        {
            return String.format("SLICE{%s, NOT IN %s}", slice, skippedValues);
        }

        /**
         * Similar to bounds(), but returns one ByteBuffer per-component in the bound instead of a single
         * ByteBuffer to represent the entire bound.
         * @param b the bound type
         * @param options the query options
         * @return one ByteBuffer per-component in the bound
         */
        private List<ByteBuffer> componentBounds(Bound b, QueryOptions options)
        {
            if (!slice.hasBound(b))
                return Collections.emptyList();

            List<ByteBuffer> bounds = bindTuple(slice.bound(b).bind(options), options);

            assert bounds.size() <= columnDefs.size();
            int hasNullAt = bounds.indexOf(null);
            if (hasNullAt != -1)
                throw new InvalidRequestException(String.format(
                    "Invalid null value in condition for column %s", columnDefs.get(hasNullAt).name));

            return bounds;
        }

        private static List<ByteBuffer> bindTuple(Terminal terminal, QueryOptions options)
        {
            return terminal instanceof Tuples.Value
                ? ((Tuples.Value) terminal).getElements()
                : Collections.singletonList(terminal.get(options.getProtocolVersion()));
        }

        private boolean hasComponent(Bound b, int index, EnumMap<Bound, List<ByteBuffer>> componentBounds)
        {
            return componentBounds.get(b).size() > index;
        }
    }

    public static class NotNullRestriction extends MultiColumnRestriction
    {
        public NotNullRestriction(List<ColumnMetadata> columnDefs)
        {
            super(columnDefs);
            assert columnDefs.size() == 1;
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
        }

        @Override
        public boolean isNotNull()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "IS NOT NULL";
        }

        @Override
        public SingleRestriction doMergeWith(SingleRestriction otherRestriction)
        {
            throw invalidRequest("%s cannot be restricted by a relation if it includes an IS NOT NULL clause",
                                 getColumnsInCommons(otherRestriction));
        }

        @Override
        protected boolean isSupportedBy(Index index, ColumnMetadata column)
        {
            return index.supportsExpression(column, Operator.IS_NOT);
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            throw new UnsupportedOperationException("Cannot use IS NOT NULL restriction for slicing");
        }

        @Override
        public final void addToRowFilter(RowFilter filter, IndexRegistry indexRegistry, QueryOptions options)
        {
            throw new UnsupportedOperationException("Secondary indexes do not support IS NOT NULL restrictions");
        }
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}

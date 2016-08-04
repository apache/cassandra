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
import java.util.*;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.Term.Terminal;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public abstract class MultiColumnRestriction implements SingleRestriction
{
    /**
     * The columns to which the restriction apply.
     */
    protected final List<ColumnDefinition> columnDefs;

    public MultiColumnRestriction(List<ColumnDefinition> columnDefs)
    {
        this.columnDefs = columnDefs;
    }

    @Override
    public boolean isMultiColumn()
    {
        return true;
    }

    @Override
    public ColumnDefinition getFirstColumn()
    {
        return columnDefs.get(0);
    }

    @Override
    public ColumnDefinition getLastColumn()
    {
        return columnDefs.get(columnDefs.size() - 1);
    }

    @Override
    public List<ColumnDefinition> getColumnDefs()
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
        Set<ColumnDefinition> commons = new HashSet<>(getColumnDefs());
        commons.retainAll(otherRestriction.getColumnDefs());
        StringBuilder builder = new StringBuilder();
        for (ColumnDefinition columnDefinition : commons)
        {
            if (builder.length() != 0)
                builder.append(" ,");
            builder.append(columnDefinition.name);
        }
        return builder.toString();
    }

    @Override
    public final boolean hasSupportingIndex(SecondaryIndexManager indexManager)
    {
        for (Index index : indexManager.listIndexes())
           if (isSupportedBy(index))
               return true;
        return false;
    }

    /**
     * Check if this type of restriction is supported for by the specified index.
     * @param index the secondary index
     *
     * @return <code>true</code> this type of restriction is supported by the specified index,
     * <code>false</code> otherwise.
     */
    protected abstract boolean isSupportedBy(Index index);

    public static class EQRestriction extends MultiColumnRestriction
    {
        protected final Term value;

        public EQRestriction(List<ColumnDefinition> columnDefs, Term value)
        {
            super(columnDefs);
            this.value = value;
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
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes an Equal",
                                 getColumnsInCommons(otherRestriction));
        }

        @Override
        protected boolean isSupportedBy(Index index)
        {
            for(ColumnDefinition column : columnDefs)
                if (index.supportsExpression(column, Operator.EQ))
                    return true;
            return false;
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            Tuples.Value t = ((Tuples.Value) value.bind(options));
            List<ByteBuffer> values = t.getElements();
            for (int i = 0, m = values.size(); i < m; i++)
            {
                builder.addElementToAll(values.get(i));
                checkFalse(builder.containsNull(), "Invalid null value for column %s", columnDefs.get(i).name);
            }
            return builder;
        }

        @Override
        public final void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexMananger, QueryOptions options)
        {
            Tuples.Value t = ((Tuples.Value) value.bind(options));
            List<ByteBuffer> values = t.getElements();

            for (int i = 0, m = columnDefs.size(); i < m; i++)
            {
                ColumnDefinition columnDef = columnDefs.get(i);
                filter.add(columnDef, Operator.EQ, values.get(i));
            }
        }
    }

    public abstract static class INRestriction extends MultiColumnRestriction
    {
        public INRestriction(List<ColumnDefinition> columnDefs)
        {
            super(columnDefs);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            List<List<ByteBuffer>> splitInValues = splitValues(options);
            builder.addAllElementsToAll(splitInValues);

            if (builder.containsNull())
                throw invalidRequest("Invalid null value in condition for columns: %s", ColumnDefinition.toIdentifiers(columnDefs));
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
        protected boolean isSupportedBy(Index index)
        {
            for (ColumnDefinition column: columnDefs)
                if (index.supportsExpression(column, Operator.IN))
                    return true;
            return false;
        }

        @Override
        public final void addRowFilterTo(RowFilter filter,
                                         SecondaryIndexManager indexManager,
                                         QueryOptions options)
        {
            throw  invalidRequest("IN restrictions are not supported on indexed columns");
        }

        protected abstract List<List<ByteBuffer>> splitValues(QueryOptions options);
    }

    /**
     * An IN restriction that has a set of terms for in values.
     * For example: "SELECT ... WHERE (a, b, c) IN ((1, 2, 3), (4, 5, 6))" or "WHERE (a, b, c) IN (?, ?)"
     */
    public static class InRestrictionWithValues extends INRestriction
    {
        protected final List<Term> values;

        public InRestrictionWithValues(List<ColumnDefinition> columnDefs, List<Term> values)
        {
            super(columnDefs);
            this.values = values;
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            Terms.addFunctions(values, functions);
        }

        @Override
        public String toString()
        {
            return String.format("IN(%s)", values);
        }

        @Override
        protected List<List<ByteBuffer>> splitValues(QueryOptions options)
        {
            List<List<ByteBuffer>> buffers = new ArrayList<>(values.size());
            for (Term value : values)
            {
                Term.MultiItemTerminal term = (Term.MultiItemTerminal) value.bind(options);
                buffers.add(term.getElements());
            }
            return buffers;
        }
    }

    /**
     * An IN restriction that uses a single marker for a set of IN values that are tuples.
     * For example: "SELECT ... WHERE (a, b, c) IN ?"
     */
    public static class InRestrictionWithMarker extends INRestriction
    {
        protected final AbstractMarker marker;

        public InRestrictionWithMarker(List<ColumnDefinition> columnDefs, AbstractMarker marker)
        {
            super(columnDefs);
            this.marker = marker;
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
        }

        @Override
        public String toString()
        {
            return "IN ?";
        }

        @Override
        protected List<List<ByteBuffer>> splitValues(QueryOptions options)
        {
            Tuples.InMarker inMarker = (Tuples.InMarker) marker;
            Tuples.InValue inValue = inMarker.bind(options);
            checkNotNull(inValue, "Invalid null value for IN restriction");
            return inValue.getSplitValues();
        }
    }

    public static class SliceRestriction extends MultiColumnRestriction
    {
        private final TermSlice slice;

        public SliceRestriction(List<ColumnDefinition> columnDefs, Bound bound, boolean inclusive, Term term)
        {
            this(columnDefs, TermSlice.newInstance(bound, inclusive, term));
        }

        SliceRestriction(List<ColumnDefinition> columnDefs, TermSlice slice)
        {
            super(columnDefs);
            this.slice = slice;
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
            boolean reversed = getFirstColumn().isReversedType();

            EnumMap<Bound, List<ByteBuffer>> componentBounds = new EnumMap<Bound, List<ByteBuffer>>(Bound.class);
            componentBounds.put(Bound.START, componentBounds(Bound.START, options));
            componentBounds.put(Bound.END, componentBounds(Bound.END, options));

            List<List<ByteBuffer>> toAdd = new ArrayList<>();
            List<ByteBuffer> values = new ArrayList<>();

            for (int i = 0, m = columnDefs.size(); i < m; i++)
            {
                ColumnDefinition column = columnDefs.get(i);
                Bound b = bound.reverseIfNeeded(column);

                // For mixed order columns, we need to create additional slices when 2 columns are in reverse order
                if (reversed != column.isReversedType())
                {
                    reversed = column.isReversedType();
                    // As we are switching direction we need to add the current composite
                    toAdd.add(values);

                    // The new bound side has no value for this component.  just stop
                    if (!hasComponent(b, i, componentBounds))
                        continue;

                    // The other side has still some components. We need to end the slice that we have just open.
                    if (hasComponent(b.reverse(), i, componentBounds))
                        toAdd.add(values);

                    // We need to rebuild where we are in this bound side
                    values = new ArrayList<ByteBuffer>();

                    List<ByteBuffer> vals = componentBounds.get(b);

                    int n = Math.min(i, vals.size());
                    for (int j = 0; j < n; j++)
                    {
                        ByteBuffer v = checkNotNull(vals.get(j),
                                                    "Invalid null value in condition for column %s",
                                                    columnDefs.get(j).name);
                        values.add(v);
                    }
                }

                if (!hasComponent(b, i, componentBounds))
                    continue;

                ByteBuffer v = checkNotNull(componentBounds.get(b).get(i), "Invalid null value in condition for column %s", columnDefs.get(i).name);
                values.add(v);
            }
            toAdd.add(values);

            if (bound.isEnd())
                Collections.reverse(toAdd);

            return builder.addAllElementsToAll(toAdd);
        }

        @Override
        protected boolean isSupportedBy(Index index)
        {
            for(ColumnDefinition def : columnDefs)
                if (slice.isSupportedBy(def, index))
                    return true;
            return false;
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
                ColumnDefinition column = getFirstColumn().position() > otherRestriction.getFirstColumn().position()
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
            List<ColumnDefinition> newColumnDefs = columnDefs.size() >= otherSlice.columnDefs.size() ?  columnDefs : otherSlice.columnDefs;

            return new SliceRestriction(newColumnDefs, slice.merge(otherSlice.slice));
        }

        @Override
        public final void addRowFilterTo(RowFilter filter,
                                         SecondaryIndexManager indexManager,
                                         QueryOptions options)
        {
            throw invalidRequest("Multi-column slice restrictions cannot be used for filtering.");
        }

        @Override
        public String toString()
        {
            return "SLICE" + slice;
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

            Terminal terminal = slice.bound(b).bind(options);

            if (terminal instanceof Tuples.Value)
            {
                return ((Tuples.Value) terminal).getElements();
            }

            return Collections.singletonList(terminal.get(options.getProtocolVersion()));
        }

        private boolean hasComponent(Bound b, int index, EnumMap<Bound, List<ByteBuffer>> componentBounds)
        {
            return componentBounds.get(b).size() > index;
        }
    }

    public static class NotNullRestriction extends MultiColumnRestriction
    {
        public NotNullRestriction(List<ColumnDefinition> columnDefs)
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
        protected boolean isSupportedBy(Index index)
        {
            for(ColumnDefinition column : columnDefs)
                if (index.supportsExpression(column, Operator.IS_NOT))
                    return true;
            return false;
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            throw new UnsupportedOperationException("Cannot use IS NOT NULL restriction for slicing");
        }

        @Override
        public final void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexMananger, QueryOptions options)
        {
            throw new UnsupportedOperationException("Secondary indexes do not support IS NOT NULL restrictions");
        }
    }
}

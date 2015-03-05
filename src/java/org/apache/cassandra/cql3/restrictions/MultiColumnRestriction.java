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
import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Tuples;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public abstract class MultiColumnRestriction extends AbstractRestriction
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
    public Collection<ColumnDefinition> getColumnDefs()
    {
        return columnDefs;
    }

    @Override
    public final Restriction mergeWith(Restriction otherRestriction) throws InvalidRequestException
    {
            checkTrue(otherRestriction.isMultiColumn(),
                      "Mixing single column relations and multi column relations on clustering columns is not allowed");
            return doMergeWith(otherRestriction);
    }

    protected abstract Restriction doMergeWith(Restriction otherRestriction) throws InvalidRequestException;

    /**
     * Returns the names of the columns that are specified within this <code>Restrictions</code> and the other one
     * as a comma separated <code>String</code>.
     *
     * @param otherRestrictions the other restrictions
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
        for (ColumnDefinition columnDef : columnDefs)
        {
            SecondaryIndex index = indexManager.getIndexForColumn(columnDef.name.bytes);
            if (index != null && isSupportedBy(index))
                return true;
        }
        return false;
    }

    /**
     * Check if this type of restriction is supported for by the specified index.
     * @param index the Secondary index
     *
     * @return <code>true</code> this type of restriction is supported by the specified index,
     * <code>false</code> otherwise.
     */
    protected abstract boolean isSupportedBy(SecondaryIndex index);

    public static class EQ  extends MultiColumnRestriction
    {
        protected final Term value;

        public EQ(List<ColumnDefinition> columnDefs, Term value)
        {
            super(columnDefs);
            this.value = value;
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return usesFunction(value, ksName, functionName);
        }

        @Override
        public String toString()
        {
            return String.format("EQ(%s)", value);
        }

        @Override
        public Restriction doMergeWith(Restriction otherRestriction) throws InvalidRequestException
        {
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes an Equal",
                                 getColumnsInCommons(otherRestriction));
        }

        @Override
        protected boolean isSupportedBy(SecondaryIndex index)
        {
            return index.supportsOperator(Operator.EQ);
        }

        @Override
        public CompositesBuilder appendTo(CompositesBuilder builder, QueryOptions options)
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
        public final void addIndexExpressionTo(List<IndexExpression> expressions,
                                               SecondaryIndexManager indexManager,
                                               QueryOptions options) throws InvalidRequestException
        {
            Tuples.Value t = ((Tuples.Value) value.bind(options));
            List<ByteBuffer> values = t.getElements();

            for (int i = 0, m = columnDefs.size(); i < m; i++)
            {
                ColumnDefinition columnDef = columnDefs.get(i);
                ByteBuffer component = validateIndexedValue(columnDef, values.get(i));
                expressions.add(new IndexExpression(columnDef.name.bytes, Operator.EQ, component));
            }
        }
    }

    public abstract static class IN extends MultiColumnRestriction
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public CompositesBuilder appendTo(CompositesBuilder builder, QueryOptions options)
        {
            List<List<ByteBuffer>> splitInValues = splitValues(options);
            builder.addAllElementsToAll(splitInValues);

            if (builder.containsNull())
                throw invalidRequest("Invalid null value in condition for columns: %s", ColumnDefinition.toIdentifiers(columnDefs));
            return builder;
        }

        public IN(List<ColumnDefinition> columnDefs)
        {
            super(columnDefs);
        }

        @Override
        public boolean isIN()
        {
            return true;
        }

        @Override
        public Restriction doMergeWith(Restriction otherRestriction) throws InvalidRequestException
        {
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes a IN",
                                 getColumnsInCommons(otherRestriction));
        }

        @Override
        protected boolean isSupportedBy(SecondaryIndex index)
        {
            return index.supportsOperator(Operator.IN);
        }

        @Override
        public final void addIndexExpressionTo(List<IndexExpression> expressions,
                                               SecondaryIndexManager indexManager,
                                               QueryOptions options) throws InvalidRequestException
        {
            List<List<ByteBuffer>> splitInValues = splitValues(options);
            checkTrue(splitInValues.size() == 1, "IN restrictions are not supported on indexed columns");
            List<ByteBuffer> values = splitInValues.get(0);

            for (int i = 0, m = columnDefs.size(); i < m; i++)
            {
                ColumnDefinition columnDef = columnDefs.get(i);
                ByteBuffer component = validateIndexedValue(columnDef, values.get(i));
                expressions.add(new IndexExpression(columnDef.name.bytes, Operator.EQ, component));
            }
        }

        protected abstract List<List<ByteBuffer>> splitValues(QueryOptions options) throws InvalidRequestException;
    }

    /**
     * An IN restriction that has a set of terms for in values.
     * For example: "SELECT ... WHERE (a, b, c) IN ((1, 2, 3), (4, 5, 6))" or "WHERE (a, b, c) IN (?, ?)"
     */
    public static class InWithValues extends MultiColumnRestriction.IN
    {
        protected final List<Term> values;

        public InWithValues(List<ColumnDefinition> columnDefs, List<Term> values)
        {
            super(columnDefs);
            this.values = values;
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return usesFunction(values, ksName, functionName);
        }

        @Override
        public String toString()
        {
            return String.format("IN(%s)", values);
        }

        @Override
        protected List<List<ByteBuffer>> splitValues(QueryOptions options) throws InvalidRequestException
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
    public static class InWithMarker extends MultiColumnRestriction.IN
    {
        protected final AbstractMarker marker;

        public InWithMarker(List<ColumnDefinition> columnDefs, AbstractMarker marker)
        {
            super(columnDefs);
            this.marker = marker;
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "IN ?";
        }

        @Override
        protected List<List<ByteBuffer>> splitValues(QueryOptions options) throws InvalidRequestException
        {
            Tuples.InMarker inMarker = (Tuples.InMarker) marker;
            Tuples.InValue inValue = inMarker.bind(options);
            checkNotNull(inValue, "Invalid null value for IN restriction");
            return inValue.getSplitValues();
        }
    }

    public static class Slice extends MultiColumnRestriction
    {
        private final TermSlice slice;

        public Slice(List<ColumnDefinition> columnDefs, Bound bound, boolean inclusive, Term term)
        {
            this(columnDefs, TermSlice.newInstance(bound, inclusive, term));
        }

        private Slice(List<ColumnDefinition> columnDefs, TermSlice slice)
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
        public CompositesBuilder appendTo(CompositesBuilder builder, QueryOptions options)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompositesBuilder appendBoundTo(CompositesBuilder builder, Bound bound, QueryOptions options)
        {
            List<ByteBuffer> vals = componentBounds(bound, options);

            for (int i = 0, m = vals.size(); i < m; i++)
            {
                ByteBuffer v = checkNotNull(vals.get(i), "Invalid null value in condition for column %s", columnDefs.get(i).name);
                builder.addElementToAll(v);
            }
            return builder;
        }

        @Override
        protected boolean isSupportedBy(SecondaryIndex index)
        {
            return slice.isSupportedBy(index);
        }

        @Override
        public boolean hasBound(Bound b)
        {
            return slice.hasBound(b);
        }

        @Override
        public boolean usesFunction(String ksName, String functionName)
        {
            return (slice.hasBound(Bound.START) && usesFunction(slice.bound(Bound.START), ksName, functionName))
                    || (slice.hasBound(Bound.END) && usesFunction(slice.bound(Bound.END), ksName, functionName));
        }

        @Override
        public boolean isInclusive(Bound b)
        {
            return slice.isInclusive(b);
        }

        @Override
        public Restriction doMergeWith(Restriction otherRestriction) throws InvalidRequestException
        {
            checkTrue(otherRestriction.isSlice(),
                      "Column \"%s\" cannot be restricted by both an equality and an inequality relation",
                      getColumnsInCommons(otherRestriction));

            Slice otherSlice = (Slice) otherRestriction;

            if (!getFirstColumn().equals(otherRestriction.getFirstColumn()))
            {
                ColumnDefinition column = getFirstColumn().position() > otherRestriction.getFirstColumn().position()
                        ? getFirstColumn() : otherRestriction.getFirstColumn();

                throw invalidRequest("Column \"%s\" cannot be restricted by two tuple-notation inequalities not starting with the same column",
                                     column.name);
            }

            checkFalse(hasBound(Bound.START) && otherSlice.hasBound(Bound.START),
                       "More than one restriction was found for the start bound on %s",
                       getColumnsInCommons(otherRestriction));
            checkFalse(hasBound(Bound.END) && otherSlice.hasBound(Bound.END),
                       "More than one restriction was found for the end bound on %s",
                       getColumnsInCommons(otherRestriction));

            List<ColumnDefinition> newColumnDefs = columnDefs.size() >= otherSlice.columnDefs.size() ?  columnDefs : otherSlice.columnDefs;
            return new Slice(newColumnDefs, slice.merge(otherSlice.slice));
        }

        @Override
        public final void addIndexExpressionTo(List<IndexExpression> expressions,
                                               SecondaryIndexManager indexManager,
                                               QueryOptions options) throws InvalidRequestException
        {
            throw invalidRequest("Slice restrictions are not supported on indexed columns");
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
         * @throws InvalidRequestException if the components cannot be retrieved
         */
        private List<ByteBuffer> componentBounds(Bound b, QueryOptions options) throws InvalidRequestException
        {
            Tuples.Value value = (Tuples.Value) slice.bound(b).bind(options);
            return value.getElements();
        }
    }
}

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
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.Term.Terminal;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkBindValueSet;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public abstract class SingleColumnRestriction extends AbstractRestriction
{
    /**
     * The definition of the column to which apply the restriction.
     */
    protected final ColumnDefinition columnDef;

    public SingleColumnRestriction(ColumnDefinition columnDef)
    {
        this.columnDef = columnDef;
    }

    @Override
    public List<ColumnDefinition> getColumnDefs()
    {
        return Collections.singletonList(columnDef);
    }

    @Override
    public ColumnDefinition getFirstColumn()
    {
        return columnDef;
    }

    @Override
    public ColumnDefinition getLastColumn()
    {
        return columnDef;
    }

    @Override
    public boolean hasSupportingIndex(SecondaryIndexManager indexManager)
    {
        for (Index index : indexManager.listIndexes())
            if (isSupportedBy(index))
                return true;

        return false;
    }

    @Override
    public final Restriction mergeWith(Restriction otherRestriction) throws InvalidRequestException
    {
        // We want to allow query like: b > ? AND (b,c) < (?, ?)
        if (otherRestriction.isMultiColumn() && canBeConvertedToMultiColumnRestriction())
        {
            return toMultiColumnRestriction().mergeWith(otherRestriction);
        }

        return doMergeWith(otherRestriction);
    }

    protected abstract Restriction doMergeWith(Restriction otherRestriction) throws InvalidRequestException;

    /**
     * Converts this <code>SingleColumnRestriction</code> into a {@link MultiColumnRestriction}
     *
     * @return the <code>MultiColumnRestriction</code> corresponding to this
     */
    abstract MultiColumnRestriction toMultiColumnRestriction();

    /**
     * Checks if this <code>Restriction</code> can be converted into a {@link MultiColumnRestriction}
     *
     * @return <code>true</code> if this <code>Restriction</code> can be converted into a
     * {@link MultiColumnRestriction}, <code>false</code> otherwise.
     */
    boolean canBeConvertedToMultiColumnRestriction()
    {
        return true;
    }

    /**
     * Check if this type of restriction is supported by the specified index.
     *
     * @param index the secondary index
     * @return <code>true</code> this type of restriction is supported by the specified index,
     * <code>false</code> otherwise.
     */
    protected abstract boolean isSupportedBy(Index index);

    public static final class EQRestriction extends SingleColumnRestriction
    {
        private final Term value;

        public EQRestriction(ColumnDefinition columnDef, Term value)
        {
            super(columnDef);
            this.value = value;
        }

        @Override
        public Iterable<Function> getFunctions()
        {
            return value.getFunctions();
        }

        @Override
        public boolean isEQ()
        {
            return true;
        }

        @Override
        MultiColumnRestriction toMultiColumnRestriction()
        {
            return new MultiColumnRestriction.EQRestriction(Collections.singletonList(columnDef), value);
        }

        @Override
        public void addRowFilterTo(RowFilter filter,
                                   SecondaryIndexManager indexManager,
                                   QueryOptions options)
        {
            filter.add(columnDef, Operator.EQ, value.bindAndGet(options));
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            builder.addElementToAll(value.bindAndGet(options));
            checkFalse(builder.containsNull(), "Invalid null value in condition for column %s", columnDef.name);
            checkFalse(builder.containsUnset(), "Invalid unset value for column %s", columnDef.name);
            return builder;
        }

        @Override
        public String toString()
        {
            return String.format("EQ(%s)", value);
        }

        @Override
        public Restriction doMergeWith(Restriction otherRestriction) throws InvalidRequestException
        {
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes an Equal", columnDef.name);
        }

        @Override
        protected boolean isSupportedBy(Index index)
        {
            return index.supportsExpression(columnDef, Operator.EQ);
        }
    }

    public static abstract class INRestriction extends SingleColumnRestriction
    {
        public INRestriction(ColumnDefinition columnDef)
        {
            super(columnDef);
        }

        @Override
        public final boolean isIN()
        {
            return true;
        }

        @Override
        public final Restriction doMergeWith(Restriction otherRestriction) throws InvalidRequestException
        {
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes a IN", columnDef.name);
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            builder.addEachElementToAll(getValues(options));
            checkFalse(builder.containsNull(), "Invalid null value in condition for column %s", columnDef.name);
            checkFalse(builder.containsUnset(), "Invalid unset value for column %s", columnDef.name);
            return builder;
        }

        @Override
        public void addRowFilterTo(RowFilter filter,
                                   SecondaryIndexManager indexManager,
                                   QueryOptions options) throws InvalidRequestException
        {
            List<ByteBuffer> values = getValues(options);
            checkTrue(values.size() == 1, "IN restrictions are not supported on indexed columns");

            filter.add(columnDef, Operator.EQ, values.get(0));
        }

        @Override
        protected final boolean isSupportedBy(Index index)
        {
            return index.supportsExpression(columnDef, Operator.IN);
        }

        protected abstract List<ByteBuffer> getValues(QueryOptions options) throws InvalidRequestException;
    }

    public static class InRestrictionWithValues extends INRestriction
    {
        protected final List<Term> values;

        public InRestrictionWithValues(ColumnDefinition columnDef, List<Term> values)
        {
            super(columnDef);
            this.values = values;
        }

        @Override
        MultiColumnRestriction toMultiColumnRestriction()
        {
            return new MultiColumnRestriction.InRestrictionWithValues(Collections.singletonList(columnDef), values);
        }

        @Override
        public Iterable<Function> getFunctions()
        {
            return Terms.getFunctions(values);
        }

        @Override
        protected List<ByteBuffer> getValues(QueryOptions options) throws InvalidRequestException
        {
            List<ByteBuffer> buffers = new ArrayList<>(values.size());
            for (Term value : values)
                buffers.add(value.bindAndGet(options));
            return buffers;
        }

        @Override
        public String toString()
        {
            return String.format("IN(%s)", values);
        }
    }

    public static class InRestrictionWithMarker extends INRestriction
    {
        protected final AbstractMarker marker;

        public InRestrictionWithMarker(ColumnDefinition columnDef, AbstractMarker marker)
        {
            super(columnDef);
            this.marker = marker;
        }

        @Override
        public Iterable<Function> getFunctions()
        {
            return Collections.emptySet();
        }

        @Override
        MultiColumnRestriction toMultiColumnRestriction()
        {
            return new MultiColumnRestriction.InRestrictionWithMarker(Collections.singletonList(columnDef), marker);
        }

        @Override
        protected List<ByteBuffer> getValues(QueryOptions options) throws InvalidRequestException
        {
            Terminal term = marker.bind(options);
            checkNotNull(term, "Invalid null value for column %s", columnDef.name);
            checkFalse(term == Constants.UNSET_VALUE, "Invalid unset value for column %s", columnDef.name);
            Term.MultiItemTerminal lval = (Term.MultiItemTerminal) term;
            return lval.getElements();
        }

        @Override
        public String toString()
        {
            return "IN ?";
        }
    }

    public static class SliceRestriction extends SingleColumnRestriction
    {
        private final TermSlice slice;

        public SliceRestriction(ColumnDefinition columnDef, Bound bound, boolean inclusive, Term term)
        {
            super(columnDef);
            slice = TermSlice.newInstance(bound, inclusive, term);
        }

        @Override
        public Iterable<Function> getFunctions()
        {
            return slice.getFunctions();
        }

        @Override
        MultiColumnRestriction toMultiColumnRestriction()
        {
            return new MultiColumnRestriction.SliceRestriction(Collections.singletonList(columnDef), slice);
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
        public boolean hasBound(Bound b)
        {
            return slice.hasBound(b);
        }

        @Override
        public MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options)
        {
            Bound b = reverseBoundIfNeeded(getFirstColumn(), bound);

            if (!hasBound(b))
                return builder;

            ByteBuffer value = slice.bound(b).bindAndGet(options);
            checkBindValueSet(value, "Invalid unset value for column %s", columnDef.name);
            return builder.addElementToAll(value);

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
                      columnDef.name);

            SingleColumnRestriction.SliceRestriction otherSlice = (SingleColumnRestriction.SliceRestriction) otherRestriction;

            checkFalse(hasBound(Bound.START) && otherSlice.hasBound(Bound.START),
                       "More than one restriction was found for the start bound on %s", columnDef.name);

            checkFalse(hasBound(Bound.END) && otherSlice.hasBound(Bound.END),
                       "More than one restriction was found for the end bound on %s", columnDef.name);

            return new SliceRestriction(columnDef,  slice.merge(otherSlice.slice));
        }

        @Override
        public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) throws InvalidRequestException
        {
            for (Bound b : Bound.values())
                if (hasBound(b))
                    filter.add(columnDef, slice.getIndexOperator(b), slice.bound(b).bindAndGet(options));
        }

        @Override
        protected boolean isSupportedBy(Index index)
        {
            return slice.isSupportedBy(columnDef, index);
        }

        @Override
        public String toString()
        {
            return String.format("SLICE%s", slice);
        }

        private SliceRestriction(ColumnDefinition columnDef, TermSlice slice)
        {
            super(columnDef);
            this.slice = slice;
        }
    }

    // This holds CONTAINS, CONTAINS_KEY, and map[key] = value restrictions because we might want to have any combination of them.
    public static final class ContainsRestriction extends SingleColumnRestriction
    {
        private List<Term> values = new ArrayList<>(); // for CONTAINS
        private List<Term> keys = new ArrayList<>(); // for CONTAINS_KEY
        private List<Term> entryKeys = new ArrayList<>(); // for map[key] = value
        private List<Term> entryValues = new ArrayList<>(); // for map[key] = value

        public ContainsRestriction(ColumnDefinition columnDef, Term t, boolean isKey)
        {
            super(columnDef);
            if (isKey)
                keys.add(t);
            else
                values.add(t);
        }

        public ContainsRestriction(ColumnDefinition columnDef, Term mapKey, Term mapValue)
        {
            super(columnDef);
            entryKeys.add(mapKey);
            entryValues.add(mapValue);
        }

        @Override
        MultiColumnRestriction toMultiColumnRestriction()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        boolean canBeConvertedToMultiColumnRestriction()
        {
            return false;
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isContains()
        {
            return true;
        }

        @Override
        public Restriction doMergeWith(Restriction otherRestriction) throws InvalidRequestException
        {
            checkTrue(otherRestriction.isContains(),
                      "Collection column %s can only be restricted by CONTAINS, CONTAINS KEY, or map-entry equality",
                      columnDef.name);

            SingleColumnRestriction.ContainsRestriction newContains = new ContainsRestriction(columnDef);

            copyKeysAndValues(this, newContains);
            copyKeysAndValues((ContainsRestriction) otherRestriction, newContains);

            return newContains;
        }

        @Override
        public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) throws InvalidRequestException
        {
            for (ByteBuffer value : bindAndGet(values, options))
                filter.add(columnDef, Operator.CONTAINS, value);
            for (ByteBuffer key : bindAndGet(keys, options))
                filter.add(columnDef, Operator.CONTAINS_KEY, key);

            List<ByteBuffer> eks = bindAndGet(entryKeys, options);
            List<ByteBuffer> evs = bindAndGet(entryValues, options);
            assert eks.size() == evs.size();
            for (int i = 0; i < eks.size(); i++)
                filter.addMapEquality(columnDef, eks.get(i), Operator.EQ, evs.get(i));
        }

        @Override
        protected boolean isSupportedBy(Index index)
        {
            boolean supported = false;

            if (numberOfValues() > 0)
                supported |= index.supportsExpression(columnDef, Operator.CONTAINS);

            if (numberOfKeys() > 0)
                supported |= index.supportsExpression(columnDef, Operator.CONTAINS_KEY);

            if (numberOfEntries() > 0)
                supported |= index.supportsExpression(columnDef, Operator.EQ);

            return supported;
        }

        public int numberOfValues()
        {
            return values.size();
        }

        public int numberOfKeys()
        {
            return keys.size();
        }

        public int numberOfEntries()
        {
            return entryKeys.size();
        }

        @Override
        public Iterable<Function> getFunctions()
        {
            return Iterables.concat(Terms.getFunctions(values),
                                    Terms.getFunctions(keys),
                                    Terms.getFunctions(entryKeys),
                                    Terms.getFunctions(entryValues));
        }

        @Override
        public String toString()
        {
            return String.format("CONTAINS(values=%s, keys=%s, entryKeys=%s, entryValues=%s)", values, keys, entryKeys, entryValues);
        }

        @Override
        public boolean hasBound(Bound b)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isInclusive(Bound b)
        {
            throw new UnsupportedOperationException();
        }

        /**
         * Binds the query options to the specified terms and returns the resulting values.
         *
         * @param terms the terms
         * @param options the query options
         * @return the value resulting from binding the query options to the specified terms
         * @throws InvalidRequestException if a problem occurs while binding the query options
         */
        private static List<ByteBuffer> bindAndGet(List<Term> terms, QueryOptions options) throws InvalidRequestException
        {
            List<ByteBuffer> buffers = new ArrayList<>(terms.size());
            for (Term value : terms)
                buffers.add(value.bindAndGet(options));
            return buffers;
        }

        /**
         * Copies the keys and value from the first <code>Contains</code> to the second one.
         *
         * @param from the <code>Contains</code> to copy from
         * @param to the <code>Contains</code> to copy to
         */
        private static void copyKeysAndValues(ContainsRestriction from, ContainsRestriction to)
        {
            to.values.addAll(from.values);
            to.keys.addAll(from.keys);
            to.entryKeys.addAll(from.entryKeys);
            to.entryValues.addAll(from.entryValues);
        }

        private ContainsRestriction(ColumnDefinition columnDef)
        {
            super(columnDef);
        }
    }

    public static final class IsNotNullRestriction extends SingleColumnRestriction
    {
        public IsNotNullRestriction(ColumnDefinition columnDef)
        {
            super(columnDef);
        }

        @Override
        public Iterable<Function> getFunctions()
        {
            return Collections.emptyList();
        }

        @Override
        public boolean isNotNull()
        {
            return true;
        }

        @Override
        MultiColumnRestriction toMultiColumnRestriction()
        {
            return new MultiColumnRestriction.NotNullRestriction(Collections.singletonList(columnDef));
        }

        @Override
        public void addRowFilterTo(RowFilter filter,
                                   SecondaryIndexManager indexManager,
                                   QueryOptions options)
        {
            throw new UnsupportedOperationException("Secondary indexes do not support IS NOT NULL restrictions");
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            throw new UnsupportedOperationException("Cannot use IS NOT NULL restriction for slicing");
        }

        @Override
        public String toString()
        {
            return "IS NOT NULL";
        }

        @Override
        public Restriction doMergeWith(Restriction otherRestriction) throws InvalidRequestException
        {
            throw invalidRequest("%s cannot be restricted by a relation if it includes an IS NOT NULL", columnDef.name);
        }

        @Override
        protected boolean isSupportedBy(Index index)
        {
            return index.supportsExpression(columnDef, Operator.IS_NOT);
        }
    }

    public static final class LikeRestriction extends SingleColumnRestriction
    {
        private static final ByteBuffer LIKE_WILDCARD = ByteBufferUtil.bytes("%");
        private final Operator operator;
        private final Term value;

        public LikeRestriction(ColumnDefinition columnDef, Operator operator, Term value)
        {
            super(columnDef);
            this.operator = operator;
            this.value = value;
        }

        @Override
        public Iterable<Function> getFunctions()
        {
            return value.getFunctions();
        }

        @Override
        public boolean isEQ()
        {
            return false;
        }

        @Override
        public boolean isLIKE()
        {
            return true;
        }

        @Override
        public boolean canBeConvertedToMultiColumnRestriction()
        {
            return false;
        }

        @Override
        MultiColumnRestriction toMultiColumnRestriction()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addRowFilterTo(RowFilter filter,
                                   SecondaryIndexManager indexManager,
                                   QueryOptions options)
        {
            Pair<Operator, ByteBuffer> operation = makeSpecific(value.bindAndGet(options));

            // there must be a suitable INDEX for LIKE_XXX expressions
            RowFilter.SimpleExpression expression = filter.add(columnDef, operation.left, operation.right);
            indexManager.getBestIndexFor(expression)
                        .orElseThrow(() -> new InvalidRequestException(expression.toString() +
                                                                       " is only supported on properly" +
                                                                       " indexed columns"));
        }

        @Override
        public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
        {
            // LIKE can be used with clustering columns, but as it doesn't
            // represent an actual clustering value, it can't be used in a
            // clustering filter.
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return operator.toString();
        }

        @Override
        public Restriction doMergeWith(Restriction otherRestriction) throws InvalidRequestException
        {
            throw invalidRequest("%s cannot be restricted by more than one relation if it includes a %s", columnDef.name, operator);
        }

        @Override
        protected boolean isSupportedBy(Index index)
        {
            return index.supportsExpression(columnDef, operator);
        }

        /**
         * As the specific subtype of LIKE (LIKE_PREFIX, LIKE_SUFFIX, LIKE_CONTAINS, LIKE_MATCHES) can only be
         * determined by examining the value, which in turn can only be known after binding, all LIKE restrictions
         * are initially created with the generic LIKE operator. This function takes the bound value, trims the
         * wildcard '%' chars from it and returns a tuple of the inferred operator subtype and the final value
         * @param value the bound value for the LIKE operation
         * @return  Pair containing the inferred LIKE subtype and the value with wildcards removed
         */
        private static Pair<Operator, ByteBuffer> makeSpecific(ByteBuffer value)
        {
            Operator operator;
            int beginIndex = value.position();
            int endIndex = value.limit() - 1;
            if (ByteBufferUtil.endsWith(value, LIKE_WILDCARD))
            {
                if (ByteBufferUtil.startsWith(value, LIKE_WILDCARD))
                {
                    operator = Operator.LIKE_CONTAINS;
                    beginIndex =+ 1;
                }
                else
                {
                    operator = Operator.LIKE_PREFIX;
                }
            }
            else if (ByteBufferUtil.startsWith(value, LIKE_WILDCARD))
            {
                operator = Operator.LIKE_SUFFIX;
                beginIndex += 1;
                endIndex += 1;
            }
            else
            {
                operator = Operator.LIKE_MATCHES;
                endIndex += 1;
            }

            if (endIndex == 0 || beginIndex == endIndex)
                throw new InvalidRequestException("LIKE value can't be empty.");

            ByteBuffer newValue = value.duplicate();
            newValue.position(beginIndex);
            newValue.limit(endIndex);
            return Pair.create(operator, newValue);
        }
    }
}

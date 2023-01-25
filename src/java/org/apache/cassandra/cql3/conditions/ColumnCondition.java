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
package org.apache.cassandra.cql3.conditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Suppliers;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Term.Terminal;
import org.apache.cassandra.cql3.Terms;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.util.stream.Collectors.toList;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.cassandra.cql3.Constants.UNSET_VALUE;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;
import static org.apache.cassandra.service.accord.AccordSerializers.columnMetadataSerializer;
import static org.apache.cassandra.service.accord.AccordSerializers.deserializeCqlCollectionAsTerm;
import static org.apache.cassandra.utils.ByteBufferUtil.UNSET_BYTE_BUFFER;
import static org.apache.cassandra.utils.ByteBufferUtil.byteBufferSerializer;
import static org.apache.cassandra.utils.ByteBufferUtil.nullableByteBufferSerializer;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeList;
import static org.apache.cassandra.utils.CollectionSerializers.readCollectionSize;
import static org.apache.cassandra.utils.CollectionSerializers.serializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializedListSize;

/**
 * A CQL3 condition on the value of a column or collection element.  For example, "UPDATE .. IF a = 0".
 */
public abstract class ColumnCondition
{
    public final ColumnMetadata column;
    public final Operator operator;
    private final Terms terms;

    private ColumnCondition(ColumnMetadata column, Operator op, Terms terms)
    {
        this.column = column;
        this.operator = op;
        this.terms = terms;
    }

    /**
     * Adds functions for the bind variables of this operation.
     *
     * @param functions the list of functions to get add
     */
    public void addFunctionsTo(List<Function> functions)
    {
        terms.addFunctionsTo(functions);
    }

    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param boundNames the list of column specification where to collect the
     * bind variables of this term in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        terms.collectMarkerSpecification(boundNames);
    }

    public abstract Bound bind(QueryOptions options);

    protected final List<ByteBuffer> bindAndGetTerms(QueryOptions options)
    {
        return filterUnsetValuesIfNeeded(checkValues(terms.bindAndGet(options)));
    }

    protected final List<Terminal> bindTerms(QueryOptions options)
    {
        return filterUnsetValuesIfNeeded(checkValues(terms.bind(options)));
    }

    /**
     * Checks that the output of a bind operations on {@code Terms} is a valid one.
     * @param values the list to check
     * @return the input list
     */
    private <T> List<T> checkValues(List<T> values)
    {
        checkFalse(values == null && operator.isIN(), "Invalid null list in IN condition");
        checkFalse(values == Terms.UNSET_LIST, "Invalid 'unset' value in condition");
        return values;
    }

    private <T> List<T> filterUnsetValuesIfNeeded(List<T> values)
    {
        if (!operator.isIN())
            return values;

        List<T> filtered = new ArrayList<>(values.size());
        for (int i = 0, m = values.size(); i < m; i++)
        {
            T value = values.get(i);
            // The value can be ByteBuffer or Constants.Value so we need to check the 2 type of UNSET
            if (value != UNSET_BYTE_BUFFER && value != UNSET_VALUE)
                filtered.add(value);
        }
        return filtered;
    }

    /**
     * Simple condition (e.g. <pre>IF v = 1</pre>).
     */
    private static final class SimpleColumnCondition extends ColumnCondition
    {
        public SimpleColumnCondition(ColumnMetadata column, Operator op, Terms values)
        {
            super(column, op, values);
        }

        public Bound bind(QueryOptions options)
        {
            if (column.type.isCollection() && column.type.isMultiCell())
                return new MultiCellCollectionBound(column, operator, bindTerms(options));

            if (column.type.isUDT() && column.type.isMultiCell())
                return new MultiCellUdtBound(column, operator, bindAndGetTerms(options));

            return new SimpleBound(column, operator, bindAndGetTerms(options));
        }
    }

    /**
     * A condition on a collection element (e.g. <pre>IF l[1] = 1</pre>).
     */
    private static class CollectionElementCondition extends ColumnCondition
    {
        private final Term collectionElement;

        public CollectionElementCondition(ColumnMetadata column, Term collectionElement, Operator op, Terms values)
        {
            super(column, op, values);
            this.collectionElement = collectionElement;
        }

        public void addFunctionsTo(List<Function> functions)
        {
            collectionElement.addFunctionsTo(functions);
            super.addFunctionsTo(functions);
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            collectionElement.collectMarkerSpecification(boundNames);
            super.collectMarkerSpecification(boundNames);
        }

        public Bound bind(QueryOptions options)
        {
            return new ElementAccessBound(column, collectionElement.bindAndGet(options), operator, bindAndGetTerms(options));
        }
    }

    /**
     *  A condition on a UDT field (e.g. <pre>IF v.a = 1</pre>).
     */
    private final static class UDTFieldCondition extends ColumnCondition
    {
        private final FieldIdentifier udtField;

        public UDTFieldCondition(ColumnMetadata column, FieldIdentifier udtField, Operator op, Terms values)
        {
            super(column, op, values);
            assert udtField != null;
            this.udtField = udtField;
        }

        public Bound bind(QueryOptions options)
        {
            return new UDTFieldAccessBound(column, udtField, operator, bindAndGetTerms(options));
        }
    }

    /**
     *  A regular column, simple condition.
     */
    public static ColumnCondition condition(ColumnMetadata column, Operator op, Terms terms)
    {
        return new SimpleColumnCondition(column, op, terms);
    }

    /**
     * A collection column, simple condition.
     */
    public static ColumnCondition condition(ColumnMetadata column, Term collectionElement, Operator op, Terms terms)
    {
        return new CollectionElementCondition(column, collectionElement, op, terms);
    }

    /**
     * A UDT column, simple condition.
     */
    public static ColumnCondition condition(ColumnMetadata column, FieldIdentifier udtField, Operator op, Terms terms)
    {
        return new UDTFieldCondition(column, udtField, op, terms);
    }

    enum BoundKind
    {
        ELEMENT_ACCESS(0, ElementAccessBound.serializer),
        MULTI_CELL_COLLECTION(1, MultiCellCollectionBound.serializer),
        MULTI_CELL_UDT(2, MultiCellUdtBound.serializer),
        SIMPLE(3, SimpleBound.serializer),
        UDT_FIELD_ACCESS(4, UDTFieldAccessBound.serializer);

        final int id;

        final BoundSerializer serializer;

        BoundKind(int id, BoundSerializer serializer)
        {
            this.id = id;
            this.serializer = serializer;
        }

        @SuppressWarnings("rawtypes")
        static BoundSerializer serializer(int id)
        {
            switch(id)
            {
                case 0:
                    return ElementAccessBound.serializer;
                case 1:
                    return MultiCellCollectionBound.serializer;
                case 2:
                    return MultiCellUdtBound.serializer;
                case 3:
                     return SimpleBound.serializer;
                case 4:
                    return UDTFieldAccessBound.serializer;
                default:
                    throw new AssertionError("Shouldn't have an enum with no serializer");
            }
        }
    }

    public static abstract class Bound
    {
        @Nonnull
        public final ColumnMetadata column;

        @Nonnull
        public final Operator comparisonOperator;

        protected Bound(ColumnMetadata column, Operator operator)
        {
            checkNotNull(column);
            checkNotNull(operator);
            this.column = column;
            // If the operator is an IN we want to compare the value using an EQ.
            this.comparisonOperator = operator.isIN() ? Operator.EQ : operator;
        }

        /**
         * Validates whether this condition applies to {@code current}.
         */
        public abstract boolean appliesTo(Row row);

        public ByteBuffer getCollectionElementValue()
        {
            return null;
        }

        /** Returns true if the operator is satisfied (i.e. "otherValue operator value == true"), false otherwise. */
        public static boolean compareWithOperator(Operator operator, AbstractType<?> type, ByteBuffer value, ByteBuffer otherValue)
        {
            if (value == UNSET_BYTE_BUFFER)
                throw invalidRequest("Invalid 'unset' value in condition");

            if (value == null)
            {
                switch (operator)
                {
                    case EQ:
                        return otherValue == null;
                    case NEQ:
                        return otherValue != null;
                    default:
                        throw invalidRequest("Invalid comparison with null for operator \"%s\"", operator);
                }
            }
            else if (otherValue == null)
            {
                // the condition value is not null, so only NEQ can return true
                return operator == Operator.NEQ;
            }
            return operator.isSatisfiedBy(type, otherValue, value);
        }

        void checkForUnsetValues(List<ByteBuffer> values)
        {
            for (ByteBuffer buffer : values)
                if (buffer == UNSET_BYTE_BUFFER)
                    throw invalidRequest("Invalid 'unset' value in condition");
        }
        protected abstract BoundKind kind();

        public static final IVersionedSerializer<Bound> serializer = new IVersionedSerializer<Bound>()
        {
            @Override
            @SuppressWarnings("unchecked")
            public void serialize(Bound bound, DataOutputPlus out, int version) throws IOException
            {
                columnMetadataSerializer.serialize(bound.column, out, version);
                bound.comparisonOperator.writeToUnsignedVInt(out);
                BoundKind kind = bound.kind();
                out.writeUnsignedVInt32(kind.ordinal());
                kind.serializer.serialize(bound, out, version);
            }

            @Override
            public Bound deserialize(DataInputPlus in, int version) throws IOException
            {
                ColumnMetadata column = columnMetadataSerializer.deserialize(in, version);
                Operator comparisonOperator = Operator.readFromUnsignedVInt(in);
                int boundKind = in.readUnsignedVInt32();
                return BoundKind.serializer(boundKind).deserialize(in, version, column, comparisonOperator);
            }

            @Override
            @SuppressWarnings("unchecked")
            public long serializedSize(Bound bound, int version)
            {
                BoundKind kind = bound.kind();
                return columnMetadataSerializer.serializedSize(bound.column, version)
                       + bound.comparisonOperator.sizeAsUnsignedVInt()
                       + sizeofUnsignedVInt(kind.ordinal())
                       + kind.serializer.serializedSize(bound, version);
            }
        };
    }

    private interface BoundSerializer<T extends Bound>
    {
        void serialize(T bound, DataOutputPlus out, int version) throws IOException;
        Bound deserialize(DataInputPlus in, int version, ColumnMetadata column, Operator operator) throws IOException;
        long serializedSize(T condition, int version);
    }

    protected static Cell<?> getCell(Row row, ColumnMetadata column)
    {
        // If we're asking for a given cell, and we didn't got any row from our read, it's
        // the same as not having said cell.
        return row == null ? null : row.getCell(column);
    }

    protected static Cell<?> getCell(Row row, ColumnMetadata column, CellPath path)
    {
        // If we're asking for a given cell, and we didn't got any row from our read, it's
        // the same as not having said cell.
        return row == null ? null : row.getCell(column, path);
    }

    protected static Iterator<Cell<?>> getCells(Row row, ColumnMetadata column)
    {
        // If we're asking for a complex cells, and we didn't got any row from our read, it's
        // the same as not having any cells for that column.
        if (row == null)
            return Collections.emptyIterator();

        ComplexColumnData complexData = row.getComplexColumnData(column);
        return complexData == null ? Collections.<Cell<?>>emptyIterator() : complexData.iterator();
    }

    protected static boolean evaluateComparisonWithOperator(int comparison, Operator operator)
    {
        // called when comparison != 0
        switch (operator)
        {
            case EQ:
                return false;
            case LT:
            case LTE:
                return comparison < 0;
            case GT:
            case GTE:
                return comparison > 0;
            case NEQ:
                return true;
            default:
                throw new AssertionError();
        }
    }

    /**
     * A condition on a single non-collection column.
     */
    private static final class SimpleBound extends Bound
    {
        /**
         * The condition values
         */
        private final List<ByteBuffer> values;

        private SimpleBound(ColumnMetadata column, Operator operator, List<ByteBuffer> values)
        {
            super(column, operator);
            checkForUnsetValues(values);
            this.values = values;
        }

        @Override
        public boolean appliesTo(Row row)
        {
            return isSatisfiedBy(rowValue(row));
        }

        private ByteBuffer rowValue(Row row)
        {
            Cell<?> c = getCell(row, column);
            return c == null ? null : c.buffer();
        }

        private boolean isSatisfiedBy(ByteBuffer rowValue)
        {
            for (ByteBuffer value : values)
            {
                if (compareWithOperator(comparisonOperator, column.type, value, rowValue))
                    return true;
            }
            return false;
        }

        @Override
        protected BoundKind kind()
        {
            return BoundKind.SIMPLE;
        }

        private static final BoundSerializer<SimpleBound> serializer = new BoundSerializer<SimpleBound>()
        {
            @Override
            public void serialize(SimpleBound bound, DataOutputPlus out, int version) throws IOException
            {
                serializeList(bound.values, out, version, nullableByteBufferSerializer);
            }

            @Override
            public SimpleBound deserialize(DataInputPlus in, int version, ColumnMetadata column, Operator operator) throws IOException
            {
                List<ByteBuffer> values = deserializeList(in, version, nullableByteBufferSerializer);
                return new SimpleBound(column, operator, values);
            }

            @Override
            public long serializedSize(SimpleBound bound, int version)
            {
                return serializedListSize(bound.values, version, nullableByteBufferSerializer);
            }
        };
    }

    /**
     * A condition on an element of a collection column.
     */
    public static final class ElementAccessBound extends Bound
    {
        /**
         * The collection element
         */
        @Nonnull
        private final ByteBuffer collectionElement;

        /**
         * The conditions values.
         */
        @Nonnull
        private final List<ByteBuffer> values;

        public ElementAccessBound(ColumnMetadata column,
                                   ByteBuffer collectionElement,
                                   Operator operator,
                                   List<ByteBuffer> values)
        {
            super(column, operator);
            checkForUnsetValues(values);

            if (collectionElement == null)
                throw invalidRequest("Invalid null value for %s element access",
                                     column.type instanceof MapType ? "map" : "list");

            this.collectionElement = collectionElement;
            this.values = values;
        }

        @Override
        public boolean appliesTo(Row row)
        {
            boolean isMap = column.type instanceof MapType;

            if (isMap)
            {
                MapType<?, ?> mapType = (MapType<?, ?>) column.type;
                ByteBuffer rowValue = rowMapValue(mapType, row);
                return isSatisfiedBy(mapType.getKeysType(), rowValue);
            }

            ListType<?> listType = (ListType<?>) column.type;
            ByteBuffer rowValue = rowListValue(listType, row);
            return isSatisfiedBy(listType.getElementsType(), rowValue);
        }

        private ByteBuffer rowMapValue(MapType<?, ?> type, Row row)
        {
            if (column.type.isMultiCell())
            {
                Cell<?> cell = getCell(row, column, CellPath.create(collectionElement));
                return cell == null ? null : cell.buffer();
            }

            Cell<?> cell = getCell(row, column);
            return cell == null
                    ? null
                    : type.getSerializer().getSerializedValue(cell.buffer(), collectionElement, type.getKeysType());
        }

        private ByteBuffer rowListValue(ListType<?> type, Row row)
        {
            if (column.type.isMultiCell())
                return cellValueAtIndex(getCells(row, column), getListIndex(collectionElement));

            Cell<?> cell = getCell(row, column);
            return cell == null
                    ? null
                    : type.getSerializer().getElement(cell.buffer(), getListIndex(collectionElement));
        }

        private static ByteBuffer cellValueAtIndex(Iterator<Cell<?>> iter, int index)
        {
            int adv = Iterators.advance(iter, index);
            if (adv == index && iter.hasNext())
                return iter.next().buffer();

            return null;
        }

        private boolean isSatisfiedBy(AbstractType<?> valueType, ByteBuffer rowValue)
        {
            for (ByteBuffer value : values)
            {
                if (compareWithOperator(comparisonOperator, valueType, value, rowValue))
                    return true;
            }
            return false;
        }

        @Override
        public ByteBuffer getCollectionElementValue()
        {
            return collectionElement;
        }

        private static int getListIndex(ByteBuffer collectionElement)
        {
            int idx = ByteBufferUtil.toInt(collectionElement);
            checkFalse(idx < 0, "Invalid negative list index %d", idx);
            return idx;
        }

        @Override
        protected BoundKind kind()
        {
            return BoundKind.ELEMENT_ACCESS;
        }

        private static final BoundSerializer<ElementAccessBound> serializer = new BoundSerializer<ElementAccessBound>()
        {
            @Override
            public void serialize(ElementAccessBound bound, DataOutputPlus out, int version) throws IOException
            {
                byteBufferSerializer.serialize(bound.collectionElement, out, version);
                serializeList(bound.values, out, version, nullableByteBufferSerializer);
            }

            @Override
            public ElementAccessBound deserialize(DataInputPlus in, int version, ColumnMetadata column, Operator operator) throws IOException
            {
                ByteBuffer collectionElement = byteBufferSerializer.deserialize(in, version);
                List<ByteBuffer> values = deserializeList(in, version, nullableByteBufferSerializer);
                return new ElementAccessBound(column, collectionElement, operator, values);
            }

            @Override
            public long serializedSize(ElementAccessBound bound , int version)
            {
                return byteBufferSerializer.serializedSize(bound.collectionElement, version) + serializedListSize(bound.values, version, nullableByteBufferSerializer);
            }
        };
    }

    /**
     * A condition on an entire collection column.
     */
    public static final class MultiCellCollectionBound extends Bound
    {
        @Nonnull
        private final List<Terminal> values;

        /**
         * Avoid recomputing the serialized terminals for serialize and serializedSize
         */
        @Nullable
        private Supplier<List<ByteBuffer>> serializedValues;

        public MultiCellCollectionBound(ColumnMetadata column, Operator operator, List<Terminal> values)
        {
            super(column, operator);
            checkNotNull(values);
            assert column.type.isMultiCell();
            this.values = values;
        }

        public Supplier<List<ByteBuffer>> serializedValues()
        {
            if (serializedValues != null)
                return serializedValues;

            serializedValues = Suppliers.memoize(() ->
                                                 values.stream()
                                                       .map(v -> v == null ? null : v.get(ProtocolVersion.CURRENT))
                                                       .collect(toList()));

            return serializedValues;
        }

        public boolean appliesTo(Row row)
        {
            return appliesTo(column, comparisonOperator, values, row);
        }

        public static boolean appliesTo(ColumnMetadata column, Operator operator, List<Terminal> values, Row row)
        {
            CollectionType<?> type = (CollectionType<?>) column.type;

            // copy iterator contents so that we can properly reuse them for each comparison with an IN value
            for (Terminal value : values)
            {
                Iterator<Cell<?>> iter = getCells(row, column);
                if (value == null)
                {
                    if (operator == Operator.EQ)
                    {
                        if (!iter.hasNext())
                            return true;
                        continue;
                    }

                    if (operator == Operator.NEQ)
                        return iter.hasNext();

                    throw invalidRequest("Invalid comparison with null for operator \"%s\"", operator);
                }

                if (valueAppliesTo(type, iter, value, operator))
                    return true;
            }
            return false;
        }

        private static boolean valueAppliesTo(CollectionType<?> type, Iterator<Cell<?>> iter, Terminal value, Operator operator)
        {
            if (value == null)
                return !iter.hasNext();

            if(operator.isContains() || operator.isContainsKey())
                return containsAppliesTo(type, iter, value.get(ProtocolVersion.CURRENT), operator);

            switch (type.kind)
            {
                case LIST:
                    List<ByteBuffer> valueList = ((Lists.Value) value).elements;
                    return setOrListAppliesTo(((ListType<?>)type).getElementsType(), iter, valueList.iterator(), operator, false);
                case SET:
                    SortedSet<ByteBuffer> valueSet = ((Sets.Value) value).elements;
                    return setOrListAppliesTo(((SetType<?>)type).getElementsType(), iter, valueSet.iterator(), operator, true);
                case MAP:
                    Map<ByteBuffer, ByteBuffer> valueMap = ((Maps.Value) value).map;
                    return mapAppliesTo((MapType<?, ?>)type, iter, valueMap, operator);
            }
            throw new AssertionError();
        }

        private static boolean setOrListAppliesTo(AbstractType<?> type, Iterator<Cell<?>> iter, Iterator<ByteBuffer> conditionIter, Operator operator, boolean isSet)
        {
            while(iter.hasNext())
            {
                if (!conditionIter.hasNext())
                    return (operator == Operator.GT) || (operator == Operator.GTE) || (operator == Operator.NEQ);

                // for lists we use the cell value; for sets we use the cell name
                ByteBuffer cellValue = isSet ? iter.next().path().get(0) : iter.next().buffer();
                ByteBuffer conditionValue = conditionIter.next();
                if (conditionValue == null)
                    conditionValue = ByteBufferUtil.EMPTY_BYTE_BUFFER;
                int comparison = type.compare(cellValue, conditionValue);
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);
            }

            if (conditionIter.hasNext())
                return (operator == Operator.LT) || (operator == Operator.LTE) || (operator == Operator.NEQ);

            // they're equal
            return operator == Operator.EQ || operator == Operator.LTE || operator == Operator.GTE;
        }

        private static boolean mapAppliesTo(MapType<?, ?> type, Iterator<Cell<?>> iter, Map<ByteBuffer, ByteBuffer> elements, Operator operator)
        {
            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> conditionIter = elements.entrySet().iterator();
            while(iter.hasNext())
            {
                if (!conditionIter.hasNext())
                    return (operator == Operator.GT) || (operator == Operator.GTE) || (operator == Operator.NEQ);

                Map.Entry<ByteBuffer, ByteBuffer> conditionEntry = conditionIter.next();
                Cell<?> c = iter.next();

                ByteBuffer conditionEntryKey = conditionEntry.getKey();

                // compare the keys
                int comparison = type.getKeysType().compare(c.path().get(0), conditionEntryKey);
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);

                ByteBuffer conditionEntryValue = conditionEntry.getValue();

                // compare the values
                comparison = type.getValuesType().compare(c.buffer(), conditionEntryValue);
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);
            }

            if (conditionIter.hasNext())
                return (operator == Operator.LT) || (operator == Operator.LTE) || (operator == Operator.NEQ);

            // they're equal
            return operator == Operator.EQ || operator == Operator.LTE || operator == Operator.GTE;
        }

        @Override
        protected BoundKind kind()
        {
            return BoundKind.MULTI_CELL_COLLECTION;
        }

        private static final BoundSerializer<MultiCellCollectionBound> serializer = new BoundSerializer<MultiCellCollectionBound>()
        {
            @Override
            public void serialize(MultiCellCollectionBound bound, DataOutputPlus out, int version) throws IOException
            {
                serializeList(bound.serializedValues().get(), out, version, nullableByteBufferSerializer);
            }

            @Override
            public MultiCellCollectionBound deserialize(DataInputPlus in, int version, ColumnMetadata column, Operator operator) throws IOException
            {
                int numTerminals = readCollectionSize(in, version);
                List<Terminal> terminals = new ArrayList<>(numTerminals);
                if (operator.isContains() || operator.isContainsKey())
                {
                    for (int i = 0; i < numTerminals; i++)
                        terminals.add(new Constants.Value(nullableByteBufferSerializer.deserialize(in, version)));
                }
                else
                {
                    for (int i = 0; i < numTerminals; i++)
                        terminals.add(deserializeCqlCollectionAsTerm(nullableByteBufferSerializer.deserialize(in, version), column.type));
                }
                return new MultiCellCollectionBound(column, operator, terminals);
            }

            @Override
            public long serializedSize(MultiCellCollectionBound bound, int version)
            {
                return serializedListSize(bound.serializedValues().get(), version, nullableByteBufferSerializer);
            }
        };
    }

    private static boolean containsAppliesTo(CollectionType<?> type, Iterator<Cell<?>> iter, ByteBuffer value, Operator operator)
    {
        AbstractType<?> compareType;
        switch (type.kind)
        {
            case LIST:
                compareType = ((ListType<?>)type).getElementsType();
                break;
            case SET:
                compareType = ((SetType<?>)type).getElementsType();
                break;
            case MAP:
                compareType = operator.isContainsKey() ? ((MapType<?, ?>)type).getKeysType() : ((MapType<?, ?>)type).getValuesType();
                break;
            default:
                throw new AssertionError();
        }
        boolean appliesToSetOrMapKeys = (type.kind == CollectionType.Kind.SET || type.kind == CollectionType.Kind.MAP && operator.isContainsKey());
        return containsAppliesTo(compareType, iter, value, appliesToSetOrMapKeys);
    }

    private static boolean containsAppliesTo(AbstractType<?> type, Iterator<Cell<?>> iter, ByteBuffer value, Boolean appliesToSetOrMapKeys)
    {
        while(iter.hasNext())
        {
            // for lists and map values we use the cell value; for sets and map keys we use the cell name
            ByteBuffer cellValue = appliesToSetOrMapKeys ? iter.next().path().get(0) : iter.next().buffer();
            if(type.compare(cellValue, value) == 0)
                return true;
        }
        return false;
    }

    /**
     * A condition on a UDT field
     */
    private static final class UDTFieldAccessBound extends Bound
    {
        /**
         * The UDT field.
         */
        @Nonnull
        private final FieldIdentifier field;

        /**
         * The conditions values.
         */
        @Nonnull
        private final List<ByteBuffer> values;

        private UDTFieldAccessBound(ColumnMetadata column, FieldIdentifier field, Operator operator, List<ByteBuffer> values)
        {
            super(column, operator);
            checkNotNull(field);
            checkNotNull(values);
            checkForUnsetValues(values);
            assert column.type.isUDT();
            this.field = field;
            this.values = values;
        }

        @Override
        public boolean appliesTo(Row row)
        {
            return isSatisfiedBy(rowValue(row));
        }

        private ByteBuffer rowValue(Row row)
        {
            UserType userType = (UserType) column.type;

            if (column.type.isMultiCell())
            {
                Cell<?> cell = getCell(row, column, userType.cellPathForField(field));
                return cell == null ? null : cell.buffer();
            }

            Cell<?> cell = getCell(row, column);
            return cell == null
                   ? null
                   : userType.split(ByteBufferAccessor.instance, cell.buffer())[userType.fieldPosition(field)];
        }

        private boolean isSatisfiedBy(ByteBuffer rowValue)
        {
            UserType userType = (UserType) column.type;
            int fieldPosition = userType.fieldPosition(field);
            AbstractType<?> valueType = userType.fieldType(fieldPosition);
            for (ByteBuffer value : values)
            {
                if (compareWithOperator(comparisonOperator, valueType, value, rowValue))
                    return true;
            }
            return false;
        }
        
        @Override
        public String toString()
        {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }

        @Override
        protected BoundKind kind()
        {
            return BoundKind.UDT_FIELD_ACCESS;
        }

        private static final BoundSerializer<UDTFieldAccessBound> serializer = new BoundSerializer<UDTFieldAccessBound>()
        {
            @Override
            public void serialize(UDTFieldAccessBound bound, DataOutputPlus out, int version) throws IOException
            {
                nullableByteBufferSerializer.serialize(bound.field.bytes, out, version);
                serializeList(bound.values, out, version, nullableByteBufferSerializer);
            }

            @Override
            public UDTFieldAccessBound deserialize(DataInputPlus in, int version, ColumnMetadata column, Operator operator) throws IOException
            {
                FieldIdentifier field = new FieldIdentifier(nullableByteBufferSerializer.deserialize(in, version));
                List<ByteBuffer> values = deserializeList(in, version, nullableByteBufferSerializer);
                return new UDTFieldAccessBound(column, field, operator, values);
            }

            @Override
            public long serializedSize(UDTFieldAccessBound bound, int version)
            {

                return nullableByteBufferSerializer.serializedSize(bound.field.bytes, version) + serializedListSize(bound.values, version, nullableByteBufferSerializer);
            }
        };
    }

    /**
     * A condition on an entire UDT.
     */
    public static final class MultiCellUdtBound extends Bound
    {
        /**
         * The conditions values.
         */
        @Nonnull
        private final List<ByteBuffer> values;

        public MultiCellUdtBound(ColumnMetadata column, Operator op, List<ByteBuffer> values)
        {
            super(column, op);
            checkNotNull(values);
            checkForUnsetValues(values);
            assert column.type.isMultiCell();
            this.values = values;
        }

        @Override
        public boolean appliesTo(Row row)
        {
            return isSatisfiedBy(rowValue(row));
        }

        private ByteBuffer rowValue(Row row)
        {
            UserType userType = (UserType) column.type;
            Iterator<Cell<?>> iter = getCells(row, column);
            // User type doesn't use the protocol version so passing in null
            return iter.hasNext() ? userType.serializeForNativeProtocol(iter) : null;
        }

        private boolean isSatisfiedBy(ByteBuffer rowValue)
        {
            for (ByteBuffer value : values)
            {
                if (compareWithOperator(comparisonOperator, column.type, value, rowValue))
                    return true;
            }
            return false;
        }
        
        @Override
        public String toString()
        {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }

        @Override
        public BoundKind kind()
        {
            return BoundKind.MULTI_CELL_UDT;
        }

        private static final BoundSerializer<MultiCellUdtBound> serializer = new BoundSerializer<MultiCellUdtBound>()
        {
            @Override
            public void serialize(MultiCellUdtBound bound, DataOutputPlus out, int version) throws IOException
            {
                serializeList(bound.values, out, version, nullableByteBufferSerializer);
            }

            @Override
            public MultiCellUdtBound deserialize(DataInputPlus in, int version, ColumnMetadata column, Operator operator) throws IOException
            {
                List<ByteBuffer> values = deserializeList(in, version, nullableByteBufferSerializer);
                // Does decode actually do what we want?
                return new MultiCellUdtBound(column, operator, values);
            }

            @Override
            public long serializedSize(MultiCellUdtBound bound, int version)
            {
                return serializedListSize(bound.values, version, nullableByteBufferSerializer);
            }
        };
    }

    public static class Raw
    {
        private final Term.Raw value;
        private final List<Term.Raw> inValues;
        private final AbstractMarker.INRaw inMarker;

        // Can be null, only used with the syntax "IF m[e] = ..." (in which case it's 'e')
        private final Term.Raw collectionElement;

        // Can be null, only used with the syntax "IF udt.field = ..." (in which case it's 'field')
        private final FieldIdentifier udtField;

        private final Operator operator;

        private Raw(Term.Raw value, List<Term.Raw> inValues, AbstractMarker.INRaw inMarker, Term.Raw collectionElement,
                    FieldIdentifier udtField, Operator op)
        {
            this.value = value;
            this.inValues = inValues;
            this.inMarker = inMarker;
            this.collectionElement = collectionElement;
            this.udtField = udtField;
            this.operator = op;
        }

        /** A condition on a column. For example: "IF col = 'foo'" */
        public static Raw simpleCondition(Term.Raw value, Operator op)
        {
            return new Raw(value, null, null, null, null, op);
        }

        /** An IN condition on a column. For example: "IF col IN ('foo', 'bar', ...)" */
        public static Raw simpleInCondition(List<Term.Raw> inValues)
        {
            return new Raw(null, inValues, null, null, null, Operator.IN);
        }

        /** An IN condition on a column with a single marker. For example: "IF col IN ?" */
        public static Raw simpleInCondition(AbstractMarker.INRaw inMarker)
        {
            return new Raw(null, null, inMarker, null, null, Operator.IN);
        }

        /** A condition on a collection element. For example: "IF col['key'] = 'foo'" */
        public static Raw collectionCondition(Term.Raw value, Term.Raw collectionElement, Operator op)
        {
            return new Raw(value, null, null, collectionElement, null, op);
        }

        /** An IN condition on a collection element. For example: "IF col['key'] IN ('foo', 'bar', ...)" */
        public static Raw collectionInCondition(Term.Raw collectionElement, List<Term.Raw> inValues)
        {
            return new Raw(null, inValues, null, collectionElement, null, Operator.IN);
        }

        /** An IN condition on a collection element with a single marker. For example: "IF col['key'] IN ?" */
        public static Raw collectionInCondition(Term.Raw collectionElement, AbstractMarker.INRaw inMarker)
        {
            return new Raw(null, null, inMarker, collectionElement, null, Operator.IN);
        }

        /** A condition on a UDT field. For example: "IF col.field = 'foo'" */
        public static Raw udtFieldCondition(Term.Raw value, FieldIdentifier udtField, Operator op)
        {
            return new Raw(value, null, null, null, udtField, op);
        }

        /** An IN condition on a collection element. For example: "IF col.field IN ('foo', 'bar', ...)" */
        public static Raw udtFieldInCondition(FieldIdentifier udtField, List<Term.Raw> inValues)
        {
            return new Raw(null, inValues, null, null, udtField, Operator.IN);
        }

        /** An IN condition on a collection element with a single marker. For example: "IF col.field IN ?" */
        public static Raw udtFieldInCondition(FieldIdentifier udtField, AbstractMarker.INRaw inMarker)
        {
            return new Raw(null, null, inMarker, null, udtField, Operator.IN);
        }

        public ColumnCondition prepare(String keyspace, ColumnMetadata receiver, TableMetadata cfm)
        {
            if (receiver.type instanceof CounterColumnType)
                throw invalidRequest("Conditions on counters are not supported");

            if (collectionElement != null)
            {
                if (!(receiver.type.isCollection()))
                    throw invalidRequest("Invalid element access syntax for non-collection column %s", receiver.name);

                ColumnSpecification elementSpec, valueSpec;
                switch ((((CollectionType<?>) receiver.type).kind))
                {
                    case LIST:
                        elementSpec = Lists.indexSpecOf(receiver);
                        valueSpec = Lists.valueSpecOf(receiver);
                        break;
                    case MAP:
                        elementSpec = Maps.keySpecOf(receiver);
                        valueSpec = Maps.valueSpecOf(receiver);
                        break;
                    case SET:
                        throw invalidRequest("Invalid element access syntax for set column %s", receiver.name);
                    default:
                        throw new AssertionError();
                }

                validateOperationOnDurations(valueSpec.type);
                return condition(receiver, collectionElement.prepare(keyspace, elementSpec), operator, prepareTerms(keyspace, valueSpec));
            }

            if (udtField != null)
            {
                UserType userType = (UserType) receiver.type;
                int fieldPosition = userType.fieldPosition(udtField);
                if (fieldPosition == -1)
                    throw invalidRequest("Unknown field %s for column %s", udtField, receiver.name);

                ColumnSpecification fieldReceiver = UserTypes.fieldSpecOf(receiver, fieldPosition);
                validateOperationOnDurations(fieldReceiver.type);
                return condition(receiver, udtField, operator, prepareTerms(keyspace, fieldReceiver));
            }

            validateOperationOnDurations(receiver.type);
            return condition(receiver, operator, prepareTerms(keyspace, receiver));
        }

        private Terms prepareTerms(String keyspace, ColumnSpecification receiver)
        {
            checkFalse(operator.isContainsKey() && !(receiver.type instanceof MapType), "Cannot use CONTAINS KEY on non-map column %s", receiver.name);
            checkFalse(operator.isContains() && !(receiver.type.isCollection()), "Cannot use CONTAINS on non-collection column %s", receiver.name);

            if (operator.isIN())
            {
                return inValues == null ? Terms.ofListMarker(inMarker.prepare(keyspace, receiver), receiver.type)
                                        : Terms.of(prepareTerms(keyspace, receiver, inValues));
            }

            if (operator.isContains() || operator.isContainsKey())
                receiver = ((CollectionType<?>) receiver.type).makeCollectionReceiver(receiver, operator.isContainsKey());

            return Terms.of(value.prepare(keyspace, receiver));
        }

        private static List<Term> prepareTerms(String keyspace, ColumnSpecification receiver, List<Term.Raw> raws)
        {
            List<Term> terms = new ArrayList<>(raws.size());
            for (int i = 0, m = raws.size(); i < m; i++)
            {
                Term.Raw raw = raws.get(i);
                terms.add(raw.prepare(keyspace, receiver));
            }
            return terms;
        }

        private void validateOperationOnDurations(AbstractType<?> type)
        {
            if (type.referencesDuration() && operator.isSlice())
            {
                checkFalse(type.isCollection(), "Slice conditions are not supported on collections containing durations");
                checkFalse(type.isTuple(), "Slice conditions are not supported on tuples containing durations");
                checkFalse(type.isUDT(), "Slice conditions are not supported on UDTs containing durations");
                throw invalidRequest("Slice conditions ( %s ) are not supported on durations", operator);
            }
        }

        public Term.Raw getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }
}

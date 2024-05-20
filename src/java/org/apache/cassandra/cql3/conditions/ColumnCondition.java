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

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterators;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.terms.Constants;
import org.apache.cassandra.cql3.terms.Lists;
import org.apache.cassandra.cql3.terms.Maps;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.cql3.terms.Terms;
import org.apache.cassandra.cql3.terms.UserTypes;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import static org.apache.cassandra.cql3.statements.RequestValidations.*;

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

    public abstract ColumnCondition.Bound bind(QueryOptions options);

    protected final List<ByteBuffer> bindAndGetTerms(QueryOptions options)
    {
        List<ByteBuffer> buffers = terms.bindAndGet(options);
        checkFalse(buffers == null && operator.isIN(), "Invalid null list in IN condition");
        checkFalse(buffers == Term.UNSET_LIST, "Invalid 'unset' value in condition");
        return filterUnsetValuesIfNeeded(buffers, ByteBufferUtil.UNSET_BYTE_BUFFER);
    }

    protected final Terms.Terminals bindTerms(QueryOptions options)
    {
        Terms.Terminals terminals = terms.bind(options);
        checkFalse(terminals == null, "Invalid null list in IN condition");
        checkFalse(terminals == Terms.UNSET_TERMINALS, "Invalid 'unset' value in condition");
        return Terms.Terminals.of(filterUnsetValuesIfNeeded(terminals.asList(), Constants.UNSET_VALUE));
    }

    private <T> List<T> filterUnsetValuesIfNeeded(List<T> values, T unsetValue)
    {
        if (!operator.isIN())
            return values;

        List<T> filtered = new ArrayList<>(values.size());
        for (int i = 0, m = values.size(); i < m; i++)
        {
            T value = values.get(i);
            if (value != unsetValue)
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
                return new MultiCellUdtBound(column, operator, bindAndGetTerms(options), options.getProtocolVersion());

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

    public static abstract class Bound
    {
        public final ColumnMetadata column;
        public final Operator comparisonOperator;

        protected Bound(ColumnMetadata column, Operator operator)
        {
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
        protected static boolean compareWithOperator(Operator operator, AbstractType<?> type, ByteBuffer value, ByteBuffer otherValue)
        {
            if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
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
    }

    /**
     * A condition on an element of a collection column.
     */
    private static final class ElementAccessBound extends Bound
    {
        /**
         * The collection element
         */
        private final ByteBuffer collectionElement;

        /**
         * The conditions values.
         */
        private final List<ByteBuffer> values;

        private ElementAccessBound(ColumnMetadata column,
                                   ByteBuffer collectionElement,
                                   Operator operator,
                                   List<ByteBuffer> values)
        {
            super(column, operator);

            this.collectionElement = collectionElement;
            this.values = values;
        }

        @Override
        public boolean appliesTo(Row row)
        {
            boolean isMap = column.type instanceof MapType;

            if (collectionElement == null)
                throw invalidRequest("Invalid null value for %s element access", isMap ? "map" : "list");

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
    }

    /**
     * A condition on an entire collection column.
     */
    private static final class MultiCellCollectionBound extends Bound
    {
        private final Terms.Terminals values;

        public MultiCellCollectionBound(ColumnMetadata column, Operator operator, Terms.Terminals values)
        {
            super(column, operator);
            assert column.type.isMultiCell();
            this.values = values;
        }

        public boolean appliesTo(Row row)
        {
            CollectionType<?> type = (CollectionType<?>) column.type;

            // copy iterator contents so that we can properly reuse them for each comparison with an IN value
            for (Term.Terminal value : values.asList())
            {
                Iterator<Cell<?>> iter = getCells(row, column);
                if (value == null)
                {
                    if (comparisonOperator == Operator.EQ)
                    {
                        if (!iter.hasNext())
                            return true;
                        continue;
                    }

                    if (comparisonOperator == Operator.NEQ)
                        return iter.hasNext();

                    throw invalidRequest("Invalid comparison with null for operator \"%s\"", comparisonOperator);
                }

                if (valueAppliesTo(type, iter, value, comparisonOperator))
                    return true;
            }
            return false;
        }

        private static boolean valueAppliesTo(CollectionType<?> type, Iterator<Cell<?>> iter, Term.Terminal value, Operator operator)
        {
            if (value == null)
                return !iter.hasNext();

            if(operator == Operator.CONTAINS || operator == Operator.CONTAINS_KEY)
                return containsAppliesTo(type, iter, value.get(), operator);

            switch (type.kind)
            {
                case LIST:
                    return listAppliesTo((ListType<?>)type, iter, value.getElements(), operator);
                case SET:
                    return setAppliesTo((SetType<?>)type, iter, value.getElements(), operator);
                case MAP:
                    return mapAppliesTo((MapType<?, ?>)type, iter, value.getElements(), operator);
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
                int comparison = type.compare(cellValue, conditionIter.next());
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);
            }

            if (conditionIter.hasNext())
                return (operator == Operator.LT) || (operator == Operator.LTE) || (operator == Operator.NEQ);

            // they're equal
            return operator == Operator.EQ || operator == Operator.LTE || operator == Operator.GTE;
        }

        private static boolean listAppliesTo(ListType<?> type, Iterator<Cell<?>> iter, List<ByteBuffer> elements, Operator operator)
        {
            return setOrListAppliesTo(type.getElementsType(), iter, elements.iterator(), operator, false);
        }

        private static boolean setAppliesTo(SetType<?> type, Iterator<Cell<?>> iter, List<ByteBuffer> elements, Operator operator)
        {
            // The elements are alread sorted as expected by the SetType
            return setOrListAppliesTo(type.getElementsType(), iter, elements.iterator(), operator, true);
        }

        private static boolean mapAppliesTo(MapType<?, ?> type, Iterator<Cell<?>> iter, List<ByteBuffer> elements, Operator operator)
        {
            Iterator<ByteBuffer> conditionIter = elements.iterator();
            while(iter.hasNext())
            {
                if (!conditionIter.hasNext())
                    return (operator == Operator.GT) || (operator == Operator.GTE) || (operator == Operator.NEQ);

                ByteBuffer key = conditionIter.next();
                ByteBuffer value = conditionIter.next();
                Cell<?> c = iter.next();

                // compare the keys
                int comparison = type.getKeysType().compare(c.path().get(0), key);
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);

                // compare the values
                comparison = type.getValuesType().compare(c.buffer(), value);
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);
            }

            if (conditionIter.hasNext())
                return (operator == Operator.LT) || (operator == Operator.LTE) || (operator == Operator.NEQ);

            // they're equal
            return operator == Operator.EQ || operator == Operator.LTE || operator == Operator.GTE;
        }
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
                compareType = operator == Operator.CONTAINS_KEY ? ((MapType<?, ?>)type).getKeysType() : ((MapType<?, ?>)type).getValuesType();
                break;
            default:
                throw new AssertionError();
        }
        boolean appliesToSetOrMapKeys = (type.kind == CollectionType.Kind.SET || type.kind == CollectionType.Kind.MAP && operator == Operator.CONTAINS_KEY);
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
        private final FieldIdentifier field;

        /**
         * The conditions values.
         */
        private final List<ByteBuffer> values;

        private UDTFieldAccessBound(ColumnMetadata column, FieldIdentifier field, Operator operator, List<ByteBuffer> values)
        {
            super(column, operator);
            assert column.type.isUDT() && field != null;
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
                   : userType.unpack(cell.buffer()).get(userType.fieldPosition(field));
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
    }

    /**
     * A condition on an entire UDT.
     */
    private static final class MultiCellUdtBound extends Bound
    {
        /**
         * The conditions values.
         */
        private final List<ByteBuffer> values;

        /**
         * The protocol version
         */
        private final ProtocolVersion protocolVersion;

        private MultiCellUdtBound(ColumnMetadata column, Operator op, List<ByteBuffer> values, ProtocolVersion protocolVersion)
        {
            super(column, op);
            assert column.type.isMultiCell();
            this.values = values;
            this.protocolVersion = protocolVersion;
        }

        @Override
        public boolean appliesTo(Row row)
        {
            return isSatisfiedBy(rowValue(row));
        }

        private final ByteBuffer rowValue(Row row)
        {
            UserType userType = (UserType) column.type;
            Iterator<Cell<?>> iter = getCells(row, column);
            return iter.hasNext() ? userType.serializeForNativeProtocol(iter, protocolVersion) : null;
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
    }

    public static class Raw
    {
        private final Term.Raw value;
        private final Terms.Raw inValues;

        // Can be null, only used with the syntax "IF m[e] = ..." (in which case it's 'e')
        private final Term.Raw collectionElement;

        // Can be null, only used with the syntax "IF udt.field = ..." (in which case it's 'field')
        private final FieldIdentifier udtField;

        private final Operator operator;

        private Raw(Term.Raw value, Terms.Raw inValues, Term.Raw collectionElement,
                    FieldIdentifier udtField, Operator op)
        {
            this.value = value;
            this.inValues = inValues;
            this.collectionElement = collectionElement;
            this.udtField = udtField;
            this.operator = op;
        }

        /** A condition on a column. For example: "IF col = 'foo'" */
        public static Raw simpleCondition(Term.Raw value, Operator op)
        {
            return new Raw(value, null, null, null,  op);
        }

        /** An IN condition on a column. For example: "IF col IN ('foo', 'bar', ...)" */
        public static Raw simpleInCondition(Terms.Raw inValues)
        {
            return new Raw(null, inValues, null, null, Operator.IN);
        }

        /** A condition on a collection element. For example: "IF col['key'] = 'foo'" */
        public static Raw collectionCondition(Term.Raw value, Term.Raw collectionElement, Operator op)
        {
            return new Raw(value, null, collectionElement, null, op);
        }

        /** An IN condition on a collection element. For example: "IF col['key'] IN ('foo', 'bar', ...)" */
        public static Raw collectionInCondition(Term.Raw collectionElement, Terms.Raw inValues)
        {
            return new Raw(null, inValues, collectionElement, null, Operator.IN);
        }

        /** A condition on a UDT field. For example: "IF col.field = 'foo'" */
        public static Raw udtFieldCondition(Term.Raw value, FieldIdentifier udtField, Operator op)
        {
            return new Raw(value, null, null, udtField, op);
        }

        /** An IN condition on a collection element. For example: "IF col.field IN ('foo', 'bar', ...)" */
        public static Raw udtFieldInCondition(FieldIdentifier udtField, Terms.Raw inValues)
        {
            return new Raw(null, inValues, null, udtField, Operator.IN);
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
            checkFalse(operator == Operator.CONTAINS_KEY && !(receiver.type instanceof MapType),
                       "Cannot use CONTAINS KEY on non-map column %s", receiver.name);
            checkFalse(operator == Operator.CONTAINS && !(receiver.type.isCollection()),
                       "Cannot use CONTAINS on non-collection column %s", receiver.name);

            if (operator.isIN())
            {
                return inValues.prepare(keyspace, receiver);
            }

            if (operator == Operator.CONTAINS || operator == Operator.CONTAINS_KEY)
                receiver = ((CollectionType<?>) receiver.type).makeCollectionReceiver(receiver, operator == Operator.CONTAINS_KEY);

            return Terms.of(value.prepare(keyspace, receiver));
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

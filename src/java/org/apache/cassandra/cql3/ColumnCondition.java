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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Term.Terminal;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A CQL3 condition on the value of a column or collection element.  For example, "UPDATE .. IF a = 0".
 */
public class ColumnCondition
{
    public final ColumnDefinition column;

    // For collection, when testing the equality of a specific element, null otherwise.
    private final Term collectionElement;

    // For UDT, when testing the equality of a specific field, null otherwise.
    private final FieldIdentifier field;

    private final Term value;  // a single value or a marker for a list of IN values
    private final List<Term> inValues;

    public final Operator operator;

    private ColumnCondition(ColumnDefinition column, Term collectionElement, FieldIdentifier field, Term value, List<Term> inValues, Operator op)
    {
        this.column = column;
        this.collectionElement = collectionElement;
        this.field = field;
        this.value = value;
        this.inValues = inValues;
        this.operator = op;

        assert field == null || collectionElement == null;
        if (operator != Operator.IN)
            assert this.inValues == null;
    }

    // Public for SuperColumn tables support only
    public Term value()
    {
        return value;
    }

    public static ColumnCondition condition(ColumnDefinition column, Term value, Operator op)
    {
        return new ColumnCondition(column, null, null, value, null, op);
    }

    public static ColumnCondition condition(ColumnDefinition column, Term collectionElement, Term value, Operator op)
    {
        return new ColumnCondition(column, collectionElement, null, value, null, op);
    }

    public static ColumnCondition condition(ColumnDefinition column, FieldIdentifier udtField, Term value, Operator op)
    {
        return new ColumnCondition(column, null, udtField, value, null, op);
    }

    public static ColumnCondition inCondition(ColumnDefinition column, List<Term> inValues)
    {
        return new ColumnCondition(column, null, null, null, inValues, Operator.IN);
    }

    public static ColumnCondition inCondition(ColumnDefinition column, Term collectionElement, List<Term> inValues)
    {
        return new ColumnCondition(column, collectionElement, null, null, inValues, Operator.IN);
    }

    public static ColumnCondition inCondition(ColumnDefinition column, FieldIdentifier udtField, List<Term> inValues)
    {
        return new ColumnCondition(column, null, udtField, null, inValues, Operator.IN);
    }

    public static ColumnCondition inCondition(ColumnDefinition column, Term inMarker)
    {
        return new ColumnCondition(column, null, null, inMarker, null, Operator.IN);
    }

    public static ColumnCondition inCondition(ColumnDefinition column, Term collectionElement, Term inMarker)
    {
        return new ColumnCondition(column, collectionElement, null, inMarker, null, Operator.IN);
    }

    public static ColumnCondition inCondition(ColumnDefinition column, FieldIdentifier udtField, Term inMarker)
    {
        return new ColumnCondition(column, null, udtField, inMarker, null, Operator.IN);
    }

    public void addFunctionsTo(List<Function> functions)
    {
        if (collectionElement != null)
           collectionElement.addFunctionsTo(functions);
        if (value != null)
           value.addFunctionsTo(functions);
        if (inValues != null)
            for (Term value : inValues)
                if (value != null)
                    value.addFunctionsTo(functions);
    }

    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param boundNames the list of column specification where to collect the
     * bind variables of this term in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        if (collectionElement != null)
            collectionElement.collectMarkerSpecification(boundNames);

        if ((operator == Operator.IN) && inValues != null)
        {
            for (Term value : inValues)
                value.collectMarkerSpecification(boundNames);
        }
        else
        {
            value.collectMarkerSpecification(boundNames);
        }
    }

    public ColumnCondition.Bound bind(QueryOptions options) throws InvalidRequestException
    {
        boolean isInCondition = operator == Operator.IN;
        if (column.type instanceof CollectionType)
        {
            if (collectionElement != null)
                return isInCondition ? new ElementAccessInBound(this, options) : new ElementAccessBound(this, options);
            else
                return isInCondition ? new CollectionInBound(this, options) : new CollectionBound(this, options);
        }
        else if (column.type.isUDT())
        {
            if (field != null)
                return isInCondition ? new UDTFieldAccessInBound(this, options) : new UDTFieldAccessBound(this, options);
            else
                return isInCondition ? new UDTInBound(this, options) : new UDTBound(this, options);
        }

        return isInCondition ? new SimpleInBound(this, options) : new SimpleBound(this, options);
    }

    public static abstract class Bound
    {
        public final ColumnDefinition column;
        public final Operator operator;

        protected Bound(ColumnDefinition column, Operator operator)
        {
            this.column = column;
            this.operator = operator;
        }

        /**
         * Validates whether this condition applies to {@code current}.
         */
        public abstract boolean appliesTo(Row row) throws InvalidRequestException;

        public ByteBuffer getCollectionElementValue()
        {
            return null;
        }

        protected boolean isSatisfiedByValue(ByteBuffer value, Cell c, AbstractType<?> type, Operator operator) throws InvalidRequestException
        {
            return compareWithOperator(operator, type, value, c == null ? null : c.value());
        }

        /** Returns true if the operator is satisfied (i.e. "otherValue operator value == true"), false otherwise. */
        protected boolean compareWithOperator(Operator operator, AbstractType<?> type, ByteBuffer value, ByteBuffer otherValue) throws InvalidRequestException
        {
            if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
                throw new InvalidRequestException("Invalid 'unset' value in condition");
            if (value == null)
            {
                switch (operator)
                {
                    case EQ:
                        return otherValue == null;
                    case NEQ:
                        return otherValue != null;
                    default:
                        throw new InvalidRequestException(String.format("Invalid comparison with null for operator \"%s\"", operator));
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

    private static Cell getCell(Row row, ColumnDefinition column)
    {
        // If we're asking for a given cell, and we didn't got any row from our read, it's
        // the same as not having said cell.
        return row == null ? null : row.getCell(column);
    }

    private static Cell getCell(Row row, ColumnDefinition column, CellPath path)
    {
        // If we're asking for a given cell, and we didn't got any row from our read, it's
        // the same as not having said cell.
        return row == null ? null : row.getCell(column, path);
    }

    private static Iterator<Cell> getCells(Row row, ColumnDefinition column)
    {
        // If we're asking for a complex cells, and we didn't got any row from our read, it's
        // the same as not having any cells for that column.
        if (row == null)
            return Collections.<Cell>emptyIterator();

        ComplexColumnData complexData = row.getComplexColumnData(column);
        return complexData == null ? Collections.<Cell>emptyIterator() : complexData.iterator();
    }

    private static boolean evaluateComparisonWithOperator(int comparison, Operator operator)
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

    private static ByteBuffer cellValueAtIndex(Iterator<Cell> iter, int index)
    {
        int adv = Iterators.advance(iter, index);
        if (adv == index && iter.hasNext())
            return iter.next().value();
        else
            return null;
    }

    /**
     * A condition on a single non-collection column. This does not support IN operators (see SimpleInBound).
     */
    static class SimpleBound extends Bound
    {
        public final ByteBuffer value;

        private SimpleBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert !(column.type instanceof CollectionType) && condition.field == null;
            assert condition.operator != Operator.IN;
            this.value = condition.value.bindAndGet(options);
        }

        public boolean appliesTo(Row row) throws InvalidRequestException
        {
            return isSatisfiedByValue(value, getCell(row, column), column.type, operator);
        }
    }

    /**
     * An IN condition on a single non-collection column.
     */
    static class SimpleInBound extends Bound
    {
        public final List<ByteBuffer> inValues;

        private SimpleInBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert !(column.type instanceof CollectionType) && condition.field == null;
            assert condition.operator == Operator.IN;
            if (condition.inValues == null)
            {
                Terminal terminal = condition.value.bind(options);

                if (terminal == null)
                    throw new InvalidRequestException("Invalid null list in IN condition");

                if (terminal == Constants.UNSET_VALUE)
                    throw new InvalidRequestException("Invalid 'unset' value in condition");

                this.inValues = ((Lists.Value) terminal).getElements();
            }
            else
            {
                this.inValues = new ArrayList<>(condition.inValues.size());
                for (Term value : condition.inValues)
                {
                    ByteBuffer buffer = value.bindAndGet(options);
                    if (buffer != ByteBufferUtil.UNSET_BYTE_BUFFER)
                        this.inValues.add(value.bindAndGet(options));
                }
            }
        }

        public boolean appliesTo(Row row) throws InvalidRequestException
        {
            Cell c = getCell(row, column);
            for (ByteBuffer value : inValues)
            {
                if (isSatisfiedByValue(value, c, column.type, Operator.EQ))
                    return true;
            }
            return false;
        }
    }

    /** A condition on an element of a collection column. IN operators are not supported here, see ElementAccessInBound. */
    static class ElementAccessBound extends Bound
    {
        public final ByteBuffer collectionElement;
        public final ByteBuffer value;

        private ElementAccessBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type instanceof CollectionType && condition.collectionElement != null;
            assert condition.operator != Operator.IN;
            this.collectionElement = condition.collectionElement.bindAndGet(options);
            this.value = condition.value.bindAndGet(options);
        }

        public boolean appliesTo(Row row) throws InvalidRequestException
        {
            if (collectionElement == null)
                throw new InvalidRequestException("Invalid null value for " + (column.type instanceof MapType ? "map" : "list") + " element access");

            if (column.type instanceof MapType)
            {
                MapType mapType = (MapType) column.type;
                if (column.type.isMultiCell())
                {
                    Cell cell = getCell(row, column, CellPath.create(collectionElement));
                    return isSatisfiedByValue(value, cell, ((MapType) column.type).getValuesType(), operator);
                }
                else
                {
                    Cell cell = getCell(row, column);
                    ByteBuffer mapElementValue = mapType.getSerializer().getSerializedValue(cell.value(), collectionElement, mapType.getKeysType());
                    return compareWithOperator(operator, mapType.getValuesType(), value, mapElementValue);
                }
            }

            // sets don't have element access, so it's a list
            ListType listType = (ListType) column.type;
            if (column.type.isMultiCell())
            {
                ByteBuffer columnValue = cellValueAtIndex(getCells(row, column), getListIndex(collectionElement));
                return compareWithOperator(operator, ((ListType)column.type).getElementsType(), value, columnValue);
            }
            else
            {
                Cell cell = getCell(row, column);
                ByteBuffer listElementValue = listType.getSerializer().getElement(cell.value(), getListIndex(collectionElement));
                return compareWithOperator(operator, listType.getElementsType(), value, listElementValue);
            }
        }

        static int getListIndex(ByteBuffer collectionElement) throws InvalidRequestException
        {
            int idx = ByteBufferUtil.toInt(collectionElement);
            if (idx < 0)
                throw new InvalidRequestException(String.format("Invalid negative list index %d", idx));
            return idx;
        }

        public ByteBuffer getCollectionElementValue()
        {
            return collectionElement;
        }
    }

    static class ElementAccessInBound extends Bound
    {
        public final ByteBuffer collectionElement;
        public final List<ByteBuffer> inValues;

        private ElementAccessInBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type instanceof CollectionType && condition.collectionElement != null;
            this.collectionElement = condition.collectionElement.bindAndGet(options);

            if (condition.inValues == null)
            {
                Terminal terminal = condition.value.bind(options);
                if (terminal == Constants.UNSET_VALUE)
                    throw new InvalidRequestException("Invalid 'unset' value in condition");
                this.inValues = ((Lists.Value) terminal).getElements();
            }
            else
            {
                this.inValues = new ArrayList<>(condition.inValues.size());
                for (Term value : condition.inValues)
                {
                    ByteBuffer buffer = value.bindAndGet(options);
                    // We want to ignore unset values
                    if (buffer != ByteBufferUtil.UNSET_BYTE_BUFFER)
                        this.inValues.add(buffer);
                }
            }
        }

        public boolean appliesTo(Row row) throws InvalidRequestException
        {
            if (collectionElement == null)
                throw new InvalidRequestException("Invalid null value for " + (column.type instanceof MapType ? "map" : "list") + " element access");

            ByteBuffer cellValue;
            AbstractType<?> valueType;
            if (column.type instanceof MapType)
            {
                MapType mapType = (MapType) column.type;
                valueType = mapType.getValuesType();
                if (column.type.isMultiCell())
                {
                    Cell cell = getCell(row, column, CellPath.create(collectionElement));
                    cellValue = cell == null ? null : cell.value();
                }
                else
                {
                    Cell cell = getCell(row, column);
                    cellValue = cell == null
                              ? null
                              : mapType.getSerializer().getSerializedValue(cell.value(), collectionElement, mapType.getKeysType());
                }
            }
            else // ListType
            {
                ListType listType = (ListType) column.type;
                valueType = listType.getElementsType();
                if (column.type.isMultiCell())
                {
                    cellValue = cellValueAtIndex(getCells(row, column), ElementAccessBound.getListIndex(collectionElement));
                }
                else
                {
                    Cell cell = getCell(row, column);
                    cellValue = cell == null
                              ? null
                              : listType.getSerializer().getElement(cell.value(), ElementAccessBound.getListIndex(collectionElement));
                }
            }

            for (ByteBuffer value : inValues)
            {
                if (compareWithOperator(Operator.EQ, valueType, value, cellValue))
                    return true;
            }
            return false;
        }
    }

    /** A condition on an entire collection column. IN operators are not supported here, see CollectionInBound. */
    static class CollectionBound extends Bound
    {
        private final Term.Terminal value;

        private CollectionBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type.isCollection() && condition.collectionElement == null;
            assert condition.operator != Operator.IN;
            this.value = condition.value.bind(options);
        }

        public boolean appliesTo(Row row) throws InvalidRequestException
        {
            CollectionType type = (CollectionType)column.type;

            if (type.isMultiCell())
            {
                Iterator<Cell> iter = getCells(row, column);
                if (value == null)
                {
                    if (operator == Operator.EQ)
                        return !iter.hasNext();
                    else if (operator == Operator.NEQ)
                        return iter.hasNext();
                    else
                        throw new InvalidRequestException(String.format("Invalid comparison with null for operator \"%s\"", operator));
                }

                return valueAppliesTo(type, iter, value, operator);
            }

            // frozen collections
            Cell cell = getCell(row, column);
            if (value == null)
            {
                if (operator == Operator.EQ)
                    return cell == null;
                else if (operator == Operator.NEQ)
                    return cell != null;
                else
                    throw new InvalidRequestException(String.format("Invalid comparison with null for operator \"%s\"", operator));
            }
            else if (cell == null) // cell is null but condition has a value
            {
                return false;
            }

            // make sure we use v3 serialization format for comparison
            ByteBuffer conditionValue;
            if (type.kind == CollectionType.Kind.LIST)
                conditionValue = ((Lists.Value) value).get(ProtocolVersion.V3);
            else if (type.kind == CollectionType.Kind.SET)
                conditionValue = ((Sets.Value) value).get(ProtocolVersion.V3);
            else
                conditionValue = ((Maps.Value) value).get(ProtocolVersion.V3);

            return compareWithOperator(operator, type, conditionValue, cell.value());
        }

        static boolean valueAppliesTo(CollectionType type, Iterator<Cell> iter, Term.Terminal value, Operator operator)
        {
            if (value == null)
                return !iter.hasNext();

            switch (type.kind)
            {
                case LIST:
                    List<ByteBuffer> valueList = ((Lists.Value) value).elements;
                    return listAppliesTo((ListType)type, iter, valueList, operator);
                case SET:
                    Set<ByteBuffer> valueSet = ((Sets.Value) value).elements;
                    return setAppliesTo((SetType)type, iter, valueSet, operator);
                case MAP:
                    Map<ByteBuffer, ByteBuffer> valueMap = ((Maps.Value) value).map;
                    return mapAppliesTo((MapType)type, iter, valueMap, operator);
            }
            throw new AssertionError();
        }

        private static boolean setOrListAppliesTo(AbstractType<?> type, Iterator<Cell> iter, Iterator<ByteBuffer> conditionIter, Operator operator, boolean isSet)
        {
            while(iter.hasNext())
            {
                if (!conditionIter.hasNext())
                    return (operator == Operator.GT) || (operator == Operator.GTE) || (operator == Operator.NEQ);

                // for lists we use the cell value; for sets we use the cell name
                ByteBuffer cellValue = isSet ? iter.next().path().get(0) : iter.next().value();
                int comparison = type.compare(cellValue, conditionIter.next());
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);
            }

            if (conditionIter.hasNext())
                return (operator == Operator.LT) || (operator == Operator.LTE) || (operator == Operator.NEQ);

            // they're equal
            return operator == Operator.EQ || operator == Operator.LTE || operator == Operator.GTE;
        }

        static boolean listAppliesTo(ListType type, Iterator<Cell> iter, List<ByteBuffer> elements, Operator operator)
        {
            return setOrListAppliesTo(type.getElementsType(), iter, elements.iterator(), operator, false);
        }

        static boolean setAppliesTo(SetType type, Iterator<Cell> iter, Set<ByteBuffer> elements, Operator operator)
        {
            ArrayList<ByteBuffer> sortedElements = new ArrayList<>(elements.size());
            sortedElements.addAll(elements);
            Collections.sort(sortedElements, type.getElementsType());
            return setOrListAppliesTo(type.getElementsType(), iter, sortedElements.iterator(), operator, true);
        }

        static boolean mapAppliesTo(MapType type, Iterator<Cell> iter, Map<ByteBuffer, ByteBuffer> elements, Operator operator)
        {
            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> conditionIter = elements.entrySet().iterator();
            while(iter.hasNext())
            {
                if (!conditionIter.hasNext())
                    return (operator == Operator.GT) || (operator == Operator.GTE) || (operator == Operator.NEQ);

                Map.Entry<ByteBuffer, ByteBuffer> conditionEntry = conditionIter.next();
                Cell c = iter.next();

                // compare the keys
                int comparison = type.getKeysType().compare(c.path().get(0), conditionEntry.getKey());
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);

                // compare the values
                comparison = type.getValuesType().compare(c.value(), conditionEntry.getValue());
                if (comparison != 0)
                    return evaluateComparisonWithOperator(comparison, operator);
            }

            if (conditionIter.hasNext())
                return (operator == Operator.LT) || (operator == Operator.LTE) || (operator == Operator.NEQ);

            // they're equal
            return operator == Operator.EQ || operator == Operator.LTE || operator == Operator.GTE;
        }
    }

    public static class CollectionInBound extends Bound
    {
        private final List<Term.Terminal> inValues;

        private CollectionInBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type instanceof CollectionType && condition.collectionElement == null;
            assert condition.operator == Operator.IN;
            inValues = new ArrayList<>();
            if (condition.inValues == null)
            {
                // We have a list of serialized collections that need to be deserialized for later comparisons
                CollectionType collectionType = (CollectionType) column.type;
                Lists.Marker inValuesMarker = (Lists.Marker) condition.value;
                Terminal terminal = inValuesMarker.bind(options);

                if (terminal == null)
                    throw new InvalidRequestException("Invalid null list in IN condition");

                if (terminal == Constants.UNSET_VALUE)
                    throw new InvalidRequestException("Invalid 'unset' value in condition");

                if (column.type instanceof ListType)
                {
                    ListType deserializer = ListType.getInstance(collectionType.valueComparator(), false);
                    for (ByteBuffer buffer : ((Lists.Value) terminal).elements)
                    {
                        if (buffer == ByteBufferUtil.UNSET_BYTE_BUFFER)
                            continue;

                        if (buffer == null)
                            this.inValues.add(null);
                        else
                            this.inValues.add(Lists.Value.fromSerialized(buffer, deserializer, options.getProtocolVersion()));
                    }
                }
                else if (column.type instanceof MapType)
                {
                    MapType deserializer = MapType.getInstance(collectionType.nameComparator(), collectionType.valueComparator(), false);
                    for (ByteBuffer buffer : ((Lists.Value) terminal).elements)
                    {
                        if (buffer == ByteBufferUtil.UNSET_BYTE_BUFFER)
                            continue;

                        if (buffer == null)
                            this.inValues.add(null);
                        else
                            this.inValues.add(Maps.Value.fromSerialized(buffer, deserializer, options.getProtocolVersion()));
                    }
                }
                else if (column.type instanceof SetType)
                {
                    SetType deserializer = SetType.getInstance(collectionType.valueComparator(), false);
                    for (ByteBuffer buffer : ((Lists.Value) terminal).elements)
                    {
                        if (buffer == ByteBufferUtil.UNSET_BYTE_BUFFER)
                            continue;

                        if (buffer == null)
                            this.inValues.add(null);
                        else
                            this.inValues.add(Sets.Value.fromSerialized(buffer, deserializer, options.getProtocolVersion()));
                    }
                }
            }
            else
            {
                for (Term value : condition.inValues)
                {
                    Terminal terminal = value.bind(options);
                    if (terminal != Constants.UNSET_VALUE)
                        this.inValues.add(terminal);
                }
            }
        }

        public boolean appliesTo(Row row) throws InvalidRequestException
        {
            CollectionType type = (CollectionType)column.type;
            if (type.isMultiCell())
            {
                // copy iterator contents so that we can properly reuse them for each comparison with an IN value
                for (Term.Terminal value : inValues)
                {
                    if (CollectionBound.valueAppliesTo(type, getCells(row, column), value, Operator.EQ))
                        return true;
                }
                return false;
            }
            else
            {
                Cell cell = getCell(row, column);
                for (Term.Terminal value : inValues)
                {
                    if (value == null)
                    {
                        if (cell == null)
                            return true;
                    }
                    else if (type.compare(value.get(ProtocolVersion.V3), cell.value()) == 0)
                    {
                        return true;
                    }
                }
                return false;
            }
        }
    }

    /** A condition on a UDT field. IN operators are not supported here, see UDTFieldAccessInBound. */
    static class UDTFieldAccessBound extends Bound
    {
        public final FieldIdentifier field;
        public final ByteBuffer value;

        private UDTFieldAccessBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type.isUDT() && condition.field != null;
            assert condition.operator != Operator.IN;
            this.field = condition.field;
            this.value = condition.value.bindAndGet(options);
        }

        public boolean appliesTo(Row row) throws InvalidRequestException
        {
            UserType userType = (UserType) column.type;
            int fieldPosition = userType.fieldPosition(field);
            assert fieldPosition >= 0;

            ByteBuffer cellValue;
            if (column.type.isMultiCell())
            {
                Cell cell = getCell(row, column, userType.cellPathForField(field));
                cellValue = cell == null ? null : cell.value();
            }
            else
            {
                Cell cell = getCell(row, column);
                cellValue = cell == null
                          ? null
                          : userType.split(cell.value())[fieldPosition];
            }
            return compareWithOperator(operator, userType.fieldType(fieldPosition), value, cellValue);
        }
    }

    /** An IN condition on a UDT field.  For example: IF user.name IN ('a', 'b') */
    static class UDTFieldAccessInBound extends Bound
    {
        public final FieldIdentifier field;
        public final List<ByteBuffer> inValues;

        private UDTFieldAccessInBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type.isUDT() && condition.field != null;
            this.field = condition.field;

            if (condition.inValues == null)
                this.inValues = ((Lists.Value) condition.value.bind(options)).getElements();
            else
            {
                this.inValues = new ArrayList<>(condition.inValues.size());
                for (Term value : condition.inValues)
                    this.inValues.add(value.bindAndGet(options));
            }
        }

        public boolean appliesTo(Row row) throws InvalidRequestException
        {
            UserType userType = (UserType) column.type;
            int fieldPosition = userType.fieldPosition(field);
            assert fieldPosition >= 0;

            ByteBuffer cellValue;
            if (column.type.isMultiCell())
            {
                Cell cell = getCell(row, column, userType.cellPathForField(field));
                cellValue = cell == null ? null : cell.value();
            }
            else
            {
                Cell cell = getCell(row, column);
                cellValue = cell == null ? null : userType.split(getCell(row, column).value())[fieldPosition];
            }

            AbstractType<?> valueType = userType.fieldType(fieldPosition);
            for (ByteBuffer value : inValues)
            {
                if (compareWithOperator(Operator.EQ, valueType, value, cellValue))
                    return true;
            }
            return false;
        }
    }

    /** A non-IN condition on an entire UDT.  For example: IF user = {name: 'joe', age: 42}). */
    static class UDTBound extends Bound
    {
        private final ByteBuffer value;
        private final ProtocolVersion protocolVersion;

        private UDTBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type.isUDT() && condition.field == null;
            assert condition.operator != Operator.IN;
            protocolVersion = options.getProtocolVersion();
            value = condition.value.bindAndGet(options);
        }

        public boolean appliesTo(Row row) throws InvalidRequestException
        {
            UserType userType = (UserType) column.type;
            ByteBuffer rowValue;
            if (userType.isMultiCell())
            {
                Iterator<Cell> iter = getCells(row, column);
                rowValue = iter.hasNext() ? userType.serializeForNativeProtocol(iter, protocolVersion) : null;
            }
            else
            {
                Cell cell = getCell(row, column);
                rowValue = cell == null ? null : cell.value();
            }

            if (value == null)
            {
                if (operator == Operator.EQ)
                    return rowValue == null;
                else if (operator == Operator.NEQ)
                    return rowValue != null;
                else
                    throw new InvalidRequestException(String.format("Invalid comparison with null for operator \"%s\"", operator));
            }

            return compareWithOperator(operator, userType, value, rowValue);
        }
    }

    /** An IN condition on an entire UDT.  For example: IF user IN ({name: 'joe', age: 42}, {name: 'bob', age: 23}). */
    public static class UDTInBound extends Bound
    {
        private final List<ByteBuffer> inValues;
        private final ProtocolVersion protocolVersion;

        private UDTInBound(ColumnCondition condition, QueryOptions options) throws InvalidRequestException
        {
            super(condition.column, condition.operator);
            assert column.type.isUDT() && condition.field == null;
            assert condition.operator == Operator.IN;
            protocolVersion = options.getProtocolVersion();
            inValues = new ArrayList<>();
            if (condition.inValues == null)
            {
                Lists.Marker inValuesMarker = (Lists.Marker) condition.value;
                Terminal terminal = inValuesMarker.bind(options);
                if (terminal == null)
                    throw new InvalidRequestException("Invalid null list in IN condition");

                if (terminal == Constants.UNSET_VALUE)
                    throw new InvalidRequestException("Invalid 'unset' value in condition");

                for (ByteBuffer buffer : ((Lists.Value)terminal).elements)
                    this.inValues.add(buffer);
            }
            else
            {
                for (Term value : condition.inValues)
                    this.inValues.add(value.bindAndGet(options));
            }
        }

        public boolean appliesTo(Row row) throws InvalidRequestException
        {
            UserType userType = (UserType) column.type;
            ByteBuffer rowValue;
            if (userType.isMultiCell())
            {
                Iterator<Cell> cells = getCells(row, column);
                rowValue = cells.hasNext() ? userType.serializeForNativeProtocol(cells, protocolVersion) : null;
            }
            else
            {
                Cell cell = getCell(row, column);
                rowValue = cell == null ? null : cell.value();
            }

            for (ByteBuffer value : inValues)
            {
                if (value == null || rowValue == null)
                {
                    if (value == rowValue) // both null
                        return true;
                }
                else if (userType.compare(value, rowValue) == 0)
                {
                    return true;
                }
            }
            return false;
        }
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

        public ColumnCondition prepare(String keyspace, ColumnDefinition receiver, CFMetaData cfm) throws InvalidRequestException
        {
            if (receiver.type instanceof CounterColumnType)
                throw new InvalidRequestException("Conditions on counters are not supported");

            if (collectionElement != null)
            {
                if (!(receiver.type.isCollection()))
                    throw new InvalidRequestException(String.format("Invalid element access syntax for non-collection column %s", receiver.name));

                ColumnSpecification elementSpec, valueSpec;
                switch ((((CollectionType) receiver.type).kind))
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
                        throw new InvalidRequestException(String.format("Invalid element access syntax for set column %s", receiver.name));
                    default:
                        throw new AssertionError();
                }

                if (operator == Operator.IN)
                {
                    if (inValues == null)
                        return ColumnCondition.inCondition(receiver, collectionElement.prepare(keyspace, elementSpec), inMarker.prepare(keyspace, valueSpec));
                    List<Term> terms = new ArrayList<>(inValues.size());
                    for (Term.Raw value : inValues)
                        terms.add(value.prepare(keyspace, valueSpec));
                    return ColumnCondition.inCondition(receiver, collectionElement.prepare(keyspace, elementSpec), terms);
                }
                else
                {
                    validateOperationOnDurations(valueSpec.type);
                    return ColumnCondition.condition(receiver, collectionElement.prepare(keyspace, elementSpec), value.prepare(keyspace, valueSpec), operator);
                }
            }
            else if (udtField != null)
            {
                UserType userType = (UserType) receiver.type;
                int fieldPosition = userType.fieldPosition(udtField);
                if (fieldPosition == -1)
                    throw new InvalidRequestException(String.format("Unknown field %s for column %s", udtField, receiver.name));

                ColumnSpecification fieldReceiver = UserTypes.fieldSpecOf(receiver, fieldPosition);
                if (operator == Operator.IN)
                {
                    if (inValues == null)
                        return ColumnCondition.inCondition(receiver, udtField, inMarker.prepare(keyspace, fieldReceiver));

                    List<Term> terms = new ArrayList<>(inValues.size());
                    for (Term.Raw value : inValues)
                        terms.add(value.prepare(keyspace, fieldReceiver));
                    return ColumnCondition.inCondition(receiver, udtField, terms);
                }
                else
                {
                    validateOperationOnDurations(fieldReceiver.type);
                    return ColumnCondition.condition(receiver, udtField, value.prepare(keyspace, fieldReceiver), operator);
                }
            }
            else
            {
                if (operator == Operator.IN)
                {
                    if (inValues == null)
                        return ColumnCondition.inCondition(receiver, inMarker.prepare(keyspace, receiver));
                    List<Term> terms = new ArrayList<>(inValues.size());
                    for (Term.Raw value : inValues)
                        terms.add(value.prepare(keyspace, receiver));
                    return ColumnCondition.inCondition(receiver, terms);
                }
                else
                {
                    validateOperationOnDurations(receiver.type);
                    return ColumnCondition.condition(receiver, value.prepare(keyspace, receiver), operator);
                }
            }
        }

        private void validateOperationOnDurations(AbstractType<?> type)
        {
            if (type.referencesDuration() && operator.isSlice())
            {
                checkFalse(type.isCollection(), "Slice conditions are not supported on collections containing durations");
                checkFalse(type.isTuple(), "Slice conditions are not supported on tuples containing durations");
                checkFalse(type.isUDT(), "Slice conditions are not supported on UDTs containing durations");
                throw invalidRequest("Slice conditions are not supported on durations", operator);
            }
        }
    }
}

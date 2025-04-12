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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ColumnsExpression;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.cql3.terms.Terms;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.MultiElementType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.RequestValidations.*;

/**
 * A CQL3 condition on the value of a column or collection element.  For example, "UPDATE .. IF a = 0".
 */
public final class ColumnCondition
{
    /**
     * The columns expression to which the condition applies.
     */
    public final ColumnsExpression columnsExpression;

    /**
     * The operator
     */
    public final Operator operator;

    /**
     * The values
     */
    private final Terms values;

    public ColumnCondition(ColumnsExpression columnsExpression, Operator operator, Terms values)
    {
        this.columnsExpression = columnsExpression;
        this.operator = operator;
        this.values = values;
    }

    /**
     * Adds functions for the bind variables of this operation.
     *
     * @param functions the list of functions to get add
     */
    public void addFunctionsTo(List<Function> functions)
    {
        columnsExpression.addFunctionsTo(functions);
        values.addFunctionsTo(functions);
    }

    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param boundNames the list of column specification where to collect the
     * bind variables of this term in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        columnsExpression.collectMarkerSpecification(boundNames);
        values.collectMarkerSpecification(boundNames);
    }

    public ColumnCondition.Bound bind(QueryOptions options)
    {
        switch (columnsExpression.kind())
        {
            case SINGLE_COLUMN:
                return bindSingleColumn(options);
            case ELEMENT:
                return bindElement(options);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private Bound bindSingleColumn(QueryOptions options)
    {
        ColumnMetadata column = columnsExpression.firstColumn();
        if (column.type.isMultiCell())
            return new MultiCellBound(column, operator, toValue(column.type, bindAndGetTerms(options)));

        return new SimpleBound(column, operator, toValue(column.type, bindAndGetTerms(options)));
    }

    private ColumnCondition.Bound bindElement(QueryOptions options)
    {
        ColumnMetadata column = columnsExpression.firstColumn();
        ByteBuffer keyOrIndex = columnsExpression.element(options);
        if (column.type.isCollection())
        {
            checkNotNull(keyOrIndex, "Invalid null value for %s element access", column.type instanceof MapType ? "map" : "list");
        }
        return new ElementOrFieldAccessBound(column, keyOrIndex, operator, toValue(columnsExpression.type(), bindAndGetTerms(options)));
    }

    private ByteBuffer toValue(AbstractType<?> type, List<ByteBuffer> values)
    {
        if (operator.isIN())
            return ListType.getInstance(type, false).pack(values);

        ByteBuffer value = values.get(0);
        if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
            throw invalidRequest("Invalid 'unset' value in condition");

        return value;
    }

    private List<ByteBuffer> bindAndGetTerms(QueryOptions options)
    {
        List<ByteBuffer> buffers = values.bindAndGet(options);
        checkFalse(buffers == null && operator.isIN(), "Invalid null list in IN condition");
        checkFalse(buffers == Term.UNSET_LIST, "Invalid 'unset' value in condition");
        return filterUnsetValuesIfNeeded(buffers, ByteBufferUtil.UNSET_BYTE_BUFFER);
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

    public String toCQLString()
    {
        return operator.buildCQLString(columnsExpression, values);
    }

    public static abstract class Bound
    {
        protected final ColumnMetadata column;
        protected final Operator operator;
        protected final ByteBuffer value;

        protected Bound(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            this.column = column;
            this.operator = operator;
            this.value = value;
        }

        /**
         * Validates whether this condition applies to {@code current}.
         */
        public abstract boolean appliesTo(Row row);
    }

    /**
     * A condition on a single non-collection column.
     */
    private static final class SimpleBound extends Bound
    {
        private SimpleBound(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
        }

        @Override
        public boolean appliesTo(Row row)
        {
            return operator.isSatisfiedBy(column.type, rowValue(row), value);
        }

        private ByteBuffer rowValue(Row row)
        {
            // If we're asking for a given cell, and we didn't get any row from our read, it's
            // the same as not having said cell.
            if (row == null)
                return null;

            Cell<?> c = row.getCell(column);
            return c == null ? null : c.buffer();
        }
    }

    /**
     * A condition on a collection element or a UDT field.
     */
    private static final class ElementOrFieldAccessBound extends Bound
    {
        /**
         * The collection element or UDT field type.
         */
        private final AbstractType<?> elementType;

        /**
         * The map key, list index or UDT fieldname.
         */
        private final ByteBuffer keyOrIndex;


        private ElementOrFieldAccessBound(ColumnMetadata column,
                                          ByteBuffer keyOrIndex,
                                          Operator operator,
                                          ByteBuffer value)
        {
            super(column, operator, value);
            this.elementType = ((MultiElementType<?>) column.type).elementType(keyOrIndex);
            this.keyOrIndex = keyOrIndex;
        }

        @Override
        public boolean appliesTo(Row row)
        {
            ByteBuffer element = ((MultiElementType<?>) column.type).getElement(columnData(row), keyOrIndex);
            return operator.isSatisfiedBy(elementType, element, value);
        }

        /**
         * Returns the column data for the given row.
         * @param row the row
         * @return the column data for the given row.
         */
        private ColumnData columnData(Row row)
        {
            return row == null ? null : row.getColumnData(column);
        }
    }

    /**
     * A condition on a multicell column.
     */
    private static final class MultiCellBound extends Bound
    {
        public MultiCellBound(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
            assert column.type.isMultiCell();
        }

        public boolean appliesTo(Row row)
        {
            ComplexColumnData columnData = row == null ? null : row.getComplexColumnData(column);
            return operator.isSatisfiedBy((MultiElementType<?>) column.type, columnData, value);
        }
    }

    public static class Raw
    {
        private final ColumnsExpression.Raw rawExpressions;

        private final Operator operator;

        private final Terms.Raw values;

        private Raw(ColumnsExpression.Raw columnExpressions, Operator op, Terms.Raw values)
        {
            this.rawExpressions = columnExpressions;
            this.operator = op;
            this.values = values;
        }

        /**
         * Create condition on a column. For example: "IF col = 'foo'" or "IF col IN ('foo', 'bar', ...)"
         */
        public static Raw simpleCondition(ColumnIdentifier column, Operator op, Terms.Raw values)
        {
            return new Raw(ColumnsExpression.Raw.singleColumn(column), op, values);
        }

        /**
         * Create a condition on a collection element. For example: "IF col['key'] = 'foo'"
         */
        public static Raw collectionElementCondition(ColumnIdentifier column, Term.Raw collectionElement, Operator op, Terms.Raw values)
        {
            return new Raw(ColumnsExpression.Raw.collectionElement(column, collectionElement), op, values);
        }

        /**
         * Create a condition on a UDT field. For example: "IF col.field = 'foo'"
         */
        public static Raw udtFieldCondition(ColumnIdentifier column, FieldIdentifier udtField, Operator op, Terms.Raw values)
        {
            return new Raw(ColumnsExpression.Raw.udtField(column, udtField), op, values);
        }

        public ColumnsExpression.Raw columnExpression()
        {
            return rawExpressions;
        }

        public ColumnCondition prepare(TableMetadata table)
        {
            ColumnsExpression expression = rawExpressions.prepare(table);
            ColumnSpecification receiver = expression.columnSpecification();

            checkFalse(expression.columnsKind().isPrimaryKeyKind(), "PRIMARY KEY column '%s' cannot have IF conditions", receiver.name);

            if (receiver.type instanceof CounterColumnType)
                throw invalidRequest("Conditions on counters are not supported");

            validateOperationOnDurations(receiver.type);
            return new ColumnCondition(expression, operator, prepareTerms(table.keyspace, receiver));
        }

        private Terms prepareTerms(String keyspace, ColumnSpecification receiver)
        {
            checkFalse(operator == Operator.CONTAINS_KEY && !(receiver.type instanceof MapType),
                       "Cannot use CONTAINS KEY on non-map column %s", receiver.name);
            checkFalse(operator == Operator.CONTAINS && !(receiver.type.isCollection()),
                       "Cannot use CONTAINS on non-collection column %s", receiver.name);

            if (operator == Operator.CONTAINS || operator == Operator.CONTAINS_KEY)
                receiver = ((CollectionType<?>) receiver.type).makeCollectionReceiver(receiver, operator == Operator.CONTAINS_KEY);

            return values.prepare(keyspace, receiver);
        }

        private void validateOperationOnDurations(AbstractType<?> type)
        {
            if (type.referencesDuration() && operator.isSlice() && operator != Operator.NEQ)
            {
                checkFalse(type.isCollection(), "Slice conditions are not supported on collections containing durations");
                checkFalse(type.isTuple(), "Slice conditions are not supported on tuples containing durations");
                checkFalse(type.isUDT(), "Slice conditions are not supported on UDTs containing durations");
                throw invalidRequest("Slice conditions ( %s ) are not supported on durations", operator);
            }
        }

        /**
         * Checks if this raw condition contains bind markers.
         * @return {@code true} if this raw condition contains bind markers, {@code false} otherwise.
         */
        public boolean containsBindMarkers()
        {
            return rawExpressions.containsBindMarkers() || values.containsBindMarkers();
        }

        @VisibleForTesting
        public String toCQLString()
        {
            return operator.buildCQLString(rawExpressions, values);
        }

        @Override
        public String toString()
        {
            return toCQLString();
        }
    }
}

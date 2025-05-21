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
import org.apache.cassandra.io.ParameterisedUnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.RequestValidations.*;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;
import static org.apache.cassandra.service.accord.AccordSerializers.columnMetadataSerializer;
import static org.apache.cassandra.utils.ByteBufferUtil.nullableByteBufferSerializer;

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
        TableMetadata table = columnsExpression.table();
        if (column.type.isMultiCell())
            return new MultiCellBound(column, table, operator, toValue(column.type, bindAndGetTerms(options)));

        return new SimpleBound(column, table, operator, toValue(column.type, bindAndGetTerms(options)));
    }

    private ColumnCondition.Bound bindElement(QueryOptions options)
    {
        ColumnMetadata column = columnsExpression.firstColumn();
        TableMetadata table = columnsExpression.table();
        ByteBuffer keyOrIndex = columnsExpression.element(options);
        if (column.type.isCollection())
        {
            checkNotNull(keyOrIndex, "Invalid null value for %s element access", column.type instanceof MapType ? "map" : "list");
        }
        return new ElementOrFieldAccessBound(column, table, keyOrIndex, operator, toValue(columnsExpression.type(), bindAndGetTerms(options)));
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

    public interface BoundSerializer<T extends Bound>
    {
        default void serialize(T bound, DataOutputPlus out) throws IOException {}
        Bound deserialize(DataInputPlus in, ColumnMetadata column, TableMetadata table, Operator operator, ByteBuffer value) throws IOException;
        default long serializedSize(T condition) { return 0; }
    }

    public enum BoundKind
    {
        Simple(0, SimpleBound.serializer),
        ElementOrFieldAccess(1, ElementOrFieldAccessBound.serializer),
        MultiCell(2, MultiCellBound.serializer);

        private final int id;
        @SuppressWarnings("rawtypes")
        public final BoundSerializer serializer;

        BoundKind(int id, BoundSerializer<?> serializer)
        {
            this.id = id;
            this.serializer = serializer;
        }

        public static BoundKind valueOf(int id)
        {
            switch (id)
            {
                case 0: return BoundKind.Simple;
                case 1: return BoundKind.ElementOrFieldAccess;
                case 2: return BoundKind.MultiCell;
                default: throw new IllegalArgumentException("Unknown id: " + id);
            }
        }
    }

    public static abstract class Bound
    {
        public final ColumnMetadata column;
        public final TableMetadata table;
        public final Operator operator;
        public final ByteBuffer value;

        protected Bound(ColumnMetadata column, TableMetadata table, Operator operator, ByteBuffer value)
        {
            this.column = column;
            this.table = table;
            this.operator = operator;
            this.value = value;
        }

        /**
         * Validates whether this condition applies to {@code current}.
         */
        public abstract boolean appliesTo(Row row);

        public abstract boolean isNull(Row row);

        public abstract BoundKind kind();

        public static final ParameterisedUnversionedSerializer<Bound, TableMetadatas> serializer = new ParameterisedUnversionedSerializer<>() {
            @Override
            public void serialize(Bound bound, TableMetadatas tables, DataOutputPlus out) throws IOException
            {
                tables.serialize(bound.table, out);
                columnMetadataSerializer.serialize(bound.column, bound.table, out);
                bound.operator.writeToUnsignedVInt(out);
                nullableByteBufferSerializer.serialize(bound.value, out);
                ColumnCondition.BoundKind kind = bound.kind();
                out.writeUnsignedVInt32(kind.ordinal());
                kind.serializer.serialize(bound, out);
            }

            @Override
            public Bound deserialize(TableMetadatas tables, DataInputPlus in) throws IOException
            {
                TableMetadata table = tables.deserialize(in);
                ColumnMetadata column = columnMetadataSerializer.deserialize(table, in);
                Operator operator = Operator.readFromUnsignedVInt(in);
                ByteBuffer value = nullableByteBufferSerializer.deserialize(in);
                ColumnCondition.BoundKind boundKind = ColumnCondition.BoundKind.valueOf(in.readUnsignedVInt32());
                return boundKind.serializer.deserialize(in, column, table, operator, value);
            }

            @Override
            public long serializedSize(Bound bound, TableMetadatas tables)
            {
                ColumnCondition.BoundKind kind = bound.kind();
                return tables.serializedSize(bound.table)
                       + columnMetadataSerializer.serializedSize(bound.column, bound.table)
                       + bound.operator.sizeAsUnsignedVInt()
                       + nullableByteBufferSerializer.serializedSize(bound.value)
                       + sizeofUnsignedVInt(kind.ordinal())
                       + kind.serializer.serializedSize(bound);
            }
        };
    }

    /**
     * A condition on a single non-collection column.
     */
    public static class SimpleBound extends Bound
    {
        private static final BoundSerializer<SimpleBound> serializer = (in, column, table, operator, value) -> new SimpleBound(column, table, operator, value);

        public SimpleBound(ColumnMetadata column, TableMetadata table, Operator operator, ByteBuffer value)
        {
            super(column, table, operator, value);
        }

        @Override
        public boolean appliesTo(Row row)
        {
            return operator.isSatisfiedBy(column.type, rowValue(row), value);
        }

        @Override
        public boolean isNull(Row row)
        {
            return column.type.isNull(rowValue(row));
        }

        protected ByteBuffer rowValue(Row row)
        {
            // If we're asking for a given cell, and we didn't get any row from our read, it's
            // the same as not having said cell.
            if (row == null)
                return null;

            Cell<?> c = row.getCell(column);
            return c == null ? null : c.buffer();
        }

        @Override
        public BoundKind kind()
        {
            return BoundKind.Simple;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SimpleBound bound = (SimpleBound) o;
            return column.equals(bound.column) && operator == bound.operator && Objects.equals(value, bound.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(column, operator, value);
        }
    }

    public static class SimpleClusteringBound extends SimpleBound
    {
        public SimpleClusteringBound(ColumnMetadata column, TableMetadata table, Operator operator, ByteBuffer value)
        {
            super(column, table, operator, value);
            assert column.isClusteringColumn() : String.format("Column must be a clustering column, but given %s", column);
        }

        @Override
        protected ByteBuffer rowValue(Row row)
        {
            return row == null ? null : row.clustering().bufferAt(column.position());
        }
    }

    /**
     * A condition on a collection element or a UDT field.
     */
    public static final class ElementOrFieldAccessBound extends Bound
    {
        private static final BoundSerializer<ElementOrFieldAccessBound> serializer = new BoundSerializer<>()
        {
            @Override
            public void serialize(ElementOrFieldAccessBound bound, DataOutputPlus out) throws IOException
            {
                nullableByteBufferSerializer.serialize(bound.keyOrIndex, out);
            }

            @Override
            public Bound deserialize(DataInputPlus in, ColumnMetadata column, TableMetadata table, Operator operator, ByteBuffer value) throws IOException
            {
                ByteBuffer keyOrIndex = nullableByteBufferSerializer.deserialize(in);
                return new ElementOrFieldAccessBound(column, table, keyOrIndex, operator, value);
            }

            @Override
            public long serializedSize(ElementOrFieldAccessBound condition)
            {
                return nullableByteBufferSerializer.serializedSize(condition.keyOrIndex);
            }
        };
        /**
         * The collection element or UDT field type.
         */
        private final AbstractType<?> elementType;

        /**
         * The map key, list index or UDT fieldname.
         */
        private final ByteBuffer keyOrIndex;


        public ElementOrFieldAccessBound(ColumnMetadata column,
                                         TableMetadata table,
                                         ByteBuffer keyOrIndex,
                                         Operator operator,
                                         ByteBuffer value)
        {
            super(column, table, operator, value);
            this.elementType = ((MultiElementType<?>) column.type).elementType(keyOrIndex);
            this.keyOrIndex = keyOrIndex;
        }

        @Override
        public BoundKind kind()
        {
            return BoundKind.ElementOrFieldAccess;
        }

        @Override
        public boolean appliesTo(Row row)
        {
            ByteBuffer element = elementValue(row);
            return operator.isSatisfiedBy(elementType, element, value);
        }

        private ByteBuffer elementValue(Row row)
        {
            return ((MultiElementType<?>) column.type).getElement(columnData(row), keyOrIndex);
        }

        @Override
        public boolean isNull(Row row)
        {
            return column.type.isNull(elementValue(row));
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

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ElementOrFieldAccessBound bound = (ElementOrFieldAccessBound) o;
            return column.equals(bound.column) && operator == bound.operator && Objects.equals(value, bound.value) && Objects.equals(keyOrIndex, bound.keyOrIndex);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(column, operator, value);
        }
    }

    /**
     * A condition on a multicell column.
     */
    public static final class MultiCellBound extends Bound
    {
        private static final BoundSerializer<MultiCellBound> serializer = (in, column, table, operator, value) -> new MultiCellBound(column, table, operator, value);

        public MultiCellBound(ColumnMetadata column, TableMetadata table, Operator operator, ByteBuffer value)
        {
            super(column, table, operator, value);
            assert column.type.isMultiCell() : String.format("Unexpected type: %s", column.type);
        }

        @Override
        public BoundKind kind()
        {
            return BoundKind.MultiCell;
        }

        public boolean appliesTo(Row row)
        {
            ComplexColumnData columnData = row == null ? null : row.getComplexColumnData(column);
            return operator.isSatisfiedBy((MultiElementType<?>) column.type, columnData, value);
        }

        @Override
        public boolean isNull(Row row)
        {
            return row == null || row.getComplexColumnData(column) == null;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MultiCellBound bound = (MultiCellBound) o;
            return column.equals(bound.column) && operator == bound.operator && Objects.equals(value, bound.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(column, operator, value);
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

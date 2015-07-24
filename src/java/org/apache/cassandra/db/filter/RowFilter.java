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
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Objects;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkBindValueSet;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;

/**
 * A filter on which rows a given query should include or exclude.
 * <p>
 * This corresponds to the restrictions on rows that are not handled by the query
 * {@link ClusteringIndexFilter}. Some of the expressions of this filter may
 * be handled by a 2ndary index, and the rest is simply filtered out from the
 * result set (the later can only happen if the query was using ALLOW FILTERING).
 */
public abstract class RowFilter implements Iterable<RowFilter.Expression>
{
    public static final Serializer serializer = new Serializer();
    public static final RowFilter NONE = new CQLFilter(Collections.<Expression>emptyList());

    protected final List<Expression> expressions;

    protected RowFilter(List<Expression> expressions)
    {
        this.expressions = expressions;
    }

    public static RowFilter create()
    {
        return new CQLFilter(new ArrayList<Expression>());
    }

    public static RowFilter create(int capacity)
    {
        return new CQLFilter(new ArrayList<Expression>(capacity));
    }

    public static RowFilter forThrift(int capacity)
    {
        return new ThriftFilter(new ArrayList<Expression>(capacity));
    }

    public void add(ColumnDefinition def, Operator op, ByteBuffer value)
    {
        expressions.add(new SimpleExpression(def, op, value));
    }

    public void addMapEquality(ColumnDefinition def, ByteBuffer key, Operator op, ByteBuffer value)
    {
        expressions.add(new MapEqualityExpression(def, key, op, value));
    }

    public void addThriftExpression(CFMetaData metadata, ByteBuffer name, Operator op, ByteBuffer value)
    {
        assert (this instanceof ThriftFilter);
        expressions.add(new ThriftExpression(metadata, name, op, value));
    }

    public List<Expression> getExpressions()
    {
        return expressions;
    }

    /**
     * Filters the provided iterator so that only the row satisfying the expression of this filter
     * are included in the resulting iterator.
     *
     * @param iter the iterator to filter
     * @param nowInSec the time of query in seconds.
     * @return the filtered iterator.
     */
    public abstract UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iter, int nowInSec);

    /**
     * Returns this filter but without the provided expression. This method
     * *assumes* that the filter contains the provided expression.
     */
    public RowFilter without(Expression expression)
    {
        assert expressions.contains(expression);
        if (expressions.size() == 1)
            return RowFilter.NONE;

        List<Expression> newExpressions = new ArrayList<>(expressions.size() - 1);
        for (Expression e : expressions)
            if (!e.equals(expression))
                newExpressions.add(e);

        return withNewExpressions(newExpressions);
    }

    protected abstract RowFilter withNewExpressions(List<Expression> expressions);

    public boolean isEmpty()
    {
        return expressions.isEmpty();
    }

    public Iterator<Expression> iterator()
    {
        return expressions.iterator();
    }

    private static Clustering makeCompactClustering(CFMetaData metadata, ByteBuffer name)
    {
        assert metadata.isCompactTable();
        if (metadata.isCompound())
        {
            List<ByteBuffer> values = CompositeType.splitName(name);
            return new Clustering(values.toArray(new ByteBuffer[metadata.comparator.size()]));
        }
        else
        {
            return new Clustering(name);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < expressions.size(); i++)
        {
            if (i > 0)
                sb.append(" AND ");
            sb.append(expressions.get(i));
        }
        return sb.toString();
    }

    private static class CQLFilter extends RowFilter
    {
        private CQLFilter(List<Expression> expressions)
        {
            super(expressions);
        }

        public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iter, int nowInSec)
        {
            if (expressions.isEmpty())
                return iter;

            return new AlteringUnfilteredPartitionIterator(iter)
            {
                protected Row computeNext(DecoratedKey partitionKey, Row row)
                {
                    // We filter tombstones when passing the row to isSatisfiedBy so that the method doesn't have to bother with them.
                    Row purged = row.purge(DeletionPurger.PURGE_ALL, nowInSec);
                    return purged != null && CQLFilter.this.isSatisfiedBy(partitionKey, purged) ? row : null;
                }
            };
        }

        /**
         * Returns whether the provided row (with it's partition key) satisfies
         * this row filter or not (that is, if it satisfies all of its expressions).
         */
        private boolean isSatisfiedBy(DecoratedKey partitionKey, Row row)
        {
            for (Expression e : expressions)
                if (!e.isSatisfiedBy(partitionKey, row))
                    return false;

            return true;
        }

        protected RowFilter withNewExpressions(List<Expression> expressions)
        {
            return new CQLFilter(expressions);
        }
    }

    private static class ThriftFilter extends RowFilter
    {
        private ThriftFilter(List<Expression> expressions)
        {
            super(expressions);
        }

        public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iter, final int nowInSec)
        {
            if (expressions.isEmpty())
                return iter;

            return new WrappingUnfilteredPartitionIterator(iter)
            {
                @Override
                public UnfilteredRowIterator computeNext(final UnfilteredRowIterator iter)
                {
                    // Thrift does not filter rows, it filters entire partition if any of the expression is not
                    // satisfied, which forces us to materialize the result (in theory we could materialize only
                    // what we need which might or might not be everything, but we keep it simple since in practice
                    // it's not worth that it has ever been).
                    ImmutableBTreePartition result = ImmutableBTreePartition.create(iter);

                    // The partition needs to have a row for every expression, and the expression needs to be valid.
                    for (Expression expr : expressions)
                    {
                        assert expr instanceof ThriftExpression;
                        Row row = result.getRow(makeCompactClustering(iter.metadata(), expr.column().name.bytes));
                        if (row == null || !expr.isSatisfiedBy(iter.partitionKey(), row))
                            return null;
                    }
                    // If we get there, it means all expressions where satisfied, so return the original result
                    return result.unfilteredIterator();
                }
            };
        }

        protected RowFilter withNewExpressions(List<Expression> expressions)
        {
            return new ThriftFilter(expressions);
        }
    }

    public static abstract class Expression
    {
        private static final Serializer serializer = new Serializer();

        // Note: the order of this enum matter, it's used for serialization
        protected enum Kind { SIMPLE, MAP_EQUALITY, THRIFT_DYN_EXPR }

        abstract Kind kind();
        protected final ColumnDefinition column;
        protected final Operator operator;
        protected final ByteBuffer value;

        protected Expression(ColumnDefinition column, Operator operator, ByteBuffer value)
        {
            this.column = column;
            this.operator = operator;
            this.value = value;
        }

        public ColumnDefinition column()
        {
            return column;
        }

        public Operator operator()
        {
            return operator;
        }

        /**
         * Checks if the operator of this <code>IndexExpression</code> is a <code>CONTAINS</code> operator.
         *
         * @return <code>true</code> if the operator of this <code>IndexExpression</code> is a <code>CONTAINS</code>
         * operator, <code>false</code> otherwise.
         */
        public boolean isContains()
        {
            return Operator.CONTAINS == operator;
        }

        /**
         * Checks if the operator of this <code>IndexExpression</code> is a <code>CONTAINS_KEY</code> operator.
         *
         * @return <code>true</code> if the operator of this <code>IndexExpression</code> is a <code>CONTAINS_KEY</code>
         * operator, <code>false</code> otherwise.
         */
        public boolean isContainsKey()
        {
            return Operator.CONTAINS_KEY == operator;
        }

        /**
         * If this expression is used to query an index, the value to use as
         * partition key for that index query.
         */
        public ByteBuffer getIndexValue()
        {
            return value;
        }

        public void validateForIndexing() throws InvalidRequestException
        {
            checkNotNull(value, "Unsupported null value for indexed column %s", column.name);
            checkBindValueSet(value, "Unsupported unset value for indexed column %s", column.name);
            checkFalse(value.remaining() > FBUtilities.MAX_UNSIGNED_SHORT, "Index expression values may not be larger than 64K");
        }

        /**
         * Returns whether the provided row satisfied this expression or not.
         *
         * @param partitionKey the partition key for row to check.
         * @param row the row to check. It should *not* contain deleted cells
         * (i.e. it should come from a RowIterator).
         * @return whether the row is satisfied by this expression.
         */
        public abstract boolean isSatisfiedBy(DecoratedKey partitionKey, Row row);

        protected ByteBuffer getValue(DecoratedKey partitionKey, Row row)
        {
            switch (column.kind)
            {
                case PARTITION_KEY:
                    return column.isOnAllComponents()
                         ? partitionKey.getKey()
                         : CompositeType.extractComponent(partitionKey.getKey(), column.position());
                case CLUSTERING:
                    return row.clustering().get(column.position());
                default:
                    Cell cell = row.getCell(column);
                    return cell == null ? null : cell.value();
            }
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof Expression))
                return false;

            Expression that = (Expression)o;

            return Objects.equal(this.kind(), that.kind())
                && Objects.equal(this.column.name, that.column.name)
                && Objects.equal(this.operator, that.operator)
                && Objects.equal(this.value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column.name, operator, value);
        }

        private static class Serializer
        {
            public void serialize(Expression expression, DataOutputPlus out, int version) throws IOException
            {
                ByteBufferUtil.writeWithShortLength(expression.column.name.bytes, out);
                expression.operator.writeTo(out);

                if (version >= MessagingService.VERSION_30)
                    out.writeByte(expression.kind().ordinal());

                switch (expression.kind())
                {
                    case SIMPLE:
                        ByteBufferUtil.writeWithShortLength(((SimpleExpression)expression).value, out);
                        break;
                    case MAP_EQUALITY:
                        MapEqualityExpression mexpr = (MapEqualityExpression)expression;
                        if (version < MessagingService.VERSION_30)
                        {
                            ByteBufferUtil.writeWithShortLength(mexpr.getIndexValue(), out);
                        }
                        else
                        {
                            ByteBufferUtil.writeWithShortLength(mexpr.key, out);
                            ByteBufferUtil.writeWithShortLength(mexpr.value, out);
                        }
                        break;
                    case THRIFT_DYN_EXPR:
                        ByteBufferUtil.writeWithShortLength(((ThriftExpression)expression).value, out);
                        break;
                }
            }

            public Expression deserialize(DataInputPlus in, int version, CFMetaData metadata) throws IOException
            {
                ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
                Operator operator = Operator.readFrom(in);

                ColumnDefinition column = metadata.getColumnDefinition(name);
                if (!metadata.isCompactTable() && column == null)
                    throw new RuntimeException("Unknown (or dropped) column " + UTF8Type.instance.getString(name) + " during deserialization");

                Kind kind;
                if (version >= MessagingService.VERSION_30)
                {
                    kind = Kind.values()[in.readByte()];
                }
                else
                {
                    if (column == null)
                        kind = Kind.THRIFT_DYN_EXPR;
                    else if (column.type instanceof MapType && operator == Operator.EQ)
                        kind = Kind.MAP_EQUALITY;
                    else
                        kind = Kind.SIMPLE;
                }

                switch (kind)
                {
                    case SIMPLE:
                        return new SimpleExpression(column, operator, ByteBufferUtil.readWithShortLength(in));
                    case MAP_EQUALITY:
                        ByteBuffer key, value;
                        if (version < MessagingService.VERSION_30)
                        {
                            ByteBuffer composite = ByteBufferUtil.readWithShortLength(in);
                            key = CompositeType.extractComponent(composite, 0);
                            value = CompositeType.extractComponent(composite, 0);
                        }
                        else
                        {
                            key = ByteBufferUtil.readWithShortLength(in);
                            value = ByteBufferUtil.readWithShortLength(in);
                        }
                        return new MapEqualityExpression(column, key, operator, value);
                    case THRIFT_DYN_EXPR:
                        return new ThriftExpression(metadata, name, operator, ByteBufferUtil.readWithShortLength(in));
                }
                throw new AssertionError();
            }

            public long serializedSize(Expression expression, int version)
            {
                long size = ByteBufferUtil.serializedSizeWithShortLength(expression.column().name.bytes)
                          + expression.operator.serializedSize();

                switch (expression.kind())
                {
                    case SIMPLE:
                        size += ByteBufferUtil.serializedSizeWithShortLength(((SimpleExpression)expression).value);
                        break;
                    case MAP_EQUALITY:
                        MapEqualityExpression mexpr = (MapEqualityExpression)expression;
                        if (version < MessagingService.VERSION_30)
                            size += ByteBufferUtil.serializedSizeWithShortLength(mexpr.getIndexValue());
                        else
                            size += ByteBufferUtil.serializedSizeWithShortLength(mexpr.key)
                                  + ByteBufferUtil.serializedSizeWithShortLength(mexpr.value);
                        break;
                    case THRIFT_DYN_EXPR:
                        size += ByteBufferUtil.serializedSizeWithShortLength(((ThriftExpression)expression).value);
                        break;
                }
                return size;
            }
        }
    }

    /**
     * An expression of the form 'column' 'op' 'value'.
     */
    private static class SimpleExpression extends Expression
    {
        public SimpleExpression(ColumnDefinition column, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
        }

        public boolean isSatisfiedBy(DecoratedKey partitionKey, Row row)
        {
            // We support null conditions for LWT (in ColumnCondition) but not for RowFilter.
            // TODO: we should try to merge both code someday.
            assert value != null;

            if (row.isStatic() != column.isStatic())
                return true;

            switch (operator)
            {
                case EQ:
                case LT:
                case LTE:
                case GTE:
                case GT:
                case NEQ:
                    {
                        assert !column.isComplex() : "Only CONTAINS and CONTAINS_KEY are supported for 'complex' types";
                        ByteBuffer foundValue = getValue(partitionKey, row);
                        // Note that CQL expression are always of the form 'x < 4', i.e. the tested value is on the left.
                        return foundValue != null && operator.isSatisfiedBy(column.type, foundValue, value);
                    }
                case CONTAINS:
                    assert column.type.isCollection();
                    CollectionType<?> type = (CollectionType<?>)column.type;
                    if (column.isComplex())
                    {
                        ComplexColumnData complexData = row.getComplexColumnData(column);
                        for (Cell cell : complexData)
                        {
                            if (type.kind == CollectionType.Kind.SET)
                            {
                                if (type.nameComparator().compare(cell.path().get(0), value) == 0)
                                    return true;
                            }
                            else
                            {
                                if (type.valueComparator().compare(cell.value(), value) == 0)
                                    return true;
                            }
                        }
                        return false;
                    }
                    else
                    {
                        ByteBuffer foundValue = getValue(partitionKey, row);
                        if (foundValue == null)
                            return false;

                        switch (type.kind)
                        {
                            case LIST:
                                ListType<?> listType = (ListType<?>)type;
                                return listType.compose(foundValue).contains(listType.getElementsType().compose(value));
                            case SET:
                                SetType<?> setType = (SetType<?>)type;
                                return setType.compose(foundValue).contains(setType.getElementsType().compose(value));
                            case MAP:
                                MapType<?,?> mapType = (MapType<?, ?>)type;
                                return mapType.compose(foundValue).containsValue(mapType.getValuesType().compose(value));
                        }
                        throw new AssertionError();
                    }
                case CONTAINS_KEY:
                    assert column.type.isCollection() && column.type instanceof MapType;
                    MapType<?, ?> mapType = (MapType<?, ?>)column.type;
                    if (column.isComplex())
                    {
                         return row.getCell(column, CellPath.create(value)) != null;
                    }
                    else
                    {
                        ByteBuffer foundValue = getValue(partitionKey, row);
                        return foundValue != null && mapType.getSerializer().getSerializedValue(foundValue, value, mapType.getKeysType()) != null;
                    }

                case IN:
                    // It wouldn't be terribly hard to support this (though doing so would imply supporting
                    // IN for 2ndary index) but currently we don't.
                    throw new AssertionError();
            }
            throw new AssertionError();
        }

        @Override
        public String toString()
        {
            AbstractType<?> type = column.type;
            switch (operator)
            {
                case CONTAINS:
                    assert type instanceof CollectionType;
                    CollectionType<?> ct = (CollectionType<?>)type;
                    type = ct.kind == CollectionType.Kind.SET ? ct.nameComparator() : ct.valueComparator();
                    break;
                case CONTAINS_KEY:
                    assert type instanceof MapType;
                    type = ((MapType<?, ?>)type).nameComparator();
                    break;
                case IN:
                    type = ListType.getInstance(type, false);
                    break;
                default:
                    break;
            }
            return String.format("%s %s %s", column.name, operator, type.getString(value));
        }

        @Override
        Kind kind()
        {
            return Kind.SIMPLE;
        }
    }

    /**
     * An expression of the form 'column' ['key'] = 'value' (which is only
     * supported when 'column' is a map).
     */
    private static class MapEqualityExpression extends Expression
    {
        private final ByteBuffer key;

        public MapEqualityExpression(ColumnDefinition column, ByteBuffer key, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
            assert column.type instanceof MapType && operator == Operator.EQ;
            this.key = key;
        }

        @Override
        public void validateForIndexing() throws InvalidRequestException
        {
            super.validateForIndexing();
            checkNotNull(key, "Unsupported null value for key of map column %s", column.name);
            checkBindValueSet(key, "Unsupported unset value for key of map column %s", column.name);
        }

        @Override
        public ByteBuffer getIndexValue()
        {
            return CompositeType.build(key, value);
        }

        public boolean isSatisfiedBy(DecoratedKey partitionKey, Row row)
        {
            assert key != null;
            // We support null conditions for LWT (in ColumnCondition) but not for RowFilter.
            // TODO: we should try to merge both code someday.
            assert value != null;

            if (row.isStatic() != column.isStatic())
                return true;

            MapType<?, ?> mt = (MapType<?, ?>)column.type;
            if (column.isComplex())
            {
                Cell cell = row.getCell(column, CellPath.create(key));
                return cell != null && mt.valueComparator().compare(cell.value(), value) == 0;
            }
            else
            {
                ByteBuffer serializedMap = getValue(partitionKey, row);
                if (serializedMap == null)
                    return false;

                ByteBuffer foundValue = mt.getSerializer().getSerializedValue(serializedMap, key, mt.getKeysType());
                return foundValue != null && mt.valueComparator().compare(foundValue, value) == 0;
            }
        }

        @Override
        public String toString()
        {
            MapType<?, ?> mt = (MapType<?, ?>)column.type;
            return String.format("%s[%s] = %s", column.name, mt.nameComparator().getString(key), mt.valueComparator().getString(value));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof MapEqualityExpression))
                return false;

            MapEqualityExpression that = (MapEqualityExpression)o;

            return Objects.equal(this.column.name, that.column.name)
                && Objects.equal(this.operator, that.operator)
                && Objects.equal(this.key, that.key)
                && Objects.equal(this.value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column.name, operator, key, value);
        }

        @Override
        Kind kind()
        {
            return Kind.MAP_EQUALITY;
        }
    }

    /**
     * An expression of the form 'name' = 'value', but where 'name' is actually the
     * clustering value for a compact table. This is only for thrift.
     */
    private static class ThriftExpression extends Expression
    {
        private final CFMetaData metadata;

        public ThriftExpression(CFMetaData metadata, ByteBuffer name, Operator operator, ByteBuffer value)
        {
            super(makeDefinition(metadata, name), operator, value);
            assert metadata.isCompactTable();
            this.metadata = metadata;
        }

        private static ColumnDefinition makeDefinition(CFMetaData metadata, ByteBuffer name)
        {
            ColumnDefinition def = metadata.getColumnDefinition(name);
            if (def != null)
                return def;

            // In thrift, we actually allow expression on non-defined columns for the sake of filtering. To accomodate
            // this we create a "fake" definition. This is messy but it works so is probably good enough.
            return ColumnDefinition.regularDef(metadata, name, metadata.compactValueColumn().type);
        }

        public boolean isSatisfiedBy(DecoratedKey partitionKey, Row row)
        {
            assert value != null;

            // On thrift queries, even if the column expression is a "static" one, we'll have convert it as a "dynamic"
            // one in ThriftResultsMerger, so we always expect it to be a dynamic one. Further, we expect this is only
            // called when the row clustering does match the column (see ThriftFilter above).
            assert row.clustering().equals(makeCompactClustering(metadata, column.name.bytes));
            Cell cell = row.getCell(metadata.compactValueColumn());
            return cell != null && operator.isSatisfiedBy(column.type, cell.value(), value);
        }

        @Override
        public String toString()
        {
            return String.format("%s %s %s", column.name, operator, column.type.getString(value));
        }

        @Override
        Kind kind()
        {
            return Kind.THRIFT_DYN_EXPR;
        }
    }

    public static class Serializer
    {
        public void serialize(RowFilter filter, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(filter instanceof ThriftFilter);
            out.writeVInt(filter.expressions.size());
            for (Expression expr : filter.expressions)
                Expression.serializer.serialize(expr, out, version);
        }

        public RowFilter deserialize(DataInputPlus in, int version, CFMetaData metadata) throws IOException
        {
            boolean forThrift = in.readBoolean();
            int size = (int)in.readVInt();
            List<Expression> expressions = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                expressions.add(Expression.serializer.deserialize(in, version, metadata));
            return forThrift
                 ? new ThriftFilter(expressions)
                 : new CQLFilter(expressions);
        }

        public long serializedSize(RowFilter filter, int version)
        {
            long size = 1 // forThrift
                      + TypeSizes.sizeofVInt(filter.expressions.size());
            for (Expression expr : filter.expressions)
                size += Expression.serializer.serializedSize(expr, version);
            return size;
        }
    }
}

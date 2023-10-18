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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
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
    private static final Logger logger = LoggerFactory.getLogger(RowFilter.class);

    public static final Serializer serializer = new Serializer();

    protected final List<Expression> expressions;

    protected RowFilter(List<Expression> expressions)
    {
        this.expressions = expressions;
    }

    public static RowFilter create()
    {
        return new CQLFilter(new ArrayList<>());
    }

    public static RowFilter create(int capacity)
    {
        return new CQLFilter(new ArrayList<>(capacity));
    }

    public static RowFilter none()
    {
        return CQLFilter.NONE;
    }

    public SimpleExpression add(ColumnMetadata def, Operator op, ByteBuffer value)
    {
        SimpleExpression expression = new SimpleExpression(def, op, value);
        add(expression);
        return expression;
    }

    public void addMapEquality(ColumnMetadata def, ByteBuffer key, Operator op, ByteBuffer value)
    {
        add(new MapEqualityExpression(def, key, op, value));
    }

    public void addCustomIndexExpression(TableMetadata metadata, IndexMetadata targetIndex, ByteBuffer value)
    {
        add(new CustomExpression(metadata, targetIndex, value));
    }

    private void add(Expression expression)
    {
        expression.validate();
        expressions.add(expression);
    }

    public void addUserExpression(UserExpression e)
    {
        expressions.add(e);
    }

    public List<Expression> getExpressions()
    {
        return expressions;
    }

    /**
     * Checks if some of the expressions apply to clustering or regular columns.
     * @return {@code true} if some of the expressions apply to clustering or regular columns, {@code false} otherwise.
     */
    public boolean hasExpressionOnClusteringOrRegularColumns()
    {
        for (Expression expression : expressions)
        {
            ColumnMetadata column = expression.column();
            if (column.isClusteringColumn() || column.isRegular())
                return true;
        }
        return false;
    }

    protected abstract Transformation<BaseRowIterator<?>> filter(TableMetadata metadata, long nowInSec);

    /**
     * Filters the provided iterator so that only the row satisfying the expression of this filter
     * are included in the resulting iterator.
     *
     * @param iter the iterator to filter
     * @param nowInSec the time of query in seconds.
     * @return the filtered iterator.
     */
    public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iter, long nowInSec)
    {
        return expressions.isEmpty() ? iter : Transformation.apply(iter, filter(iter.metadata(), nowInSec));
    }

    /**
     * Filters the provided iterator so that only the row satisfying the expression of this filter
     * are included in the resulting iterator.
     *
     * @param iter the iterator to filter
     * @param nowInSec the time of query in seconds.
     * @return the filtered iterator.
     */
    public PartitionIterator filter(PartitionIterator iter, TableMetadata metadata, long nowInSec)
    {
        return expressions.isEmpty() ? iter : Transformation.apply(iter, filter(metadata, nowInSec));
    }

    /**
     * Whether the provided row in the provided partition satisfies this filter.
     *
     * @param metadata the table metadata.
     * @param partitionKey the partition key for partition to test.
     * @param row the row to test.
     * @param nowInSec the current time in seconds (to know what is live and what isn't).
     * @return {@code true} if {@code row} in partition {@code partitionKey} satisfies this row filter.
     */
    public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
    {
        // We purge all tombstones as the expressions isSatisfiedBy methods expects it
        Row purged = row.purge(DeletionPurger.PURGE_ALL, nowInSec, metadata.enforceStrictLiveness());
        if (purged == null)
            return expressions.isEmpty();

        for (Expression e : expressions)
        {
            if (!e.isSatisfiedBy(metadata, partitionKey, purged))
                return false;
        }
        return true;
    }

    /**
     * Returns true if all of the expressions within this filter that apply to the partition key are satisfied by
     * the given key, false otherwise.
     */
    public boolean partitionKeyRestrictionsAreSatisfiedBy(DecoratedKey key, AbstractType<?> keyValidator)
    {
        for (Expression e : expressions)
        {
            if (!e.column.isPartitionKey())
                continue;

            ByteBuffer value = keyValidator instanceof CompositeType
                             ? ((CompositeType) keyValidator).split(key.getKey())[e.column.position()]
                             : key.getKey();
            if (!e.operator().isSatisfiedBy(e.column.type, value, e.value))
                return false;
        }
        return true;
    }

    /**
     * Returns true if all of the expressions within this filter that apply to the clustering key are satisfied by
     * the given Clustering, false otherwise.
     */
    public boolean clusteringKeyRestrictionsAreSatisfiedBy(Clustering<?> clustering)
    {
        for (Expression e : expressions)
        {
            if (!e.column.isClusteringColumn())
                continue;

            if (!e.operator().isSatisfiedBy(e.column.type, clustering.bufferAt(e.column.position()), e.value))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns this filter but without the provided expression. This method
     * *assumes* that the filter contains the provided expression.
     */
    public RowFilter without(Expression expression)
    {
        assert expressions.contains(expression);
        if (expressions.size() == 1)
            return RowFilter.none();

        List<Expression> newExpressions = new ArrayList<>(expressions.size() - 1);
        for (Expression e : expressions)
            if (!e.equals(expression))
                newExpressions.add(e);

        return withNewExpressions(newExpressions);
    }

    /**
     * Returns a copy of this filter but without the provided expression. If this filter doesn't contain the specified
     * expression this method will just return an identical copy of this filter.
     */
    public RowFilter without(ColumnMetadata column, Operator op, ByteBuffer value)
    {
        if (isEmpty())
            return this;

        List<Expression> newExpressions = new ArrayList<>(expressions.size() - 1);
        for (Expression e : expressions)
            if (!e.column().equals(column) || e.operator() != op || !e.value.equals(value))
                newExpressions.add(e);

        return withNewExpressions(newExpressions);
    }

    public RowFilter withoutExpressions()
    {
        return withNewExpressions(Collections.emptyList());
    }

    public RowFilter restrict(Predicate<Expression> filter)
    {
        return withNewExpressions(expressions.stream().filter(filter).collect(Collectors.toList()));
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

    @Override
    public String toString()
    {
        return toString(false);
    }

    /**
     * Returns a CQL representation of this row filter.
     *
     * @return a CQL representation of this row filter
     */
    public String toCQLString()
    {
        return toString(true);
    }

    private String toString(boolean cql)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < expressions.size(); i++)
        {
            if (i > 0)
                sb.append(" AND ");
            sb.append(expressions.get(i).toString(cql));
        }
        return sb.toString();
    }

    private static class CQLFilter extends RowFilter
    {
        static CQLFilter NONE = new CQLFilter(Collections.emptyList());

        private CQLFilter(List<Expression> expressions)
        {
            super(expressions);
        }

        protected Transformation<BaseRowIterator<?>> filter(TableMetadata metadata, long nowInSec)
        {
            List<Expression> partitionLevelExpressions = new ArrayList<>();
            List<Expression> rowLevelExpressions = new ArrayList<>();
            for (Expression e: expressions)
            {
                if (e.column.isStatic() || e.column.isPartitionKey())
                    partitionLevelExpressions.add(e);
                else
                    rowLevelExpressions.add(e);
            }

            long numberOfRegularColumnExpressions = rowLevelExpressions.size();
            final boolean filterNonStaticColumns = numberOfRegularColumnExpressions > 0;

            return new Transformation<BaseRowIterator<?>>()
            {
                DecoratedKey pk;

                @SuppressWarnings("resource")
                protected BaseRowIterator<?> applyToPartition(BaseRowIterator<?> partition)
                {
                    pk = partition.partitionKey();

                    // Short-circuit all partitions that won't match based on static and partition keys
                    for (Expression e : partitionLevelExpressions)
                        if (!e.isSatisfiedBy(metadata, partition.partitionKey(), partition.staticRow()))
                        {
                            partition.close();
                            return null;
                        }

                    BaseRowIterator<?> iterator = partition instanceof UnfilteredRowIterator
                                                  ? Transformation.apply((UnfilteredRowIterator) partition, this)
                                                  : Transformation.apply((RowIterator) partition, this);

                    if (filterNonStaticColumns && !iterator.hasNext())
                    {
                        iterator.close();
                        return null;
                    }

                    return iterator;
                }

                public Row applyToRow(Row row)
                {
                    Row purged = row.purge(DeletionPurger.PURGE_ALL, nowInSec, metadata.enforceStrictLiveness());
                    if (purged == null)
                        return null;

                    for (Expression e : rowLevelExpressions)
                        if (!e.isSatisfiedBy(metadata, pk, purged))
                            return null;

                    return row;
                }
            };
        }

        protected RowFilter withNewExpressions(List<Expression> expressions)
        {
            return new CQLFilter(expressions);
        }
    }

    public static abstract class Expression
    {
        private static final Serializer serializer = new Serializer();

        // Note: the order of this enum matter, it's used for serialization,
        // and this is why we have some UNUSEDX for values we don't use anymore
        // (we could clean those on a major protocol update, but it's not worth
        // the trouble for now)
        protected enum Kind { SIMPLE, MAP_EQUALITY, UNUSED1, CUSTOM, USER }

        protected abstract Kind kind();
        protected final ColumnMetadata column;
        protected final Operator operator;
        protected final ByteBuffer value;

        protected Expression(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            this.column = column;
            this.operator = operator;
            this.value = value;
        }

        public boolean isCustom()
        {
            return kind() == Kind.CUSTOM;
        }

        public boolean isUserDefined()
        {
            return kind() == Kind.USER;
        }

        public ColumnMetadata column()
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

        public void validate()
        {
            checkNotNull(value, "Unsupported null value for column %s", column.name);
            checkBindValueSet(value, "Unsupported unset value for column %s", column.name);
        }

        /** @deprecated See CASSANDRA-6377 */
        @Deprecated(since = "3.5")
        public void validateForIndexing()
        {
            checkFalse(value.remaining() > FBUtilities.MAX_UNSIGNED_SHORT,
                       "Index expression values may not be larger than 64K");
        }

        /**
         * Returns whether the provided row satisfied this expression or not.
         *
         *
         * @param metadata
         * @param partitionKey the partition key for row to check.
         * @param row the row to check. It should *not* contain deleted cells
         * (i.e. it should come from a RowIterator).
         * @return whether the row is satisfied by this expression.
         */
        public abstract boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row);

        protected ByteBuffer getValue(TableMetadata metadata, DecoratedKey partitionKey, Row row)
        {
            switch (column.kind)
            {
                case PARTITION_KEY:
                    return metadata.partitionKeyType instanceof CompositeType
                         ? CompositeType.extractComponent(partitionKey.getKey(), column.position())
                         : partitionKey.getKey();
                case CLUSTERING:
                    return row.clustering().bufferAt(column.position());
                default:
                    Cell<?> cell = row.getCell(column);
                    return cell == null ? null : cell.buffer();
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

        @Override
        public String toString()
        {
            return toString(false);
        }

        /**
         * Returns a CQL representation of this expression.
         *
         * @return a CQL representation of this expression
         */
        public String toCQLString()
        {
            return toString(true);
        }

        protected abstract String toString(boolean cql);

        private static class Serializer
        {
            public void serialize(Expression expression, DataOutputPlus out, int version) throws IOException
            {
                out.writeByte(expression.kind().ordinal());

                // Custom expressions include neither a column or operator, but all
                // other expressions do.
                if (expression.kind() == Kind.CUSTOM)
                {
                    IndexMetadata.serializer.serialize(((CustomExpression)expression).targetIndex, out, version);
                    ByteBufferUtil.writeWithShortLength(expression.value, out);
                    return;
                }

                if (expression.kind() == Kind.USER)
                {
                    UserExpression.serialize((UserExpression)expression, out, version);
                    return;
                }

                ByteBufferUtil.writeWithShortLength(expression.column.name.bytes, out);
                expression.operator.writeTo(out);

                switch (expression.kind())
                {
                    case SIMPLE:
                        ByteBufferUtil.writeWithShortLength(expression.value, out);
                        break;
                    case MAP_EQUALITY:
                        MapEqualityExpression mexpr = (MapEqualityExpression)expression;
                        ByteBufferUtil.writeWithShortLength(mexpr.key, out);
                        ByteBufferUtil.writeWithShortLength(mexpr.value, out);
                        break;
                }
            }

            public Expression deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
            {
                Kind kind = Kind.values()[in.readByte()];

                // custom expressions (3.0+ only) do not contain a column or operator, only a value
                if (kind == Kind.CUSTOM)
                {
                    return new CustomExpression(metadata,
                            IndexMetadata.serializer.deserialize(in, version, metadata),
                            ByteBufferUtil.readWithShortLength(in));
                }

                if (kind == Kind.USER)
                    return UserExpression.deserialize(in, version, metadata);

                ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
                Operator operator = Operator.readFrom(in);
                ColumnMetadata column = metadata.getColumn(name);

                // Compact storage tables, when used with thrift, used to allow falling through this withouot throwing an
                // exception. However, since thrift was removed in 4.0, this behaviour was not restored in CASSANDRA-16217
                if (column == null)
                    throw new RuntimeException("Unknown (or dropped) column " + UTF8Type.instance.getString(name) + " during deserialization");

                switch (kind)
                {
                    case SIMPLE:
                        return new SimpleExpression(column, operator, ByteBufferUtil.readWithShortLength(in));
                    case MAP_EQUALITY:
                        ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                        ByteBuffer value = ByteBufferUtil.readWithShortLength(in);
                        return new MapEqualityExpression(column, key, operator, value);
                }
                throw new AssertionError();
            }

            public long serializedSize(Expression expression, int version)
            {
                long size = 1; // kind byte

                // Custom expressions include neither a column or operator, but all
                // other expressions do.
                if (expression.kind() != Kind.CUSTOM && expression.kind() != Kind.USER)
                    size += ByteBufferUtil.serializedSizeWithShortLength(expression.column().name.bytes)
                            + expression.operator.serializedSize();

                switch (expression.kind())
                {
                    case SIMPLE:
                        size += ByteBufferUtil.serializedSizeWithShortLength(((SimpleExpression)expression).value);
                        break;
                    case MAP_EQUALITY:
                        MapEqualityExpression mexpr = (MapEqualityExpression)expression;
                        size += ByteBufferUtil.serializedSizeWithShortLength(mexpr.key)
                              + ByteBufferUtil.serializedSizeWithShortLength(mexpr.value);
                        break;
                    case CUSTOM:
                        size += IndexMetadata.serializer.serializedSize(((CustomExpression)expression).targetIndex, version)
                               + ByteBufferUtil.serializedSizeWithShortLength(expression.value);
                        break;
                    case USER:
                        size += UserExpression.serializedSize((UserExpression)expression, version);
                        break;
                }
                return size;
            }
        }
    }

    /**
     * An expression of the form 'column' 'op' 'value'.
     */
    public static class SimpleExpression extends Expression
    {
        SimpleExpression(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
        }

        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row)
        {
            // We support null conditions for LWT (in ColumnCondition) but not for RowFilter.
            // TODO: we should try to merge both code someday.
            assert value != null;

            switch (operator)
            {
                case EQ:
                case IN:
                case LT:
                case LTE:
                case GTE:
                case GT:
                    {
                        assert !column.isComplex() : "Only CONTAINS and CONTAINS_KEY are supported for 'complex' types";

                        // In order to support operators on Counter types, their value has to be extracted from internal
                        // representation. See CASSANDRA-11629
                        if (column.type.isCounter())
                        {
                            ByteBuffer foundValue = getValue(metadata, partitionKey, row);
                            if (foundValue == null)
                                return false;

                            ByteBuffer counterValue = LongType.instance.decompose(CounterContext.instance().total(foundValue, ByteBufferAccessor.instance));
                            return operator.isSatisfiedBy(LongType.instance, counterValue, value);
                        }
                        else
                        {
                            // Note that CQL expression are always of the form 'x < 4', i.e. the tested value is on the left.
                            ByteBuffer foundValue = getValue(metadata, partitionKey, row);
                            return foundValue != null && operator.isSatisfiedBy(column.type, foundValue, value);
                        }
                    }
                case NEQ:
                case LIKE_PREFIX:
                case LIKE_SUFFIX:
                case LIKE_CONTAINS:
                case LIKE_MATCHES:
                    {
                        assert !column.isComplex() : "Only CONTAINS and CONTAINS_KEY are supported for 'complex' types";
                        ByteBuffer foundValue = getValue(metadata, partitionKey, row);
                        // Note that CQL expression are always of the form 'x < 4', i.e. the tested value is on the left.
                        return foundValue != null && operator.isSatisfiedBy(column.type, foundValue, value);
                    }
                case CONTAINS:
                    assert column.type.isCollection();
                    CollectionType<?> type = (CollectionType<?>)column.type;
                    if (column.isComplex())
                    {
                        ComplexColumnData complexData = row.getComplexColumnData(column);
                        if (complexData != null)
                        {
                            for (Cell<?> cell : complexData)
                            {
                                if (type.kind == CollectionType.Kind.SET)
                                {
                                    if (type.nameComparator().compare(cell.path().get(0), value) == 0)
                                        return true;
                                }
                                else
                                {
                                    if (type.valueComparator().compare(cell.buffer(), value) == 0)
                                        return true;
                                }
                            }
                        }
                        return false;
                    }
                    else
                    {
                        ByteBuffer foundValue = getValue(metadata, partitionKey, row);
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
                        ByteBuffer foundValue = getValue(metadata, partitionKey, row);
                        return foundValue != null && mapType.getSerializer().getSerializedValue(foundValue, value, mapType.getKeysType()) != null;
                    }
            }
            throw new AssertionError();
        }

        @Override
        protected String toString(boolean cql)
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
            return cql
                 ? String.format("%s %s %s", column.name.toCQLString(), operator, type.toCQLString(value) )
                 : String.format("%s %s %s", column.name.toString(), operator, type.getString(value));
        }

        @Override
        protected Kind kind()
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

        public MapEqualityExpression(ColumnMetadata column, ByteBuffer key, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
            assert column.type instanceof MapType && operator == Operator.EQ;
            this.key = key;
        }

        @Override
        public void validate() throws InvalidRequestException
        {
            checkNotNull(key, "Unsupported null map key for column %s", column.name);
            checkBindValueSet(key, "Unsupported unset map key for column %s", column.name);
            checkNotNull(value, "Unsupported null map value for column %s", column.name);
            checkBindValueSet(value, "Unsupported unset map value for column %s", column.name);
        }

        @Override
        public ByteBuffer getIndexValue()
        {
            return CompositeType.build(ByteBufferAccessor.instance, key, value);
        }

        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row)
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
                Cell<?> cell = row.getCell(column, CellPath.create(key));
                return cell != null && mt.valueComparator().compare(cell.buffer(), value) == 0;
            }
            else
            {
                ByteBuffer serializedMap = getValue(metadata, partitionKey, row);
                if (serializedMap == null)
                    return false;

                ByteBuffer foundValue = mt.getSerializer().getSerializedValue(serializedMap, key, mt.getKeysType());
                return foundValue != null && mt.valueComparator().compare(foundValue, value) == 0;
            }
        }

        @Override
        protected String toString(boolean cql)
        {
            MapType<?, ?> mt = (MapType<?, ?>) column.type;
            AbstractType<?> nt = mt.nameComparator();
            AbstractType<?> vt = mt.valueComparator();
            return cql
                 ? String.format("%s[%s] = %s", column.name.toCQLString(), nt.toCQLString(key), vt.toCQLString(value))
                 : String.format("%s[%s] = %s", column.name.toString(), nt.getString(key), vt.getString(value));
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
        protected Kind kind()
        {
            return Kind.MAP_EQUALITY;
        }
    }

    /**
     * A custom index expression for use with 2i implementations which support custom syntax and which are not
     * necessarily linked to a single column in the base table.
     */
    public static final class CustomExpression extends Expression
    {
        private final IndexMetadata targetIndex;
        private final TableMetadata table;

        public CustomExpression(TableMetadata table, IndexMetadata targetIndex, ByteBuffer value)
        {
            // The operator is not relevant, but Expression requires it so for now we just hardcode EQ
            super(makeDefinition(table, targetIndex), Operator.EQ, value);
            this.targetIndex = targetIndex;
            this.table = table;
        }

        private static ColumnMetadata makeDefinition(TableMetadata table, IndexMetadata index)
        {
            // Similarly to how we handle non-defined columns in thift, we create a fake column definition to
            // represent the target index. This is definitely something that can be improved though.
            return ColumnMetadata.regularColumn(table, ByteBuffer.wrap(index.name.getBytes()), BytesType.instance);
        }

        public IndexMetadata getTargetIndex()
        {
            return targetIndex;
        }

        public ByteBuffer getValue()
        {
            return value;
        }

        @Override
        protected String toString(boolean cql)
        {
            return String.format("expr(%s, %s)",
                                 cql ? ColumnIdentifier.maybeQuote(targetIndex.name) : targetIndex.name,
                                 Keyspace.openAndGetStore(table)
                                         .indexManager
                                         .getIndex(targetIndex)
                                         .customExpressionValueType());
        }

        protected Kind kind()
        {
            return Kind.CUSTOM;
        }

        // Filtering by custom expressions isn't supported yet, so just accept any row
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row)
        {
            return true;
        }
    }

    /**
     * A user defined filtering expression. These may be added to RowFilter programmatically by a
     * QueryHandler implementation. No concrete implementations are provided and adding custom impls
     * to the classpath is a task for operators (needless to say, this is something of a power
     * user feature). Care must also be taken to register implementations, via the static register
     * method during system startup. An implementation and its corresponding Deserializer must be
     * registered before sending or receiving any messages containing expressions of that type.
     * Use of custom filtering expressions in a mixed version cluster should be handled with caution
     * as the order in which types are registered is significant: if continuity of use during upgrades
     * is important, new types should registered last and obsoleted types should still be registered (
     * or dummy implementations registered in their place) to preserve consistent identifiers across
     * the cluster).
     *
     * During serialization, the identifier for the Deserializer implementation is prepended to the
     * implementation specific payload. To deserialize, the identifier is read first to obtain the
     * Deserializer, which then provides the concrete expression instance.
     */
    public static abstract class UserExpression extends Expression
    {
        private static final DeserializerRegistry deserializers = new DeserializerRegistry();
        private static final class DeserializerRegistry
        {
            private final AtomicInteger counter = new AtomicInteger(0);
            private final ConcurrentMap<Integer, Deserializer> deserializers = new ConcurrentHashMap<>();
            private final ConcurrentMap<Class<? extends UserExpression>, Integer> registeredClasses = new ConcurrentHashMap<>();

            public void registerUserExpressionClass(Class<? extends UserExpression> expressionClass,
                                                    UserExpression.Deserializer deserializer)
            {
                int id = registeredClasses.computeIfAbsent(expressionClass, (cls) -> counter.getAndIncrement());
                deserializers.put(id, deserializer);

                logger.debug("Registered user defined expression type {} and serializer {} with identifier {}",
                             expressionClass.getName(), deserializer.getClass().getName(), id);
            }

            public Integer getId(UserExpression expression)
            {
                return registeredClasses.get(expression.getClass());
            }

            public Deserializer getDeserializer(int id)
            {
                return deserializers.get(id);
            }
        }

        protected static abstract class Deserializer
        {
            protected abstract UserExpression deserialize(DataInputPlus in,
                                                          int version,
                                                          TableMetadata metadata) throws IOException;
        }

        public static void register(Class<? extends UserExpression> expressionClass, Deserializer deserializer)
        {
            deserializers.registerUserExpressionClass(expressionClass, deserializer);
        }

        private static UserExpression deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            int id = in.readInt();
            Deserializer deserializer = deserializers.getDeserializer(id);
            assert deserializer != null : "No user defined expression type registered with id " + id;
            return deserializer.deserialize(in, version, metadata);
        }

        private static void serialize(UserExpression expression, DataOutputPlus out, int version) throws IOException
        {
            Integer id = deserializers.getId(expression);
            assert id != null : "User defined expression type " + expression.getClass().getName() + " is not registered";
            out.writeInt(id);
            expression.serialize(out, version);
        }

        private static long serializedSize(UserExpression expression, int version)
        {   // 4 bytes for the expression type id
            return 4 + expression.serializedSize(version);
        }

        protected UserExpression(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
        }

        protected Kind kind()
        {
            return Kind.USER;
        }

        protected abstract void serialize(DataOutputPlus out, int version) throws IOException;
        protected abstract long serializedSize(int version);
    }

    public static class Serializer
    {
        public void serialize(RowFilter filter, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(false); // Old "is for thrift" boolean
            out.writeUnsignedVInt32(filter.expressions.size());
            for (Expression expr : filter.expressions)
                Expression.serializer.serialize(expr, out, version);

        }

        public RowFilter deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            in.readBoolean(); // Unused
            int size = in.readUnsignedVInt32();
            List<Expression> expressions = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                expressions.add(Expression.serializer.deserialize(in, version, metadata));

            return new CQLFilter(expressions);
        }

        public long serializedSize(RowFilter filter, int version)
        {
            long size = 1 // unused boolean
                      + TypeSizes.sizeofUnsignedVInt(filter.expressions.size());
            for (Expression expr : filter.expressions)
                size += Expression.serializer.serializedSize(expr, version);
            return size;
        }
    }
}

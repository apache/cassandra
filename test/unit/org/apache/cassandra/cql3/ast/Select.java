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

package org.apache.cassandra.cql3.ast;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.cql3.ast.Elements.newLine;

public class Select implements Statement
{
    /*
SELECT * | select_expression | DISTINCT partition //TODO DISTINCT
FROM [keyspace_name.] table_name
[WHERE partition_value
   [AND clustering_filters
   [AND static_filters]]]
[ORDER BY PK_column_name ASC|DESC]
[LIMIT N]
[ALLOW FILTERING] //TODO
     */
    // select
    public final List<Expression> selections;
    // from
    public final Optional<TableReference> source;
    // where
    public final Optional<Conditional> where;
    public final Optional<OrderBy> orderBy;
    public final Optional<Value> limit;
    public final boolean allowFiltering;

    public Select(List<Expression> selections)
    {
        this(selections, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public Select(List<Expression> selections, Optional<TableReference> source, Optional<Conditional> where, Optional<OrderBy> orderBy, Optional<Value> limit)
    {
        this(selections, source, where, orderBy, limit, false);
    }

    public Select(List<Expression> selections, Optional<TableReference> source, Optional<Conditional> where, Optional<OrderBy> orderBy, Optional<Value> limit, boolean allowFiltering)
    {
        this.selections = selections;
        this.source = source;
        this.where = where;
        this.orderBy = orderBy;
        this.limit = limit;
        this.allowFiltering = allowFiltering;

        if (!source.isPresent())
        {
            if (where.isPresent())
                throw new IllegalArgumentException("Can not have a WHERE clause when there isn't a FROM");
            if (orderBy.isPresent())
                throw new IllegalArgumentException("Can not have a ORDER BY clause when there isn't a FROM");
            if (limit.isPresent())
                throw new IllegalArgumentException("Can not have a LIMIT clause when there isn't a FROM");
            if (allowFiltering)
                throw new IllegalArgumentException("Can not have a ALLOW FILTERING clause when there isn't a FROM");
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public Select withAllowFiltering()
    {
        return new Select(selections, source, where, orderBy, limit, true);
    }

    @Override
    public void toCQL(StringBuilder sb, int indent)
    {
        sb.append("SELECT ");
        if (selections.isEmpty())
        {
            sb.append('*');
        }
        else
        {
            int finalIndent = indent;
            selections.forEach(s -> {
                s.toCQL(sb, finalIndent);
                sb.append(", ");
            });
            sb.setLength(sb.length() - 2); // last ', '
        }
        if (source.isPresent())
        {
            newLine(sb, indent);
            sb.append("FROM ");
            source.get().toCQL(sb, indent);
            if (where.isPresent())
            {
                newLine(sb, indent);
                sb.append("WHERE ");
                where.get().toCQL(sb, indent);
            }
            if (orderBy.isPresent())
            {
                newLine(sb, indent);
                sb.append("ORDER BY ");
                orderBy.get().toCQL(sb, indent);
            }
            if (limit.isPresent())
            {
                newLine(sb, indent);
                sb.append("LIMIT ");
                limit.get().toCQL(sb, indent);
            }

            if (allowFiltering)
            {
                newLine(sb, indent);
                sb.append("ALLOW FILTERING");
            }
        }
    }

    @Override
    public Stream<? extends Element> stream()
    {
        List<Element> es = new ArrayList<>(selections.size()
                                           + (source.isPresent() ? 1 : 0)
                                           + (where.isPresent() ? 1 : 0)
                                           + (orderBy.isPresent() ? 1 : 0)
                                           + (limit.isPresent() ? 1 : 0));
        es.addAll(selections);
        if (source.isPresent())
            es.add(source.get());
        if (where.isPresent())
            es.add(where.get());
        if (orderBy.isPresent())
            es.add(orderBy.get());
        if (limit.isPresent())
            es.add(limit.get());
        return es.stream();
    }

    @Override
    public String toString()
    {
        return detailedToString();
    }

    @Override
    public Kind kind()
    {
        return Kind.SELECT;
    }

    public static class OrderBy implements Element
    {
        public enum Ordering
        {ASC, DESC}

        public final List<Ordered> ordered;

        public OrderBy(List<Ordered> ordered)
        {
            if (ordered.isEmpty())
                throw new IllegalArgumentException("Can not ORDER BY an empty list");
            this.ordered = ordered;
        }

        @Override
        public void toCQL(StringBuilder sb, int indent)
        {
            if (ordered.size() == 1)
            {
                ordered.get(0).toCQL(sb, indent);
                return;
            }

            String postfix = ", ";
            for (Ordered o : ordered)
            {
                o.toCQL(sb, indent);
                sb.append(postfix);
            }
            sb.setLength(sb.length() - postfix.length());
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return ordered.stream();
        }

        public static class Ordered implements Element
        {
            public final ReferenceExpression expression;
            public final Ordering ordering;

            public Ordered(ReferenceExpression expression, Ordering ordering)
            {
                this.expression = expression;
                this.ordering = ordering;
            }

            @Override
            public void toCQL(StringBuilder sb, int indent)
            {
                expression.toCQL(sb, indent);
                sb.append(' ');
                sb.append(ordering.name());
            }

            @Override
            public Stream<? extends Element> stream()
            {
                return Stream.of(expression);
            }
        }

        public static class Builder
        {
            private final List<OrderBy.Ordered> ordered = new ArrayList<>();

            public boolean isEmpty()
            {
                return ordered.isEmpty();
            }

            public Builder add(ReferenceExpression expression, Ordering ordering)
            {
                ordered.add(new Ordered(expression, ordering));
                return this;
            }

            public OrderBy build()
            {
                return new OrderBy(ImmutableList.copyOf(ordered));
            }
        }
    }

    public static class Builder
    {
        private boolean filtering = false;
        @Nullable // null means wildcard
        private List<Expression> selections = new ArrayList<>();
        private Optional<TableReference> source = Optional.empty();
        private Conditional.Builder where = new Conditional.Builder();
        private OrderBy.Builder orderBy = new OrderBy.Builder();
        private Optional<Value> limit = Optional.empty();

        public Builder withWildcard()
        {
            if (selections != null && !selections.isEmpty())
                throw new IllegalStateException("Attempted to use * for selection but existing selections exist: " + selections);
            selections = null;
            return this;
        }

        public Builder withColumnSelection(String name, AbstractType<?> type)
        {
            return withSelection(Reference.of(new Symbol(name, type)));
        }

        public Builder allowFiltering()
        {
            filtering = true;
            return this;
        }

        public Builder withSelection(Expression e)
        {
            if (selections == null)
                throw new IllegalStateException("Unable to add '" + e.name() + "' as a selection as * was already requested");
            selections.add(e);
            return this;
        }

        public Builder withTable(String ks, String name)
        {
            source = Optional.of(new TableReference(Optional.of(ks), name));
            return this;
        }

        public Builder withTable(String name)
        {
            source = Optional.of(new TableReference(name));
            return this;
        }

        public Builder withTable(TableMetadata table)
        {
            source = Optional.of(TableReference.from(table));
            return this;
        }

        public Builder withWhere(ReferenceExpression ref, Where.Inequalities kind, Expression expression)
        {
            where.where(kind, ref, expression);
            return this;
        }

        public Builder withWhere(String name, Where.Inequalities kind, int value)
        {
            return withWhere(kind, name, value, Int32Type.instance);
        }

        public <T> Builder withWhere(Where.Inequalities kind, String name, T value, AbstractType<T> type)
        {
            return withWhere(Reference.of(new Symbol(name, type)), kind, new Literal(value, type));
        }

        public Builder withIn(ReferenceExpression symbol, Expression... expressions)
        {
            where.in(symbol, expressions);
            return this;
        }

        public Builder withIn(ReferenceExpression symbol, List<Expression> expressions)
        {
            where.in(symbol, expressions);
            return this;
        }

        public Builder withIn(String name, int... values)
        {
            where.in(new Symbol(name, Int32Type.instance), IntStream.of(values).mapToObj(i -> new Literal(i, Int32Type.instance)).collect(Collectors.toList()));
            return this;
        }

        /**
         * When the column type/value type isn't known, this will fall back to byte type
         */
        public Builder withColumnEquals(String column, ByteBuffer value)
        {
            BytesType type = BytesType.instance;
            return withWhere(Reference.of(new Symbol(column, type)), Where.Inequalities.EQUAL, new Bind(value, type));
        }

        public Builder withColumnEquals(Symbol column, Expression value)
        {
            return withWhere(Reference.of(column), Where.Inequalities.EQUAL, value);
        }

        public Builder withOrderByColumn(String name, AbstractType<?> type, OrderBy.Ordering ordering)
        {
            orderBy.add(Reference.of(new Symbol(name, type)), ordering);
            return this;
        }

        public Builder withLimit(Value limit)
        {
            this.limit = Optional.of(limit);
            return this;
        }

        public Builder withLimit(long limit)
        {
            return withLimit(new Literal(limit, LongType.instance));
        }

        public Select build()
        {
            return new Select((selections == null || selections.isEmpty()) ? Collections.emptyList() : ImmutableList.copyOf(selections),
                              source,
                              where.isEmpty() ? Optional.empty() : Optional.of(where.build()),
                              orderBy.isEmpty() ? Optional.empty() : Optional.of(orderBy.build()),
                              limit,
                              filtering);
        }
    }
}

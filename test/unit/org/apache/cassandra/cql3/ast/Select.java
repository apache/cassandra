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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.TableMetadata;

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
[ALLOW FILTERING]
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

    public static TableBasedBuilder builder(TableMetadata metadata)
    {
        return new TableBasedBuilder(metadata);
    }

    public Select withAllowFiltering()
    {
        return new Select(selections, source, where, orderBy, limit, true);
    }

    @Override
    public void toCQL(StringBuilder sb, CQLFormatter formatter)
    {
        sb.append("SELECT ");
        if (selections.isEmpty())
        {
            sb.append('*');
        }
        else
        {
            selections.forEach(s -> {
                s.toCQL(sb, formatter);
                sb.append(", ");
            });
            sb.setLength(sb.length() - 2); // last ', '
        }
        if (source.isPresent())
        {
            formatter.section(sb);
            sb.append("FROM ");
            source.get().toCQL(sb, formatter);
            if (where.isPresent())
            {
                formatter.section(sb);
                sb.append("WHERE ");
                where.get().toCQL(sb, formatter);
            }
            if (orderBy.isPresent())
            {
                formatter.section(sb);
                sb.append("ORDER BY ");
                orderBy.get().toCQL(sb, formatter);
            }
            if (limit.isPresent())
            {
                formatter.section(sb);
                sb.append("LIMIT ");
                limit.get().toCQL(sb, formatter);
            }

            if (allowFiltering)
            {
                formatter.section(sb);
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

    @Override
    public Statement visit(Visitor v)
    {
        var u = v.visit(this);
        if (u != this) return u;
        boolean updated = false;
        List<Expression> selections = new ArrayList<>(this.selections.size());
        for (Expression e : this.selections)
        {
            Expression update = e.visit(v);
            updated |= e != update;
            selections.add(update);
        }
        Optional<Conditional> where;
        if (this.where.isPresent())
        {
            var c = this.where.get();
            var update = c.visit(v);
            updated |= c != update;
            where = Optional.ofNullable(update);
        }
        else
        {
            where = this.where;
        }
        Optional<Value> limit;
        if (this.limit.isPresent())
        {
            var l = this.limit.get();
            var update = l.visit(v);
            updated |= l != update;
            limit = Optional.ofNullable(update);
        }
        else
        {
            limit = this.limit;
        }
        if (!updated) return this;
        return new Select(selections, source, where, orderBy, limit, allowFiltering);
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
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            if (ordered.size() == 1)
            {
                ordered.get(0).toCQL(sb, formatter);
                return;
            }

            String postfix = ", ";
            for (Ordered o : ordered)
            {
                o.toCQL(sb, formatter);
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
            public void toCQL(StringBuilder sb, CQLFormatter formatter)
            {
                expression.toCQL(sb, formatter);
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
            private final List<Ordered> ordered = new ArrayList<>();

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

    public static class BaseBuilder<T extends BaseBuilder<T>> implements Conditional.ConditionalBuilder<T>
    {
        @Nullable // null means wildcard
        private List<Expression> selections = new ArrayList<>();
        protected Optional<TableReference> source = Optional.empty();
        private Conditional.Builder where = new Conditional.Builder();
        private OrderBy.Builder orderBy = new OrderBy.Builder();
        private Optional<Value> limit = Optional.empty();
        private boolean allowFiltering = false;

        public T wildcard()
        {
            if (selections != null && !selections.isEmpty())
                throw new IllegalStateException("Attempted to use * for selection but existing selections exist: " + selections);
            selections = null;
            return (T) this;
        }

        public T columnSelection(String name, AbstractType<?> type)
        {
            return selection(new Symbol(name, type));
        }

        public T allowFiltering()
        {
            allowFiltering = true;
            return (T) this;
        }

        public T selection(Expression e)
        {
            if (selections == null)
                throw new IllegalStateException("Unable to add '" + e.name() + "' as a selection as * was already requested");
            selections.add(e);
            return (T) this;
        }

        @Override
        public T where(Expression ref, Conditional.Where.Inequality kind, Expression expression)
        {
            where.where(ref, kind, expression);
            return (T) this;
        }

        @Override
        public T between(Expression ref, Expression start, Expression end)
        {
            where.between(ref, start, end);
            return (T) this;
        }

        @Override
        public T in(ReferenceExpression ref, List<? extends Expression> expressions)
        {
            where.in(ref, expressions);
            return (T) this;
        }

        @Override
        public T is(Symbol ref, Conditional.Is.Kind kind)
        {
            where.is(ref, kind);
            return (T) this;
        }

        public T orderByColumn(String name, AbstractType<?> type, OrderBy.Ordering ordering)
        {
            orderBy.add(new Symbol(name, type), ordering);
            return (T) this;
        }

        public T limit(Value limit)
        {
            this.limit = Optional.of(limit);
            return (T) this;
        }

        public T limit(int limit)
        {
            return limit(Bind.of(limit));
        }

        public Select build()
        {
            return new Select((selections == null || selections.isEmpty()) ? Collections.emptyList() : ImmutableList.copyOf(selections),
                              source,
                              where.isEmpty() ? Optional.empty() : Optional.of(where.build()),
                              orderBy.isEmpty() ? Optional.empty() : Optional.of(orderBy.build()),
                              limit,
                              allowFiltering);
        }
    }

    public static class Builder extends BaseBuilder<Builder>
    {
        public Builder table(TableReference ref)
        {
            source = Optional.of(ref);
            return this;
        }

        public Builder table(String ks, String name)
        {
            return table(new TableReference(Optional.of(ks), name));
        }

        public Builder table(String name)
        {
            return table(new TableReference(name));
        }

        public Builder table(TableMetadata table)
        {
            return table(TableReference.from(table));
        }
    }

    public static class TableBasedBuilder extends BaseBuilder<TableBasedBuilder> implements Conditional.ConditionalBuilderPlus<TableBasedBuilder>
    {
        private final TableMetadata metadata;

        public TableBasedBuilder(TableMetadata metadata)
        {
            this.metadata = metadata;
            source = Optional.of(TableReference.from(metadata));
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }
    }
}

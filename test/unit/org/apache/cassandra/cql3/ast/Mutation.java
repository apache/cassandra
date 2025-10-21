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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.harry.model.DetailedTableMetadata;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public abstract class Mutation implements Statement
{
    public enum Kind
    {INSERT, UPDATE, DELETE}

    public final Kind kind;
    public final TableReference table;

    protected Mutation(Kind kind, TableReference table)
    {
        this.kind = kind;
        this.table = table;
    }

    public Insert asInsert()
    {
        return (Insert) this;
    }

    public Update asUpdate()
    {
        return (Update) this;
    }

    public Delete asDelete()
    {
        return (Delete) this;
    }

    public abstract long timestampOrDefault(long defaultValue);

    public abstract boolean isCas();

    public abstract Mutation withoutTimestamp();

    public Mutation withTimestamp(long timestamp)
    {
        return withTimestamp(new Timestamp(new Literal(timestamp, LongType.instance)));
    }

    public abstract Mutation withTimestamp(Timestamp timestamp);


    public abstract Optional<? extends CasCondition> casCondition();

    public final Kind mutationKind()
    {
        return kind;
    }

    @Override
    public final Statement.Kind kind()
    {
        return Statement.Kind.MUTATION;
    }

    @Override
    public String toString()
    {
        return detailedToString();
    }

    public static InsertBuilder insert(TableMetadata metadata)
    {
        return new InsertBuilder(metadata);
    }

    public static TableBasedUpdateBuilder update(TableMetadata metadata)
    {
        return new TableBasedUpdateBuilder(metadata);
    }

    public static BasicUpdateBuilder update(String table)
    {
        return new BasicUpdateBuilder(new TableReference(table));
    }

    public static BasicUpdateBuilder update(String ks, String table)
    {
        return new BasicUpdateBuilder(new TableReference(Optional.of(ks), table));
    }

    public static DeleteBuilder delete(TableMetadata metadata)
    {
        return new DeleteBuilder(metadata);
    }

    public static LinkedHashSet<Symbol> toSet(Iterable<ColumnMetadata> columns)
    {
        LinkedHashSet<Symbol> set = new LinkedHashSet<>();
        for (var col : columns)
            set.add(Symbol.from(col));
        return set;
    }

    private static void toCQLWhere(Conditional conditional, StringBuilder sb, CQLFormatter formatter)
    {
        formatter.section(sb);
        sb.append("WHERE ");
        List<Conditional> where = conditional.simplify();
        formatter.group(sb);
        for (var c : where)
        {
            formatter.element(sb);
            c.toCQL(sb, formatter);
            sb.append(" AND ");
        }
        sb.setLength(sb.length() - 5); // 5 = " AND "
        formatter.endgroup(sb);
    }

    public static class TTL implements Element
    {
        public final Value value;

        public TTL(Value value)
        {
            this.value = value;
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            sb.append("TTL ");
            value.toCQL(sb, formatter);
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(value);
        }
    }

    public static class Timestamp implements Element
    {
        public final Value value;

        public Timestamp(Value value)
        {
            this.value = value;
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            sb.append("TIMESTAMP ");
            value.toCQL(sb, formatter);
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(value);
        }

        public long get()
        {
            if (value.value() instanceof Long)
                return (long) value.value();
            return LongType.instance.compose(value.valueEncoded());
        }
    }

    public static class Using implements Element
    {
        public final Optional<TTL> ttl;
        public final Optional<Timestamp> timestamp;

        private Using(Optional<TTL> ttl, Optional<Timestamp> timestamp)
        {
            this.ttl = ttl;
            this.timestamp = timestamp;
            if (ttl.isEmpty() && timestamp.isEmpty())
                throw new IllegalStateException("Empty USING isnt allowed");
        }

        public static Optional<Using> create(Optional<TTL> ttl, Optional<Timestamp> timestamp)
        {
            if (ttl.isEmpty() && timestamp.isEmpty()) return Optional.empty();
            return Optional.of(new Using(ttl, timestamp));
        }

        public Optional<Using> withoutTimestamp()
        {
            if (ttl.isEmpty()) return Optional.empty();
            return Optional.of(new Using(ttl, Optional.empty()));
        }

        public Using withTimestamp(Timestamp timestamp)
        {
            return new Using(ttl, Optional.of(timestamp));
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            sb.append("USING ");
            if (ttl.isPresent())
                ttl.get().toCQL(sb, formatter);
            if (timestamp.isPresent())
            {
                if (ttl.isPresent())
                    sb.append(" AND ");
                timestamp.get().toCQL(sb, formatter);
            }
        }

        @Override
        public Stream<? extends Element> stream()
        {
            int size = (ttl.isPresent() ? 1 : 0) + (timestamp.isPresent() ? 1 : 0);
            switch (size)
            {
                case 0: return Stream.empty();
                case 1: return Stream.of(ttl.isPresent() ? ttl.get() : timestamp.get());
                default: return Stream.of(ttl.get(), timestamp.get());
            }
        }
    }

    public static class Insert extends Mutation
    {
        public final LinkedHashMap<Symbol, Expression> values;
        public final boolean ifNotExists;
        public final Optional<Using> using;

        public Insert(TableReference table, LinkedHashMap<Symbol, Expression> values, boolean ifNotExists, Optional<Using> using)
        {
            super(Mutation.Kind.INSERT, table);
            this.values = values;
            this.ifNotExists = ifNotExists;
            this.using = using;
        }

        @Override
        public long timestampOrDefault(long defaultValue)
        {
            if (using.isEmpty()) return defaultValue;
            var opt = using.get().timestamp;
            if (opt.isEmpty()) return defaultValue;
            var timestamp = opt.get();
            return timestamp.get();
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            // INSERT INTO [keyspace_name.] table_name (column_list)
            sb.append("INSERT INTO ");
            table.toCQL(sb, formatter);
            sb.append(" (");
            for (Symbol col : values.keySet())
            {
                col.toCQL(sb, formatter);
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2); // 2 = ", "
            sb.append(")");
            // VALUES (column_values)
            formatter.section(sb);
            sb.append("VALUES (");
            for (Expression e : values.values())
            {
                e.toCQL(sb, formatter);
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2); // 2 = ", "
            sb.append(")");
            // [IF NOT EXISTS]
            if (ifNotExists)
            {
                formatter.section(sb);
                sb.append("IF NOT EXISTS");
            }
            // [USING TTL seconds | TIMESTAMP epoch_in_microseconds]
            if (using.isPresent())
            {
                formatter.section(sb);
                using.get().toCQL(sb, formatter);
            }
        }

        @Override
        public Stream<? extends Element> stream()
        {
            List<Element> elements = new ArrayList<>(1 + (using.isPresent() ? 1 : 0) + (values.size() * 2));
            elements.add(table);
            elements.addAll(values.keySet());
            elements.addAll(values.values());
            if (using.isPresent())
                elements.add(using.get());
            return elements.stream();
        }

        @Override
        public Statement visit(Visitor v)
        {
            var u = v.visit(this);
            if (u != this) return u;
            boolean updated = false;
            LinkedHashMap<Symbol, Expression> copied = new LinkedHashMap<>(values.size());
            for (var e : values.entrySet())
            {
                Symbol s = e.getKey();
                Symbol s2 = s.visit("INSERT", v);
                if (s != s2)
                    updated = true;
                Expression ex = e.getValue();
                Expression ex2 = ex.visit(v);
                if (ex != ex2)
                    updated = true;
                copied.put(s2, ex2);
            }
            if (!updated) return this;
            return new Insert(table, copied, ifNotExists, using);
        }

        @Override
        public boolean isCas()
        {
            return ifNotExists;
        }

        @Override
        public Mutation withoutTimestamp()
        {
            return new Insert(table, values, ifNotExists, using.isEmpty()
                                                          ? using
                                                          : using.flatMap(u -> u.withoutTimestamp()));
        }

        @Override
        public Insert withTimestamp(Timestamp timestamp)
        {
            return new Insert(table, values, ifNotExists, using.isEmpty()
                                                          ? Optional.of(new Using(Optional.empty(), Optional.of(timestamp)))
                                                          : using.map(u -> u.withTimestamp(timestamp)));
        }

        @Override
        public Optional<? extends CasCondition> casCondition()
        {
            return ifNotExists ? Optional.of(CasCondition.Simple.NotExists) : Optional.empty();
        }
    }

    public static class Update extends Mutation
    {
        public final Optional<Using> using;
        public final LinkedHashMap<Symbol, Expression> set;
        public final Conditional where;
        public final Optional<? extends CasCondition> casCondition;

        public Update(TableReference table, Optional<Using> using, LinkedHashMap<Symbol, Expression> set, Conditional where, Optional<? extends CasCondition> casCondition)
        {
            super(Mutation.Kind.UPDATE, table);
            this.using = using;
            this.set = set;
            this.where = where;
            this.casCondition = casCondition;
        }

        @Override
        public long timestampOrDefault(long defaultValue)
        {
            if (using.isEmpty()) return defaultValue;
            var opt = using.get().timestamp;
            if (opt.isEmpty()) return defaultValue;
            var timestamp = opt.get();
            return timestamp.get();
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            // UPDATE [keyspace_name.] table_name
            sb.append("UPDATE ");
            table.toCQL(sb, formatter);
            // [USING TTL time_value | USING TIMESTAMP timestamp_value]
            if (using.isPresent())
            {
                formatter.section(sb);
                using.get().toCQL(sb, formatter);
            }
            // SET assignment [, assignment] . . .
            formatter.section(sb);
            sb.append("SET");
            formatter.group(sb);
            for (var e : set.entrySet())
            {
                formatter.element(sb);
                e.getKey().toCQL(sb, formatter);
                // when a AssignmentOperator the `=` is added there so don't add
                //TODO this is super hacky...
                if (!(e.getValue() instanceof AssignmentOperator))
                    sb.append('=');
                e.getValue().toCQL(sb, formatter);
                sb.append(",");
            }
            sb.setLength(sb.length() - 1); // 1 = ","
            formatter.endgroup(sb);
            // WHERE row_specification
            toCQLWhere(this.where, sb, formatter);
            // [IF EXISTS | IF condition [AND condition] . . .] ;
            if (casCondition.isPresent())
            {
                formatter.section(sb);
                casCondition.get().toCQL(sb, formatter);
            }
        }

        @Override
        public Stream<? extends Element> stream()
        {
            List<Element> elements = new ArrayList<>(2 +
                                                     (using.isPresent() ? 1 : 0) +
                                                     (casCondition.isPresent() ? 1 : 0) +
                                                     (set.size() * 2));
            elements.add(table);
            if (using.isPresent())
                elements.add(using.get());
            for (var e : set.entrySet())
            {
                elements.add(e.getKey());
                elements.add(e.getValue());
            }
            elements.add(where);
            if (casCondition.isPresent())
                elements.add(casCondition.get());
            return elements.stream();
        }

        @Override
        public Statement visit(Visitor v)
        {
            var u = v.visit(this);
            if (u != this) return u;
            boolean updated = false;
            LinkedHashMap<Symbol, Expression> copied = new LinkedHashMap<>(set.size());
            for (var e : set.entrySet())
            {
                Symbol s = e.getKey().visit("UPDATE", v);
                if (s != e.getKey())
                    updated = true;
                Expression ex = e.getValue().visit(v);
                if (e.getValue() != ex)
                    updated = true;
                copied.put(s, ex);
            }
            Conditional copiedWhere = where.visit(v);
            if (where != copiedWhere)
                updated = true;
            Optional<? extends CasCondition> updatedCasCondition = casCondition;
            if (casCondition.isPresent())
            {
                CasCondition original = casCondition.get();
                var casCopy = original.visit(v);
                if (casCopy != original)
                {
                    updatedCasCondition = Optional.ofNullable(casCopy);
                    updated = true;
                }
            }

            if (!updated) return this;
            return new Update(table, using, copied, copiedWhere, updatedCasCondition);
        }

        @Override
        public boolean isCas()
        {
            return casCondition.isPresent();
        }

        @Override
        public Mutation withoutTimestamp()
        {
            return new Update(table, using.isEmpty() ? using : using.flatMap(u -> u.withoutTimestamp()), set, where, casCondition);
        }

        @Override
        public Update withTimestamp(Timestamp timestamp)
        {
            var updated = using.isEmpty()
                          ? Optional.of(new Using(Optional.empty(), Optional.of(timestamp)))
                          : using.map(u -> u.withTimestamp(timestamp));
            return new Update(table, updated, set, where, casCondition);
        }

        @Override
        public Optional<? extends CasCondition> casCondition()
        {
            return casCondition;
        }
    }

    public static class Delete extends Mutation
    {
        public final List<Symbol> columns;
        public final Optional<Timestamp> timestamp;
        public final Conditional where;
        public final Optional<? extends CasCondition> casCondition;

        public Delete(List<Symbol> columns,
                      TableReference table,
                      Optional<Timestamp> timestamp,
                      Conditional where,
                      Optional<? extends CasCondition> casCondition)
        {
            super(Mutation.Kind.DELETE, table);
            this.columns = columns;
            this.timestamp = timestamp;
            this.where = where;
            this.casCondition = casCondition;
        }

        @Override
        public long timestampOrDefault(long defaultValue)
        {
            var opt = timestamp;
            if (opt.isEmpty()) return defaultValue;
            var timestamp = opt.get();
            return timestamp.get();
        }

        /*
DELETE [column_name (term)][, ...]
FROM [keyspace_name.] table_name
[USING TIMESTAMP timestamp_value]
WHERE PK_column_conditions
[IF EXISTS | IF static_column_conditions]
         */
        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            // DELETE [column_name (term)][, ...]
            sb.append("DELETE");
            if (!columns.isEmpty())
            {
                sb.append(" ");
                for (var c : columns)
                {
                    c.toCQL(sb, formatter);
                    sb.append(", ");
                }
                sb.setLength(sb.length() - 2); // 2 = ", "
            }
            // FROM [keyspace_name.] table_name
            formatter.section(sb);
            sb.append("FROM ");
            table.toCQL(sb, formatter);
            // [USING TIMESTAMP timestamp_value]
            if (timestamp.isPresent())
            {
                formatter.section(sb);
                sb.append("USING ");
                timestamp.get().toCQL(sb, formatter);
            }
            // WHERE PK_column_conditions
            toCQLWhere(this.where, sb, formatter);
            // [IF EXISTS | IF static_column_conditions]
            if (casCondition.isPresent())
            {
                formatter.section(sb);
                casCondition.get().toCQL(sb, formatter);
            }
        }

        @Override
        public Stream<? extends Element> stream()
        {
            List<Element> elements = new ArrayList<>(columns.size() + 4);
            elements.addAll(columns);
            elements.add(table);
            if (timestamp.isPresent())
                elements.add(timestamp.get());
            elements.add(where);
            if (casCondition.isPresent())
                elements.add(casCondition.get());
            return elements.stream();
        }

        @Override
        public Statement visit(Visitor v)
        {
            var u = v.visit(this);
            if (u != this)
                return u;
            boolean updated = false;
            List<Symbol> copiedColumns = new ArrayList<>(columns.size());
            for (var s : columns)
            {
                var s2 = s.visit("DELETE", v);
                if (s != s2)
                    updated = true;
                copiedColumns.add(s2);
            }
            var copiedWhere = where.visit(v);
            if (copiedWhere != where)
                updated = true;
            Optional<? extends CasCondition> updatedCasCondition = casCondition;
            if (casCondition.isPresent())
            {
                CasCondition original = casCondition.get();
                var casCopy = original.visit(v);
                if (casCopy != original)
                {
                    updatedCasCondition = Optional.ofNullable(casCopy);
                    updated = true;
                }
            }

            if (!updated) return this;
            return new Delete(copiedColumns, table, timestamp, copiedWhere, updatedCasCondition);
        }

        @Override
        public boolean isCas()
        {
            return casCondition.isPresent();
        }

        @Override
        public Mutation withoutTimestamp()
        {
            return new Delete(columns, table, Optional.empty(), where, casCondition);
        }

        @Override
        public Delete withTimestamp(Timestamp timestamp)
        {
            return new Delete(columns, table, Optional.of(timestamp), where, casCondition);
        }

        @Override
        public Optional<? extends CasCondition> casCondition()
        {
            return casCondition;
        }
    }

    public static abstract class TableBasedBuilder<T, B extends TableBasedBuilder<T, B>> implements Conditional.EqBuilderPlus<B>
    {
        private final Kind kind;
        protected final TableMetadata metadata;
        protected final LinkedHashSet<Symbol> partitionColumns, clusteringColumns, primaryColumns, regularAndStatic, allColumns;
        private boolean includeKeyspace = true;
        private final PkAware pkAware;

        protected TableBasedBuilder(Kind kind, TableMetadata table)
        {
            this.kind = kind;
            this.metadata = table;

            // partition key is always required, so validate
            this.partitionColumns = toSet(table.partitionKeyColumns());
            this.clusteringColumns = toSet(table.clusteringColumns());
            this.primaryColumns = new LinkedHashSet<>();
            this.primaryColumns.addAll(partitionColumns);
            this.primaryColumns.addAll(clusteringColumns);
            this.regularAndStatic = new LinkedHashSet<>();
            this.regularAndStatic.addAll(toSet(table.regularAndStaticColumns()));
            this.allColumns = toSet(table.columnsInFixedOrder());
            this.pkAware = new PkAware(new DetailedTableMetadata(metadata));
        }

        protected Symbol find(String name)
        {
            return allColumns.stream().filter(s -> s.symbol.equals(name)).findAny().get();
        }

        public abstract T build();

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        protected void assertAllPksHaveEq()
        {
            pkAware.assertAllPksHaveEq();
        }

        protected void maybePkEq(Expression symbol)
        {
            pkAware.maybePkEq(symbol);
        }

        protected TableReference tableRef()
        {
            return includeKeyspace ? TableReference.from(metadata) : new TableReference(metadata.name);
        }
    }

    public static class InsertBuilder extends TableBasedBuilder<Insert, InsertBuilder>
    {
        private final LinkedHashMap<Symbol, Expression> values = new LinkedHashMap<>();
        private boolean ifNotExists = false;
        private @Nullable TTL ttl;
        private @Nullable Timestamp timestamp;

        protected InsertBuilder(TableMetadata table)
        {
            super(Kind.INSERT, table);
        }

        public InsertBuilder ifNotExists()
        {
            ifNotExists = true;
            return this;
        }

        public InsertBuilder timestamp(long value)
        {
            return timestamp(Literal.of(value));
        }

        public InsertBuilder timestamp(Value value)
        {
            this.timestamp = new Timestamp(value);
            return this;
        }

        public InsertBuilder ttl(Value value)
        {
            this.ttl = new TTL(value);
            return this;
        }

        public InsertBuilder ttl(int value)
        {
            return ttl(Bind.of(value));
        }

        @Override
        public InsertBuilder value(Symbol ref, Expression e)
        {
            maybePkEq(ref);
            values.put(ref, e);
            return this;
        }

        @Override
        public Insert build()
        {
            assertAllPksHaveEq();
            return new Insert(tableRef(),
                              new LinkedHashMap<>(values),
                              ifNotExists,
                              (ttl == null && timestamp == null) ? Optional.empty() : Optional.of(new Using(Optional.ofNullable(ttl), Optional.ofNullable(timestamp))));
        }
    }

    public interface WithTTl<B extends WithTTl<B>>
    {
        B ttl(Value value);
        default B ttl(int value)
        {
            return ttl(Bind.of(value));
        }
    }

    public interface WithTimestamp<B extends WithTimestamp<B>>
    {
        default B timestamp(long value)
        {
            return timestamp(Literal.of(value));
        }

        B timestamp(Value value);
    }

    public interface UpdateBuilder<B extends UpdateBuilder<B>> extends Conditional.ConditionalBuilder<B>,
                                                                       Conditional.EqBuilder<B>,
                                                                       WithTTl<B>, WithTimestamp<B>
    {
        B set(Symbol column, Expression value);
        default B set(String column, Object value, AbstractType<?> type)
        {
            return set(new Symbol(column, type), new Literal(value, type));
        }
        default B ifExists()
        {
            return ifCondition(CasCondition.Simple.Exists);
        }

        B ifCondition(CasCondition condition);

        Update build();
    }

    @SuppressWarnings("unchecked")
    public static class BaseUpdateBuilder<B extends BaseUpdateBuilder<B>> implements UpdateBuilder<B>
    {
        private TableReference table;
        private @Nullable TTL ttl;
        private @Nullable Timestamp timestamp;
        private final LinkedHashMap<Symbol, Expression> set = new LinkedHashMap<>();
        private final Conditional.Builder where = new Conditional.Builder();
        private @Nullable CasCondition casCondition;

        public BaseUpdateBuilder(TableReference table)
        {
            this.table = table;
        }

        @Override
        public B timestamp(Value value)
        {
            this.timestamp = new Timestamp(value);
            return (B) this;
        }

        @Override
        public B ttl(Value value)
        {
            this.ttl = new TTL(value);
            return (B) this;
        }

        @Override
        public B ifCondition(CasCondition condition)
        {
            casCondition = condition;
            return (B) this;
        }

        @Override
        public B set(Symbol column, Expression value)
        {
            set.put(column, value);
            return (B) this;
        }

        @Override
        public B where(Expression ref, Conditional.Where.Inequality kind, Expression expression)
        {
            where.where(ref, kind, expression);
            return (B) this;
        }

        @Override
        public B between(Expression ref, Expression start, Expression end)
        {
            where.between(ref, start, end);
            return (B) this;
        }

        @Override
        public B in(ReferenceExpression ref, List<? extends Expression> expressions)
        {
            where.in(ref, expressions);
            return (B) this;
        }

        @Override
        public B is(ReferenceExpression ref, Conditional.Is.Kind kind)
        {
            where.is(ref, kind);
            return (B) this;
        }

        @Override
        public Update build()
        {
            if (set.isEmpty())
                throw new IllegalStateException("Unable to create an Update without a SET section; set function was never called");
            return new Update(table,
                              (ttl == null && timestamp == null) ? Optional.empty() : Optional.of(new Using(Optional.ofNullable(ttl), Optional.ofNullable(timestamp))),
                              new LinkedHashMap<>(set),
                              where.build(),
                              Optional.ofNullable(casCondition));
        }
    }

    public static class BasicUpdateBuilder extends BaseUpdateBuilder<BasicUpdateBuilder>
    {
        public BasicUpdateBuilder(TableReference table)
        {
            super(table);
        }
    }

    private static class PkAware
    {
        private final Set<Symbol> neededPks = new HashSet<>();
        private final DetailedTableMetadata metadata;

        private PkAware(DetailedTableMetadata metadata)
        {
            this.metadata = metadata;
            neededPks.addAll(metadata.partitionColumns);
        }

        protected void assertAllPksHaveEq()
        {
            if (neededPks.isEmpty())
                return;
            throw new IllegalStateException("Attempted to create a mutation but not all partition columns have an equality condition; missing " + neededPks);
        }

        protected void maybePkEq(Expression symbol)
        {
            if (symbol instanceof Symbol)
                pkEq((Symbol) symbol);
        }

        private void pkEq(Symbol symbol)
        {
            neededPks.remove(symbol);
        }
    }

    public static class TableBasedUpdateBuilder extends BaseUpdateBuilder<TableBasedUpdateBuilder>
                                                implements UpdateBuilder<TableBasedUpdateBuilder>, Conditional.ConditionalBuilderPlus<TableBasedUpdateBuilder>
    {
        private final DetailedTableMetadata metadata;
        private final PkAware pkAware;

        protected TableBasedUpdateBuilder(TableMetadata table)
        {
            super(TableReference.from(table));
            this.metadata = new DetailedTableMetadata(table);
            this.pkAware = new PkAware(metadata);
        }

        @Override
        public TableBasedUpdateBuilder set(Symbol column, Expression value)
        {
            if (!metadata.regularAndStaticColumns.contains(column))
                throw new IllegalArgumentException("Attempted to set a non regular or static column " + column + "; expected " + metadata.regularAndStaticColumns);
            return super.set(column, value);
        }

        public TableBasedUpdateBuilder set(String column, int value)
        {
            Symbol symbol = metadata.find(column);
            if (!symbol.type().equals(Int32Type.instance))
                throw new AssertionError("Expected int type but given " + symbol.type().asCQL3Type());
            return set(symbol, Bind.of(value));
        }

        public TableBasedUpdateBuilder set(String column, Object value)
        {
            Symbol symbol = metadata.find(column);
            return set(symbol, new Bind(value, symbol.type()));
        }

        public TableBasedUpdateBuilder set(String column, Expression expression)
        {
            return set(metadata.find(column), expression);
        }

        public TableBasedUpdateBuilder set(String column, Function<Symbol, Expression> fn)
        {
            Symbol symbol = metadata.find(column);
            return set(symbol, fn.apply(symbol));
        }

        public TableBasedUpdateBuilder set(String column, String value)
        {
            Symbol symbol = metadata.find(column);
            return set(symbol, new Bind(symbol.type().asCQL3Type().fromCQLLiteral(value), symbol.type()));
        }

        @Override
        public TableBasedUpdateBuilder where(Expression ref, Conditional.Where.Inequality kind, Expression expression)
        {
            if (kind == Conditional.Where.Inequality.EQUAL)
                pkAware.maybePkEq(ref);
            return super.where(ref, kind, expression);
        }

        @Override
        public TableBasedUpdateBuilder in(ReferenceExpression ref, List<? extends Expression> expressions)
        {
            pkAware.maybePkEq(ref);
            return super.in(ref, expressions);
        }

        @Override
        public Update build()
        {
            pkAware.assertAllPksHaveEq();
            return super.build();
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata.metadata;
        }
    }

    public static class DeleteBuilder extends TableBasedBuilder<Delete, DeleteBuilder> implements Conditional.ConditionalBuilderPlus<DeleteBuilder>
    {
        private final List<Symbol> columns = new ArrayList<>();
        private @Nullable Timestamp timestamp = null;
        private final Conditional.Builder where = new Conditional.Builder();
        //TODO (now): casCondition
        private @Nullable CasCondition casCondition;

        public DeleteBuilder(TableMetadata table)
        {
            super(Kind.DELETE, table);
        }

        public DeleteBuilder ifExists()
        {
            casCondition = CasCondition.Simple.Exists;
            return this;
        }

        public DeleteBuilder ifCondition(CasCondition condition)
        {
            casCondition = condition;
            return this;
        }

        public List<Symbol> columns()
        {
            return Collections.unmodifiableList(columns);
        }

        public DeleteBuilder columns(String... names)
        {
            Stream.of(names).map(this::find).forEach(this::column);
            return this;
        }

        public DeleteBuilder column(String columnName)
        {
            return column(find(columnName));
        }

        public DeleteBuilder column(Symbol symbol)
        {
            if (!regularAndStatic.contains(symbol))
                throw new IllegalArgumentException("Can not delete column " + symbol + "; only regular/static columns can be deleted, expected " + regularAndStatic);
            columns.add(symbol);
            return this;
        }

        public DeleteBuilder column(Symbol... symbols)
        {
            return column(Arrays.asList(symbols));
        }

        public DeleteBuilder column(List<Symbol> symbols)
        {
            symbols.forEach(this::column);
            return this;
        }

        public DeleteBuilder timestamp(long value)
        {
            return timestamp(Literal.of(value));
        }

        public DeleteBuilder timestamp(Value value)
        {
            this.timestamp = new Timestamp(value);
            return this;
        }

        @Override
        public DeleteBuilder where(Expression ref, Conditional.Where.Inequality kind, Expression expression)
        {
            if (kind == Conditional.Where.Inequality.EQUAL)
                maybePkEq(ref);
            where.where(ref, kind, expression);
            return this;
        }

        @Override
        public DeleteBuilder between(Expression ref, Expression start, Expression end)
        {
            where.between(ref, start, end);
            return this;
        }

        @Override
        public DeleteBuilder in(ReferenceExpression ref, List<? extends Expression> expressions)
        {
            maybePkEq(ref);
            where.in(ref, expressions);
            return this;
        }

        @Override
        public DeleteBuilder is(ReferenceExpression ref, Conditional.Is.Kind kind)
        {
            where.is(ref, kind);
            return this;
        }

        @Override
        public Delete build()
        {
            assertAllPksHaveEq();
            return new Delete(columns.isEmpty() ? Collections.emptyList() : new ArrayList<>(columns),
                              tableRef(),
                              Optional.ofNullable(timestamp),
                              where.build(),
                              Optional.ofNullable(casCondition));
        }
    }
}

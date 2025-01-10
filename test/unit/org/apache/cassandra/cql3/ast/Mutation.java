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
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ASTGenerators;

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

    public abstract boolean isCas();

    public final Mutation.Kind mutationKind()
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

    public static UpdateBuilder update(TableMetadata metadata)
    {
        return new UpdateBuilder(metadata);
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
    }

    public static class Using implements Element
    {
        public final Optional<TTL> ttl;
        public final Optional<Timestamp> timestamp;

        public Using(Optional<TTL> ttl, Optional<Timestamp> timestamp)
        {
            this.ttl = ttl;
            this.timestamp = timestamp;
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            if (ttl.isEmpty() && timestamp.isEmpty())
                return;
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

            if (!updated) return this;
            return new Update(table, using, copied, copiedWhere, casCondition);
        }

        @Override
        public boolean isCas()
        {
            return casCondition.isPresent();
        }
    }

    public static class Delete extends Mutation
    {
        public final List<Symbol> columns;
        public final Optional<Timestamp> using;
        public final Conditional where;
        public final Optional<? extends CasCondition> casCondition;

        public Delete(List<Symbol> columns,
                      TableReference table,
                      Optional<Timestamp> using,
                      Conditional where,
                      Optional<? extends CasCondition> casCondition)
        {
            super(Mutation.Kind.DELETE, table);
            this.columns = columns;
            this.using = using;
            this.where = where;
            this.casCondition = casCondition;
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
            if (using.isPresent())
            {
                formatter.section(sb);
                using.get().toCQL(sb, formatter);
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
            if (using.isPresent())
                elements.add(using.get());
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

            if (!updated) return this;
            return new Delete(copiedColumns, table, using, copiedWhere, casCondition);
        }

        @Override
        public boolean isCas()
        {
            return casCondition.isPresent();
        }
    }

    public static abstract class BaseBuilder<T, B extends BaseBuilder<T, B>>
    {
        private final Mutation.Kind kind;
        private final TableMetadata metadata;
        protected final LinkedHashSet<Symbol> partitionColumns, clusteringColumns, primaryColumns, regularAndStatic, allColumns;
        private boolean includeKeyspace = true;
        private final Set<Symbol> neededPks = new HashSet<>();

        protected BaseBuilder(Kind kind, TableMetadata table)
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
            this.allColumns = toSet(ASTGenerators.safeColumns(table));
            neededPks.addAll(partitionColumns);
        }

        public abstract T build();

        protected void assertAllPksHaveEq()
        {
            if (neededPks.isEmpty())
                return;
            throw new IllegalStateException("Attempted to create a " + kind + " but not all partition columns have an equality condition; missing " + neededPks);
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

        public B includeKeyspace(boolean value)
        {
            this.includeKeyspace = value;
            return (B) this;
        }

        public B includeKeyspace()
        {
            return includeKeyspace(true);
        }

        public B excludeKeyspace()
        {
            return includeKeyspace(false);
        }

        protected TableReference tableRef()
        {
            return includeKeyspace ? TableReference.from(metadata) : new TableReference(metadata.name);
        }
    }

    public static class InsertBuilder extends BaseBuilder<Insert, InsertBuilder> implements Conditional.EqBuilder<InsertBuilder>
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

    public static class UpdateBuilder extends BaseBuilder<Update, UpdateBuilder> implements Conditional.ConditionalBuilder<UpdateBuilder>
    {
        private @Nullable TTL ttl;
        private @Nullable Timestamp timestamp;
        private final LinkedHashMap<Symbol, Expression> set = new LinkedHashMap<>();
        private final Conditional.Builder where = new Conditional.Builder();
        private @Nullable CasCondition casCondition;

        protected UpdateBuilder(TableMetadata table)
        {
            super(Kind.UPDATE, table);
        }

        public UpdateBuilder timestamp(Value value)
        {
            this.timestamp = new Timestamp(value);
            return this;
        }

        public UpdateBuilder ttl(Value value)
        {
            this.ttl = new TTL(value);
            return this;
        }

        public UpdateBuilder ttl(int value)
        {
            return ttl(Bind.of(value));
        }

        public UpdateBuilder ifExists()
        {
            casCondition = CasCondition.Simple.Exists;
            return this;
        }

        public UpdateBuilder ifCondition(CasCondition condition)
        {
            casCondition = condition;
            return this;
        }

        public UpdateBuilder set(Symbol column, Expression value)
        {
            if (!regularAndStatic.contains(column))
                throw new IllegalArgumentException("Attempted to set a non regular or static column " + column + "; expected " + regularAndStatic);
            set.put(column, value);
            return this;
        }

        public UpdateBuilder set(String column, int value)
        {
            return set(new Symbol(column, Int32Type.instance), Bind.of(value));
        }

        @Override
        public UpdateBuilder where(Expression ref, Conditional.Where.Inequality kind, Expression expression)
        {
            if (kind == Conditional.Where.Inequality.EQUAL)
                maybePkEq(ref);
            where.where(ref, kind, expression);
            return this;
        }

        @Override
        public UpdateBuilder between(Expression ref, Expression start, Expression end)
        {
            where.between(ref, start, end);
            return this;
        }

        @Override
        public UpdateBuilder in(ReferenceExpression ref, List<? extends Expression> expressions)
        {
            maybePkEq(ref);
            where.in(ref, expressions);
            return this;
        }

        @Override
        public UpdateBuilder is(Symbol ref, Conditional.Is.Kind kind)
        {
            where.is(ref, kind);
            return this;
        }

        @Override
        public Update build()
        {
            assertAllPksHaveEq();
            if (set.isEmpty())
                throw new IllegalStateException("Unable to create an Update without a SET section; set function was never called");

            return new Update(tableRef(),
                              (ttl == null && timestamp == null) ? Optional.empty() : Optional.of(new Using(Optional.ofNullable(ttl), Optional.ofNullable(timestamp))),
                              new LinkedHashMap<>(set),
                              where.build(),
                              Optional.ofNullable(casCondition));
        }
    }

    public static class DeleteBuilder extends BaseBuilder<Delete, DeleteBuilder> implements Conditional.ConditionalBuilder<DeleteBuilder>
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
        public DeleteBuilder is(Symbol ref, Conditional.Is.Kind kind)
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

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

package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import com.google.common.collect.Iterables;

import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.cql3.KnownIssue;
import org.apache.cassandra.cql3.ast.AssignmentOperator;
import org.apache.cassandra.cql3.ast.Bind;
import org.apache.cassandra.cql3.ast.CasCondition;
import org.apache.cassandra.cql3.ast.Conditional;
import org.apache.cassandra.cql3.ast.CreateIndexDDL;
import org.apache.cassandra.cql3.ast.Expression;
import org.apache.cassandra.cql3.ast.Literal;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Operator;
import org.apache.cassandra.cql3.ast.Reference;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.cql3.ast.TableReference;
import org.apache.cassandra.cql3.ast.Txn;
import org.apache.cassandra.cql3.ast.TypeHint;
import org.apache.cassandra.cql3.ast.Value;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;

import org.apache.cassandra.harry.model.ASTSingleTableModel;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.Constraint;

import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.apache.cassandra.utils.Generators.SYMBOL_GEN;

public class ASTGenerators
{
    public static final EnumSet<KnownIssue> IGNORED_ISSUES = KnownIssue.ignoreAll();

    public static Gen<LinkedHashMap<Symbol, Object>> columnValues(List<Symbol> columns)
    {
        List<Gen<?>> gens = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++)
            gens.add(getTypeSupport(columns.get(i).type()).valueGen);
        return rs -> {
            LinkedHashMap<Symbol, Object> vs = new LinkedHashMap<>();
            for (int i = 0; i < columns.size(); i++)
                vs.put(columns.get(i), gens.get(i).generate(rs));
            return vs;
        };
    }

    public static Select select(TableMetadata metadata, LinkedHashMap<Symbol, Object> map)
    {
        Select.TableBasedBuilder builder = Select.builder(metadata);
        for (var e : map.entrySet())
            builder.value(e.getKey(), e.getValue());
        return builder.build();
    }

    static Gen<Value> valueGen(Object value, AbstractType<?> type)
    {
        Gen<Boolean> bool = SourceDSL.booleans().all();
        return rnd -> bool.generate(rnd) ? new Bind(value, type) : new Literal(value, type);
    }

    static Gen<Value> valueGen(AbstractType<?> type)
    {
        Gen<?> v = getTypeSupport(type).valueGen;
        return rnd -> valueGen(v.generate(rnd), type).generate(rnd);
    }

    private static <K, V> Map<K, V> assertDeterministic(Map<K, V> map)
    {
        if (map instanceof LinkedHashMap || map instanceof TreeMap || map instanceof EnumMap)
            return map;
        if (map.size() == 1)
            return map;
        throw new AssertionError("Unsupported map type: " + map.getClass());
    }

    private static Gen<AssignmentOperator> assignmentOperatorGen(EnumSet<AssignmentOperator.Kind> allowed, Expression right)
    {
        if (allowed.isEmpty())
            throw new IllegalArgumentException("Unable to create a operator gen for empty set of allowed operators");
        if (allowed.size() == 1)
            return SourceDSL.arbitrary().constant(new AssignmentOperator(Iterables.getFirst(allowed, null), right));

        Gen<AssignmentOperator.Kind> kind = SourceDSL.arbitrary().pick(new ArrayList<>(allowed));
        return kind.map(k -> new AssignmentOperator(k, right));
    }

    public static Gen<Operator> operatorGen(Set<Operator.Kind> allowed, Expression e, Gen<Value> paramValueGen)
    {
        if (allowed.isEmpty())
            throw new IllegalArgumentException("Unable to create a operator gen for empty set of allowed operators");
        Gen<Operator.Kind> kindGen = allowed.size() == 1 ?
                                     SourceDSL.arbitrary().constant(Iterables.getFirst(allowed, null))
                                                         : SourceDSL.arbitrary().pick(new ArrayList<>(allowed));
        Gen<Boolean> bool = SourceDSL.booleans().all();
        return rnd -> {
            Gen<Value> valueGen = paramValueGen;
            Operator.Kind kind = kindGen.generate(rnd);
            if (kind == Operator.Kind.SUBTRACT && e.type() instanceof MapType)
            {
                // `map - set` not `map - map`
                valueGen = valueGen.map(v -> {
                    // since we know E is of type map we know the value is a map
                    Map<?, ?> map = (Map<?, ?>) v.value();
                    Set<?> newValue = map.keySet();
                    SetType<Object> newType = SetType.getInstance(((MapType) e.type()).nameComparator(), false);
                    return v.with(newValue, newType);
                });
            }
            Expression other = valueGen.generate(rnd);
            Expression left, right;
            if (bool.generate(rnd))
            {
                left = e;
                right = other;
            }
            else
            {
                left = other;
                right = e;
            }
            //TODO (correctness): "(smallint) ? - 16250" failed, but is this general or is it a small int thing?
            //NOTE: (smallint) of -11843 and 3749 failed as well...
            //NOTE: (long) was found and didn't fail...
            //NOTE: see https://the-asf.slack.com/archives/CK23JSY2K/p1724819303058669 - varint didn't fail but serialized using int32 which causes equality mismatches for pk/ck lookups
            if ((e.type().unwrap() == ShortType.instance
                 || e.type().unwrap() == IntegerType.instance)
                && IGNORED_ISSUES.contains(KnownIssue.SHORT_AND_VARINT_GET_INT_FUNCTIONS)) // seed=7525457176675272023L
            {
                left = new TypeHint(left);
                right = new TypeHint(right);
            }
            return new Operator(kind, TypeHint.maybeApplyTypeHint(left), TypeHint.maybeApplyTypeHint(right));
        };
    }

    public static class ExpressionBuilder
    {
        private final AbstractType<?> type;
        private final EnumSet<Operator.Kind> allowedOperators;
        private Gen<?> valueGen;
        private Gen<Boolean> useOperator = SourceDSL.booleans().all();
        private Gen<Boolean> useEmpty = SourceDSL.arbitrary().constant(false);
        private Gen<Boolean> useNull = SourceDSL.arbitrary().constant(false);
        private BiFunction<Object, AbstractType<?>, Gen<Value>> literalOrBindGen = ASTGenerators::valueGen;

        public ExpressionBuilder(AbstractType<?> type)
        {
            this.type = type.unwrap();
            this.valueGen = getTypeSupport(this.type).valueGen;
            this.allowedOperators = Operator.supportsOperators(this.type);
        }

        public ExpressionBuilder allowEmpty()
        {
            if (!type.allowsEmpty()) return this;
            useEmpty = SourceDSL.integers().between(1, 100).map(i -> i < 10);
            return this;
        }

        public ExpressionBuilder disallowEmpty()
        {
            useEmpty = i -> false;
            return this;
        }

        public ExpressionBuilder allowNull()
        {
            useNull = SourceDSL.integers().between(1, 100).map(i -> i < 10);
            return this;
        }

        public ExpressionBuilder withOperators()
        {
            useOperator = i -> true;
            return this;
        }

        public ExpressionBuilder withOperators(Gen<Boolean> useOperator)
        {
            this.useOperator = Objects.requireNonNull(useOperator);
            return this;
        }

        public ExpressionBuilder withoutOperators()
        {
            useOperator = i -> false;
            return this;
        }

        public ExpressionBuilder allowOperators()
        {
            useOperator = SourceDSL.booleans().all();
            return this;
        }

        public ExpressionBuilder withLiteralOrBindGen(BiFunction<Object, AbstractType<?>, Gen<Value>> literalOrBindGen)
        {
            this.literalOrBindGen = literalOrBindGen;
            return this;
        }

        public Gen<Expression> build()
        {
            //TODO (coverage): rather than single level operators, allow nested (a + b + c + d)
            Gen<Value> leaf = rs -> literalOrBindGen.apply(valueGen.generate(rs), type).generate(rs);
            return rs -> {
                if (useNull.generate(rs))
                    return new Bind(null, type);
                if (useEmpty.generate(rs))
                    return new Bind(ByteBufferUtil.EMPTY_BYTE_BUFFER, type);
                Expression e = leaf.generate(rs);
                if (!allowedOperators.isEmpty() && useOperator.generate(rs))
                    e = operatorGen(allowedOperators, e, leaf).generate(rs);
                return e;
            };
        }
    }

    public static class SelectGenBuilder
    {
        private final TableMetadata metadata;
        private Gen<List<Expression>> selectGen;
        private Gen<Map<Symbol, Expression>> keyGen;
        private Gen<Optional<Value>> limitGen;
        private BiFunction<Object, AbstractType<?>, Gen<Value>> literalOrBindGen = ASTGenerators::valueGen;

        public SelectGenBuilder(TableMetadata metadata)
        {
            this.metadata = Objects.requireNonNull(metadata);
            this.selectGen = selectColumns(metadata);
            this.keyGen = partitionKeyGen(metadata);

            withDefaultLimit();
        }

        public SelectGenBuilder withLiteralOrBindGen(BiFunction<Object, AbstractType<?>, Gen<Value>> literalOrBindGen)
        {
            this.literalOrBindGen = literalOrBindGen;
            return this;
        }

        public SelectGenBuilder withSelectStar()
        {
            selectGen = ignore -> Collections.emptyList();
            return this;
        }

        public SelectGenBuilder withDefaultLimit()
        {
            Gen<Optional<Value>> non = ignore -> Optional.empty();
            Constraint limitLength = Constraint.between(1, 10_000);
            Gen<Optional<Value>> positive = rnd -> Optional.of(valueGen(Math.toIntExact(rnd.next(limitLength)), Int32Type.instance).generate(rnd));
            limitGen = non.mix(positive);
            return this;
        }

        public SelectGenBuilder withLimit1()
        {
            this.limitGen = rnd -> Optional.of(valueGen(1, Int32Type.instance).generate(rnd));
            return this;
        }

        public SelectGenBuilder withoutLimit()
        {
            this.limitGen = ignore -> Optional.empty();
            return this;
        }

        public SelectGenBuilder withKeys(Gen<? extends Map<Symbol, Object>> partitionKeys, Gen<? extends Map<Symbol, Object>> clusteringKeys)
        {
            keyGen = rs -> {
                Map<Symbol, Expression> keys = new LinkedHashMap<>();
                for (Map.Entry<Symbol, Object> e : assertDeterministic(partitionKeys.generate(rs)).entrySet())
                    keys.put(e.getKey(), literalOrBindGen.apply(e.getValue(), e.getKey().type()).generate(rs));
                if (!metadata.clusteringColumns().isEmpty())
                {
                    for (Map.Entry<Symbol, Object> e : assertDeterministic(clusteringKeys.generate(rs)).entrySet())
                        keys.put(e.getKey(), literalOrBindGen.apply(e.getValue(), e.getKey().type()).generate(rs));
                }
                return keys;
            };
            return this;
        }

        public Gen<Select> build()
        {
            Optional<TableReference> ref = Optional.of(TableReference.from(metadata));
            return rnd -> {
                List<Expression> select = selectGen.generate(rnd);
                Conditional keyClause = and(keyGen.generate(rnd));
                Optional<Value> limit = limitGen.generate(rnd);
                return new Select(select, ref, Optional.of(keyClause), Optional.empty(), limit);
            };
        }

        private static Conditional and(Map<Symbol, Expression> data)
        {
            Conditional.Builder builder = new Conditional.Builder();
            for (Map.Entry<Symbol, Expression> e : assertDeterministic(data).entrySet())
                builder.where(e.getKey(), Conditional.Where.Inequality.EQUAL, e.getValue());
            return builder.build();
        }

        private static Gen<List<Expression>> selectColumns(TableMetadata metadata)
        {
            List<ColumnMetadata> columns = metadata.columnsInFixedOrder();
            Constraint between = Constraint.between(0, columns.size() - 1);
            Gen<int[]> indexGen = rnd -> {
                int size = Math.toIntExact(rnd.next(between)) + 1;
                Set<Integer> dedup = new LinkedHashSet<>();
                while (dedup.size() < size)
                    dedup.add(Math.toIntExact(rnd.next(between)));
                return dedup.stream().mapToInt(Integer::intValue).toArray();
            };
            return rnd -> {
                int[] indexes = indexGen.generate(rnd);
                List<Expression> es = new ArrayList<>(indexes.length);
                IntStream.of(indexes).mapToObj(columns::get).forEach(c -> es.add(new Symbol(c)));
                return es;
            };
        }

        private static Gen<Map<Symbol, Expression>> partitionKeyGen(TableMetadata metadata)
        {
            Map<ColumnMetadata, Gen<?>> gens = new LinkedHashMap<>();
            for (ColumnMetadata col : metadata.columnsInFixedOrder())
                gens.put(col, getTypeSupport(col.type).valueGen);
            return rnd -> {
                Map<Symbol, Expression> output = new LinkedHashMap<>();
                for (ColumnMetadata col : metadata.partitionKeyColumns())
                    output.put(new Symbol(col), gens.get(col)
                                                    .map(o -> valueGen(o, col.type).generate(rnd))
                                                    .generate(rnd));
                return output;
            };
        }
    }

    public static class MutationGenBuilder
    {
        public enum DeleteKind { Partition, Row, Column }
        private final TableMetadata metadata;
        private final LinkedHashSet<Symbol> allColumns;
        private final LinkedHashSet<Symbol> partitionColumns, clusteringColumns;
        private final LinkedHashSet<Symbol> primaryColumns;
        private final LinkedHashSet<Symbol> regularColumns, staticColumns, regularAndStaticColumns;
        private Gen<Mutation.Kind> kindGen = SourceDSL.arbitrary().enumValues(Mutation.Kind.class);
        private Gen<OptionalInt> ttlGen = SourceDSL.integers().between(1, Math.toIntExact(TimeUnit.DAYS.toSeconds(10))).map(i -> i % 2 == 0 ? OptionalInt.empty() : OptionalInt.of(i));
        private Gen<OptionalLong> timestampGen = SourceDSL.longs().between(1, Long.MAX_VALUE).map(i -> i % 2 == 0 ? OptionalLong.empty() : OptionalLong.of(i));
        private Collection<Reference> references = Collections.emptyList();
        private Gen<Boolean> withCasGen = SourceDSL.booleans().all();
        private Gen<Boolean> useCasIf = SourceDSL.booleans().all();
        private BiFunction<RandomnessSource, List<Symbol>, List<Symbol>> ifConditionFilter = (rnd, symbols) -> symbols;
        private Gen<DeleteKind> deleteKindGen = SourceDSL.arbitrary().enumValues(DeleteKind.class);
        private Map<Symbol, ExpressionBuilder> columnExpressions = new LinkedHashMap<>();
        private boolean allowPartitionOnlyUpdate = true;
        private boolean allowPartitionOnlyInsert = true;
        private boolean allowUpdateMultiplePartitionKeys = true;
        private boolean allowUpdateMultipleClusteringKeys = true;
        private EnumSet<KnownIssue> ignoreIssues = IGNORED_ISSUES;

        public MutationGenBuilder(TableMetadata metadata)
        {
            this.metadata = Objects.requireNonNull(metadata);
            this.allColumns = Mutation.toSet(metadata::allColumnsInSelectOrder);
            this.partitionColumns = Mutation.toSet(metadata.partitionKeyColumns());
            this.clusteringColumns = Mutation.toSet(metadata.clusteringColumns());
            this.primaryColumns = Mutation.toSet(metadata.primaryKeyColumns());
            this.regularColumns = Mutation.toSet(metadata.regularColumns());
            this.staticColumns = Mutation.toSet(metadata.staticColumns());
            this.regularAndStaticColumns = new LinkedHashSet<>();
            regularAndStaticColumns.addAll(staticColumns);
            regularAndStaticColumns.addAll(regularColumns);

            for (Symbol symbol : allColumns)
                columnExpressions.put(symbol, new ExpressionBuilder(symbol.type()));
        }

        public MutationGenBuilder withIgnoreIssues(EnumSet<KnownIssue> ignoreIssues)
        {
            this.ignoreIssues = Objects.requireNonNull(ignoreIssues);
            return this;
        }

        public MutationGenBuilder withAllowPartitionOnlyUpdate(boolean value)
        {
            this.allowPartitionOnlyUpdate = value;
            return this;
        }

        public MutationGenBuilder withAllowPartitionOnlyInsert(boolean value)
        {
            this.allowPartitionOnlyInsert = value;
            return this;
        }

        public MutationGenBuilder allowUpdateMultiplePartitionKeys()
        {
            return withAllowUpdateMultiplePartitionKeys(true);
        }

        public MutationGenBuilder disallowUpdateMultiplePartitionKeys()
        {
            return withAllowUpdateMultiplePartitionKeys(false);
        }

        private MutationGenBuilder withAllowUpdateMultiplePartitionKeys(boolean allowUpdateMultiplePartitionKeys)
        {
            this.allowUpdateMultiplePartitionKeys = allowUpdateMultiplePartitionKeys;
            return this;
        }

        public MutationGenBuilder allowUpdateMultipleClusteringKeys()
        {
            return withAllowUpdateMultipleClusteringKeys(true);
        }

        public MutationGenBuilder disallowUpdateMultipleClusteringKeys()
        {
            return withAllowUpdateMultipleClusteringKeys(false);
        }

        private MutationGenBuilder withAllowUpdateMultipleClusteringKeys(boolean allowUpdateMultipleClusteringKeys)
        {
            this.allowUpdateMultipleClusteringKeys = allowUpdateMultipleClusteringKeys;
            return this;
        }

        /**
         * Tells the generator to avoid producing updates that do multiple partition and clustering keys.
         */
        public MutationGenBuilder disallowUpdateMultipleRows()
        {
            return disallowUpdateMultiplePartitionKeys().disallowUpdateMultipleClusteringKeys();
        }

        public MutationGenBuilder withColumnExpressions(Consumer<ExpressionBuilder> fn)
        {
            for (Symbol symbol : allColumns)
                fn.accept(columnExpressions.get(symbol));
            return this;
        }

        public MutationGenBuilder allowEmpty(Symbol symbol)
        {
            columnExpressions.get(symbol).allowEmpty();
            return this;
        }

        public MutationGenBuilder disallowEmpty()
        {
            columnExpressions.values().forEach(ExpressionBuilder::disallowEmpty);
            return this;
        }

        public MutationGenBuilder disallowEmpty(Symbol symbol)
        {
            columnExpressions.get(symbol).disallowEmpty();
            return this;
        }

        public MutationGenBuilder allowNull(Symbol symbol)
        {
            columnExpressions.get(symbol).allowNull();
            return this;
        }

        public MutationGenBuilder withDeletionKind(Gen<DeleteKind> deleteKindGen)
        {
            this.deleteKindGen = deleteKindGen;
            return this;
        }

        public MutationGenBuilder withDeletionKind(DeleteKind... values)
        {
            return withDeletionKind(SourceDSL.arbitrary().pick(values));
        }

        public MutationGenBuilder withLiteralOrBindGen(BiFunction<Object, AbstractType<?>, Gen<Value>> literalOrBindGen)
        {
            columnExpressions.values().forEach(e -> e.withLiteralOrBindGen(literalOrBindGen));
            return this;
        }

        public MutationGenBuilder withTxnSafe()
        {
            return withoutTimestamp()
                   .withoutTtl()
                   .withoutTransaction();
        }

        public MutationGenBuilder withoutTransaction()
        {
            withoutCas();
            return this;
        }

        public MutationGenBuilder withCas()
        {
            withCasGen = SourceDSL.arbitrary().constant(true);
            return this;
        }

        public MutationGenBuilder withoutCas()
        {
            withCasGen = SourceDSL.arbitrary().constant(false);
            return this;
        }

        public MutationGenBuilder withCasGen(Gen<Boolean> withCasGen)
        {
            this.withCasGen = Objects.requireNonNull(withCasGen);
            return this;
        }

        public MutationGenBuilder withCasIf()
        {
            useCasIf = SourceDSL.arbitrary().constant(true);
            return this;
        }

        public MutationGenBuilder withoutCasIf()
        {
            useCasIf = SourceDSL.arbitrary().constant(false);
            return this;
        }

        public MutationGenBuilder withCasIfGen(Gen<Boolean> gen)
        {
            useCasIf = Objects.requireNonNull(gen);
            return this;
        }

        public MutationGenBuilder withIfColumnFilter(BiFunction<RandomnessSource, List<Symbol>, List<Symbol>> ifConditionFilter)
        {
            this.ifConditionFilter = Objects.requireNonNull(ifConditionFilter);
            return this;
        }

        public MutationGenBuilder withoutTimestamp()
        {
            timestampGen = ignore -> OptionalLong.empty();
            return this;
        }

        public MutationGenBuilder withoutTtl()
        {
            ttlGen = ignore -> OptionalInt.empty();
            return this;
        }

        public MutationGenBuilder withOperators()
        {
            columnExpressions.values().forEach(e -> e.withOperators());
            return this;
        }

        public MutationGenBuilder withoutOperators()
        {
            columnExpressions.values().forEach(e -> e.withoutOperators());
            return this;
        }

        public MutationGenBuilder withReferences(Collection<Reference> references)
        {
            this.references = references;
            return this;
        }

        private Gen<? extends Map<Symbol, Object>> partitionValueGen = null;
        private Gen<? extends Map<Symbol, Object>> clusteringValueGen = null;

        public MutationGenBuilder withPartitions(Gen<? extends Map<Symbol, Object>> values)
        {
            this.partitionValueGen = values;
            return this;
        }

        public MutationGenBuilder withClusterings(Gen<? extends Map<Symbol, Object>> values)
        {
            this.clusteringValueGen = values;
            return this;
        }

        private static void values(RandomnessSource rnd,
                                   Map<Symbol, ExpressionBuilder> columnExpressions,
                                   Conditional.EqBuilder<?> builder,
                                   LinkedHashSet<Symbol> columns,
                                   @Nullable Gen<? extends Map<Symbol, Object>> gen)
        {
            if (gen != null)
            {
                Map<Symbol, Object> map = gen.generate(rnd);
                for (Map.Entry<Symbol, ?> e : assertDeterministic(map).entrySet())
                    builder.value(e.getKey(), valueGen(e.getValue(), e.getKey().type()).generate(rnd));
            }
            else
            {
                for (Symbol s : columns)
                    builder.value(s, columnExpressions.get(s).build().generate(rnd));
            }
        }

        private static void where(RandomnessSource rnd,
                                  Map<Symbol, ExpressionBuilder> columnExpressions,
                                  Conditional.ConditionalBuilder<?> builder,
                                  LinkedHashSet<Symbol> columns,
                                  @Nullable Gen<? extends Map<Symbol, Object>> gen)
        {
            if (gen != null)
            {
                Map<Symbol, Object> map = gen.generate(rnd);
                for (Map.Entry<Symbol, ?> e : assertDeterministic(map).entrySet())
                    builder.value(e.getKey(), valueGen(e.getValue(), e.getKey().type()).generate(rnd));
                return;
            }

            for (Symbol s : columns)
            {
                if (SourceDSL.booleans().all().generate(rnd))
                {
                    builder.value(s, columnExpressions.get(s).build().generate(rnd));
                    continue;
                }
                var valueGen = columnExpressions.get(s).build();
                builder.in(s, SourceDSL.lists().of(valueGen).ofSizeBetween(1, 3).generate(rnd));
            }
        }

        public Gen<Mutation> build()
        {
            Gen<Boolean> bool = SourceDSL.booleans().all();
            Map<? extends AbstractType<?>, List<Reference>> typeToReference = references.stream().collect(Collectors.groupingBy(Reference::type));
            if (allowUpdateMultipleClusteringKeys
                && ignoreIssues.contains(KnownIssue.STATIC_LIST_APPEND_WITH_CLUSTERING_IN)
                && staticColumns.stream().anyMatch(s -> s.type().isMultiCell() && s.type().getClass() == ListType.class))
                allowUpdateMultipleClusteringKeys = false;
            return rnd -> {
                Mutation.Kind kind = kindGen.generate(rnd);
                // when there are not non-primary-columns then can't support UPDATE
                if (kind == Mutation.Kind.UPDATE && regularColumns.isEmpty())
                {
                    int i;
                    int maxRetries = 42;
                    for (i = 0; i < maxRetries && kind == Mutation.Kind.UPDATE; i++)
                        kind = kindGen.generate(rnd);
                    if (i == maxRetries)
                        throw new IllegalArgumentException("Kind gen kept returning UPDATE, but not supported when there are no non-primary columns");
                }
                boolean isCas = withCasGen.generate(rnd);
                boolean isTransaction = isCas; //TODO (coverage): add accord support
                switch (kind)
                {
                    case INSERT:
                    {
                        Mutation.InsertBuilder builder = Mutation.insert(metadata);
                        if (isCas)
                            builder.ifNotExists();
                        var ttl = ttlGen.generate(rnd);
                        if (ttl.isPresent())
                            builder.ttl(valueGen(ttl.getAsInt(), Int32Type.instance).generate(rnd));
                        var timestamp = timestampGen.generate(rnd);
                        if (timestamp.isPresent())
                            builder.timestamp(valueGen(timestamp.getAsLong(), LongType.instance).generate(rnd));
                        values(rnd, columnExpressions, builder, partitionColumns, partitionValueGen);
                        if (!staticColumns.isEmpty() && allowPartitionOnlyInsert && bool.generate(rnd))
                        {
                            var columnsToGenerate = new LinkedHashSet<>(subset(rnd, staticColumns));
                            generateRemaining(rnd, bool, Mutation.Kind.INSERT, isTransaction, typeToReference, builder, columnsToGenerate);
                            return builder.build();
                        }
                        values(rnd, columnExpressions, builder, clusteringColumns, clusteringValueGen);
                        LinkedHashSet<Symbol> columnsToGenerate;
                        if (regularAndStaticColumns.isEmpty())
                        {
                            columnsToGenerate = new LinkedHashSet<>(0);
                        }
                        else if (regularAndStaticColumns.size() == 1 || bool.generate(rnd))
                        {
                            // all columns
                            columnsToGenerate = new LinkedHashSet<>(regularAndStaticColumns);
                        }
                        else
                        {
                            // subset
                            columnsToGenerate = new LinkedHashSet<>(subsetRegularAndStaticColumns(rnd));
                        }

                        generateRemaining(rnd, bool, Mutation.Kind.INSERT, isTransaction, typeToReference, builder, columnsToGenerate);
                        return builder.build();
                    }
                    case UPDATE:
                    {
                        Mutation.TableBasedUpdateBuilder builder = Mutation.update(metadata);
                        var ttl = ttlGen.generate(rnd);
                        if (ttl.isPresent())
                            builder.ttl(valueGen(ttl.getAsInt(), Int32Type.instance).generate(rnd));
                        var timestamp = timestampGen.generate(rnd);
                        if (timestamp.isPresent())
                            builder.timestamp(valueGen(timestamp.getAsLong(), LongType.instance).generate(rnd));
                        if (allowUpdateMultiplePartitionKeys)
                            where(rnd, columnExpressions, builder, partitionColumns, partitionValueGen);
                        else
                            values(rnd, columnExpressions, builder, partitionColumns, partitionValueGen);

                        if (!staticColumns.isEmpty() && allowPartitionOnlyUpdate && bool.generate(rnd))
                        {
                            var columnsToGenerate = new LinkedHashSet<>(subset(rnd, staticColumns));
                            Conditional.EqBuilder<Mutation.TableBasedUpdateBuilder> setBuilder = builder::set;
                            generateRemaining(rnd, bool, Mutation.Kind.UPDATE, isTransaction, typeToReference, setBuilder, columnsToGenerate);

                            if (isCas)
                            {
                                if (useCasIf.generate(rnd))
                                {
                                    ifGen(new ArrayList<>(staticColumns)).generate(rnd).ifPresent(c -> builder.ifCondition(c));
                                }
                                else
                                {
                                    builder.ifExists();
                                }
                            }
                            return builder.build();
                        }
                        if (allowUpdateMultipleClusteringKeys)
                            where(rnd, columnExpressions, builder, clusteringColumns, clusteringValueGen);
                        else
                            values(rnd, columnExpressions, builder, clusteringColumns, clusteringValueGen);

                        if (isCas)
                        {
                            if (useCasIf.generate(rnd))
                            {
                                ifGen(new ArrayList<>(regularAndStaticColumns)).generate(rnd).ifPresent(c -> builder.ifCondition(c));
                            }
                            else
                            {
                                builder.ifExists();
                            }
                        }

                        LinkedHashSet<Symbol> columnsToGenerate;
                        if (regularAndStaticColumns.size() == 1 || bool.generate(rnd))
                        {
                            // all columns
                            columnsToGenerate = new LinkedHashSet<>(regularAndStaticColumns);
                        }
                        else
                        {
                            // subset must include a regular column
                            columnsToGenerate = new LinkedHashSet<>(subset(rnd, regularColumns));
                            if (!staticColumns.isEmpty() && bool.generate(rnd))
                                columnsToGenerate.addAll(subset(rnd, staticColumns));
                        }
                        Conditional.EqBuilder<Mutation.TableBasedUpdateBuilder> setBuilder = builder::set;
                        generateRemaining(rnd, bool, Mutation.Kind.UPDATE, isTransaction, typeToReference, setBuilder, columnsToGenerate);
                        return builder.build();
                    }
                    case DELETE:
                    {
                        Mutation.DeleteBuilder builder = Mutation.delete(metadata);

                        // 3 types of delete: partition, row, columns
                        DeleteKind deleteKind = deleteKindGen.generate(rnd);
                        // if there are no columns to delete, fallback to row
                        if (deleteKind == DeleteKind.Column && regularAndStaticColumns.isEmpty())
                            deleteKind = DeleteKind.Row;
                        if (deleteKind == DeleteKind.Row && clusteringColumns.isEmpty())
                            deleteKind = DeleteKind.Partition;

                        if (allowUpdateMultiplePartitionKeys)
                            where(rnd, columnExpressions, builder, partitionColumns, partitionValueGen);
                        else
                            values(rnd, columnExpressions, builder, partitionColumns, partitionValueGen);

                        switch (deleteKind)
                        {
                            case Partition:
                                // nothing to do here, already handled
                                break;
                            case Row:
                                values(rnd, columnExpressions, builder, clusteringColumns, clusteringValueGen);
                                break;
                            case Column:
                                if (clusteringColumns.isEmpty())
                                {
                                    subsetRegularAndStaticColumns(rnd).forEach(builder::column);
                                }
                                else if (staticColumns.isEmpty())
                                {
                                    subset(rnd, regularColumns).forEach(builder::column);
                                    values(rnd, columnExpressions, builder, clusteringColumns, clusteringValueGen);
                                }
                                else if (regularColumns.isEmpty())
                                {
                                    subset(rnd, staticColumns).forEach(builder::column);
                                }
                                else
                                {
                                    // 2 possible states:
                                    // 1) select a row then delete the columns
                                    // 2) select a partition then select static columns only
                                    if (bool.generate(rnd))
                                    {
                                        // select static
                                        subset(rnd, staticColumns).forEach(builder::column);
                                    }
                                    else
                                    {
                                        // select a row, at least 1 regular, and 0 or more statics
                                        values(rnd, columnExpressions, builder, clusteringColumns, clusteringValueGen);
                                        subset(rnd, regularColumns).forEach(builder::column);
                                        if (bool.generate(rnd))
                                            subset(rnd, staticColumns).forEach(builder::column);
                                    }
                                }
                                if (!clusteringColumns.isEmpty() && !staticColumns.isEmpty())
                                {
                                    if (bool.generate(rnd))
                                    {
                                        // static only
                                        subset(rnd, staticColumns).forEach(builder::column);
                                    }
                                    else
                                    {
                                        // mixed (piss
                                    }
                                }
                                break;
                            default:
                                throw new UnsupportedOperationException();
                        }

                        var timestamp = timestampGen.generate(rnd);
                        if (timestamp.isPresent())
                            builder.timestamp(valueGen(timestamp.getAsLong(), LongType.instance).generate(rnd));
                        if (isCas)
                        {
                            boolean existAllowed = true;
                            List<Symbol> columns;
                            switch (deleteKind)
                            {
                                case Partition:
                                {
                                    // As of this moment delete if partition exists does a full partition read, so its blocked
                                    // due to being too costly... this query is logically correct so we should support as only
                                    // liveness information is needed, but its not supported right now so need to work around
                                    // see ML "[DISCUSS] CASSANDRA-20163 DELETE partition IF static column condition is currently blocked"
                                    // I tried to enable delete partition if static column condition in CASSANDRA-20156, but was
                                    // asked to abandon the patch for consistency reasons.
                                    // Delete partition when there are clustering columns is unsupported, so avoid generating
                                    if (clusteringColumns.isEmpty())
                                    {
                                        // this is the same as delete row
                                        columns = new ArrayList<>(regularAndStaticColumns);
                                        existAllowed = true;
                                    }
                                    else
                                    {
                                        columns = Collections.emptyList();
                                        existAllowed = false;
                                    }
                                }
                                break;
                                case Row:
                                {
                                    columns = new ArrayList<>(regularAndStaticColumns);
                                }
                                break;
                                case Column:
                                {
                                    // some column deletes support without clustering, others dont... to avoid
                                    // relearning this, only allow conditions on the followin columns:
                                    // 1) the columns in the query; only valid columns are present
                                    // 2) static columns; these are always safe to include
                                    LinkedHashSet<Symbol> uniq = new LinkedHashSet<>(builder.columns());
                                    uniq.addAll(staticColumns);
                                    columns = new ArrayList<>(uniq);
                                }
                                break;
                                default:
                                    throw new UnsupportedOperationException(deleteKind.name());
                            }
                            if (!columns.isEmpty() && useCasIf.generate(rnd))
                            {
                                ifGen(columns).generate(rnd).ifPresent(builder::ifCondition);
                            }
                            else if (existAllowed)
                            {
                                builder.ifExists();
                            }
                            else
                            {
                                // can't do a CAS query
                            }
                        }
                        return builder.build();
                    }
                    default:
                        throw new UnsupportedOperationException(kind.name());
                }
            };
        }

        private void generateRemaining(RandomnessSource rnd,
                                       Gen<Boolean> bool,
                                       Mutation.Kind kind,
                                       boolean isTransaction,
                                       Map<? extends AbstractType<?>, List<Reference>> typeToReference,
                                       Conditional.EqBuilder<?> builder,
                                       LinkedHashSet<Symbol> columnsToGenerate)
        {
            //TODO (flexability): since expression offers visit to replace things, could also keep the expression in tact but just replace Value with the Reference?
            if (!typeToReference.isEmpty())
            {
                List<Symbol> allowed = new ArrayList<>(columnsToGenerate);
                for (Symbol s : allowed)
                {
                    List<Reference> matches = typeToReference.get(s.type());
                    if (matches == null)
                        continue;
                    if (bool.generate(rnd))
                    {
                        columnsToGenerate.remove(s);
                        builder.value(s, SourceDSL.arbitrary().pick(matches).generate(rnd));
                    }
                }
            }
            if (kind == Mutation.Kind.UPDATE)
            {
                for (Symbol c : new ArrayList<>(columnsToGenerate))
                {
                    var useOperator = columnExpressions.get(c).useOperator;
                    EnumSet<AssignmentOperator.Kind> additionOperatorAllowed = AssignmentOperator.supportsOperators(c.type(), isTransaction);
                    if (!additionOperatorAllowed.isEmpty() && useOperator.generate(rnd))
                    {
                        Expression expression = columnExpressions.get(c).build().generate(rnd);
                        builder.value(c, assignmentOperatorGen(additionOperatorAllowed, expression).generate(rnd));
                        columnsToGenerate.remove(c);
                    }
                }
            }
            columnsToGenerate.forEach(s -> builder.value(s, columnExpressions.get(s).build().generate(rnd)));
        }

        private List<Symbol> subsetRegularAndStaticColumns(RandomnessSource rnd)
        {
            return subset(rnd, regularAndStaticColumns);
        }

        private static List<Symbol> subset(RandomnessSource rnd, LinkedHashSet<Symbol> columns)
        {
            if (columns.size() == 1)
                return new ArrayList<>(columns);
            int numColumns = Math.toIntExact(rnd.next(Constraint.between(1, columns.size())));
            List<Symbol> subset = Generators.uniqueList(SourceDSL.arbitrary().pick(new ArrayList<>(columns)), i -> numColumns).generate(rnd);
            return subset;
        }

        private Gen<Optional<CasCondition.IfCondition>> ifGen(List<Symbol> possibleColumns)
        {
            return rnd -> {
                List<Symbol> symbols = ifConditionFilter.apply(rnd, possibleColumns);
                if (symbols == null || symbols.isEmpty())
                    return Optional.empty();
                Conditional.Builder builder = new Conditional.Builder();
                for (Symbol symbol : symbols)
                    builder.where(symbol, Conditional.Where.Inequality.EQUAL, columnExpressions.get(symbol).build().generate(rnd));
                return Optional.of(new CasCondition.IfCondition(builder.build()));
            };
        }
    }

    public static class TxnGenBuilder
    {
        public enum TxReturn { NONE, TABLE, REF}
        private final TableMetadata metadata;
        private Constraint letRange = Constraint.between(0, 3);
        private Constraint ifUpdateRange = Constraint.between(1, 3);
        private Constraint updateRange = Constraint.between(0, 3);
        private Gen<Select> selectGen;
        private Gen<TxReturn> txReturnGen = SourceDSL.arbitrary().enumValues(TxReturn.class);
        private boolean allowReferences = true;

        public TxnGenBuilder(TableMetadata metadata)
        {
            this.metadata = metadata;
            this.selectGen = new SelectGenBuilder(metadata)
                             .withLimit1()
                             .build();
        }

        public TxnGenBuilder withoutReferences()
        {
            this.allowReferences = false;
            return this;
        }

        public Gen<Txn> build()
        {
            Gen<Boolean> bool = SourceDSL.booleans().all();
            return rnd -> {
                Txn.Builder builder = new Txn.Builder();
                do
                {
                    int numLets = Math.toIntExact(rnd.next(letRange));
                    for (int i = 0; i < numLets; i++)
                    {
                        // LET doesn't use normal symbol logic and acts closer to a common lanaguage; name does not lower
                        // case... it is possible that a reserved word gets used, so make sure to use a generator that
                        // filters those out.
                        String name;
                        while (builder.lets().containsKey(name = SYMBOL_GEN.generate(rnd))) {}
                        builder.addLet(name, selectGen.generate(rnd));
                    }
                    Gen<Reference> refGen = SourceDSL.arbitrary().pick(new ArrayList<>(builder.allowedReferences()));
                    if (allowReferences)
                    {
                        switch (txReturnGen.generate(rnd))
                        {
                            case REF:
                            {
                                if (!builder.allowedReferences().isEmpty())
                                {
                                    Gen<List<Reference>> refsGen = SourceDSL.lists().of(refGen).ofSizeBetween(1, Math.max(10, builder.allowedReferences().size()));
                                    builder.addReturn(new Select((List<Expression>) (List<?>) refsGen.generate(rnd)));
                                }
                            }
                            break;
                            case TABLE:
                                builder.addReturn(selectGen.generate(rnd));
                                break;
                        }
                    }
                    else
                    {
                        builder.addReturn(selectGen.generate(rnd));
                    }
                    MutationGenBuilder mutationBuilder = new MutationGenBuilder(metadata)
                                                         .withTxnSafe()
                                                         .disallowUpdateMultiplePartitionKeys()
                                                         .withReferences(new ArrayList<>(builder.allowedReferences()));
                    if (!allowReferences)
                        mutationBuilder.withReferences(Collections.emptyList());
                    Gen<Mutation> updateGen = mutationBuilder.build();
                    if (allowReferences && !builder.lets().isEmpty() && bool.generate(rnd))
                    {
                        Gen<Conditional> conditionalGen = conditionalGen(refGen);
                        int numUpdates = Math.toIntExact(rnd.next(ifUpdateRange));
                        List<Mutation> mutations = new ArrayList<>(numUpdates);
                        for (int i = 0; i < numUpdates; i++)
                            mutations.add(updateGen.generate(rnd));
                        builder.addIf(new Txn.If(conditionalGen.generate(rnd), mutations));
                    }
                    else
                    {
                        // Current limitation is that mutations are tied to the condition if present; can't have
                        // a condition and mutations that don't belong to it in v1... once multiple conditions are
                        // supported then can always attempt to add updates
                        int numUpdates = Math.toIntExact(rnd.next(updateRange));
                        for (int i = 0; i < numUpdates; i++)
                            builder.addUpdate(updateGen.generate(rnd));
                    }
                } while (builder.isEmpty());
                return builder.build();
            };
        }

        private static Gen<Conditional> conditionalGen(Gen<Reference> refGen)
        {
            Constraint numConditionsConstraint = Constraint.between(1, 10);
            return rnd -> {
                //TODO support OR
                Gen<Conditional.Where> whereGen = whereGen(refGen.generate(rnd));
                int size = Math.toIntExact(rnd.next(numConditionsConstraint));
                Conditional accum = whereGen.generate(rnd);
                for (int i = 1; i < size; i++)
                    accum = new Conditional.And(accum, whereGen.generate(rnd));
                return accum;
            };
        }

        private static Gen<Conditional.Where> whereGen(Reference ref)
        {
            Gen<Conditional.Where.Inequality> kindGen = SourceDSL.arbitrary().enumValues(Conditional.Where.Inequality.class);
            Gen<?> dataGen = getTypeSupport(ref.type()).valueGen;
            return rnd -> {
                Conditional.Where.Inequality kind = kindGen.generate(rnd);
                return Conditional.Where.create(kind, ref, valueGen(dataGen.generate(rnd), ref.type()).generate(rnd));
            };
        }
    }

    public static MutationGenBuilder mutationBuilder(RandomSource rs,
                                                     ASTSingleTableModel model,
                                                     List<LinkedHashMap<Symbol, Object>> uniquePartitions,
                                                     Function<Symbol, CreateIndexDDL.IndexedColumn> indexes)
    {
        return mutationBuilder(IGNORED_ISSUES, rs, model, uniquePartitions, indexes);
    }

    public static MutationGenBuilder mutationBuilder(EnumSet<KnownIssue> ignoredIssues,
                                                     RandomSource rs,
                                                     ASTSingleTableModel model,
                                                     List<LinkedHashMap<Symbol, Object>> uniquePartitions,
                                                     Function<Symbol, CreateIndexDDL.IndexedColumn> indexes)
    {
        var boolDistribution = Gens.bools().mixedDistribution();
        TableMetadata metadata = model.factory.metadata;
        MutationGenBuilder builder = new MutationGenBuilder(metadata)
                                     .withTxnSafe()
                                     .withPartitions(uniquePartitions.size() == 1
                                                     ? SourceDSL.arbitrary().constant(uniquePartitions.get(0))
                                                     : Generators.fromGen(Gens.mixedDistribution(uniquePartitions).next(rs)))
                                     .withColumnExpressions(e -> e.withOperators(Generators.fromGen(boolDistribution.next(rs))))
                                     .withIgnoreIssues(ignoredIssues);
        if (ignoredIssues.contains(KnownIssue.SAI_EMPTY_TYPE))
        {
            model.factory.regularAndStaticColumns.stream()
                                                 // exclude SAI indexed columns
                                                 .filter(s -> indexes.apply(s) == null || indexes.apply(s).indexDDL.indexer != CreateIndexDDL.SAI)
                                                 .forEach(builder::allowEmpty);
        }
        else
        {
            model.factory.regularAndStaticColumns.forEach(builder::allowEmpty);
        }
        model.factory.regularAndStaticColumns.forEach(builder::allowNull);
        return builder;
    }

    public static class ModelBasedTxnGenBuilder
    {
        private final RandomSource rs; //TODO (now): refactor so ASTGenerators no longer is quicktheories, its too annoying to go back and forth...
        private final TableMetadata metadata;
        private final ASTSingleTableModel model;
        private final Function<Symbol, CreateIndexDDL.IndexedColumn> indexes;
        private final Gen<LinkedHashMap<Symbol, Object>> partitionKeyValuesGen;
        private Gen<Boolean> bindOrLiteralGen = SourceDSL.booleans().all();
        private boolean allowEmpty = true;

        public ModelBasedTxnGenBuilder(RandomSource rs,
                                       ASTSingleTableModel model,
                                       Function<Symbol, CreateIndexDDL.IndexedColumn> indexes,
                                       Gen<LinkedHashMap<Symbol, Object>> partitionKeyValuesGen)
        {
            this.rs = rs;
            this.metadata = model.factory.metadata;
            this.model = model;
            this.indexes = indexes;
            this.partitionKeyValuesGen = partitionKeyValuesGen;
        }

        public ModelBasedTxnGenBuilder disallowEmpty()
        {
            allowEmpty = false;
            return this;
        }

        public ModelBasedTxnGenBuilder withBindOrLiteralGen(Gen<Boolean> bindOrLiteralGen)
        {
            this.bindOrLiteralGen = bindOrLiteralGen;
            return this;
        }

        private Gen<Mutation> mutationGen(RandomSource rs, LinkedHashMap<Symbol, Object> pk)
        {
            MutationGenBuilder builder = mutationBuilder(IGNORED_ISSUES, rs, model, List.of(pk), indexes)
                                         .withTxnSafe()
                                         .disallowUpdateMultiplePartitionKeys();
            if (!allowEmpty)
                builder.disallowEmpty();
            return builder.build();
        }

        private Value value(RandomnessSource rs, ByteBuffer bb, AbstractType<?> type)
        {
            return bindOrLiteralGen.generate(rs) ? new Bind(bb, type) : new Literal(bb, type);
        }

        public Gen<Txn> build()
        {
            Gen<Boolean> boolGen = SourceDSL.booleans().all();
            return rnd -> {
                var pk = partitionKeyValuesGen.generate(rnd);
                var mutation = mutationGen(rs, pk).generate(rnd);

                Select select = select(metadata, pk).withLimit(1);
                var columns = model.columns(select);
                Txn.Builder builder = Txn.builder();
                builder.addLet("r1", select);
                Reference ref = Reference.of(Symbol.unknownType("r1"));

                builder.addReturn(select(metadata, pk));

                Conditional.Builder condition = Conditional.builder();
                for (var col : columns)
                {
                    if (boolGen.generate(rnd)) continue;
                    Reference colRef = ref.add(col);
                    if (boolGen.generate(rnd))
                        condition.is(colRef, SourceDSL.arbitrary().enumValues(Conditional.Is.Kind.class).generate(rnd));
                    if (boolGen.generate(rnd))
                    {
                        Expression lhs = colRef;
                        Expression rhs = value(rnd, getTypeSupport(lhs.type()).bytesGen().generate(rnd), lhs.type());
                        if (boolGen.generate(rnd))
                        {
                            var tmp = lhs;
                            lhs = rhs;
                            rhs = tmp;
                        }
                        Conditional.Where.Inequality inequality = SourceDSL.arbitrary().enumValues(Conditional.Where.Inequality.class).generate(rnd);
                        condition.where(lhs, inequality, rhs);
                    }
                }
                if (condition.isEmpty())
                    condition.is("r1", Conditional.Is.Kind.NotNull);
                builder.addIf(condition.build(), mutation);

                return builder.build();
            };
        }
    }
}

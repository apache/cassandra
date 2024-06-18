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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.ast.And;
import org.apache.cassandra.cql3.ast.AssignmentOperator;
import org.apache.cassandra.cql3.ast.Bind;
import org.apache.cassandra.cql3.ast.CasCondition;
import org.apache.cassandra.cql3.ast.Conditional;
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
import org.apache.cassandra.cql3.ast.Where;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.Constraint;

import static org.apache.cassandra.utils.Generators.SYMBOL_NOT_RESERVED_KEYWORD_GEN;

public class ASTGenerators
{
    static Gen<Value> valueGen(Object value, AbstractType<?> type)
    {
        Gen<Boolean> bool = SourceDSL.booleans().all();
        return rnd -> bool.generate(rnd) ? new Bind(value, type) : new Literal(value, type);
    }

    static Gen<Value> valueGen(AbstractType<?> type)
    {
        Gen<?> v = AbstractTypeGenerators.getTypeSupport(type).valueGen;
        return rnd -> valueGen(v.generate(rnd), type).generate(rnd);
    }

    public static Gen<AssignmentOperator> assignmentOperatorGen(EnumSet<AssignmentOperator.Kind> allowed, Expression right)
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
            if (e.type().unwrap() == ShortType.instance
                || e.type().unwrap() == IntegerType.instance) // seed=7525457176675272023L
            {
                left = new TypeHint(left);
                right = new TypeHint(right);
            }
            return new Operator(kind, TypeHint.maybeApplyTypeHint(left), TypeHint.maybeApplyTypeHint(right));
        };
    }

    public static class SelectGenBuilder
    {
        private final TableMetadata metadata;
        private Gen<List<Expression>> selectGen;
        private Gen<Map<Symbol, Expression>> partitionKeyGen;
        private Gen<Optional<Value>> limit;

        public SelectGenBuilder(TableMetadata metadata)
        {
            this.metadata = Objects.requireNonNull(metadata);
            this.selectGen = selectColumns(metadata);
            this.partitionKeyGen = partitionKeyGen(metadata);

            withDefaultLimit();
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
            limit = non.mix(positive);
            return this;
        }

        public SelectGenBuilder withLimit1()
        {
            this.limit = rnd -> Optional.of(valueGen(1, Int32Type.instance).generate(rnd));
            return this;
        }

        public SelectGenBuilder withoutLimit()
        {
            this.limit = ignore -> Optional.empty();
            return this;
        }

        public SelectGenBuilder withPrimaryKeys(Map<Symbol, Expression> primaryKeys)
        {
            partitionKeyGen = ignore -> primaryKeys;
            return this;
        }

        public Gen<Select> build()
        {
            return rnd -> {
                List<Expression> select = selectGen.generate(rnd);
                Map<Symbol, Expression> partitionKey = partitionKeyGen.generate(rnd);
                Conditional partitionKeyClause = and(partitionKey);
                return new Select(select, Optional.of(TableReference.from(metadata)), Optional.of(partitionKeyClause), Optional.empty(), limit.generate(rnd));
            };
        }

        private static Conditional and(Map<Symbol, Expression> data)
        {
            Conditional.Builder builder = new Conditional.Builder();
            for (Map.Entry<Symbol, Expression> e : data.entrySet())
                builder.where(Where.Inequalities.EQUAL, e.getKey(), e.getValue());
            return builder.build();
        }

        private static Gen<List<Expression>> selectColumns(TableMetadata metadata)
        {
            List<ColumnMetadata> columns = new ArrayList<>(metadata.columns());
            Constraint between = Constraint.between(0, columns.size() - 1);
            Gen<int[]> indexGen = rnd -> {
                int size = Math.toIntExact(rnd.next(between)) + 1;
                Set<Integer> dedup = new HashSet<>();
                while (dedup.size() < size)
                    dedup.add(Math.toIntExact(rnd.next(between)));
                return dedup.stream().mapToInt(Integer::intValue).toArray();
            };
            return rnd -> {
                int[] indexes = indexGen.generate(rnd);
                List<Expression> es = new ArrayList<>(indexes.length);
                IntStream.of(indexes).mapToObj(columns::get).forEach(c -> {
                    Reference ref = Reference.of(new Symbol(c));
                    es.add(ref);
                });
                return es;
            };
        }

        private static Gen<Map<Symbol, Expression>> partitionKeyGen(TableMetadata metadata)
        {
            Map<ColumnMetadata, Gen<?>> gens = new LinkedHashMap<>();
            for (ColumnMetadata col : metadata.columns())
                gens.put(col, AbstractTypeGenerators.getTypeSupport(col.type).valueGen);
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
        private final TableMetadata metadata;
        private final Set<Symbol> allColumns;
        private final Set<Symbol> primaryColumns;
        private final Set<Symbol> nonPrimaryColumns;
        private Gen<Mutation.Kind> kindGen = SourceDSL.arbitrary().enumValues(Mutation.Kind.class);
        private Gen<OptionalInt> ttlGen = SourceDSL.integers().between(1, Math.toIntExact(TimeUnit.DAYS.toSeconds(10))).map(i -> i % 2 == 0 ? OptionalInt.empty() : OptionalInt.of(i));
        private Gen<OptionalLong> timestampGen = SourceDSL.longs().between(1, Long.MAX_VALUE).map(i -> i % 2 == 0 ? OptionalLong.empty() : OptionalLong.of(i));
        private Gen<Map<Symbol, Expression>> valuesGen;
        private boolean allowOperators = true;
        private Collection<Reference> references = Collections.emptyList();
        private Gen<Boolean> withCasGen = SourceDSL.booleans().all();
        private Gen<Boolean> useCasIf = SourceDSL.booleans().all();
        private BiFunction<RandomnessSource, List<Symbol>, List<Symbol>> ifConditionFilter = (rnd, symbols) -> symbols;
        private final Map<Symbol, Gen<?>> columnDataGens;

        public MutationGenBuilder(TableMetadata metadata)
        {
            this.metadata = Objects.requireNonNull(metadata);
            this.allColumns = Mutation.toSet(metadata.columns());
            this.primaryColumns = Mutation.toSet(metadata.primaryKeyColumns());
            this.nonPrimaryColumns = Sets.difference(allColumns, primaryColumns);

            columnDataGens = new LinkedHashMap<>();
            for (ColumnMetadata col : metadata.columns())
                columnDataGens.put(new Symbol(col), AbstractTypeGenerators.getTypeSupport(col.type).valueGen);
            this.valuesGen = rnd -> {
                Map<Symbol, Expression> map = new HashMap<>();
                for (Symbol name : allColumns)
                {
                    Gen<?> gen = columnDataGens.get(name);
                    map.put(name, valueGen(gen.generate(rnd), name.type()).generate(rnd));
                }
                return map;
            };
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
            withCasGen = Objects.requireNonNull(withCasGen);
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

        public MutationGenBuilder withOperators()
        {
            allowOperators = true;
            return this;
        }

        public MutationGenBuilder withoutOperators()
        {
            allowOperators = false;
            return this;
        }

        public MutationGenBuilder withReferences(Collection<Reference> references)
        {
            this.references = references;
            return this;
        }

        public Gen<Mutation> build()
        {
            Map<Symbol, ColumnMetadata> allColumnsMap = metadata.columns().stream().collect(Collectors.toMap(m -> new Symbol(m), Function.identity()));
            List<Symbol> allColumns = new ArrayList<>(allColumnsMap.keySet());
            Collections.sort(allColumns);
            Gen<Boolean> bool = SourceDSL.booleans().all();
            return rnd -> {
                Mutation.Kind kind = kindGen.generate(rnd);
                boolean isCas = withCasGen.generate(rnd);
                boolean isTransaction = isCas; //TODO (coverage): add accord support
                // when there are not non-primary-columns then can't support UPDATE
                if (nonPrimaryColumns.isEmpty())
                {
                    int i;
                    int maxRetries = 42;
                    for (i = 0; i < maxRetries && kind == Mutation.Kind.UPDATE; i++)
                        kind = kindGen.generate(rnd);
                    if (i == maxRetries)
                        throw new IllegalArgumentException("Kind gen kept returning UPDATE, but not supported when there are no non-primary columns");
                }

                Map<Symbol, Expression> values = valuesGen.generate(rnd);
                Map<? extends AbstractType<?>, List<Reference>> typeToReference = references.stream().collect(Collectors.groupingBy(Reference::type));
                if (!typeToReference.isEmpty())
                {
                    // Due to being able to discover the partition key *before running*, we don't
                    // allow WHERE clause to have references, so the type of mutation dicates what columns
                    // are allowed to be mutated
                    List<Symbol> allowed = nonPrimaryColumns.stream().filter(values::containsKey).collect(Collectors.toList());
                    for (Symbol s : allowed)
                    {
                        List<Reference> matches = typeToReference.get(s.type());
                        if (matches == null)
                            continue;
                        if (bool.generate(rnd))
                            values.put(s, SourceDSL.arbitrary().pick(matches).generate(rnd));
                    }
                }
                if (allowOperators)
                {
                    for (Symbol c : allColumns)
                    {
                        Set<Operator.Kind> operatorAllowed = Operator.supportsOperators(allColumnsMap.get(c).type);
                        EnumSet<AssignmentOperator.Kind> additionOperatorAllowed = isTransaction
                                                                                   ? AssignmentOperator.supportsOperators(allColumnsMap.get(c).type)
                                                                                   : EnumSet.noneOf(AssignmentOperator.Kind.class);
                        if (values.containsKey(c))
                        {
                            if (!operatorAllowed.isEmpty() && bool.generate(rnd))
                            {
                                Expression e = values.get(c);
                                //TODO remove hack; currently INSERT references with operators fails
                                if (!(e instanceof Reference))
                                {
                                    Gen<Operator> operatorGen = operatorGen(operatorAllowed, e, valueGen(e.type()));
                                    values.put(c, operatorGen.generate(rnd));
                                }
                            }
                            if (!additionOperatorAllowed.isEmpty() && kind == Mutation.Kind.UPDATE && nonPrimaryColumns.contains(c) && bool.generate(rnd))
                            {
                                values.put(c, assignmentOperatorGen(additionOperatorAllowed, values.get(c)).generate(rnd));
                            }
                        }
                    }
                }
                Optional<? extends CasCondition> casCondition = Optional.empty();
                if (isCas)
                {
                    switch (kind)
                    {
                        case INSERT:
                            casCondition = Optional.of(CasCondition.Simple.NotExists);
                            break;
                        case UPDATE:
                        {
                            if (useCasIf.generate(rnd))
                            {
                                casCondition = ifGen(metadata.regularAndStaticColumns()).generate(rnd);
                            }
                            else
                            {
                                casCondition = Optional.of(CasCondition.Simple.Exists);
                            }
                        }
                        break;
                        case DELETE:
                        {
                            if (metadata.staticColumns().isEmpty() || !useCasIf.generate(rnd))
                            {
                                casCondition = Optional.of(CasCondition.Simple.Exists);
                            }
                            else
                            {
                                casCondition = ifGen(metadata.staticColumns()).generate(rnd);
                            }
                        }
                        break;
                        default:
                            throw new AssertionError();
                    }
                }
                return new Mutation(kind, metadata, values, ttlGen.generate(rnd), timestampGen.generate(rnd), casCondition);
            };
        }

        private Gen<Optional<CasCondition.IfCondition>> ifGen(Iterable<ColumnMetadata> iterable)
        {
            List<Symbol> possibleColumns = StreamSupport.stream(iterable.spliterator(), false).map(Symbol::new).collect(Collectors.toList());
            return rnd -> {
                List<Symbol> symbols = ifConditionFilter.apply(rnd, possibleColumns);
                if (symbols == null || symbols.isEmpty())
                    return Optional.empty();
                Conditional.Builder builder = new Conditional.Builder();
                for (Symbol symbol : symbols)
                    builder.where(Where.Inequalities.EQUAL, symbol, valueGen(columnDataGens.get(symbol).generate(rnd), symbol.type()).generate(rnd));
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
                        while (builder.lets().containsKey(name = SYMBOL_NOT_RESERVED_KEYWORD_GEN.generate(rnd))) {}
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
                                                          .withoutCas()
                                                          .withoutTimestamp()
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
                Gen<Where> whereGen = whereGen(refGen.generate(rnd));
                int size = Math.toIntExact(rnd.next(numConditionsConstraint));
                Conditional accum = whereGen.generate(rnd);
                for (int i = 1; i < size; i++)
                    accum = new And(accum, whereGen.generate(rnd));
                return accum;
            };
        }

        private static Gen<Where> whereGen(Reference ref)
        {
            Gen<Where.Inequalities> kindGen = SourceDSL.arbitrary().enumValues(Where.Inequalities.class);
            Gen<?> dataGen = AbstractTypeGenerators.getTypeSupport(ref.type()).valueGen;
            return rnd -> {
                Where.Inequalities kind = kindGen.generate(rnd);
                return Where.create(kind, ref, valueGen(dataGen.generate(rnd), ref.type()).generate(rnd));
            };
        }
    }
}

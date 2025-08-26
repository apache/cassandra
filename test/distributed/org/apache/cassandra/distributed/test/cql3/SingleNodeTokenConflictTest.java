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

package org.apache.cassandra.distributed.test.cql3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Property;
import accord.utils.RandomSource;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.KnownIssue;
import org.apache.cassandra.cql3.ast.Conditional.Where.Inequality;
import org.apache.cassandra.cql3.ast.FunctionCall;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.cql3.ast.Value;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.ASTGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeGenBuilder;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeSupport;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.ImmutableUniqueList;
import org.quicktheories.generators.SourceDSL;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static org.apache.cassandra.dht.Murmur3Partitioner.LongToken.keyForToken;
import static org.apache.cassandra.utils.Generators.toGen;

public class SingleNodeTokenConflictTest extends StatefulASTBase
{
    private static final Logger logger = LoggerFactory.getLogger(SingleNodeTokenConflictTest.class);

    private static final Gen.IntGen NUM_TOKENS_GEN = Gens.pickInt(1, 10, 100);
    /**
     * {@code vector<bigint, 2>} is 16 bytes, which perfectly lines up with {@link LongToken#keyForToken(long)}.
     */
    private static final VectorType<Long> PK_TYPE = VectorType.getInstance(LongType.instance, 2);
    private static final TypeSupport<List<Long>> TYPE_SUPPORT = AbstractTypeGenerators.getTypeSupport(PK_TYPE);
    private static final Symbol PK = new Symbol("pk0", PK_TYPE);
    private static final TypeGenBuilder SUPPORTED_TYPES =
    AbstractTypeGenerators.withoutUnsafeEquality(AbstractTypeGenerators.builder()
                                                                       .withTypeKinds(TypeKind.PRIMITIVE));

    @Nullable
    private final TransactionalMode transactionalMode;

    protected SingleNodeTokenConflictTest(@Nullable TransactionalMode transactionalMode)
    {
        this.transactionalMode = transactionalMode;
    }

    public SingleNodeTokenConflictTest()
    {
        this(null);
    }

    protected void preCheck(Cluster cluster, Property.StatefulBuilder builder)
    {
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
    }

    public static Property.Command<State, Void, ?> pkEq(RandomSource rs, State state)
    {
        ByteBuffer value = state.pkGen.next(rs);
        Select select = Select.builder()
                              .table(state.tableRef)
                              .value(PK, state.pkValue(rs, value))
                              .build();
        return state.command(rs, select, "pk EQ");
    }

    public static Property.Command<State, Void, ?> pkIn(RandomSource rs, State state)
    {
        List<Value> expressions = state.randomPksAsValue(rs);
        Select select = Select.builder()
                              .table(state.tableRef)
                              .in(PK, expressions)
                              .build();
        return state.command(rs, select, "pk IN");
    }

    public static Property.Command<State, Void, ?> pkBetween(RandomSource rs, State state)
    {
        ByteBuffer left = state.pkGen.next(rs);
        ByteBuffer right = state.betweenEqGen.next(rs) ? left : state.pkGen.next(rs);
        int rc = PK_TYPE.compare(left, right);
        if (rc > 0 && IGNORED_ISSUES.contains(KnownIssue.BETWEEN_START_LARGER_THAN_END))
        {
            ByteBuffer tmp = left;
            left = right;
            right = tmp;
            rc = PK_TYPE.compare(left, right);
        }
        Select select = Select.builder()
                              .table(state.tableRef)
                              .between(PK, state.pkValue(rs, left), state.pkValue(rs, right))
                              .allowFiltering()
                              .build();
        return state.command(rs, select, "pk BETWEEN, rc=" + rc);
    }

    public static Property.Command<State, Void, ?> pkRange(RandomSource rs, State state)
    {
        ByteBuffer value = state.pkGen.next(rs);
        Inequality inequality = state.rangeInequalityGen.next(rs);
        Select select = Select.builder()
                              .table(state.tableRef)
                              .where(PK, inequality, value)
                              .allowFiltering()
                              .build();
        return state.command(rs, select, "pk " + inequality.value);
    }

    public static Property.Command<State, Void, ?> pkBoundRange(RandomSource rs, State state)
    {
        ByteBuffer left = state.pkGen.next(rs);
        Inequality lefIneq = state.lessThanGen.next(rs);

        ByteBuffer right = state.pkGen.next(rs);
        Inequality rightIneq = state.greaterThanGen.next(rs);
        Select select = Select.builder()
                              .table(state.tableRef)
                              .where(PK, lefIneq, left)
                              .where(PK, rightIneq, right)
                              .allowFiltering()
                              .build();
        return state.command(rs, select, "pk " + lefIneq.value + " AND " + rightIneq.value);
    }

    public static Property.Command<State, Void, ?> tokenEq(RandomSource rs, State state)
    {
        ByteBuffer value = state.pkGen.next(rs);
        Select select = Select.builder()
                              .table(state.tableRef)
                              .where(FunctionCall.tokenByColumns(PK), Inequality.EQUAL, FunctionCall.tokenByValue(state.pkValue(rs, value)))
                              .build();
        return state.command(rs, select, "token EQ");
    }

    // As of 5.1 IN clause is limited to columns / fields / collections
//    public static Property.Command<State, Void, ?> tokenIn(RandomSource rs, State state)
//    {
//        List<Value> expressions = state.randomPksAsValue(rs);
//        Select select = Select.builder()
//                              .table(state.tableRef)
//                              .in(FunctionCall.tokenByColumns(PK), expressions.stream().map(FunctionCall::tokenByValue).collect(Collectors.toList()))
//                              .build();
//        return state.command(select, "token IN");
//    }

    public static Property.Command<State, Void, ?> tokenBetween(RandomSource rs, State state)
    {
        ByteBuffer left = state.pkGen.next(rs);
        ByteBuffer right = state.betweenEqGen.next(rs) ? left : state.pkGen.next(rs);
        LongToken start = Murmur3Partitioner.instance.getToken(left);
        LongToken end = Murmur3Partitioner.instance.getToken(right);
        int rc = start.compareTo(end);
        if (rc > 0 && IGNORED_ISSUES.contains(KnownIssue.BETWEEN_START_LARGER_THAN_END))
        {
            ByteBuffer tmp = left;
            left = right;
            right = tmp;
            LongToken tmp2 = start;
            start = end;
            end = tmp2;
            rc = start.compareTo(end);
        }
        Select select = Select.builder()
                              .table(state.tableRef)
                              .between(FunctionCall.tokenByColumns(PK),
                                       FunctionCall.tokenByValue(state.pkValue(rs, left)),
                                       FunctionCall.tokenByValue(state.pkValue(rs, right)))
                              .build();
        return state.command(rs, select, "token BETWEEN, rc=" + rc
                                         + ", start token=" + start
                                         + ", end token=" + end);
    }

    public static Property.Command<State, Void, ?> tokenRange(RandomSource rs, State state)
    {
        ByteBuffer value = state.pkGen.next(rs);
        Inequality inequality = state.rangeInequalityGen.next(rs);
        Select select = Select.builder()
                              .table(state.tableRef)
                              .where(FunctionCall.tokenByColumns(PK), inequality, FunctionCall.tokenByValue(state.pkValue(rs, value)))
                              .allowFiltering()
                              .build();
        return state.command(rs, select, "token " + inequality.value + " " + Murmur3Partitioner.instance.getToken(value));
    }

    public static Property.Command<State, Void, ?> tokenBoundRange(RandomSource rs, State state)
    {
        ByteBuffer left = state.pkGen.next(rs);
        Inequality lefIneq = state.lessThanGen.next(rs);

        ByteBuffer right = state.pkGen.next(rs);
        Inequality rightIneq = state.greaterThanGen.next(rs);
        Select select = Select.builder()
                              .table(state.tableRef)
                              .where(FunctionCall.tokenByColumns(PK), lefIneq, FunctionCall.tokenByValue(state.pkValue(rs, left)))
                              .where(FunctionCall.tokenByColumns(PK), rightIneq, FunctionCall.tokenByValue(state.pkValue(rs, right)))
                              .allowFiltering()
                              .build();
        return state.command(rs, select, "token " + lefIneq.value + " " + Murmur3Partitioner.instance.getToken(left)
                                         + " AND " + rightIneq.value + " " + Murmur3Partitioner.instance.getToken(right));
    }

    protected Cluster createCluster() throws IOException
    {
        return createCluster(1);
    }

    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = createCluster())
        {
            Property.StatefulBuilder statefulBuilder = stateful().withExamples(10);
            preCheck(cluster, statefulBuilder);
            statefulBuilder.check(commands(() -> rs -> createState(rs, cluster))
                                  .add(StatefulASTBase::insert)
                                  .addIf(State::allowUsingTimestamp, StatefulASTBase::validateUsingTimestamp)
                                  //TODO (now, coverage): this is flakey and non-deterministic.  When this fails (gives bad response) rerunning the seed yields a passing test!
//                                  .add(StatefulASTBase::fullTableScan)
                                  .add(SingleNodeTokenConflictTest::pkEq)
                                  .add(SingleNodeTokenConflictTest::pkIn)
                                  .add(SingleNodeTokenConflictTest::pkBetween)
                                  .add(SingleNodeTokenConflictTest::pkRange)
                                  .add(SingleNodeTokenConflictTest::pkBoundRange)
                                  .add(SingleNodeTokenConflictTest::tokenEq)
                                  // there is no tokenIn, as of this moment token in does not compile in CQL
                                  .add(SingleNodeTokenConflictTest::tokenBetween)
                                  .add(SingleNodeTokenConflictTest::tokenRange)
                                  .add(SingleNodeTokenConflictTest::tokenBoundRange)
                                  .addIf(State::hasPartitions, StatefulASTBase::selectMinTokenRange)
                                  .addIf(State::hasEnoughMemtable, StatefulASTBase::flushTable)
                                  .addIf(State::hasEnoughSSTables, StatefulASTBase::compactTable)
                                  .destroyState(State::close)
                                  .onSuccess(onSuccess(logger))
                                  .build());
        }
    }

    protected State createState(RandomSource rs, Cluster cluster)
    {
        return new State(rs, cluster);
    }

    protected TableMetadata defineTable(RandomSource rs, String ks)
    {
        TableMetadata metadata = toGen(new TableMetadataBuilder()
                                       .withTableKinds(TableMetadata.Kind.REGULAR)
                                       .withKnownMemtables()
                                       .withKeyspaceName(ks).withTableName("tbl")
                                       .withSimpleColumnNames()
                                       .withDefaultTypeGen(SUPPORTED_TYPES)
                                       .withPartitioner(Murmur3Partitioner.instance)
                                       .withPartitionColumnsCount(1)
                                       // this should produce vector<bigint, 2>... should make this easier...
                                       .withPartitionColumnTypeGen(new TypeGenBuilder()
                                                                   .withMaxDepth(0)
                                                                   .withTypeKinds(TypeKind.VECTOR)
                                                                   .withPrimitives(LongType.instance)
                                                                   .withVectorSizeGen(i -> 2)
                                                                   .withDefaultSizeGen(1))
                                       .build())
                                 .next(rs);
        if (transactionalMode != null)
            metadata = metadata.withSwapped(metadata.params.unbuild().transactionalMode(transactionalMode).build());
        return metadata;
    }

    class State extends CommonState
    {
        private final List<ByteBuffer> neighbors;
        private final List<ByteBuffer> pkValues;
        private final Gen<ByteBuffer> pkGen;
        private final TreeMap<ByteBuffer, ByteBuffer> realToSynthMap;
        private final TreeSet<ByteBuffer> order;

        private final Gen<Mutation> mutationGen;

        State(RandomSource rs, Cluster cluster)
        {
            super(rs, cluster, defineTable(rs, nextKeyspace()));
            {
                int numTokens = NUM_TOKENS_GEN.nextInt(rs);
                var pkValues = Gens.lists(Generators.toGen(TYPE_SUPPORT.bytesGen())).unique().ofSize(numTokens).next(rs);
                // now create conflicting values
                var tokenValues = pkValues.stream().map(SingleNodeTokenConflictTest::toTokenValue).collect(Collectors.toList());
                // this is low probability... but just in case... if there are duplicates, drop them!
                Set<ByteBuffer> seen = new HashSet<>();
                for (int i = 0; i < pkValues.size(); i++)
                {
                    var real = pkValues.get(i);
                    var synth = tokenValues.get(i);
                    if (real.equals(synth) || !seen.add(real) || !seen.add(synth))
                    {
                        // drop
                        pkValues.remove(i);
                        tokenValues.remove(i);
                        i--;
                    }
                }
                if (pkValues.isEmpty())
                    throw new AssertionError("There are no values after filtering duplicates...");
                this.neighbors = rs.nextBoolean() ? Collections.emptyList() : extractNeighbors(pkValues);
                // in case neighbors conflicts with pkValues or tokenValues, use ImmutableUniqueList which will ignore rather than fail
                this.pkValues = ImmutableUniqueList.<ByteBuffer>builder()
                                                   .addAll(pkValues)
                                                   .addAll(tokenValues)
                                                   .addAll(neighbors)
                                                   .build();
                this.pkGen = Gens.pick(pkValues);
                this.order = new TreeSet<>(PK_TYPE);
                realToSynthMap = new TreeMap<>(Comparator.comparing(Murmur3Partitioner.instance::getToken));
                for (int i = 0; i < pkValues.size(); i++)
                {
                    ByteBuffer r = pkValues.get(i);
                    ByteBuffer s = tokenValues.get(i);
                    realToSynthMap.put(r, s);
                    order.add(r);
                    order.add(s);
                }
            }

            // double check pk0
            if (!PK.equals(Symbol.from(metadata.getColumn(new ColumnIdentifier("pk0", false)))))
                throw new AssertionError("Table doesn't match what the test expects;\n" + metadata.toCqlString(false, false, false));

            cluster.forEach(i -> i.nodetoolResult("disableautocompaction", metadata.keyspace, this.metadata.name).asserts().success());

            List<Map<Symbol, Object>> uniquePartitions = new ArrayList<>(pkValues.size());
            pkValues.forEach(bb -> uniquePartitions.add(Map.of(PK, bb)));


            this.mutationGen = toGen(new ASTGenerators.MutationGenBuilder(metadata)
                                     .withTxnSafe()
                                     .withPartitions(SourceDSL.arbitrary().pick(uniquePartitions))
                                     .withIgnoreIssues(IGNORED_ISSUES)
                                     .build());
        }

        @Override
        protected Gen<Mutation> mutationGen()
        {
            return mutationGen;
        }

        private List<ByteBuffer> extractNeighbors(List<ByteBuffer> values)
        {
            // if the same value is added multiple times this data structure will ignore the addition, making sure the
            // returned list only has unique values
            ImmutableUniqueList.Builder<ByteBuffer> neighbors = ImmutableUniqueList.builder();
            for (ByteBuffer bb : values)
            {
                var token = Murmur3Partitioner.instance.getToken(bb);
                if (token.token > Long.MIN_VALUE + 1)
                    neighbors.add(keyForToken(token.token - 1));
                if (token.token < Long.MAX_VALUE)
                    neighbors.add(keyForToken(token.token + 1));
            }
            return neighbors.build();
        }

        private LinkedHashSet<ByteBuffer> randomPks(RandomSource rs)
        {
            int numPks = rs.nextInt(1, pkValues.size());
            // when numPks is large the cost to keep trying to find the few remaining pk values is costly... by cloning
            // the set can be mutated to remove the value, making it so this logic can avoid retries
            LinkedHashSet<ByteBuffer> available = new LinkedHashSet<>(pkValues);
            LinkedHashSet<ByteBuffer> pks = new LinkedHashSet<>();
            for (int i = 0; i < numPks; i++)
            {
                ByteBuffer value = rs.pickOrderedSet(available);
                pks.add(value);
                available.remove(value);
            }
            return pks;
        }

        private List<Value> randomPksAsValue(RandomSource rs)
        {
            LinkedHashSet<ByteBuffer> pks = randomPks(rs);
            List<Value> expressions = new ArrayList<>(pks.size());
            pks.forEach(bb -> expressions.add(pkValue(rs, bb)));
            return expressions;
        }

        private Value pkValue(RandomSource rs, ByteBuffer bb)
        {
            return value(rs, bb, PK_TYPE);
        }

        private String pkCQL(ByteBuffer bb)
        {
            return PK_TYPE.toCQLString(bb);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            toString(sb);
            sb.append("\n\n-- Value to Conflicting Token Map\n");
            sb.append(TableBuilder.toStringPiped(Arrays.asList("Real", "Synthetic"),
                                                 realToSynthMap.entrySet().stream().map(e -> Arrays.asList(pkCQL(e.getKey()), pkCQL(e.getValue()))).collect(Collectors.toList())));
            sb.append("\n\n-- Ordered values");
            order.forEach(e -> sb.append("\n\t").append(pkCQL(e)).append('\t'));
            sb.append("\n\n-- Neighbors");
            neighbors.forEach(bb -> sb.append("\n\t").append(pkCQL(bb)).append('\t'));
            return sb.toString();
        }
    }

    private static ByteBuffer toTokenValue(ByteBuffer buffer)
    {
        return keyForToken(Murmur3Partitioner.instance.getToken(buffer));
    }
}

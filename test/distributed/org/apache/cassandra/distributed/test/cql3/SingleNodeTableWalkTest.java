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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Property;
import accord.utils.RandomSource;
import org.apache.cassandra.cql3.ast.Bind;
import org.apache.cassandra.cql3.ast.Conditional;
import org.apache.cassandra.cql3.ast.CreateIndexDDL;
import org.apache.cassandra.cql3.ast.FunctionCall;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.ReferenceExpression;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.cql3.ast.TableReference;
import org.apache.cassandra.cql3.ast.Value;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.harry.model.BytesPartitionState;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.ASTGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeGenBuilder;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.ImmutableUniqueList;
import org.quicktheories.generators.SourceDSL;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.apache.cassandra.utils.Generators.toGen;

//TODO (coverage): add partition restricted clustering range queries: eg. WHERE pk=? and ck BETWEEN ? AND ?
public class SingleNodeTableWalkTest extends StatefulASTBase
{
    private static final Logger logger = LoggerFactory.getLogger(SingleNodeTableWalkTest.class);

    @Nullable
    private final TransactionalMode transactionalMode;

    public SingleNodeTableWalkTest()
    {
        this(null);
    }

    protected SingleNodeTableWalkTest(@Nullable TransactionalMode transactionalMode)
    {
        this.transactionalMode = transactionalMode;
    }

    protected void preCheck(Cluster cluster, Property.StatefulBuilder builder)
    {
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
        // CQL operations may have opertors such as +, -, and / (example 4 + 4), to "apply" them to get a constant value
        // CQL_DEBUG_APPLY_OPERATOR = true;
    }

    protected TypeGenBuilder supportedTypes()
    {
        return AbstractTypeGenerators.withoutUnsafeEquality(AbstractTypeGenerators.builder()
                                                                                  .withTypeKinds(AbstractTypeGenerators.TypeKind.PRIMITIVE));
    }

    protected List<CreateIndexDDL.Indexer> supportedIndexers()
    {
        // since legacy is async it's not clear how the test can wait for the background write to complete...
        return Collections.singletonList(CreateIndexDDL.SAI);
    }

    public Property.Command<State, Void, ?> selectExisting(RandomSource rs, State state)
    {
        NavigableSet<BytesPartitionState.Ref> keys = state.model.partitionKeys();
        BytesPartitionState.Ref ref = rs.pickOrderedSet(keys);
        Clustering<ByteBuffer> key = ref.key;

        Select.Builder builder = Select.builder().table(state.metadata);
        ImmutableUniqueList<Symbol> pks = state.model.factory.partitionColumns;
        ImmutableUniqueList<Symbol> cks = state.model.factory.clusteringColumns;
        for (Symbol pk : pks)
            builder.value(pk, key.bufferAt(pks.indexOf(pk)));

        boolean wholePartition = cks.isEmpty() || rs.nextBoolean();
        if (!wholePartition)
        {
            // find a row to select
            BytesPartitionState partition = state.model.get(ref);
            if (partition.isEmpty())
            {
                wholePartition = true;
            }
            else
            {
                NavigableSet<Clustering<ByteBuffer>> clusteringKeys = partition.clusteringKeys();
                Clustering<ByteBuffer> clusteringKey = rs.pickOrderedSet(clusteringKeys);
                for (Symbol ck : cks)
                    builder.value(ck, clusteringKey.bufferAt(cks.indexOf(ck)));
            }
        }
        Select select = builder.build();
        return state.command(rs, select, (wholePartition ? "Whole Partition" : "Single Row"));
    }

    public Property.Command<State, Void, ?> selectToken(RandomSource rs, State state)
    {
        NavigableSet<BytesPartitionState.Ref> keys = state.model.partitionKeys();
        BytesPartitionState.Ref ref = rs.pickOrderedSet(keys);

        Select.Builder builder = Select.builder().table(state.metadata);
        builder.where(FunctionCall.tokenByColumns(state.model.factory.partitionColumns),
                      Conditional.Where.Inequality.EQUAL,
                      token(state, ref));

        Select select = builder.build();
        return state.command(rs, select, "by token");
    }

    public Property.Command<State, Void, ?> selectTokenRange(RandomSource rs, State state)
    {
        NavigableSet<BytesPartitionState.Ref> keys = state.model.partitionKeys();
        BytesPartitionState.Ref start, end;
        switch (keys.size())
        {
            case 1:
                start = end = Iterables.get(keys, 0);
                break;
            case 2:
                start = Iterables.get(keys, 0);
                end = Iterables.get(keys, 1);
                break;
            case 0:
                throw new IllegalArgumentException("Unable to select token ranges when no partitions exist");
            default:
            {
                int si = rs.nextInt(0, keys.size() - 1);
                int ei = rs.nextInt(si + 1, keys.size());
                start = Iterables.get(keys, si);
                end = Iterables.get(keys, ei);
            }
            break;
        }
        Select.Builder builder = Select.builder().table(state.metadata);
        FunctionCall pkToken = FunctionCall.tokenByColumns(state.model.factory.partitionColumns);
        boolean startInclusive = rs.nextBoolean();
        boolean endInclusive = rs.nextBoolean();
        if (startInclusive && endInclusive && rs.nextBoolean())
        {
            // between
            builder.between(pkToken, token(state, start), token(state, end));
        }
        else
        {
            builder.where(pkToken,
                          startInclusive ? Conditional.Where.Inequality.GREATER_THAN_EQ : Conditional.Where.Inequality.GREATER_THAN,
                          token(state, start));
            builder.where(pkToken,
                          endInclusive ? Conditional.Where.Inequality.LESS_THAN_EQ : Conditional.Where.Inequality.LESS_THAN,
                          token(state, end));
        }
        Select select = builder.build();
        return state.command(rs, select, "by token range");
    }

    protected State createState(RandomSource rs, Cluster cluster)
    {
        return new State(rs, cluster);
    }

    protected Cluster createCluster() throws IOException
    {
        return createCluster(1, i -> {});
    }

    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = createCluster())
        {
            Property.StatefulBuilder statefulBuilder = stateful().withExamples(10).withSteps(400);
            preCheck(cluster, statefulBuilder);
            statefulBuilder.check(commands(() -> rs -> createState(rs, cluster))
                                  .add(StatefulASTBase::insert)
                                  .addIf(State::hasPartitions, this::selectExisting)
                                  .addAllIf(State::supportTokens, b -> b.add(this::selectToken)
                                                                        .add(this::selectTokenRange))
                                  .addIf(State::hasEnoughMemtable, StatefulASTBase::flushTable)
                                  .addIf(State::hasEnoughSSTables, StatefulASTBase::compactTable)
                                  .destroyState(State::close)
                                  .onSuccess(onSuccess(logger))
                                  .build());
        }
    }

    private TableMetadata defineTable(RandomSource rs, String ks)
    {
        //TODO (correctness): the id isn't correct... this is what we use to create the table, so would miss the actual ID
        // Defaults may also be incorrect, but given this is the same version it "shouldn't"
        //TODO (coverage): partition is defined at the cluster level, so have to hard code in this model as the table is changed rather than cluster being recreated... this limits coverage
        var metadata = Generators.toGen(new TableMetadataBuilder()
                                        .withTableKinds(TableMetadata.Kind.REGULAR)
                                        .withKnownMemtables()
                                        .withKeyspaceName(ks).withTableName("tbl")
                                        .withSimpleColumnNames()
                                        .withDefaultTypeGen(supportedTypes())
                                        .withPartitioner(Murmur3Partitioner.instance)
                                        .build())
                                 .next(rs);
        if (transactionalMode != null)
            metadata = metadata.withSwapped(metadata.params.unbuild().transactionalMode(transactionalMode).build());
        return metadata;
    }

    private List<CreateIndexDDL.Indexer> columnSupportsIndexing(TableMetadata metadata, ColumnMetadata col)
    {
        return supportedIndexers().stream()
                                  .filter(i -> i.supported(metadata, col))
                                  .collect(Collectors.toList());
    }

    private static FunctionCall token(State state, BytesPartitionState.Ref ref)
    {
        Preconditions.checkNotNull(ref.key);
        List<Value> values = new ArrayList<>(ref.key.size());
        for (int i = 0; i < ref.key.size(); i++)
        {
            ByteBuffer bb = ref.key.bufferAt(i);
            Symbol type = state.model.factory.partitionColumns.get(i);
            values.add(new Bind(bb, type.type()));
        }
        return FunctionCall.tokenByValue(values);
    }

    public class State extends CommonState
    {
        protected final LinkedHashMap<Symbol, IndexedColumn> indexes;
        private final Gen<Mutation> mutationGen;

        public State(RandomSource rs, Cluster cluster)
        {
            super(rs, cluster, defineTable(rs, nextKeyspace()));

            this.indexes = createIndexes(rs, metadata);

            cluster.forEach(i -> i.nodetoolResult("disableautocompaction", metadata.keyspace, this.metadata.name).asserts().success());

            List<LinkedHashMap<Symbol, Object>> uniquePartitions;
            {
                int unique = rs.nextInt(1, 10);
                List<Symbol> columns = model.factory.partitionColumns;
                List<Gen<?>> gens = new ArrayList<>(columns.size());
                for (int i = 0; i < columns.size(); i++)
                    gens.add(toGen(getTypeSupport(columns.get(i).type()).valueGen));
                uniquePartitions = Gens.lists(r2 -> {
                    LinkedHashMap<Symbol, Object> vs = new LinkedHashMap<>();
                    for (int i = 0; i < columns.size(); i++)
                        vs.put(columns.get(i), gens.get(i).next(r2));
                    return vs;
                }).uniqueBestEffort().ofSize(unique).next(rs);
            }

            this.mutationGen = toGen(new ASTGenerators.MutationGenBuilder(metadata)
                                     .withoutTransaction()
                                     .withoutTtl()
                                     .withoutTimestamp()
                                     .withPartitions(SourceDSL.arbitrary().pick(uniquePartitions))
                                     .build());
        }

        @Override
        protected Gen<Mutation> mutationGen()
        {
            return mutationGen;
        }

        private LinkedHashMap<Symbol, IndexedColumn> createIndexes(RandomSource rs, TableMetadata metadata)
        {
            LinkedHashMap<Symbol, IndexedColumn> indexed = new LinkedHashMap<>();
            // for some test runs, avoid using indexes
            if (rs.nextBoolean())
                return indexed;
            for (ColumnMetadata col : metadata.columnsInFixedOrder())
            {
                Symbol symbol = Symbol.from(col);
                AbstractType<?> type = symbol.type();

//                if (col.name.toString().length() >= 48) continue; // TODO (correctness): https://issues.apache.org/jira/browse/CASSANDRA-19897

                if (type.isCollection() && !type.isFrozenCollection()) continue; //TODO (coverage): include non-frozen collections;  the index part works fine, its the select that fails... basic equality isn't allowed for map type... so how do you query?
                List<CreateIndexDDL.Indexer> allowed = columnSupportsIndexing(metadata, col);
                if (allowed.isEmpty()) continue;
                CreateIndexDDL.Indexer indexer = rs.pick(allowed);
                ReferenceExpression colExpression = Symbol.from(col);
                if (type.isFrozenCollection())
                    colExpression = new CreateIndexDDL.CollectionReference(CreateIndexDDL.CollectionReference.Kind.FULL, colExpression);

                String name = "tbl_" + col.name;
                CreateIndexDDL ddl = new CreateIndexDDL(rs.pick(CreateIndexDDL.Version.values()),
                                                        indexer,
                                                        Optional.of(new Symbol(name, UTF8Type.instance)),
                                                        TableReference.from(metadata),
                                                        Collections.singletonList(colExpression),
                                                        Collections.emptyMap());
                String stmt = ddl.toCQL();
                logger.info(stmt);
                cluster.schemaChange(stmt);

                //noinspection OptionalGetWithoutIsPresent
                SAIUtil.waitForIndexQueryable(cluster, metadata.keyspace, ddl.name.get().name());

                indexed.put(symbol, new IndexedColumn(symbol, ddl));
            }
            return indexed;
        }

        public boolean hasPartitions()
        {
            return !model.isEmpty();
        }

        public boolean supportTokens()
        {
            return hasPartitions();
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("\nSetup:\n");
            toString(sb);
            indexes.values().forEach(c -> sb.append('\n').append(c.indexDDL.toCQL()).append(';'));
            return sb.toString();
        }
    }

    public static class IndexedColumn
    {
        public final Symbol symbol;
        public final CreateIndexDDL indexDDL;

        public IndexedColumn(Symbol symbol, CreateIndexDDL indexDDL)
        {
            this.symbol = symbol;
            this.indexDDL = indexDDL;
        }
    }
}

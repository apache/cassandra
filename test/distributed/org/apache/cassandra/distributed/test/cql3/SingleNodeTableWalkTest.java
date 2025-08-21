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
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Property;
import accord.utils.RandomSource;
import org.apache.cassandra.cql3.KnownIssue;
import org.apache.cassandra.cql3.ast.Bind;
import org.apache.cassandra.cql3.ast.Conditional.Where.Inequality;
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
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.utils.LoggingCommand;
import org.apache.cassandra.harry.model.BytesPartitionState;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ASTGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeGenBuilder;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.ImmutableUniqueList;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.apache.cassandra.utils.Generators.toGen;

public class SingleNodeTableWalkTest extends StatefulASTBase
{
    private static final Gen<Gen<Boolean>> BOOLEAN_DISTRIBUTION = Gens.bools().mixedDistribution();
    //TODO (coverage): COMPOSITE, DYNAMIC_COMPOSITE
    private static final Gen<Gen<TypeKind>> TYPE_KIND_DISTRIBUTION = Gens.mixedDistribution(TypeKind.PRIMITIVE,
                                                                                            TypeKind.SET, TypeKind.LIST, TypeKind.MAP,
                                                                                            TypeKind.TUPLE, TypeKind.UDT,
                                                                                            TypeKind.VECTOR
    );
    private static final Gen<Gen<AbstractType<?>>> PRIMITIVE_DISTRIBUTION = Gens.mixedDistribution(AbstractTypeGenerators.knownPrimitiveTypes()
                                                                                                                         .stream()
                                                                                                                         .filter(t -> !AbstractTypeGenerators.isUnsafeEquality(t))
                                                                                                                         .collect(Collectors.toList()));
    private static final Logger logger = LoggerFactory.getLogger(SingleNodeTableWalkTest.class);

    protected static boolean READ_AFTER_WRITE = false;

    public SingleNodeTableWalkTest()
    {
    }

    protected void preCheck(Cluster cluster, Property.StatefulBuilder builder)
    {
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
        // CQL operations may have opertors such as +, -, and / (example 4 + 4), to "apply" them to get a constant value
        // CQL_DEBUG_APPLY_OPERATOR = true;
        // When mutations look to be lost as seen by more complex SELECTs, it can be useful to just SELECT the partition/row right after to write to see if it was safe at the time.
        // READ_AFTER_WRITE = true;
    }

    protected TypeGenBuilder supportedTypes(RandomSource rs)
    {
        return AbstractTypeGenerators.builder()
                                     .withTypeKinds(Generators.fromGen(TYPE_KIND_DISTRIBUTION.next(rs)))
                                     .withPrimitives(Generators.fromGen(PRIMITIVE_DISTRIBUTION.next(rs)))
                                     .withUserTypeFields(AbstractTypeGenerators.UserTypeFieldsGen.simpleNames())
                                     .withMaxDepth(1);
    }

    protected TypeGenBuilder supportedPrimaryColumnTypes(RandomSource rs)
    {
        return AbstractTypeGenerators.builder()
                                     .withTypeKinds(TypeKind.PRIMITIVE)
                                     .withPrimitives(Generators.fromGen(PRIMITIVE_DISTRIBUTION.next(rs)));
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
        return state.command(rs, select, (wholePartition ? "By Partition Key" : "By Primary Key"));
    }

    public Property.Command<State, Void, ?> selectToken(RandomSource rs, State state)
    {
        NavigableSet<BytesPartitionState.Ref> keys = state.model.partitionKeys();
        BytesPartitionState.Ref ref = rs.pickOrderedSet(keys);

        Select.Builder builder = Select.builder().table(state.metadata);
        builder.where(FunctionCall.tokenByColumns(state.model.factory.partitionColumns),
                      Inequality.EQUAL,
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
                          startInclusive ? Inequality.GREATER_THAN_EQ : Inequality.GREATER_THAN,
                          token(state, start));
            builder.where(pkToken,
                          endInclusive ? Inequality.LESS_THAN_EQ : Inequality.LESS_THAN,
                          token(state, end));
        }
        Select select = builder.build();
        return state.command(rs, select, "by token range");
    }

    public Property.Command<State, Void, ?> partitionRestrictedQuery(RandomSource rs, State state)
    {
        //TODO (now): remove duplicate logic
        NavigableSet<BytesPartitionState.Ref> keys = state.model.partitionKeys();
        BytesPartitionState.Ref ref = rs.pickOrderedSet(keys);
        Clustering<ByteBuffer> key = ref.key;

        Select.Builder builder = Select.builder().table(state.metadata);
        ImmutableUniqueList<Symbol> pks = state.model.factory.partitionColumns;
        for (Symbol pk : pks)
            builder.value(pk, key.bufferAt(pks.indexOf(pk)));


        List<Symbol> searchableColumns = state.searchableNonPartitionColumns;
        Symbol symbol = rs.pick(searchableColumns);

        TreeMap<ByteBuffer, List<BytesPartitionState.PrimaryKey>> universe = state.model.index(ref, symbol);
        // we need to index 'null' so LT works, but we can not directly query it... so filter out when selecting values
        NavigableSet<ByteBuffer> allowed = Sets.filter(universe.navigableKeySet(), b -> !ByteBufferUtil.EMPTY_BYTE_BUFFER.equals(b));
        if (allowed.isEmpty())
            return Property.ignoreCommand();
        ByteBuffer value = rs.pickOrderedSet(allowed);

        EnumSet<CreateIndexDDL.QueryType> supported = !state.indexes.containsKey(symbol)
                                                      ? EnumSet.noneOf(CreateIndexDDL.QueryType.class)
                                                      : state.indexes.get(symbol).supportedQueries();
        if (supported.isEmpty() || !supported.contains(CreateIndexDDL.QueryType.Range))
            builder.allowFiltering();

        // there are known SAI bugs, so need to avoid them to stay stable...
        if (state.indexes.containsKey(symbol) && state.indexes.get(symbol).indexDDL.indexer == CreateIndexDDL.SAI)
        {
            if (symbol.type() == InetAddressType.instance
                && IGNORED_ISSUES.contains(KnownIssue.SAI_INET_MIXED))
                return eqSearch(rs, state, symbol, value, builder);
        }

        if (rs.nextBoolean())
            return simpleRangeSearch(rs, state, symbol, value, builder);
        //TODO (coverage): define search that has a upper and lower bound: a > and a < | a beteeen ? and ?
        return eqSearch(rs, state, symbol, value, builder);
    }

    public Property.Command<State, Void, ?> nonPartitionQuery(RandomSource rs, State state)
    {
        Symbol symbol = rs.pick(state.searchableColumns);
        TreeMap<ByteBuffer, List<BytesPartitionState.PrimaryKey>> universe = state.model.index(symbol);
        // we need to index 'null' so LT works, but we can not directly query it... so filter out when selecting values
        NavigableSet<ByteBuffer> allowed = Sets.filter(universe.navigableKeySet(), b -> !ByteBufferUtil.EMPTY_BYTE_BUFFER.equals(b));
        if (allowed.isEmpty())
            return Property.ignoreCommand();
        ByteBuffer value = rs.pickOrderedSet(allowed);
        Select.Builder builder = Select.builder().table(state.metadata);

        EnumSet<CreateIndexDDL.QueryType> supported = !state.indexes.containsKey(symbol) ? EnumSet.noneOf(CreateIndexDDL.QueryType.class) : state.indexes.get(symbol).supportedQueries();
        if (supported.isEmpty() || !supported.contains(CreateIndexDDL.QueryType.Range))
            builder.allowFiltering();

        // there are known SAI bugs, so need to avoid them to stay stable...
        if (state.indexes.containsKey(symbol) && state.indexes.get(symbol).indexDDL.indexer == CreateIndexDDL.SAI)
        {
            if (symbol.type() == InetAddressType.instance
                && IGNORED_ISSUES.contains(KnownIssue.SAI_INET_MIXED))
                return eqSearch(rs, state, symbol, value, builder);
        }

        if (rs.nextBoolean())
            return simpleRangeSearch(rs, state, symbol, value, builder);
        //TODO (coverage): define search that has a upper and lower bound: a > and a < | a beteeen ? and ?
        return eqSearch(rs, state, symbol, value, builder);
    }

    public Property.Command<State, Void, ?> multiColumnQuery(RandomSource rs, State state)
    {
        List<Symbol> allowedColumns = state.multiColumnQueryColumns();

        if (allowedColumns.size() <= 1)
            throw new IllegalArgumentException("Unable to do multiple column query when there is only a single column");

        int numColumns = rs.nextInt(1, allowedColumns.size()) + 1;

        List<Symbol> cols = Gens.lists(Gens.pick(allowedColumns)).unique().ofSize(numColumns).next(rs);

        Select.Builder builder = Select.builder().table(state.metadata).allowFiltering();

        for (Symbol symbol : cols)
        {
            TreeMap<ByteBuffer, List<BytesPartitionState.PrimaryKey>> universe = state.model.index(symbol);
            NavigableSet<ByteBuffer> allowed = Sets.filter(universe.navigableKeySet(), b -> !ByteBufferUtil.EMPTY_BYTE_BUFFER.equals(b));
            //TODO (now): support
            if (allowed.isEmpty())
                return Property.ignoreCommand();
            ByteBuffer value = rs.pickOrderedSet(allowed);
            builder.value(symbol, value);
        }

        Select select = builder.build();
        String annotate = cols.stream().map(symbol -> {
            var indexed = state.indexes.get(symbol);
            return symbol.detailedName() + (indexed == null ? "" : " (indexed with " + indexed.indexDDL.indexer.name() + ')');
        }).collect(Collectors.joining(", "));
        return state.command(rs, select, annotate);
    }

    private Property.Command<State, Void, ?> simpleRangeSearch(RandomSource rs, State state, Symbol symbol, ByteBuffer value, Select.Builder builder)
    {
        // do a simple search, like > or <
        Inequality kind = state.rangeInequalityGen.next(rs);
        builder.where(symbol, kind, value);
        Select select = builder.build();
        var indexed = state.indexes.get(symbol);
        return state.command(rs, select, symbol.detailedName() + (indexed == null ? "" : ", indexed with " + indexed.indexDDL.indexer.name()));
    }

    private Property.Command<State, Void, ?> eqSearch(RandomSource rs, State state, Symbol symbol, ByteBuffer value, Select.Builder builder)
    {
        builder.value(symbol, value);

        Select select = builder.build();
        var indexed = state.indexes.get(symbol);
        return state.command(rs, select, symbol.detailedName() + (indexed == null ? "" : ", indexed with " + indexed.indexDDL.indexer.name()));
    }

    protected State createState(RandomSource rs, Cluster cluster)
    {
        return new State(rs, cluster);
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
            Property.StatefulBuilder statefulBuilder = stateful().withExamples(10).withSteps(400);
            preCheck(cluster, statefulBuilder);
            statefulBuilder.check(commands(() -> rs -> createState(rs, cluster))
                                  .add(StatefulASTBase::insert)
                                  .add(StatefulASTBase::fullTableScan)
                                  .addIf(State::allowUsingTimestamp, StatefulASTBase::validateUsingTimestamp)
                                  .addIf(State::hasPartitions, this::selectExisting)
                                  .addAllIf(State::supportTokens,
                                            this::selectToken,
                                            this::selectTokenRange,
                                            StatefulASTBase::selectMinTokenRange)
                                  .addIf(State::hasEnoughMemtable, StatefulASTBase::flushTable)
                                  .addIf(State::hasEnoughSSTables, StatefulASTBase::compactTable)
                                  .addAllIf(BaseState::allowRepair,
                                            StatefulASTBase::incrementalRepair,
                                            StatefulASTBase::previewRepair)
                                  .addIf(State::allowNonPartitionQuery, this::nonPartitionQuery)
                                  .addIf(State::allowNonPartitionMultiColumnQuery, this::multiColumnQuery)
                                  .addIf(State::allowPartitionQuery, this::partitionRestrictedQuery)
                                  .destroyState(State::close)
                                  .commandsTransformer(LoggingCommand.factory())
                                  .onSuccess(onSuccess(logger))
                                  .build());
        }
    }

    protected TableMetadata defineTable(RandomSource rs, String ks)
    {
        //TODO (correctness): the id isn't correct... this is what we use to create the table, so would miss the actual ID
        // Defaults may also be incorrect, but given this is the same version it "shouldn't"
        //TODO (coverage): partition is defined at the cluster level, so have to hard code in this model as the table is changed rather than cluster being recreated... this limits coverage
        return toGen(new TableMetadataBuilder()
                     .withTableKinds(TableMetadata.Kind.REGULAR)
                     .withParams(b -> b.withKnownMemtables()
                                       .withCaching()
                                       .withCompaction()
                                       .withCompression())
                     .withKeyspaceName(ks).withTableName("tbl")
                     .withSimpleColumnNames()
                     .withDefaultTypeGen(supportedTypes(rs))
                     .withPrimaryColumnTypeGen(supportedPrimaryColumnTypes(rs))
                     .withPartitioner(Murmur3Partitioner.instance)
                     .build())
               .next(rs);
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
        private final List<Symbol> searchableNonPartitionColumns;
        private final List<Symbol> searchableColumns;
        private final List<Symbol> nonPkIndexedColumns;

        public State(RandomSource rs, Cluster cluster)
        {
            super(rs, cluster, defineTable(rs, nextKeyspace()));

            this.indexes = createIndexes(rs, metadata);

            cluster.forEach(i -> i.nodetoolResult("disableautocompaction", metadata.keyspace, this.metadata.name).asserts().success());

            ASTGenerators.MutationGenBuilder mutationGenBuilder = new ASTGenerators.MutationGenBuilder(metadata)
                                                                  .withTxnSafe()
                                                                  .withColumnExpressions(e -> e.withOperators(Generators.fromGen(BOOLEAN_DISTRIBUTION.next(rs))))
                                                                  .withIgnoreIssues(IGNORED_ISSUES);

            // Run the test with and without bound partitions
            // When using fixed partitions, each mutation will be for a single partition and will use pk=? syntax
            // When using unbounded partitions then IN clause is used on partition keys, leading to mutations touching multiple partitions
            if (rs.nextBoolean())
            {
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
                mutationGenBuilder.withPartitions(Generators.fromGen(Gens.mixedDistribution(uniquePartitions).next(rs)));
            }

            if (IGNORED_ISSUES.contains(KnownIssue.SAI_EMPTY_TYPE))
            {
                model.factory.regularAndStaticColumns.stream()
                                                     // exclude SAI indexed columns
                                                     .filter(s -> !indexes.containsKey(s) || indexes.get(s).indexDDL.indexer != CreateIndexDDL.SAI)
                                                     .forEach(mutationGenBuilder::allowEmpty);
            }
            else
            {
                model.factory.regularAndStaticColumns.forEach(mutationGenBuilder::allowEmpty);
            }
            model.factory.regularAndStaticColumns.forEach(mutationGenBuilder::allowNull);
            this.mutationGen = toMutationGen(mutationGenBuilder);

            var nonPartitionColumns = ImmutableList.<Symbol>builder()
                                                   .addAll(model.factory.clusteringColumns)
                                                   .addAll(model.factory.staticColumns)
                                                   .addAll(model.factory.regularColumns)
                                                   .build();
            searchableNonPartitionColumns = nonPartitionColumns.stream()
                                                               .filter(this::isSearchable)
                                                               .collect(Collectors.toList());
            nonPkIndexedColumns = nonPartitionColumns.stream()
                                                     .filter(indexes::containsKey)
                                                     .collect(Collectors.toList());

            searchableColumns = (metadata.partitionKeyColumns().size() > 1 ? model.factory.selectionOrder : nonPartitionColumns)
                                .stream()
                                .filter(this::isSearchable)
                                .collect(Collectors.toList());
        }

        @Override
        protected boolean readAfterWrite()
        {
            return READ_AFTER_WRITE;
        }

        protected Gen<Mutation> toMutationGen(ASTGenerators.MutationGenBuilder mutationGenBuilder)
        {
            return toGen(mutationGenBuilder.build());
        }

        private boolean isSearchable(Symbol symbol)
        {
            // See org.apache.cassandra.cql3.Operator.validateFor
            // multi cell collections can only be searched if you search their elements, not the collection as a whole
            //TODO (coverage): can you query for UDT fields?  its a single cell so you "should"?
            return !(symbol.type().isMultiCell() && (symbol.type().isCollection() || symbol.type().isUDT()));
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

                if (col.name.toString().length() >= 48
                    && IGNORED_ISSUES.contains(KnownIssue.CUSTOM_INDEX_MAX_COLUMN_48))
                    continue;

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

        public boolean supportTokens()
        {
            return hasPartitions();
        }

        public boolean allowNonPartitionQuery()
        {
            return !model.isEmpty() && !searchableColumns.isEmpty();
        }

        public boolean allowNonPartitionMultiColumnQuery()
        {
            return allowNonPartitionQuery() && multiColumnQueryColumns().size() > 1;
        }

        private List<Symbol> multiColumnQueryColumns()
        {
            List<Symbol> allowedColumns = searchableColumns;
            if (hasMultiNodeMultiColumnAllowFilteringWithLocalWritesIssue())
                allowedColumns = nonPkIndexedColumns;
            if (IGNORED_ISSUES.contains(KnownIssue.SAI_AND_VECTOR_COLUMNS) && !indexes.isEmpty())
                allowedColumns = allowedColumns.stream().filter(s -> !s.type().isVector()).collect(Collectors.toList());
            return allowedColumns;
        }

        private boolean hasMultiNodeMultiColumnAllowFilteringWithLocalWritesIssue()
        {
            return isMultiNode() && IGNORED_ISSUES.contains(KnownIssue.AF_MULTI_NODE_MULTI_COLUMN_AND_NODE_LOCAL_WRITES);
        }

        public boolean allowPartitionQuery()
        {
            return !(model.isEmpty() || searchableNonPartitionColumns.isEmpty());
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

        public EnumSet<CreateIndexDDL.QueryType> supportedQueries()
        {
            return indexDDL.indexer.supportedQueries(symbol.type());
        }
    }
}

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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Property;
import accord.utils.RandomSource;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.KnownIssue;
import org.apache.cassandra.cql3.ast.Bind;
import org.apache.cassandra.cql3.ast.CQLFormatter;
import org.apache.cassandra.cql3.ast.Conditional;
import org.apache.cassandra.cql3.ast.FunctionCall;
import org.apache.cassandra.cql3.ast.Literal;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.StandardVisitors;
import org.apache.cassandra.cql3.ast.Statement;
import org.apache.cassandra.cql3.ast.TableReference;
import org.apache.cassandra.cql3.ast.Txn;
import org.apache.cassandra.cql3.ast.Value;
import org.apache.cassandra.cql3.ast.Visitor;
import org.apache.cassandra.cql3.ast.Visitor.CompositeVisitor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.JavaDriverUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.util.DriverUtils;
import org.apache.cassandra.harry.model.ASTSingleTableModel;
import org.apache.cassandra.harry.util.StringUtils;
import org.apache.cassandra.repair.RepairGenerators;
import org.apache.cassandra.repair.RepairGenerators.PreviewType;
import org.apache.cassandra.repair.RepairGenerators.RepairType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.Generators;
import org.assertj.core.api.Assertions;
import org.quicktheories.generators.SourceDSL;

import static accord.utils.Property.ignoreCommand;
import static accord.utils.Property.multistep;
import static org.apache.cassandra.utils.AbstractTypeGenerators.overridePrimitiveTypeSupport;
import static org.apache.cassandra.utils.AbstractTypeGenerators.stringComparator;

public class StatefulASTBase extends TestBaseImpl
{
    protected static final EnumSet<KnownIssue> IGNORED_ISSUES = KnownIssue.ignoreAll();
    /**
     * mutations and selects will use operators (eg. {@code 4 + 4}, the + operator), and this will be reflected in the history output.
     *
     * When an issue is found its common to filter out insertions to different partitions/rows but this can become a problem
     * as the issue is for {@code pk=8} but the insert is to {@code 4 + 4}!
     *
     * Setting this to {@code true} will cause all operators to be "applied" or "executored" and the CQL in the history
     * will be the output (eg. {@code 4 + 4 } is replaced with {@code 8}).
     */
    protected static boolean CQL_DEBUG_APPLY_OPERATOR = false;

    protected static final Gen<Gen<Boolean>> BOOL_DISTRIBUTION = Gens.bools().mixedDistribution();
    protected static final Gen<Gen<Conditional.Where.Inequality>> LESS_THAN_DISTRO = Gens.mixedDistribution(Stream.of(Conditional.Where.Inequality.values())
                                                                                                                  .filter(i -> i == Conditional.Where.Inequality.LESS_THAN || i == Conditional.Where.Inequality.LESS_THAN_EQ)
                                                                                                                  .collect(Collectors.toList()));
    protected static final Gen<Gen<Conditional.Where.Inequality>> GREATER_THAN_DISTRO = Gens.mixedDistribution(Stream.of(Conditional.Where.Inequality.values())
                                                                                                                     .filter(i -> i == Conditional.Where.Inequality.GREATER_THAN || i == Conditional.Where.Inequality.GREATER_THAN_EQ)
                                                                                                                     .collect(Collectors.toList()));
    protected static final Gen<Gen<Conditional.Where.Inequality>> RANGE_INEQUALITY_DISTRO = Gens.mixedDistribution(Stream.of(Conditional.Where.Inequality.values())
                                                                                                                         .filter(i -> i != Conditional.Where.Inequality.EQUAL && i != Conditional.Where.Inequality.NOT_EQUAL)
                                                                                                                         .collect(Collectors.toList()));
    protected static final Gen<Gen.IntGen> FETCH_SIZE_DISTRO = Gens.mixedDistribution(new int[] {1, 10, 100, 1000, 5000});
    protected static final Gen<Gen.IntGen> LIMIT_DISTRO = Gens.mixedDistribution(1, 1001);
    protected static final Gen<Gen.IntGen> REPAIR_TYPE_EMPTY_MODEL_DISTRO = Gens.mixedDistribution(0, 2);
    protected static final Gen<Gen.IntGen> REPAIR_TYPE_DISTRO = Gens.mixedDistribution(0, 3);
    private static final ListType<Long> LONG_LIST_TYPE = ListType.getInstance(LongType.instance, false);

    static
    {
        // since this test does frequent truncates, the info table gets updated and forced flushed... which is 90% of the cost of this test...
        // this flag disables that flush
        CassandraRelevantProperties.UNSAFE_SYSTEM.setBoolean(true);
        // queries maybe dumb which could lead to performance issues causing timeouts... don't timeout!
        CassandraRelevantProperties.SAI_TEST_DISABLE_TIMEOUT.setBoolean(true);

        overridePrimitiveTypeSupport(AsciiType.instance, AbstractTypeGenerators.TypeSupport.of(AsciiType.instance, SourceDSL.strings().ascii().ofLengthBetween(1, 10), stringComparator(AsciiType.instance)));
        overridePrimitiveTypeSupport(UTF8Type.instance, AbstractTypeGenerators.TypeSupport.of(UTF8Type.instance, Generators.utf8(1, 10), stringComparator(UTF8Type.instance)));
        overridePrimitiveTypeSupport(BytesType.instance, AbstractTypeGenerators.TypeSupport.of(BytesType.instance, Generators.bytes(1, 10), FastByteOperations::compareUnsigned));
    }

    /**
     * There is an assumption that keyspace name doesn't impact this test, so to get simpler names use this counter...
     * if this assumption doesn't hold, then need to switch to random or rely on DROP KEYSPACE.
     */
    private static final AtomicInteger COUNTER = new AtomicInteger();

    protected static String nextKeyspace()
    {
        return "ks" + COUNTER.incrementAndGet();
    }

    protected void clusterConfig(IInstanceConfig config)
    {
        config.set("repair.retries.max_attempts", Integer.MAX_VALUE);
    }

    protected void clusterInitializer(ClassLoader cl, int node)
    {

    }

    protected Cluster createCluster(int nodeCount) throws IOException
    {
        return createCluster(nodeCount, this::clusterConfig, this::clusterInitializer);
    }

    protected static Cluster createCluster(int nodeCount, Consumer<IInstanceConfig> config, BiConsumer<ClassLoader, Integer> instanceInitializer) throws IOException
    {
        Cluster cluster = Cluster.build(nodeCount)
                                 .withInstanceInitializer(instanceInitializer)
                                 .withConfig(c -> {
                                     c.with(Feature.NATIVE_PROTOCOL, Feature.NETWORK, Feature.GOSSIP)
                                      // When drop tables or truncate are performed, we attempt to take snapshots.  This can be costly and isn't needed by these tests
                                      .set("incremental_backups", false);
                                     config.accept(c);
                                 })
                                 .start();
        // we don't allow setting null in yaml... but these configs support null!
        cluster.forEach(i ->  i.runOnInstance(() -> {
            // When values are large SAI will drop them... soooo... disable that... this test does not care about perf but correctness
            DatabaseDescriptor.getRawConfig().sai_frozen_term_size_warn_threshold = null;
            DatabaseDescriptor.getRawConfig().sai_frozen_term_size_fail_threshold = null;
        }));
        return cluster;
    }

    protected <S extends BaseState> Property.StatefulSuccess<S, Void> onSuccess(Logger logger)
    {
        return (state, sut, history) -> logger.info("Successful for the following:\nState {}\nHistory:\n{}", state, Property.formatList("\t\t", history));
    }

    protected static <S extends BaseState> Property.Command<S, Void, ?> flushTable(RandomSource rs, S state)
    {
        return new Property.SimpleCommand<>("nodetool flush " + state.metadata.keyspace + ' ' + state.metadata.name, s2 -> {
            s2.cluster.forEach(i -> i.nodetoolResult("flush", s2.metadata.keyspace, s2.metadata.name).asserts().success());
            s2.flush();
        });
    }

    protected static <S extends BaseState> Property.Command<S, Void, ?> compactTable(RandomSource rs, S state)
    {
        return new Property.SimpleCommand<>("nodetool compact " + state.metadata.keyspace + ' ' + state.metadata.name, s2 -> {
            state.cluster.forEach(i -> i.nodetoolResult("compact", s2.metadata.keyspace, s2.metadata.name).asserts().success());
            s2.compact();
        });
    }

    protected static <S extends CommonState> Property.Command<S, Void, ?> validateUsingTimestamp(RandomSource rs, S state)
    {
        if (state.operations == 0)
            return ignoreCommand();
        var builder = Select.builder(state.metadata);
        for (var c : state.model.factory.regularAndStaticColumns)
            builder.selection(FunctionCall.writetime(c));
        ByteBuffer upperboundTimestamp = LongType.instance.decompose((long) state.operations);
        var select = builder.build();
        var inst = state.selectInstance(rs);
        return new Property.SimpleCommand<>(state.humanReadable(select, null), s -> {
            var result = s.executeQuery(inst, Integer.MAX_VALUE, s.selectCl(), select);
            for (var row : result)
            {
                for (var col : state.model.factory.regularAndStaticColumns)
                {
                    int idx = state.model.factory.regularAndStaticColumns.indexOf(col);
                    ByteBuffer value = row[idx];
                    if (value == null) continue;
                    if (col.type().isMultiCell())
                    {
                        List<ByteBuffer> timestamps = LONG_LIST_TYPE.unpack(value);
                        int cellIndex = 0;
                        for (var timestamp : timestamps)
                        {
                            Assertions.assertThat(LongType.instance.compare(timestamp, upperboundTimestamp))
                                      .describedAs("Unexected timestamp at multi-cell index %s for col %s: %s > %s", cellIndex, col, LongType.instance.compose(timestamp), state.operations)
                                      .isLessThanOrEqualTo(state.operations);
                            cellIndex++;
                        }
                    }
                    else
                    {
                        Assertions.assertThat(LongType.instance.compare(value, upperboundTimestamp))
                                  .describedAs("Unexected timestamp for col %s: %s > %s", col, LongType.instance.compose(value), state.operations)
                                  .isLessThanOrEqualTo(state.operations);
                    }
                }
            }
        });
    }

    protected static <S extends CommonState> Property.Command<S, Void, ?> insert(RandomSource rs, S state)
    {
        int timestamp = ++state.operations;
        Mutation original = state.mutationGen().next(rs);
        Mutation mutation = state.allowUsingTimestamp()
                            ? original.withTimestamp(timestamp)
                            : original;

        if (!state.readAfterWrite())
            return state.command(rs, mutation);

        return multistep(state.command(rs, mutation),
                         state.commandSafeRandomHistory(selectForMutation(state, mutation), "Select for Mutation Validation"));
    }

    protected static <S extends BaseState> Property.Command<S, Void, ?> incrementalRepair(RandomSource rs, S state)
    {
        return repair(rs, state, state.repairArgsBuilder().withType(i -> RepairType.IR).withPreviewType(i -> PreviewType.NONE), null);
    }

    protected static <S extends BaseState> Property.Command<S, Void, ?> previewRepair(RandomSource rs, S state)
    {
        return repair(rs, state, state.repairArgsBuilder().withType(i -> RepairType.FULL).withPreviewType(i -> PreviewType.REPAIRED), null);
    }

    protected static <S extends BaseState> Property.Command<S, Void, ?> repair(RandomSource rs, S state, RepairGenerators.Builder argsBuilder, @Nullable String annotate)
    {
        IInvokableInstance inst = state.selectInstance(rs);
        Gen<List<String>> argsGen = argsBuilder.build();
        List<String> args = ImmutableList.<String>builder()
                                         .add("repair")
                                         .addAll(argsGen.next(rs))
                                         .build();
        boolean preview = RepairGenerators.isPreview(args);
        // mimic org.apache.cassandra.repair.state.CoordinatorState.getType
        String type;
        if (preview)
        {
            // mimic org.apache.cassandra.tools.nodetool.Repair.getPreviewKind
            PreviewType previewType = RepairGenerators.previewType(args);
            switch (previewType)
            {
                case REPAIRED:
                    type = "preview repaired";
                    break;
                case UNREPAIRED:
                    type = RepairGenerators.isFull(args) ? "preview full" : "preview unrepaired";
                    break;
                default:
                    throw new UnsupportedOperationException(previewType.name());
            }
        }
        else
        {
            type = RepairGenerators.isFull(args) ? "full" : "incremental";
        }

        String postfix = "type " + type + ", on " + inst;
        if (annotate == null) annotate = postfix;
        else                  annotate += ", " + postfix;

        return new Property.SimpleCommand<>("nodetool " + String.join(" ", args) + " -- " + annotate, s2 -> {
            inst.nodetoolResult(args.toArray(String[]::new)).asserts().success();
            if (!preview)
                s2.repair();
        });
    }

    private static <S extends CommonState> Select selectForMutation(S state, Mutation mutation)
    {
        var select = Select.builder(state.metadata).allowFiltering();
        switch (mutation.kind)
        {
            case INSERT:
            {
                var insert = (Mutation.Insert) mutation;
                for (var c : state.model.factory.partitionColumns)
                    select.value(c, insert.values.get(c));
            }
            break;
            default:
            {
                select.where(mutation.kind == Mutation.Kind.UPDATE
                             ? ((Mutation.Update) mutation).where
                             : ((Mutation.Delete) mutation).where);
            }
        }
        return select.build();
    }

    protected static <S extends BaseState> Property.Command<S, Void, ?> fullTableScan(RandomSource rs, S state)
    {
        Select select = Select.builder(state.metadata).build();
        return state.command(rs, select, "full table scan");
    }

    protected static <S extends BaseState> Property.Command<S, Void, ?> selectMinTokenRange(RandomSource rs, S state)
    {
        var key = rs.pickOrderedSet(state.model.partitionKeys());
        FunctionCall tokenCall = FunctionCall.tokenByColumns(state.model.factory.partitionColumns);
        Literal min = Literal.of(key.token.getLongValue());
        Literal max = Literal.of(Long.MIN_VALUE);
        if (rs.nextBoolean())
        {
            Literal tmp = min;
            min = max;
            max = tmp;
        }
        Select select;
        if (rs.nextBoolean())
        {
            select = Select.builder(state.metadata)
                           .where(tokenCall, state.greaterThanGen.next(rs), min)
                           .where(tokenCall, state.lessThanGen.next(rs), max)
                           .build();
        }
        else
        {
            // it's possible that the range was flipped, which is known bug with BETWEEN, so
            // make sure the range is not flipped until that bug is fixed
            if (IGNORED_ISSUES.contains(KnownIssue.BETWEEN_START_LARGER_THAN_END))
            {
                min = Literal.of(key.token.getLongValue());
                max = Literal.of(Long.MIN_VALUE);
            }
            select = Select.builder(state.metadata)
                           .between(tokenCall, min, max)
                           .build();
        }
        return state.command(rs, select, "min token range");
    }

    protected static abstract class BaseState implements AutoCloseable
    {
        protected final RandomSource rs;
        protected final Cluster cluster;
        protected final com.datastax.driver.core.Cluster client;
        protected final Session session;
        protected final Gen<Boolean> bindOrLiteralGen;
        protected final Gen<Boolean> betweenEqGen;
        protected final Gen<Boolean> useFetchSizeGen, usePerPartitionLimitGen, useLimitGen;
        protected final Gen.IntGen perPartitionLimitGen, limitGen;
        protected final Gen<Conditional.Where.Inequality> lessThanGen;
        protected final Gen<Conditional.Where.Inequality> greaterThanGen;
        protected final Gen<Conditional.Where.Inequality> rangeInequalityGen;
        protected final Gen.IntGen repairTypeEmptyModelGen, repairTypeGen;
        protected final Gen.IntGen fetchSizeGen;
        protected final TableMetadata metadata;
        protected final TableReference tableRef;
        protected final ASTSingleTableModel model;
        private final String sstableFormatName;
        private final Visitor debug;
        private final int enoughMemtables, enoughMemtablesForRepair;
        private final int enoughSSTables, enoughSSTablesForRepair;
        protected int numMutations, mutationsSinceLastFlush;
        protected int numFlushes, flushesSinceLastCompaction, flushesSinceLastRepair;
        protected int numCompact;
        protected int numRepairs;
        protected int operations;

        protected BaseState(RandomSource rs, Cluster cluster, TableMetadata metadata)
        {
            this.rs = rs;
            this.cluster = cluster;
            int javaDriverTimeout = Math.toIntExact(TimeUnit.MINUTES.toMillis(1));
            this.client = JavaDriverUtils.create(cluster, b -> b.withSocketOptions(new SocketOptions().setReadTimeoutMillis(javaDriverTimeout).setConnectTimeoutMillis(javaDriverTimeout)));
            this.session = client.connect();
            this.debug = CQL_DEBUG_APPLY_OPERATOR ? CompositeVisitor.of(StandardVisitors.APPLY_OPERATOR, StandardVisitors.DEBUG)
                                                  : StandardVisitors.DEBUG;

            this.bindOrLiteralGen = BOOL_DISTRIBUTION.next(rs);
            this.betweenEqGen = BOOL_DISTRIBUTION.next(rs);
            this.lessThanGen = LESS_THAN_DISTRO.next(rs);
            this.greaterThanGen = GREATER_THAN_DISTRO.next(rs);
            this.rangeInequalityGen = RANGE_INEQUALITY_DISTRO.next(rs);
            this.fetchSizeGen = FETCH_SIZE_DISTRO.next(rs);
            this.useFetchSizeGen = BOOL_DISTRIBUTION.next(rs);
            this.usePerPartitionLimitGen = BOOL_DISTRIBUTION.next(rs);
            this.useLimitGen = BOOL_DISTRIBUTION.next(rs);
            this.perPartitionLimitGen = LIMIT_DISTRO.next(rs);
            this.limitGen = LIMIT_DISTRO.next(rs);

            this.repairTypeEmptyModelGen = REPAIR_TYPE_EMPTY_MODEL_DISTRO.next(rs);
            this.repairTypeGen = REPAIR_TYPE_DISTRO.next(rs);

            this.enoughMemtables = rs.pickInt(1, 3, 10, 50);
            this.enoughMemtablesForRepair = rs.pickInt(1, 3, 10, 50);
            this.enoughSSTables = rs.pickInt(3, 10, 50);
            this.enoughSSTablesForRepair = rs.pickInt(1, 3, 10, 50);

            this.metadata = metadata;
            this.tableRef = TableReference.from(metadata);
            this.model = new ASTSingleTableModel(metadata, IGNORED_ISSUES);
            createTable(metadata);

            String sstableFormatName = this.sstableFormatName = Generators.toGen(CassandraGenerators.sstableFormatNames()).next(rs);
            cluster.forEach(i -> i.runOnInstance(() -> DatabaseDescriptor.setSelectedSSTableFormat(sstableFormatName)));
        }

        public boolean hasPartitions()
        {
            return !model.isEmpty();
        }

        protected boolean readAfterWrite()
        {
            return false;
        }

        protected boolean isMultiNode()
        {
            return cluster.size() > 1;
        }

        protected void createTable(TableMetadata metadata)
        {
            cluster.schemaChange(createKeyspaceCQL(metadata.keyspace));

            CassandraGenerators.visitUDTs(metadata, next -> cluster.schemaChange(next.toCqlString(false, false, true)));
            cluster.schemaChange(metadata.toCqlString(false, false, false));
        }

        private String createKeyspaceCQL(String ks)
        {
            return "CREATE KEYSPACE IF NOT EXISTS " + ks + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + Math.min(3, cluster.size()) + "};";
        }

        protected <S extends BaseState> Property.Command<S, Void, ?> command(RandomSource rs, Select select)
        {
            return command(rs, select, null);
        }

        protected boolean allowRepair()
        {
            return false;
        }

        protected boolean allowUsingTimestamp()
        {
            return true;
        }

        protected RepairGenerators.Builder repairArgsBuilder()
        {
            return new RepairGenerators.Builder(i -> Arrays.asList(metadata.keyspace, metadata.name))
                   // paxos cleanup's finish prepare is delayed based off CAS/Write timeout, but these tests make that 3 minutes (so CI is stable)
                   // which means this step is delayed 3 minutes, making repairs suppppper slow...
                   // see org.apache.cassandra.service.paxos.cleanup.PaxosCleanup#finishPrepare
                   .withSkipPaxosGen(i -> true)
                   .withRanges(rs -> {
                       switch (model.isEmpty() ? repairTypeEmptyModelGen.next(rs) : repairTypeGen.next(rs))
                       {
                           case 0: return RepairGenerators.LOCAL_RANGE;
                           case 1: return RepairGenerators.PRIMARY_RANGE;
                           case 2:
                           {
                               Token a = rs.pickOrderedSet(model.partitionKeys()).token;
                               return List.of("--start-token", Long.toString(a.getLongValue() - 1),
                                              "--end-token", a.toString());
                           }
                           default: throw new UnsupportedOperationException();
                       }
                   })
            ;
        }

        protected boolean allowLimit(Select select)
        {
            //TODO (coverage): allow this in the model!
            // LIMIT with IN clause on partition columns is non-deterministic which is not currently supported by the model
            if (select.where.isEmpty()) return true;
            return !select.where.get()
                                .streamRecursive(true)
                                .filter(e -> e instanceof Conditional.In)
                                .anyMatch(e -> {
                                    var in = (Conditional.In) e;
                                    // when expression is size 1, then this is deterministic
                                    if (in.expressions.size() == 1) return false;
                                    return model.factory.partitionColumns.contains(in.ref);
                                });
        }

        protected boolean allowPerPartitionLimit(Select select)
        {
            return true;
        }

        protected boolean allowPaging(Select select)
        {
            return true;
        }

        protected <S extends BaseState> Property.Command<S, Void, ?> command(RandomSource rs, Select select, @Nullable String annotate)
        {
            var inst = selectInstance(rs);
            if (allowPerPartitionLimit(select) && usePerPartitionLimitGen.next(rs))
                select = select.withPerPartitionLimit(perPartitionLimitGen.nextInt(rs));
            if (allowLimit(select) && useLimitGen.next(rs))
                select = select.withLimit(limitGen.nextInt(rs));
            int fetchSize = allowPaging(select) && useFetchSizeGen.next(rs)
                            ? fetchSizeGen.nextInt(rs)
                            : Integer.MAX_VALUE;
            String postfix = "on " + inst;
            if (fetchSize != Integer.MAX_VALUE)
                postfix += ", fetch size " + fetchSize;
            if (annotate == null) annotate = postfix;
            else                  annotate += ", " + postfix;
            Select finalSelect = select;
            return new Property.SimpleCommand<>(humanReadable(select, annotate), s -> {
                s.model.validate(s.executeQuery(inst, fetchSize, s.selectCl(), finalSelect), finalSelect);
            });
        }

        protected <S extends BaseState> Property.Command<S, Void, ?> commandSafeRandomHistory(Select select, @Nullable String annotate)
        {
            var inst = cluster.firstAlive();
            String postfix = "on " + inst;
            if (annotate == null) annotate = postfix;
            else                  annotate += ", " + postfix;
            return new Property.SimpleCommand<>(humanReadable(select, annotate), s -> {
                s.model.validate(s.executeQuery(inst, Integer.MAX_VALUE, s.selectCl(), select), select);
            });
        }

        protected ConsistencyLevel selectCl()
        {
            return ConsistencyLevel.LOCAL_QUORUM;
        }

        protected ConsistencyLevel mutationCl()
        {
            return ConsistencyLevel.LOCAL_QUORUM;
        }

        protected <S extends BaseState> Property.Command<S, Void, ?> command(RandomSource rs, Mutation mutation)
        {
            return command(rs, mutation, null);
        }

        protected <S extends BaseState> Property.Command<S, Void, ?> command(RandomSource rs, Mutation mutation, @Nullable String annotate)
        {
            var inst = selectInstance(rs);
            String postfix = "on " + inst;
            if (mutation.isCas())
            {
                postfix += ", would apply " + model.shouldApply(mutation);
                // CAS doesn't allow timestamps
                mutation = mutation.withoutTimestamp();
            }
            if (annotate == null) annotate = postfix;
            else                  annotate += ", " + postfix;
            Mutation finalMutation = mutation;
            return new Property.SimpleCommand<>(humanReadable(mutation, annotate), s -> {
                var result = s.executeQuery(inst, Integer.MAX_VALUE, s.mutationCl(), finalMutation);
                s.model.updateAndValidate(result, finalMutation);
                s.mutation();
            });
        }

        protected <S extends BaseState> Property.Command<S, Void, ?> command(RandomSource rs, Txn txn)
        {
            return command(rs, txn, null);
        }

        protected <S extends BaseState> Property.Command<S, Void, ?> command(RandomSource rs, Txn txn, @Nullable String annotate)
        {
            var inst = selectInstance(rs);
            String postfix = "on " + inst;
            if (model.isConditional(txn))
                postfix += ", would apply " + model.shouldApply(txn);
            if (annotate == null) annotate = postfix;
            else annotate += ", " + postfix;

            return new Property.SimpleCommand<>(humanReadable(txn, annotate), s -> {
                boolean hasMutation = txn.ifBlock.isPresent() || !txn.mutations.isEmpty();
                ConsistencyLevel cl = hasMutation ? s.mutationCl() : s.selectCl();
                s.model.updateAndValidate(s.executeQuery(inst, Integer.MAX_VALUE, cl, txn), txn);
                if (hasMutation)
                    s.mutation();
            });
        }

        protected IInvokableInstance selectInstance(RandomSource rs)
        {
            return cluster.get(rs.nextInt(0, cluster.size()) + 1);
        }

        protected boolean hasEnoughMemtable()
        {
            return mutationsSinceLastFlush > enoughMemtables;
        }

        protected boolean hasEnoughMemtableForRepair()
        {
            // use last flush rather than last repair as this method cares about data in the memtable
            // and not amount of mutations since repair
            return mutationsSinceLastFlush > enoughMemtablesForRepair;
        }

        protected boolean hasEnoughSSTables()
        {
            return flushesSinceLastCompaction > enoughSSTables;
        }

        protected boolean hasEnoughSSTablesForRepair()
        {
            return flushesSinceLastRepair > enoughSSTablesForRepair;
        }

        protected void mutation()
        {
            numMutations++;
            mutationsSinceLastFlush++;
        }

        protected void flush()
        {
            mutationsSinceLastFlush = 0;
            numFlushes++;
            flushesSinceLastCompaction++;
            flushesSinceLastRepair++;
        }

        protected void compact()
        {
            flushesSinceLastCompaction = 0;
            numCompact++;
        }

        protected void repair()
        {
            if (mutationsSinceLastFlush > 0)
                flush();

            numRepairs++;
            flushesSinceLastRepair = 0;
        }

        protected Value value(RandomSource rs, ByteBuffer bb, AbstractType<?> type)
        {
            return bindOrLiteralGen.next(rs) ? new Bind(bb, type) : new Literal(bb, type);
        }

        protected ByteBuffer[][] executeQuery(IInstance instance, int fetchSize, ConsistencyLevel cl, Statement stmt)
        {
            if (cl == ConsistencyLevel.NODE_LOCAL)
            {
                // This limitation is due to the fact the query column types are not known in the current QueryResult API.
                // In order to fix this we need to alter the API, and backport to each branch else we break upgrade.
                if (!(stmt instanceof Mutation))
                    throw new IllegalArgumentException("Unable to execute Statement of type " + stmt.getClass() + " when ConsistencyLevel.NODE_LOCAL is used");
                if (fetchSize != Integer.MAX_VALUE)
                    throw new IllegalArgumentException("Fetch size is not allowed for Mutations");
                instance.executeInternal(stmt.toCQL(), (Object[]) stmt.bindsEncoded());
                return new ByteBuffer[0][];
            }
            return DriverUtils.executeQuery(session, instance, fetchSize, cl, stmt);
        }

        protected String humanReadable(Statement stmt, @Nullable String annotate)
        {
            // With UTF-8 some chars can cause printing issues leading to error messages that don't reproduce the original issue.
            // To avoid this problem, always escape the CQL so nothing gets lost
            String cql = StringUtils.escapeControlChars(stmt.visit(debug).toCQL(CQLFormatter.None.instance));
            if (annotate != null)
                cql += " -- " + annotate;
            return cql;
        }

        protected void toString(StringBuilder sb)
        {
            sb.append("Config:\nsstable:\n\tselected_format: ").append(sstableFormatName);
            sb.append('\n').append(createKeyspaceCQL(metadata.keyspace));
            CassandraGenerators.visitUDTs(metadata, udt -> sb.append('\n').append(udt.toCqlString(false, false, true)).append(';'));
            sb.append('\n').append(metadata.toCqlString(false, false, false));
        }

        @Override
        public void close() throws Exception
        {
            session.close();
            client.close();
            cluster.schemaChange("DROP TABLE " + metadata);
            cluster.schemaChange("DROP KEYSPACE " + metadata.keyspace);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            toString(sb);
            return sb.toString();
        }
    }

    protected static abstract class CommonState extends BaseState
    {
        protected CommonState(RandomSource rs, Cluster cluster, TableMetadata metadata)
        {
            super(rs, cluster, metadata);
        }

        protected abstract Gen<Mutation> mutationGen();
    }
}

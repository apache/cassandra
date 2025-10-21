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

package org.apache.cassandra.simulator.test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NavigableSet;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.DefaultRandom;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.cql3.ast.CQLFormatter;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.StandardVisitors;
import org.apache.cassandra.cql3.ast.Statement;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.cql3.ast.Txn;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.impl.Query;
import org.apache.cassandra.distributed.impl.RowUtil;
import org.apache.cassandra.harry.model.ASTSingleTableModel;
import org.apache.cassandra.harry.model.BytesPartitionState;
import org.apache.cassandra.harry.util.StringUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.simulator.AbstractSimulation;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.ActionSchedule;
import org.apache.cassandra.simulator.Actions;
import org.apache.cassandra.simulator.RunnableActionScheduler;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.systems.SimulatedActionCallable;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.utils.ASTGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.Generators;
import org.quicktheories.generators.SourceDSL;

import static org.apache.cassandra.simulator.cluster.ClusterActions.InitialConfiguration.initializeAll;
import static org.apache.cassandra.utils.AbstractTypeGenerators.overridePrimitiveTypeSupport;
import static org.apache.cassandra.utils.AbstractTypeGenerators.stringComparator;
import static org.apache.cassandra.utils.Generators.toGen;

public class SingleTableASTSimulation extends SimulationTestBase.SimpleSimulation
{
    private static final int MAX_STEPS = 5000;

    static
    {
        // limit text/bytes so they are not too big; mostly for debugging than anything
        overridePrimitiveTypeSupport(AsciiType.instance, AbstractTypeGenerators.TypeSupport.of(AsciiType.instance, SourceDSL.strings().ascii().ofLengthBetween(1, 10), stringComparator(AsciiType.instance)));
        overridePrimitiveTypeSupport(UTF8Type.instance, AbstractTypeGenerators.TypeSupport.of(UTF8Type.instance, Generators.utf8(1, 10), stringComparator(UTF8Type.instance)));
        overridePrimitiveTypeSupport(BytesType.instance, AbstractTypeGenerators.TypeSupport.of(BytesType.instance, Generators.bytes(1, 10), FastByteOperations::compareUnsigned));
    }

    private final RandomSource rs;
    private ASTRunner runner;

    protected SingleTableASTSimulation(SimulatedSystems simulated, RunnableActionScheduler scheduler, Cluster cluster, ClusterActions.Options options)
    {
        super(simulated, scheduler, cluster, options);
        this.rs = new DefaultRandom(simulated.random.uniform(Long.MIN_VALUE, Long.MAX_VALUE));

        cluster.stream().forEach((IInvokableInstance i) -> simulated.failureDetector.markUp(i.config().broadcastAddress()));
    }

    @Override
    protected ActionSchedule.Mode mode()
    {
        return ActionSchedule.Mode.STREAM_LIMITED;
    }

    @Override
    protected ActionList initialize()
    {
        List<Action> actions = new ArrayList<>();
        actions.add(clusterActions.initializeCluster(initializeAll(cluster.size())));

        int[] dcSizes = new int[clusterActions.snitch.dcCount()];
        for (int nodeId = 1; nodeId <= cluster.size(); nodeId++)
            dcSizes[clusterActions.snitch.dcOf(nodeId)]++;

        actions.addAll(setupTable(dcSizes));
        return ActionList.of(actions);
    }

    protected List<Action> setupTable(int[] dcSizes)
    {
        List<Action> actions = new ArrayList<>();

        StringBuilder createKeyspace = new StringBuilder("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy'"); // when the simulation starts the RF gets populated
        for (int i = 0; i < dcSizes.length; i++)
        {
            String name = clusterActions.snitch.nameOfDc(i);
            createKeyspace.append(", '").append(name).append("': ").append(Math.min(3, dcSizes[i]));
        }
        createKeyspace.append("};");
        actions.add(clusterActions.schemaChange(1, createKeyspace.toString()));

        TableMetadata metadata = defineTable(rs, "ks");

        CassandraGenerators.visitUDTs(metadata, udt -> actions.add(clusterActions.schemaChange(1, udt.toCqlString(false, false, false))));
        actions.add(clusterActions.schemaChange(1, metadata.toCqlString(false, false, false)));

        this.runner = new ASTRunner(metadata, rs, MAX_STEPS, this);

        return actions;
    }

    protected AbstractTypeGenerators.TypeGenBuilder supportedTypes()
    {
        return AbstractTypeGenerators.withoutUnsafeEquality()
                                     .withTypeKinds(AbstractTypeGenerators.TypeKind.PRIMITIVE);
    }

    protected TableMetadata defineTable(RandomSource rs, String ks)
    {
        TableMetadata tbl = toGen(new CassandraGenerators.TableMetadataBuilder()
                                  .withTableKinds(TableMetadata.Kind.REGULAR)
                                  .withKnownMemtables()
                                  .withKeyspaceName(ks).withTableName("tbl")
                                  .withSimpleColumnNames()
                                  .withDefaultTypeGen(supportedTypes())
                                  .withPartitioner(Murmur3Partitioner.instance)
                                  .build())
                            .next(rs);
        return tbl.unbuild().params(tbl.params.unbuild().readRepair(ReadRepairStrategy.NONE).build()).build();
    }

    @Override
    protected ActionList execute()
    {
        return ActionList.of(test());
    }

    protected Action test()
    {
        return Actions.stream(1, runner::next);
    }

    public static class FullAccordSingleTableASTSimulation extends SingleTableASTSimulation
    {
        public FullAccordSingleTableASTSimulation(SimulatedSystems simulated, RunnableActionScheduler scheduler, Cluster cluster, ClusterActions.Options options)
        {
            super(simulated, scheduler, cluster, options);
        }

        @Override
        protected TableMetadata defineTable(RandomSource rs, String ks)
        {
            TableMetadata metadata = super.defineTable(rs, ks);
            return metadata.unbuild()
                           .params(metadata.params.unbuild()
                                                  .transactionalMode(TransactionalMode.full)
                                                  .build())
                           .build();
        }
    }

    public static class MixedReadsAccordSingleTableASTSimulation extends SingleTableASTSimulation
    {
        public MixedReadsAccordSingleTableASTSimulation(SimulatedSystems simulated, RunnableActionScheduler scheduler, Cluster cluster, ClusterActions.Options options)
        {
            super(simulated, scheduler, cluster, options);
        }

        @Override
        protected TableMetadata defineTable(RandomSource rs, String ks)
        {
            TableMetadata metadata = super.defineTable(rs, ks);
            return metadata.unbuild()
                           .params(metadata.params.unbuild()
                                                  .transactionalMode(TransactionalMode.mixed_reads)
                                                  .build())
                           .build();
        }
    }

    public static class ASTRunner
    {
        private static final Logger logger = LoggerFactory.getLogger(ASTRunner.class);

        private final TableMetadata metadata;
        private final ASTSingleTableModel model;
        private final RandomSource rs;
        private final int maxSteps;
        private final Gen<Action> commands;
        private final AbstractSimulation simulation;
        private int step;

        public ASTRunner(TableMetadata metadata, RandomSource rs, int maxSteps, AbstractSimulation simulation)
        {
            this.metadata = metadata;
            this.model = new ASTSingleTableModel(metadata);
            this.rs = rs;
            this.maxSteps = maxSteps;
            this.simulation = simulation;

            Gen.IntGen nodeGen = r -> r.nextInt(0, simulation.cluster.size()) + 1;

            List<LinkedHashMap<Symbol, Object>> uniquePartitions = Gens.lists(toGen(ASTGenerators.columnValues(model.factory.partitionColumns)))
                                                                       .uniqueBestEffort()
                                                                       .ofSize(rs.nextInt(1, 20))
                                                                       .next(rs);
            Gen<Action> mutationGen = toGen(ASTGenerators.mutationBuilder(rs, model, uniquePartitions, i -> null).build())
                                      .map(mutation -> query(mutation));

            Gen<Action> selectPartitionGen = Gens.pick(uniquePartitions)
                                                 .map(partition -> query(select(partition).build()));
            Gen<Action> selectRowGen = Gens.pick(uniquePartitions).map(this::selectRow);
            Gen<Action> txnGen = Generators.toGen(new ASTGenerators.ModelBasedTxnGenBuilder(rs, model, i -> null, SourceDSL.arbitrary().pick(uniquePartitions)).disallowEmpty().build())
                                 .map(txn -> query(txn));
            Gens.OneOfBuilder<Action> commandsBuilder = Gens.<Action>oneOf()
                                                            .add(mutationGen)
                                                            .add(selectPartitionGen)
                                                            .add(i -> query(Select.builder(metadata).build()))
                                                            .add(r -> {
                                                                int nodeId = nodeGen.nextInt(r);
                                                                logger.warn("[step={}] Scheduling flush on node{}", step, nodeId);
                                                                return simulation.clusterActions.flush(nodeId, metadata.keyspace, metadata.name);
                                                            })
                                                            .add(r -> {
                                                                int nodeId = nodeGen.nextInt(r);
                                                                logger.warn("[step={}] Scheduling compact on node{}", step, nodeId);
                                                                return simulation.clusterActions.compact(nodeId, metadata.keyspace, metadata.name);
                                                            });

            if (!model.factory.clusteringColumns.isEmpty())
                commandsBuilder.add(selectRowGen);
            if (metadata.params.transactionalMode.accordIsEnabled)
                commandsBuilder.add(txnGen);
            this.commands = commandsBuilder.buildWithDynamicWeights().next(rs);
        }

        public Action next()
        {
            if (step + 1 >= maxSteps)
            {
                logger.warn("Max steps reached, exiting...");
                return null;
            }
            step++;
            Action next = commands.next(rs);
            // empty actions implies that the model state doesn't have enough data to process the action, so we want to "skip"
            while (next == null)
                next = commands.next(rs);
            return next;
        }

        private Action selectRow(LinkedHashMap<Symbol, Object> partition)
        {

            var builder = select(partition);
            List<Clustering<ByteBuffer>> partitions = model.partitions(builder.build());
            switch (partitions.size())
            {
                case 0:
                    return null;
                case 1:
                    break;
                default:
                    throw new IllegalStateException("Model matched multiple partitions, only 1 is expected");
            }
            BytesPartitionState state = model.get(partitions.get(0));
            if (state == null)
                return null;
            NavigableSet<Clustering<ByteBuffer>> clusteringKeys = state.clusteringKeys();
            if (clusteringKeys.isEmpty())
                return null;
            Clustering<ByteBuffer> clusteringKey = rs.pickOrderedSet(clusteringKeys);
            for (Symbol ck : model.factory.clusteringColumns)
                builder.value(ck, clusteringKey.bufferAt(model.factory.clusteringColumns.indexOf(ck)));
            return query(builder.build());
        }

        private Select.Builder select(LinkedHashMap<Symbol, Object> partition)
        {
            Select.Builder builder = Select.builder().table(metadata);
            for (var e : partition.entrySet())
                builder.value(e.getKey(), e.getValue());
            return builder;
        }

        private Action query(Select select)
        {
            return query(select, ConsistencyLevel.ALL, o -> model.validate(RowUtil.toByteBuffer(o), select));
        }

        private Action query(Mutation mutation)
        {
            return query(mutation, ConsistencyLevel.QUORUM, o -> model.update(mutation));
        }

        private Action query(Txn txn)
        {
            return query(txn, ConsistencyLevel.ALL, o -> model.updateAndValidate(RowUtil.toByteBuffer(o), txn));
        }

        private String humanReadable(Statement stmt, @Nullable String annotate)
        {
            // With UTF-8 some chars can cause printing issues leading to error messages that don't reproduce the original issue.
            // To avoid this problem, always escape the CQL so nothing gets lost
            String cql = StringUtils.escapeControlChars(stmt.visit(StandardVisitors.DEBUG).toCQL(CQLFormatter.None.instance));
            if (annotate != null)
                cql += " -- " + annotate;
            return cql;
        }

        private Action query(Statement statement, ConsistencyLevel cl, Consumer<Object[][]> onSuccess)
        {
            // Always use node 1 since we're in single node mode
            int nodeId = 1;
            String postfix = "on node" + nodeId;
            switch (statement.kind())
            {
                case SELECT: break;
                case MUTATION: {
                    Mutation mutation = statement.asMutation();
                    if (mutation.isCas())
                        postfix += ", would apply " + model.shouldApply(mutation);
                } break;
                case TXN: {
                    Txn txn = statement.asTxn();
                    postfix += ", would apply " + model.shouldApply(txn);
                } break;
                default:
                    throw new UnsupportedOperationException(statement.kind().name());
            }
            logger.warn("[step={}] Executing query: {}", step, humanReadable(statement, postfix));
            return new SimulatedActionCallable<>(statement.getClass().getSimpleName(),
                                                 Action.Modifiers.RELIABLE_NO_TIMEOUTS,
                                                 Action.Modifiers.RELIABLE_NO_TIMEOUTS,
                                                 simulation.simulated,
                                                 simulation.cluster.get(nodeId),
                                                 query(statement, cl))
            {
                final int step_id = step;
                final long createdAtNanos = simulation.simulated.time.nanoTime();

                @Override
                public void accept(Object[][] objects, Throwable throwable)
                {
                    if (throwable != null)
                    {
                        logger.error("[step={}] failed", step_id, throwable);
                        simulated.failures.accept(throwable);
                        return;
                    }
                    logger.warn("[step={}] completed after {}", step_id, Duration.ofNanos(simulation.simulated.time.nanoTime() - createdAtNanos));
                    onSuccess.accept(objects);
                }
            };
        }

        private IIsolatedExecutor.SerializableCallable<Object[][]> query(Statement statement, ConsistencyLevel cl)
        {
            String cql = statement.toCQL();
            ByteBuffer[] encoded = statement.bindsEncoded();
            // Simulator doesn't support ByteBuffer (fails to transfer), so need to convert to byte[], which will get converted to ByteBuffer within Cassandra.
            Object[] binds = new Object[encoded.length];
            for (int i = 0; i < encoded.length; i++)
                binds[i] = encoded[i] == null ? null : ByteBufferUtil.getArray(encoded[i]);
            return () -> {
                Query q = new Query(cql, Long.MIN_VALUE, false, cl, null, binds);
                return q.call().toObjectArrays();
            };
        }
    }
}

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

package org.apache.cassandra.fuzz.topology;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.coordinate.Exhausted;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Invariants;
import accord.utils.Property;
import accord.utils.Property.Command;
import accord.utils.RandomSource;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Statement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.accord.AccordTestBase;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.ASTGenerators;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.Isolated;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.AccordGenerators.fromQT;

public class AccordTopologyMixupTest extends TopologyMixupTestBase<AccordTopologyMixupTest.Spec>
{
    private static final Logger logger = LoggerFactory.getLogger(AccordTopologyMixupTest.class);

    static
    {
        CassandraRelevantProperties.ACCORD_AGENT_CLASS.setString(InterceptAgent.class.getName());
        // enable most expensive debugging checks
        CassandraRelevantProperties.ACCORD_KEY_PARANOIA_CPU.setString(Invariants.Paranoia.QUADRATIC.name());
        CassandraRelevantProperties.ACCORD_KEY_PARANOIA_MEMORY.setString(Invariants.Paranoia.QUADRATIC.name());
        CassandraRelevantProperties.ACCORD_KEY_PARANOIA_COSTFACTOR.setString(Invariants.ParanoiaCostFactor.HIGH.name());
    }

    private static final List<TransactionalMode> TRANSACTIONAL_MODES = Stream.of(TransactionalMode.values()).filter(t -> t.accordIsEnabled).collect(Collectors.toList());

    @Override
    protected Gen<State<Spec>> stateGen()
    {
        return AccordState::new;
    }

    @Override
    protected void preCheck(Property.StatefulBuilder builder)
    {
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
    }

    private static Spec createSchemaSpec(RandomSource rs, Cluster cluster)
    {
        TransactionalMode mode = rs.pick(TRANSACTIONAL_MODES);
        boolean enableMigration = allowsMigration(mode) && rs.nextBoolean();
        TableMetadata metadata = fromQT(new CassandraGenerators.TableMetadataBuilder()
                                        .withKeyspaceName(KEYSPACE)
                                        .withTableKinds(TableMetadata.Kind.REGULAR)
                                        .withKnownMemtables()
                                        //TODO (coverage): include "fast_path = 'keyspace'" override
                                        .withTransactionalMode(enableMigration ? TransactionalMode.off : mode)
                                        .withoutEmpty()
                                        .build())
                                 .next(rs);
        maybeCreateUDTs(cluster, metadata);
        String schemaCQL = metadata.toCqlString(false, false, false);
        logger.info("Creating test table:\n{}", schemaCQL);
        cluster.schemaChange(schemaCQL);
        if (enableMigration)
        {
            cluster.schemaChange("ALTER TABLE " + metadata + " WITH " + mode.asCqlParam());
            cluster.get(1).nodetoolResult("consensus_admin", "begin-migration", "--target-protocol", "accord", metadata.keyspace, metadata.name).asserts().success();
        }
        return new Spec(mode, enableMigration, metadata);
    }

    private static CommandGen<Spec> cqlOperations(Spec spec)
    {
        Gen<Statement> select = (Gen<Statement>) (Gen<?>) fromQT(new ASTGenerators.SelectGenBuilder(spec.metadata).withLimit1().build());
        Gen<Statement> mutation = (Gen<Statement>) (Gen<?>) fromQT(new ASTGenerators.MutationGenBuilder(spec.metadata).withoutTimestamp().withoutTtl().build());
        Gen<Statement> txn = (Gen<Statement>) (Gen<?>) fromQT(new ASTGenerators.TxnGenBuilder(spec.metadata).build());
        Map<Gen<Statement>, Integer> operations = new LinkedHashMap<>();
        operations.put(select, 1);
        operations.put(mutation, 1);
        operations.put(txn, 1);
        Gen<Statement> statementGen = Gens.oneOf(operations);
        return (rs, state) -> cqlOperation(rs, state, statementGen);
    }

    private static Command<State<Spec>, Void, ?> cqlOperation(RandomSource rs, State<Spec> state, Gen<Statement> statementGen)
    {
        Statement stmt = statementGen.next(rs);
        String cql;
        //TODO (usability): are there any transaction_modes that actually need simple mutations/select to be wrapped in a BEGIN TRANSACTION?  If not then this logica can be simplified
        if (stmt.kind() == Statement.Kind.TXN || stmt.kind() == Statement.Kind.MUTATION && ((Mutation) stmt).isCas())
            cql = stmt.toCQL();
        else cql = wrapInTxn(stmt.toCQL());
        IInvokableInstance node = state.cluster.get(rs.pickInt(state.topologyHistory.up()));
        return new Property.SimpleCommand<>(node + ": " + stmt.kind() + "; epoch=" + state.currentEpoch.get(), s2 -> executeTxn(s2.cluster, node, cql, stmt.bindsEncoded()));
    }

    private static boolean allowsMigration(TransactionalMode mode)
    {
        switch (mode)
        {
            case unsafe_writes:
            case mixed_reads:
            case full:
                return true;
            default:
                return false;
        }
    }

    private static SimpleQueryResult executeTxn(Cluster cluster, IInvokableInstance node, String stmt, ByteBuffer[] binds)
    {
        if (!AccordTestBase.isIdempotent(node, stmt))
        {
            // won't be able to retry...
            return node.coordinator().executeWithResult(stmt, ConsistencyLevel.ANY, (Object[]) binds);
        }
        return AccordTestBase.executeWithRetry(cluster, node, stmt, (Object[]) binds);
    }

    private static void maybeCreateUDTs(Cluster cluster, TableMetadata metadata)
    {
        CassandraGenerators.visitUDTs(metadata, next -> {
            String cql = next.toCqlString(false, false, false);
            logger.warn("Creating UDT {}", cql);
            cluster.schemaChange(cql);
        });
    }

    public static class Spec implements TopologyMixupTestBase.SchemaSpec
    {
        private final TransactionalMode mode;
        private final boolean enableMigration;
        private final TableMetadata metadata;

        public Spec(TransactionalMode mode, boolean enableMigration, TableMetadata metadata)
        {
            this.mode = mode;
            this.enableMigration = enableMigration;
            this.metadata = metadata;
        }

        @Override
        public String name()
        {
            return metadata.name;
        }

        @Override
        public String keyspaceName()
        {
            return metadata.keyspace;
        }
    }

    private static class AccordState extends State<Spec>
    {
        private final ListenerHolder listener;

        public AccordState(RandomSource rs)
        {
            super(rs, AccordTopologyMixupTest::createSchemaSpec, AccordTopologyMixupTest::cqlOperations);

            this.listener = new ListenerHolder(this);
        }

        @Override
        protected void onConfigure(IInstanceConfig c)
        {
            c.set("accord.command_store_shard_count", 1)
             .set("accord.queue_shard_count", 1)
             .set("paxos_variant", Config.PaxosVariant.v2.name());
        }

        @Override
        protected void onStartupComplete(long tcmEpoch)
        {
            ClusterUtils.awaitAccordEpochReady(cluster, tcmEpoch);
        }

        @Override
        public void close() throws Exception
        {
            listener.close();
            super.close();
        }
    }

    public static class ListenerHolder implements AccordTopologyMixupTest.SharedState.Listener, AutoCloseable
    {
        private final Map<TxnId, Runnable> debug = new ConcurrentHashMap<>();
        private final State<?> state;

        public ListenerHolder(State<?> state)
        {
            this.state = state;
            AccordTopologyMixupTest.SharedState.listeners.add(this);
        }

        @Override
        public void debugTxn(Node.Id node, String type, TxnId txnId)
        {
            debug.putIfAbsent(txnId, () -> {
                // this runs in the main thread, so is actually thread safe
                int[] up = state.topologyHistory.up();
                logger.error("{} failed with txn id {}; global debug summary:\n{}", type, txnId, ClusterUtils.queryTxnStateAsString(state.cluster, txnId, up));
                debug.remove(txnId);
            });
        }

        public void runTasks()
        {
            for (Runnable r : debug.values())
            {
                try
                {
                    r.run();
                }
                catch (Throwable t)
                {
                    // TODO (correctness): how to handle?
                    logger.error("Unhandled error in onError listeners", t);
                }
            }
        }

        @Override
        public void close()
        {
            runTasks();
            AccordTopologyMixupTest.SharedState.listeners.remove(this);
            debug.clear();
        }
    }

    @Shared
    public static class SharedState
    {
        public interface Listener
        {
            void debugTxn(Node.Id node, String type, TxnId txnId);
        }

        public static final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

        public static void debugTxn(@Nullable Integer node, String type, String id)
        {
            Node.Id nodeId = node == null ? null : new Node.Id(node);
            TxnId txnId = TxnId.parse(id);
            listeners.forEach(l -> l.debugTxn(nodeId, type, txnId));
        }
    }

    @Isolated
    public static class InterceptAgent extends AccordAgent
    {
        @Override
        public void onFailedBarrier(TxnId id, Seekables<?, ?> keysOrRanges, Throwable cause)
        {
            if (cause instanceof Timeout || cause instanceof Preempted)
            {
                SharedState.debugTxn(null, "Repair Barrier", id.toString());
            }
        }

        @Override
        public void onFailedBootstrap(String phase, Ranges ranges, Runnable retry, Throwable failure)
        {
            if (failure instanceof Exhausted)
            {
                Exhausted e = (Exhausted) failure;
                SharedState.debugTxn(self.id, "Bootstrap#" + phase, e.txnId().toString());
            }
            super.onFailedBootstrap(phase, ranges, retry, failure);
        }
    }
}

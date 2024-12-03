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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.TxnId;
import accord.utils.Gen;
import accord.utils.Property;
import accord.utils.Property.Command;
import accord.utils.Property.PreCheckResult;
import accord.utils.Property.SimpleCommand;
import accord.utils.RandomSource;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.execution.QueryBuildingVisitExecutor;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.AssertionUtils;
import org.assertj.core.api.Condition;

import static org.apache.cassandra.distributed.shared.ClusterUtils.waitForCMSToQuiesce;

public class HarryTopologyMixupTest extends TopologyMixupTestBase<HarryTopologyMixupTest.Spec>
{
    protected static final Condition<Object> TIMEOUT_CHECKER = AssertionUtils.isInstanceof(RequestTimeoutException.class);
    private static final Logger logger = LoggerFactory.getLogger(HarryTopologyMixupTest.class);

    public static class AccordMode
    {
        public AccordMode(Kind kind, @Nullable TransactionalMode transactionalMode)
        {
            this.kind = kind;
            this.transactionalMode = transactionalMode;
        }

        public enum Kind
        {None, Direct, Passthrough}

        public final Kind kind;
        @Nullable
        public final TransactionalMode transactionalMode;
    }

    private final AccordMode mode;

    public HarryTopologyMixupTest()
    {
        this(new AccordMode(AccordMode.Kind.None, null));
    }

    protected HarryTopologyMixupTest(AccordMode mode)
    {
        this.mode = mode;
    }

    @Override
    protected Gen<State<Spec>> stateGen()
    {
        return HarryState::new;
    }

    @Override
    protected void preCheck(Property.StatefulBuilder builder)
    {
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
    }

    @Override
    protected void destroyState(State<Spec> state, @Nullable Throwable cause)
    {
        if (cause != null) return;
        if (((HarryState) state).numInserts > 0)
        {
            for (Integer pkIdx : state.schema.pkGen.generated())
                state.schema.harry.selectPartition(pkIdx);
        }
    }

    private static BiFunction<RandomSource, Cluster, Spec> createSchemaSpec(AccordMode mode)
    {
        return (rs, cluster) -> {
            EntropySource rng = new JdkRandomEntropySource(rs.nextLong());
            Generator<SchemaSpec> schemaGen;
            SchemaSpec schema;
            if (mode.kind != AccordMode.Kind.None)
            {
                schemaGen = SchemaGenerators.schemaSpecGen("harry", "table", 1000,
                                                           SchemaSpec.optionsBuilder()
                                                                     .withTransactionalMode(mode.transactionalMode)
                                                                     .addWriteTimestamps(!isWriteTimeFromAccord(mode.transactionalMode)));
            }
            else
                schemaGen = SchemaGenerators.schemaSpecGen("harry", "table", 1000);

            schema = schemaGen.generate(rng);

            HistoryBuilder harry = new ReplayingHistoryBuilder(schema.valueGenerators,
                                                               hb -> {
                                                                   InJvmDTestVisitExecutor.Builder builder = InJvmDTestVisitExecutor.builder();
                                                                   if (mode.kind == AccordMode.Kind.Direct)
                                                                       builder = builder.wrapQueries(QueryBuildingVisitExecutor.WrapQueries.TRANSACTION);
                                                                   return builder.nodeSelector(new InJvmDTestVisitExecutor.NodeSelector()
                                                                                 {
                                                                                     private final AtomicLong cnt = new AtomicLong();

                                                                                     @Override
                                                                                     public int select(long lts)
                                                                                     {
                                                                                         for (int i = 0; i < 42; i++)
                                                                                         {
                                                                                             int selected = (int) (cnt.getAndIncrement() % cluster.size() + 1);
                                                                                             if (!cluster.get(selected).isShutdown())
                                                                                                 return selected;
                                                                                         }
                                                                                         throw new IllegalStateException("Unable to find an alive instance");
                                                                                     }
                                                                                 })
                                                                                 .retryPolicy(t -> {
                                                                                     t = Throwables.getRootCause(t);
                                                                                     if (!TIMEOUT_CHECKER.matches(t))
                                                                                         return false;

                                                                                     TxnId id;
                                                                                     try
                                                                                     {
                                                                                         id = TxnId.parse(t.getMessage());
                                                                                     }
                                                                                     catch (Throwable t2)
                                                                                     {
                                                                                         return true;
                                                                                     }
                                                                                     try
                                                                                     {
                                                                                         int[] nodes = cluster.stream().filter(i -> !i.isShutdown()).mapToInt(i -> i.config().num()).toArray();
                                                                                         logger.warn("Timeout for txn {}; debug info\n{}", id, ClusterUtils.queryTxnStateAsString(cluster, id, nodes));
                                                                                     }
                                                                                     catch (Throwable t3)
                                                                                     {
                                                                                         t.addSuppressed(t3);
                                                                                     }
                                                                                     return false;
                                                                                 })
                                                                                 .build(schema, hb, cluster);
                                                               });
            cluster.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};", schema.keyspace));
            cluster.schemaChange(schema.compile());
            waitForCMSToQuiesce(cluster, cluster.get(1));
            return new Spec(harry, schema);
        };
    }

    private static class HarryCommand extends SimpleCommand<State<Spec>>
    {
        HarryCommand(Function<State<Spec>, String> name, Consumer<State<Spec>> fn)
        {
            super(name, fn);
        }

        @Override
        public PreCheckResult checkPreconditions(State<Spec> state)
        {
            int clusterSize = state.topologyHistory.up().length;
            return clusterSize >= 3 ? PreCheckResult.Ok : PreCheckResult.Ignore;
        }
    }

    private static CommandGen<Spec> cqlOperations(Spec spec)
    {
        Command<State<Spec>, Void, ?> insert = new HarryCommand(state -> "Harry Insert" + state.commandNamePostfix(), state -> {
            spec.harry.insert();
            ((HarryState) state).numInserts++;
        });
        return (rs, state) -> {
            HarryState harryState = (HarryState) state;
            TopologyHistory history = state.topologyHistory;
            // if any topology change happened, then always validate all
            if (harryState.generation != history.generation())
            {
                harryState.generation = history.generation();
                return validateAll(state);
            }
            if ((harryState.numInserts > 0 && rs.decide(0.2))) // 20% of the time do reads
                return validateAll(state);
            return insert;
        };
    }

    private static Command<State<Spec>, Void, ?> validateAll(State<Spec> state)
    {
        Spec spec = state.schema;
        List<Command<State<Spec>, Void, ?>> reads = new ArrayList<>();

        for (Integer pkIdx : spec.pkGen.generated())
        {
            long pd = spec.harry.valueGenerators().pkGen().descriptorAt(pkIdx);
            reads.add(new HarryCommand(s -> String.format("Harry Validate pd=%d%s", pd, state.commandNamePostfix()), s -> spec.harry.selectPartition(pkIdx)));

            TransactionalMode transationalMode = spec.schema.options.transactionalMode();
            if (TransactionalMode.full == transationalMode)
                reads.add(new HarryCommand(s -> String.format("Harry Reverse Validate pd=%d%s", pd, state.commandNamePostfix()), s -> spec.harry.selectPartition(pkIdx, Operations.ClusteringOrderBy.DESC)));
        }
        reads.add(new HarryCommand(s -> "Reset Harry Write State" + state.commandNamePostfix(), s -> ((HarryState) s).numInserts = 0));
        return Property.multistep(reads);
    }

    public static class Spec implements Schema
    {
        private final Generators.TrackingGenerator<Integer> pkGen;
        private final HistoryBuilder harry;
        private final SchemaSpec schema;

        public Spec(HistoryBuilder harry, SchemaSpec schema)
        {
            this.harry = harry;
            this.schema = schema;
            this.pkGen = Generators.tracking(Generators.int32(0, schema.valueGenerators.pkPopulation()));
        }

        @Override
        public String table()
        {
            return schema.table;
        }

        @Override
        public String keyspace()
        {
            return schema.keyspace;
        }
    }

    public class HarryState extends State<Spec>
    {
        private long generation;
        private int numInserts = 0;

        public HarryState(RandomSource rs)
        {
            super(rs, createSchemaSpec(mode), HarryTopologyMixupTest::cqlOperations);
        }

        @Override
        protected void onConfigure(IInstanceConfig config)
        {
            config.set("metadata_snapshot_frequency", 5);
        }
    }

    private static boolean isWriteTimeFromAccord(TransactionalMode transactionalMode)
    {
        return transactionalMode != null && transactionalMode.nonSerialWritesThroughAccord;
    }
}
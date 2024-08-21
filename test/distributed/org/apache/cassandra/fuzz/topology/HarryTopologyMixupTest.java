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

import java.util.function.BiFunction;

import accord.utils.Gen;
import accord.utils.Property;
import accord.utils.Property.Command;
import accord.utils.Property.PreCheckResult;
import accord.utils.Property.SimpleCommand;
import accord.utils.RandomSource;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.harry.HarryHelper;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.sut.injvm.InJvmSut;

import static org.apache.cassandra.distributed.shared.ClusterUtils.waitForCMSToQuiesce;

public class HarryTopologyMixupTest extends TopologyMixupTestBase<HarryTopologyMixupTest.Spec>
{
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

    private static Spec createSchemaSpec(RandomSource rs, Cluster cluster)
    {
        ReplayingHistoryBuilder harry = HarryHelper.dataGen(rs.nextLong(),
                                                            new InJvmSut(cluster),
                                                            new TokenPlacementModel.SimpleReplicationFactor(3),
                                                            SystemUnderTest.ConsistencyLevel.ALL);
        cluster.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};", HarryHelper.KEYSPACE));
        var schema = harry.schema();
        cluster.schemaChange(schema.compile().cql());
        waitForCMSToQuiesce(cluster, cluster.get(1));
        return new Spec(harry);
    }

    private static BiFunction<RandomSource, State<Spec>, Command<State<Spec>, Void, ?>> cqlOperations(Spec spec)
    {
        Command<State<Spec>, Void, ?> insert = new SimpleCommand<>("Harry Insert", s2 -> {
            spec.harry.insert();
            ((HarryState) s2).numInserts++;
        })
        {
            @Override
            public PreCheckResult checkPreconditions(State<Spec> state)
            {
                int clusterSize = state.topologyHistory.up().length;
                return clusterSize >= 3 ? PreCheckResult.Ok : PreCheckResult.Ignore;
            }
        };
        Command<State<Spec>, Void, ?> validateAll = new SimpleCommand<>("Harry Validate All", s2 -> {
            spec.harry.validateAll(spec.harry.quiescentLocalChecker());
            ((HarryState) s2).numInserts = 0;
        })
        {
            @Override
            public PreCheckResult checkPreconditions(State<Spec> state)
            {
                int clusterSize = state.topologyHistory.up().length;
                return clusterSize >= 3 ? PreCheckResult.Ok : PreCheckResult.Ignore;
            }
        };
        return (rs, state) -> {
            HarryState harryState = (HarryState) state;
            TopologyHistory history = state.topologyHistory;
            // if any topology change happened, then always validate all
            if (harryState.generation != history.generation())
            {
                harryState.generation = history.generation();
                return validateAll;
            }
            if (harryState.numInserts > 500)
                return validateAll;
            return insert;
        };
    }

    public static class Spec implements TopologyMixupTestBase.SchemaSpec
    {
        private final ReplayingHistoryBuilder harry;

        public Spec(ReplayingHistoryBuilder harry)
        {
            this.harry = harry;
        }

        @Override
        public String name()
        {
            return harry.schema().table;
        }

        @Override
        public String keyspaceName()
        {
            return HarryHelper.KEYSPACE;
        }
    }

    public static class HarryState extends State<Spec>
    {
        private long generation;
        private int numInserts = 0;
        public HarryState(RandomSource rs)
        {
            super(rs, HarryTopologyMixupTest::createSchemaSpec, HarryTopologyMixupTest::cqlOperations);
        }

        @Override
        protected void onConfigure(IInstanceConfig config)
        {
            config.set("metadata_snapshot_frequency", 5);
        }
    }
}

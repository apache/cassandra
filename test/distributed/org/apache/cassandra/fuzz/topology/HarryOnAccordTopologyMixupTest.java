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

import accord.utils.Gen;
import accord.utils.Invariants;
import accord.utils.Property;
import accord.utils.RandomSource;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.fuzz.topology.AccordTopologyMixupTest.ListenerHolder;
import org.apache.cassandra.service.consensus.TransactionalMode;

public class HarryOnAccordTopologyMixupTest extends HarryTopologyMixupTest
{
    static
    {
        CassandraRelevantProperties.ACCORD_AGENT_CLASS.setString(AccordTopologyMixupTest.InterceptAgent.class.getName());
        // enable most expensive debugging checks
        CassandraRelevantProperties.ACCORD_KEY_PARANOIA_CPU.setString(Invariants.Paranoia.QUADRATIC.name());
        CassandraRelevantProperties.ACCORD_KEY_PARANOIA_MEMORY.setString(Invariants.Paranoia.QUADRATIC.name());
        CassandraRelevantProperties.ACCORD_KEY_PARANOIA_COSTFACTOR.setString(Invariants.ParanoiaCostFactor.HIGH.name());
    }

    @Override
    protected void preCheck(Property.StatefulBuilder builder)
    {
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
    }

    @Override
    protected Gen<State<Spec>> stateGen()
    {
        return HarryOnAccordState::new;
    }

    public HarryOnAccordTopologyMixupTest()
    {
        super(new AccordMode(AccordMode.Kind.Direct, TransactionalMode.full));
    }

    public class HarryOnAccordState extends HarryState
    {
        private final ListenerHolder listener;

        public HarryOnAccordState(RandomSource rs)
        {
            super(rs);

            this.listener = new ListenerHolder(this);
        }

        @Override
        protected void onConfigure(IInstanceConfig config)
        {
            super.onConfigure(config);
            config.set("accord.shard_count", 1);
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
}

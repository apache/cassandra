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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.simulator.FixedLossNetworkScheduler;
import org.apache.cassandra.simulator.FutureActionScheduler;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.systems.SimulatedTime;
import org.apache.cassandra.simulator.utils.KindOfSequence;

public class AccordHarrySimulationTest extends HarrySimulatorTest
{

    @Override
    public Map<Verb, FutureActionScheduler> networkSchedulers(int nodes, SimulatedTime time, RandomSource random)
    {

        Set<Verb> extremelyLossy = new HashSet<>(Arrays.asList(Verb.ACCORD_SIMPLE_RSP, Verb.ACCORD_PRE_ACCEPT_RSP, Verb.ACCORD_PRE_ACCEPT_REQ,
                                                               Verb.ACCORD_ACCEPT_RSP, Verb.ACCORD_ACCEPT_REQ, Verb.ACCORD_NOT_ACCEPT_REQ,
                                                               Verb.ACCORD_READ_RSP, Verb.ACCORD_READ_REQ, Verb.ACCORD_COMMIT_REQ,
                                                               Verb.ACCORD_COMMIT_INVALIDATE_REQ, Verb.ACCORD_APPLY_RSP, Verb.ACCORD_APPLY_REQ,
                                                               Verb.ACCORD_BEGIN_RECOVER_RSP, Verb.ACCORD_BEGIN_RECOVER_REQ, Verb.ACCORD_BEGIN_INVALIDATE_RSP));

        Set<Verb> somewhatLossy = new HashSet<>(Arrays.asList(Verb.ACCORD_SYNC_NOTIFY_RSP, Verb.ACCORD_SYNC_NOTIFY_REQ, Verb.ACCORD_APPLY_AND_WAIT_REQ,
                                                              Verb.ACCORD_FETCH_TOPOLOGY_RSP, Verb.ACCORD_FETCH_TOPOLOGY_REQ));

        Map<Verb, FutureActionScheduler> schedulers = new HashMap<>();
        for (Verb verb : Verb.values())
        {
            if (extremelyLossy.contains(verb))
                schedulers.put(verb, new FixedLossNetworkScheduler(nodes, random, time, KindOfSequence.UNIFORM, .15f, .20f));
            else if (somewhatLossy.contains(verb))
                schedulers.put(verb, new FixedLossNetworkScheduler(nodes, random, time, KindOfSequence.UNIFORM, .1f, .15f));
        }
        return schedulers;
    }

    protected ConsistencyLevel validateQueryConsistency()
    {
        return ConsistencyLevel.QUORUM;
    }

    public Generator<SchemaSpec> schemaSpecGen(String keyspace, String prefix)
    {
        return SchemaGenerators.schemaSpecGen(keyspace, prefix, 1000, SchemaSpec.optionsBuilder().withTransactionalMode(TransactionalMode.full));
    }

}

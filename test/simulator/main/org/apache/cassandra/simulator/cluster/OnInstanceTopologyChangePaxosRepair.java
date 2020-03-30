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

package org.apache.cassandra.simulator.cluster;

import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableRunnable;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.net.Verb.MUTATION_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_CLEANUP_COMPLETE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_CLEANUP_FINISH_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_CLEANUP_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_CLEANUP_RSP;
import static org.apache.cassandra.net.Verb.PAXOS2_CLEANUP_RSP2;
import static org.apache.cassandra.net.Verb.PAXOS2_CLEANUP_START_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.READ_REQ;
import static org.apache.cassandra.net.Verb.SCHEMA_PULL_REQ;
import static org.apache.cassandra.net.Verb.SCHEMA_PUSH_REQ;
import static org.apache.cassandra.simulator.Action.Modifiers.NO_TIMEOUTS;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

class OnInstanceTopologyChangePaxosRepair extends ClusterAction
{
    public OnInstanceTopologyChangePaxosRepair(ClusterActions actions, int on, String reason)
    {
        this("Paxos Topology Repair on " + on, RELIABLE, NO_TIMEOUTS, actions, on, invokableTopologyChangeRepair(reason));
    }

    public OnInstanceTopologyChangePaxosRepair(String id, Modifiers self, Modifiers transitive, ClusterActions actions, int on, SerializableRunnable runnable)
    {
        super(id, RELIABLE.with(self), NO_TIMEOUTS.with(transitive), actions, on, runnable);
        setMessageModifiers(SCHEMA_PULL_REQ, RELIABLE, RELIABLE);
        setMessageModifiers(SCHEMA_PUSH_REQ, RELIABLE, RELIABLE);
        setMessageModifiers(PAXOS2_CLEANUP_START_PREPARE_REQ, RELIABLE, RELIABLE);
        setMessageModifiers(PAXOS2_CLEANUP_REQ, RELIABLE, RELIABLE);
        setMessageModifiers(PAXOS2_CLEANUP_FINISH_PREPARE_REQ, RELIABLE, RELIABLE);
        setMessageModifiers(PAXOS2_CLEANUP_COMPLETE_REQ, RELIABLE, RELIABLE);
        setMessageModifiers(PAXOS2_CLEANUP_RSP, RELIABLE, RELIABLE);
        setMessageModifiers(PAXOS2_CLEANUP_RSP2, RELIABLE, RELIABLE);
        setMessageModifiers(MUTATION_REQ, RELIABLE, RELIABLE);
        setMessageModifiers(READ_REQ, RELIABLE, RELIABLE);
    }

    protected static SerializableRunnable invokableTopologyChangeRepair(String reason)
    {
        return () -> {
            Condition condition = newOneTimeCondition();
            Future<?> future = StorageService.instance.startRepairPaxosForTopologyChange(reason);
            future.addListener(condition::signal); // add listener so we don't use Futures.addAllAsList
            condition.awaitThrowUncheckedOnInterrupt();
        };
    }
}

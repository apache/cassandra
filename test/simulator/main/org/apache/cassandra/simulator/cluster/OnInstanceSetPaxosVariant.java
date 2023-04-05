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

import org.apache.cassandra.config.Config.PaxosVariant;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;

import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE;
import static org.apache.cassandra.utils.LazyToString.lazy;

class OnInstanceSetPaxosVariant extends Action
{
    final IInvokableInstance instance;
    final int on;
    final PaxosVariant newVariant;

    OnInstanceSetPaxosVariant(ClusterActions actions, int on, PaxosVariant newVariant)
    {
        super(lazy(() -> "Set Paxos Variant to " + newVariant + " on node" + on), RELIABLE, NONE);
        this.instance = actions.cluster.get(on);
        this.on = on;
        this.newVariant = newVariant;
    }

    protected ActionList performSimple()
    {
        instance.unsafeRunOnThisThread(invokableSetVariant(newVariant));
        return ActionList.empty();
    }

    static IIsolatedExecutor.SerializableRunnable invokableSetVariant(PaxosVariant to)
    {
        return () -> Paxos.setPaxosVariant(to);
    }
}

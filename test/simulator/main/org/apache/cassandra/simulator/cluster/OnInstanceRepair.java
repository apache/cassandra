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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.compatibility.TokenRingUtils;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.progress.ProgressEventType;

import static java.util.Collections.singletonList;
import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.cluster.Utils.currentToken;
import static org.apache.cassandra.simulator.cluster.Utils.parseTokenRanges;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

public class OnInstanceRepair extends ClusterAction
{
    public enum RepairType
    {
        DATA_INCREMENTAL(true, false, false, true),
        PAXOS_FULL(false, true, false, false),
        DATA_AND_PAXOS_FULL(true, true, false, false),
        ACCORD_ONLY(false, false, true, false);

        public final boolean repairData;
        public final boolean repairPaxos;
        public final boolean repairAccord;
        public final boolean incremental;

        RepairType(boolean repairData, boolean repairPaxos, boolean repairAccord, boolean incremental)
        {
            this.repairData = repairData;
            this.repairPaxos = repairPaxos;
            this.repairAccord = repairAccord;
            this.incremental = incremental;
        }

        @Override
        public String toString()
        {
            return "RepairType{" +
                   "repairData=" + repairData +
                   ", repairPaxos=" + repairPaxos +
                   ", repairAccord=" + repairAccord +
                   ", incremental=" + incremental +
                   '}';
        }

        Modifiers modifiers()
        {
            return NONE;
        }
    }

    public OnInstanceRepair(KeyspaceActions actions, int on, RepairType repairType, boolean force)
    {
        super("Repair on " + on, repairType.modifiers(), repairType.modifiers(), actions, on, invokableBlockingRepair(actions.keyspace, repairType, false, force));
    }

    public OnInstanceRepair(KeyspaceActions actions, int on, RepairType repairType, Map.Entry<String, String> repairRange, boolean force)
    {
        this(actions, on, repairType.modifiers(), repairType.modifiers(), repairType, repairRange, force);
    }

    public OnInstanceRepair(KeyspaceActions actions, int on, Modifiers self, Modifiers transitive, RepairType repairType, Map.Entry<String, String> repairRange, boolean force)
    {
        super("Repair on " + on, self, transitive, actions, on, invokableBlockingRepair(actions.keyspace, repairType, repairRange, force));
    }

    private static IIsolatedExecutor.SerializableRunnable invokableBlockingRepair(String keyspaceName, RepairType repairType, boolean primaryRangeOnly, boolean force)
    {
        return () -> {
            Condition done = newOneTimeCondition();
            invokeRepair(keyspaceName, repairType, primaryRangeOnly, force, done::signal);
            done.awaitThrowUncheckedOnInterrupt();
        };
    }

    private static IIsolatedExecutor.SerializableRunnable invokableBlockingRepair(String keyspaceName, RepairType repairType, Map.Entry<String, String> repairRange, boolean force)
    {
        return () -> {
            Condition done = newOneTimeCondition();
            invokeRepair(keyspaceName, repairType, () -> parseTokenRanges(singletonList(repairRange)), false, force, done::signal);
            done.awaitThrowUncheckedOnInterrupt();
        };
    }

    private static void invokeRepair(String keyspaceName, RepairType repairType, boolean primaryRangeOnly, boolean force, Runnable listener)
    {
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ClusterMetadata metadata = ClusterMetadata.current();
        invokeRepair(keyspaceName, repairType,
                     () -> primaryRangeOnly ? TokenRingUtils.getPrimaryRangesFor(metadata.tokenMap.tokens(), Collections.singleton(currentToken()))
                                            : keyspace.getReplicationStrategy().getAddressReplicas(metadata).get(getBroadcastAddressAndPort()).asList(Replica::range),
                     primaryRangeOnly, force, listener);
    }

    private static void invokeRepair(String keyspaceName, RepairType repairType, IIsolatedExecutor.SerializableCallable<Collection<Range<Token>>> rangesSupplier, boolean isPrimaryRangeOnly, boolean force, Runnable listener)
    {
        Collection<Range<Token>> ranges = rangesSupplier.call();
        // no need to wait for completion, as we track all task submissions and message exchanges, and ensure they finish before continuing to next action
        StorageService.instance.repair(keyspaceName, new RepairOption(RepairParallelism.SEQUENTIAL, isPrimaryRangeOnly, repairType.incremental, false, 1, ranges, false, force, PreviewKind.NONE, false, true, repairType.repairData, repairType.repairPaxos, false, repairType.repairAccord), singletonList((tag, event) -> {
            if (event.getType() == ProgressEventType.COMPLETE)
                listener.run();
        }));
    }

}

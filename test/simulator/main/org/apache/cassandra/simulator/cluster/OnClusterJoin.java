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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.Actions;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.LazyToString.lazy;

class OnClusterJoin extends OnClusterChangeTopology
{
    final int joining;

    OnClusterJoin(KeyspaceActions actions, Topology before, Topology during, Topology after, int joining)
    {
        super(lazy(() -> String.format("node%d Joining", joining)), actions, before, after, during.pendingKeys());
        this.joining = joining;
    }

    public ActionList performSimple()
    {
        IInvokableInstance joinInstance = actions.cluster.get(joining);
        before(joinInstance);
        List<Action> actionList = new ArrayList<>();
        actionList.add(new SubmitPrepareJoin(actions, joining));
        actionList.add(new OnInstanceTopologyChangePaxosRepair(actions, joining, "Join"));

        actionList.add(Actions.of(Modifiers.STRICT, Modifiers.RELIABLE_NO_TIMEOUTS, "Start Join", () -> {
            List<Action> local = new ArrayList<>();
            local.add(new ExecuteNextStep(actions, joining, Transformation.Kind.START_JOIN));
            local.addAll(Quiesce.all(actions));
            return ActionList.of(local);
        }));

        actionList.add(Actions.of(Modifiers.STRICT, Modifiers.RELIABLE_NO_TIMEOUTS,"Mid Join", () -> {
            List<Action> local = new ArrayList<>();
            local.add(new ExecuteNextStep(actions, joining, Transformation.Kind.MID_JOIN));
            local.addAll(Quiesce.all(actions));
            return ActionList.of(local);
        }));

        actionList.add(Actions.of(Modifiers.STRICT, Modifiers.RELIABLE_NO_TIMEOUTS,"Finish Join", () -> {
            List<Action> local = new ArrayList<>();
            local.add(new ExecuteNextStep(actions, joining, Transformation.Kind.FINISH_JOIN));
            local.addAll(Quiesce.all(actions));
            return ActionList.of(local);
        }));

        return ActionList.of(actionList);
    }

    public static class SubmitPrepareJoin extends ClusterReliableAction
    {
        public SubmitPrepareJoin(ClusterActions actions, int on)
        {
            super("Prepare Join", actions, on, () -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                ClusterMetadataService.instance().commit(new PrepareJoin(metadata.myNodeId(),
                                                                         new HashSet<>(BootStrapper.getBootstrapTokens(metadata, FBUtilities.getBroadcastAddressAndPort())),
                                                                         ClusterMetadataService.instance().placementProvider(),
                                                                         true,
                                                                         true));
            });
        }
    }

    public static class ExecuteNextStep extends ClusterReliableAction
    {
        private ExecuteNextStep(ClusterActions actions, int on, Transformation.Kind kind)
        {
            this(actions, on, kind.ordinal());
        }
        private ExecuteNextStep(ClusterActions actions, int on, int kind)
        {
            super(String.format("Execute next step of the join operation: %s", Transformation.Kind.values()[kind]), actions, on, () -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                MultiStepOperation<?> sequence = metadata.inProgressSequences.get(metadata.myNodeId());

                if (!(sequence instanceof BootstrapAndJoin))
                    throw new IllegalStateException(String.format("Can not resume bootstrap as it does not appear to have been started. Found %s", sequence));

                BootstrapAndJoin bootstrapAndJoin = ((BootstrapAndJoin) sequence);
                assert bootstrapAndJoin.next.ordinal() == kind : String.format("Expected next step to be %s, but got %s", Transformation.Kind.values()[kind], bootstrapAndJoin.next);
                boolean res = bootstrapAndJoin.executeNext().isContinuable();
                assert res;
            });
        }
    }
}

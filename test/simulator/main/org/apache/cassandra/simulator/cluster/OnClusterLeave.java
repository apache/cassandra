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
import java.util.List;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.Actions;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LeaveStreams;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;
import org.apache.cassandra.tcm.transformations.PrepareLeave;

import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;
import static org.apache.cassandra.utils.LazyToString.lazy;

class OnClusterLeave extends OnClusterChangeTopology
{
    final int leaving;

    OnClusterLeave(KeyspaceActions actions, Topology before, Topology during, Topology after, int leaving)
    {
        super(lazy(() -> String.format("node%d Leaving", leaving)), actions, before, after, during.pendingKeys());
        this.leaving = leaving;
    }

    public ActionList performSimple()
    {
        IInvokableInstance joinInstance = actions.cluster.get(leaving);
        before(joinInstance);
        List<Action> actionList = new ArrayList<>();
        actionList.add(new SubmitPrepareLeave(actions, leaving));
        actionList.add(new OnInstanceTopologyChangePaxosRepair(actions, leaving, "Leave"));

        actionList.add(Actions.of(Modifiers.STRICT, Modifiers.RELIABLE_NO_TIMEOUTS, "Start Leave", () -> {
            List<Action> local = new ArrayList<>();
            local.add(new ExecuteNextStep(actions, leaving, Transformation.Kind.START_LEAVE));
            local.addAll(Quiesce.all(actions));
            return ActionList.of(local);
        }));

        actionList.add(Actions.of(Modifiers.STRICT, Modifiers.RELIABLE_NO_TIMEOUTS, "Mid Leave", () -> {
            List<Action> local = new ArrayList<>();
            local.add(new ExecuteNextStep(actions, leaving, Transformation.Kind.MID_LEAVE));
            local.addAll(Quiesce.all(actions));
            return ActionList.of(local);
        }));

        actionList.add(Actions.of(Modifiers.STRICT, Modifiers.RELIABLE_NO_TIMEOUTS, "Finish Leave", () -> {
            List<Action> local = new ArrayList<>();
            local.add(new ExecuteNextStep(actions, leaving, Transformation.Kind.FINISH_LEAVE));
            local.addAll(Quiesce.all(actions));
            return ActionList.of(local);
        }));

        return ActionList.of(actionList);
    }

    public static class SubmitPrepareLeave extends ClusterReliableAction
    {
        public SubmitPrepareLeave(ClusterActions actions, int on)
        {
            super("Prepare Leave", actions, on, () -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                ReconfigureCMS.maybeReconfigureCMS(metadata, getBroadcastAddressAndPort());

                metadata = ClusterMetadata.current();
                ClusterMetadataService.instance().commit(new PrepareLeave(metadata.myNodeId(),
                                                                          false,
                                                                          ClusterMetadataService.instance().placementProvider(),
                                                                          LeaveStreams.Kind.UNBOOTSTRAP));
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
            super(String.format("Execute next step of the leave operation %s", Transformation.Kind.values()[kind]), actions, on, () -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                MultiStepOperation<?> sequence = metadata.inProgressSequences.get(metadata.myNodeId());

                if (!(sequence instanceof UnbootstrapAndLeave))
                    throw new IllegalStateException(String.format("Can not resume leave as it does not appear to have been started. Found %s", sequence));

                UnbootstrapAndLeave unbootstrapAndLeave = ((UnbootstrapAndLeave) sequence);
                assert unbootstrapAndLeave.next.ordinal() == kind : String.format("Expected next step to be %s, but got %s", Transformation.Kind.values()[kind], unbootstrapAndLeave.next);
                boolean res = unbootstrapAndLeave.executeNext().isContinuable();
                assert res;
            });
        }
    }
}
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

import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.Actions;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.BootstrapAndReplace;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.transformations.PrepareReplace;

import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.Modifiers.STRICT;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;
import static org.apache.cassandra.utils.LazyToString.lazy;

class OnClusterReplace extends OnClusterChangeTopology
{
    final int leaving;
    final int joining;
    final Topology during;

    OnClusterReplace(KeyspaceActions actions, Topology before, Topology during, Topology after, int leaving, int joining)
    {
        super(lazy(() -> String.format("node%d Replacing node%d", joining, leaving)), actions, STRICT, NONE, before, after, during.pendingKeys());
        this.leaving = leaving;
        this.joining = joining;
        this.during = during;
    }

    public ActionList performSimple()
    {
        IInvokableInstance joinInstance = actions.cluster.get(joining);
        before(joinInstance);
        int leavingNodeId = actions.cluster.get(leaving).unsafeCallOnThisThread(() -> ClusterMetadata.current().myNodeId().id());
        List<Action> actionList = new ArrayList<>();
        actionList.add(new SubmitPrepareReplace(actions, leavingNodeId, joining));
        actionList.add(new OnInstanceTopologyChangePaxosRepair(actions, joining, "Replace"));

        actionList.add(Actions.of(Modifiers.STRICT, Modifiers.RELIABLE_NO_TIMEOUTS, "Start Replace", () -> {
            List<Action> local = new ArrayList<>();

            List<Map.Entry<String, String>> repairRanges = actions.cluster.get(leaving).unsafeApplyOnThisThread(
            (String keyspaceName) -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                return metadata.placements.get(metadata.schema.getKeyspace(keyspaceName).getMetadata().params.replication)
                       .writes.ranges()
                              .stream()
                              .map(OnClusterReplace::toStringEntry)
                              .collect(Collectors.toList());
            }, actions.keyspace);


            Map<InetSocketAddress, IInvokableInstance> lookup = Cluster.getUniqueAddressLookup(actions.cluster);

            int[] others = repairRanges.stream().mapToInt(
            repairRange -> lookup.get(actions.cluster.get(leaving).unsafeApplyOnThisThread(
                                      (String keyspaceName, String tk) -> {
                                          ClusterMetadata metadata = ClusterMetadata.current();
                                          KeyspaceMetadata keyspaceMetadata = metadata.schema.getKeyspaces().getNullable(keyspaceName);
                                          return metadata.placements.get(keyspaceMetadata.params.replication).reads
                                                 .forToken(Utils.parseToken(tk))
                                                 .get()
                                                 .stream().map(Replica::endpoint)
                                                 .filter(i -> !i.equals(getBroadcastAddressAndPort()))
                                                 .findFirst()
                                                 .orElseThrow(IllegalStateException::new);
                                      },
                                      actions.keyspace, repairRange.getValue())
            ).config().num()
            ).toArray();

            local.add(new OnClusterMarkDown(actions, leaving));
            local.add(new OnClusterRepairRanges(actions, others, true, false, repairRanges));
            local.add(new ExecuteNextStep(actions, joining, Transformation.Kind.START_REPLACE));
            local.addAll(Quiesce.all(actions));
            return ActionList.of(local);
        }));

        actionList.add(Actions.of(Modifiers.STRICT, Modifiers.RELIABLE_NO_TIMEOUTS, "Mid Replace", () -> {
            List<Action> local = new ArrayList<>();
            local.add(new ExecuteNextStep(actions, joining, Transformation.Kind.MID_REPLACE));
            local.addAll(Quiesce.all(actions));
            return ActionList.of(local);
        }));

        actionList.add(Actions.of(Modifiers.STRICT, Modifiers.RELIABLE_NO_TIMEOUTS, "Finish Replace", () -> {
            List<Action> local = new ArrayList<>();
            local.add(new ExecuteNextStep(actions, joining, Transformation.Kind.FINISH_REPLACE));
            local.addAll(Quiesce.all(actions));
            return ActionList.of(local);
        }));

        return ActionList.of(actionList);
    }

    private static class SubmitPrepareReplace extends ClusterReliableAction
    {
        public SubmitPrepareReplace(ClusterActions actions, int leavingNodeId, int joining)
        {
            super("Prepare Replace", actions, joining, () -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                NodeId leaving = new NodeId(leavingNodeId);
                ReconfigureCMS.maybeReconfigureCMS(metadata, metadata.directory.endpoint(leaving));

                metadata = ClusterMetadata.current();
                ClusterMetadataService.instance().commit(new PrepareReplace(leaving,
                                                                            metadata.myNodeId(),
                                                                            ClusterMetadataService.instance().placementProvider(),
                                                                            true,
                                                                            true));
            });
        }
    }

    private static class ExecuteNextStep extends ClusterReliableAction
    {
        private ExecuteNextStep(ClusterActions actions, int on, Transformation.Kind kind)
        {
            this(actions, on, kind.ordinal());
        }

        private ExecuteNextStep(ClusterActions actions, int on, int kind)
        {
            super(String.format("Execute next step of the replace operation: %s", Transformation.Kind.values()[kind]), actions, on, () -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                MultiStepOperation<?> sequence = metadata.inProgressSequences.get(metadata.myNodeId());

                if (!(sequence instanceof BootstrapAndReplace))
                    throw new IllegalStateException(String.format("Can not resume replace as it does not appear to have been started. Found: %s", sequence));

                BootstrapAndReplace bootstrapAndReplace = ((BootstrapAndReplace) sequence);
                assert bootstrapAndReplace.next.ordinal() == kind : String.format("Expected next step to be %s, but got %s", Transformation.Kind.values()[kind], bootstrapAndReplace.next);
                boolean res = bootstrapAndReplace.executeNext().isContinuable();
                assert res;
            });
        }
    }

    static Map.Entry<String, String> toStringEntry(Range<Token> range)
    {
        return new AbstractMap.SimpleImmutableEntry<>(range.left.toString(), range.right.toString());
    }
}

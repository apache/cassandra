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

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.Actions.SimpleAction;
import org.apache.cassandra.simulator.cluster.ClusterActionListener.TopologyChangeValidator;

import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE;
import static org.apache.cassandra.simulator.Action.Modifiers.STRICT;
import static org.apache.cassandra.simulator.ActionListener.runAfterTransitiveClosure;

abstract class OnClusterChangeTopology extends SimpleAction implements Consumer<Action>
{
    final KeyspaceActions actions;
    final int[] membersOfRing; // nodes that are members of the ring after the action completes
    final int[] membersOfQuorumDcs; // if actions are performed at LOCAL_SERIAL/LOCAL, only those in membersOfRing in the local DC
    final int quorumRfBefore;
    final int quorumRfAfter;
    int[] overlappingKeys;

    final TopologyChangeValidator validator;

    public OnClusterChangeTopology(Object description, KeyspaceActions actions, int[] membersOfRing, int[] membersOfQuorumDcs, int quorumRfBefore, int quorumRfAfter)
    {
        this(description, actions, STRICT, RELIABLE, membersOfRing, membersOfQuorumDcs, quorumRfBefore, quorumRfAfter);
    }

    public OnClusterChangeTopology(Object description, KeyspaceActions actions, Modifiers self, Modifiers children, int[] membersOfRing, int[] membersOfQuorumDcs, int quorumRfBefore, int quorumRfAfter)
    {
        super(description, self, children);
        this.actions = actions;
        this.membersOfRing = membersOfRing;
        this.membersOfQuorumDcs = membersOfQuorumDcs;
        this.quorumRfBefore = quorumRfBefore;
        this.quorumRfAfter = quorumRfAfter;
        this.validator = actions.listener.newTopologyChangeValidator(this);
        register(runAfterTransitiveClosure(this));
    }

    void before(IInvokableInstance instance)
    {
        overlappingKeys = instance.unsafeApplyOnThisThread((keyspace, table, primaryKeys) -> {
            TableMetadata metadata = Keyspace.open(keyspace).getColumnFamilyStore(table).metadata.get();
            Collection<Range<Token>> ranges = StorageService.instance.getLocalAndPendingRanges(keyspace);
            return Arrays.stream(primaryKeys)
                         .mapToObj(pk -> metadata.partitioner.decorateKey(Int32Type.instance.decompose(pk)))
                         .filter(pk -> ranges.stream().anyMatch(r -> r.contains(pk.getToken())))
                         .mapToInt(pk -> Int32Type.instance.compose(pk.getKey()))
                         .toArray();
        }, actions.keyspace, actions.table, actions.primaryKeys);
        int[][] replicasForKeys = actions.replicasForKeys(actions.keyspace, actions.table, overlappingKeys, membersOfQuorumDcs);
        validator.before(overlappingKeys, replicasForKeys, quorumRfBefore);
    }

    public void accept(Action ignore)
    {
        validator.after(actions.replicasForKeys(actions.keyspace, actions.table, overlappingKeys, membersOfQuorumDcs), quorumRfAfter);
    }
}

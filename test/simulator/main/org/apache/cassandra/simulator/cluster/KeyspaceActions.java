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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.function.IntSupplier;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.ActionPlan;
import org.apache.cassandra.simulator.ActionSequence;
import org.apache.cassandra.simulator.Actions;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.systems.SimulatedSystems;

import static java.util.Arrays.stream;
import static java.util.Collections.singletonList;
import static org.apache.cassandra.simulator.Debug.EventType.CLUSTER;
import static org.apache.cassandra.simulator.cluster.ClusterActions.TopologyChange.JOIN;
import static org.apache.cassandra.simulator.cluster.ClusterActions.TopologyChange.LEAVE;
import static org.apache.cassandra.simulator.cluster.ClusterReliableQueryAction.schemaChange;

public class KeyspaceActions extends ClusterActions
{
    final String keyspace;
    final String table;
    final String createTableCql;
    final ConsistencyLevel serialConsistency;

    final int[] primaryKeys;
    public KeyspaceActions(SimulatedSystems simulated,
                           String keyspace, String table, String createTableCql,
                           Cluster cluster,
                           Options options,
                           ConsistencyLevel serialConsistency,
                           ClusterActionListener listener,
                           int[] primaryKeys,
                           Debug debug)
    {
        super(simulated, cluster, options, listener, debug);
        this.keyspace = keyspace;
        this.table = table;
        this.createTableCql = createTableCql;
        this.primaryKeys = primaryKeys;
        this.serialConsistency = serialConsistency;
    }

    public ActionPlan plan()
    {
        class Generator extends Options
        {
            final NodeLookup nodeLookup;
            final NodesByDc all;
            final NodesByDc prejoin;
            final NodesByDc joined;
            final NodesByDc left;

            public Generator(Options copy, NodeLookup nodeLookup)
            {
                super(copy);
                this.nodeLookup = nodeLookup;

                int[] dcSizes = new int[initialRf.length];
                for (int dc : nodeLookup.nodeToDc)
                    ++dcSizes[dc];

                this.all = new NodesByDc(nodeLookup, dcSizes);
                this.prejoin = new NodesByDc(nodeLookup, dcSizes);
                this.joined = new NodesByDc(nodeLookup, dcSizes);
                this.left = new NodesByDc(nodeLookup, dcSizes);

                for (int i = 1 ; i <= nodeLookup.nodeToDc.length ; ++i)
                {
                    this.prejoin.add(i);
                    this.all.add(i);
                }
            }

            Action pre(String createKeyspaceCql, String createTableCql)
            {
                // randomise initial cluster, and return action to initialise it
                for (int dc = 0 ; dc < initialRf.length ; ++dc)
                {
                    for (int i = 0 ; i < initialRf[dc] ; ++i)
                        joined.add(prejoin.removeRandom(random, dc));
                }

                return Actions.StrictAction.of("Initialize", () -> {
                    return ActionList.of(initializeCluster(this.joined.toArray(), this.prejoin.toArray()),
                                         schemaChange("Create Keyspace", KeyspaceActions.this, 1, createKeyspaceCql, time.futureTimestamp()),
                                         schemaChange("Create Table", KeyspaceActions.this, 1, createTableCql, time.futureTimestamp()));
                });
            }

            @SuppressWarnings("StatementWithEmptyBody")
                // generating deterministic plan, with no
            ActionList plan()
            {
                boolean haveChangedVariant = false;
                EnumSet<TopologyChange> supported = EnumSet.noneOf(TopologyChange.class);
                supported.addAll(Arrays.asList(choices.options));
                int[] currentRf = initialRf.clone();
                List<Action> plan = new ArrayList<>();
                int[] membersOfQuorumDcs = serialConsistency == ConsistencyLevel.LOCAL_SERIAL
                                           ? all.dcs[0] : all.toArray();
                IntSupplier quorumRf = serialConsistency == ConsistencyLevel.LOCAL_SERIAL
                                       ? () -> currentRf[0] : () -> stream(currentRf).sum();

                while (!supported.isEmpty() && !prejoin.isEmpty() || joined.size() > stream(currentRf).sum())
                {
                    if (changePaxosVariantTo != null && !haveChangedVariant && random.decide(1f / (1 + prejoin.size())))
                    {
                        plan.add(new OnClusterSetPaxosVariant(KeyspaceActions.this, changePaxosVariantTo));
                        haveChangedVariant = true;
                    }

                    // pick a dc
                    int dc = random.uniform(0, currentRf.length);

                    // try to pick an action (and simply loop again if we cannot for this dc)
                    TopologyChange next;
                    if (prejoin.size(dc) > 0 && joined.size(dc) > currentRf[dc]) next = choices.choose(random);
                    else if (prejoin.size(dc) > 0 && supported.contains(JOIN)) next = JOIN;
                    else if (joined.size(dc) > currentRf[dc] && supported.contains(LEAVE)) next = LEAVE;
                    else continue;

                    switch (next)
                    {
                        case REPLACE:
                        {
                            int join = prejoin.removeRandom(random, dc);
                            int leave = joined.selectRandom(random, dc);
                            joined.add(join);
                            int[] membersOfRing = joined.toArray();
                            joined.remove(leave);
                            left.add(leave);
                            nodeLookup.setTokenOf(join, nodeLookup.tokenOf(leave));
                            plan.add(new OnClusterReplace(KeyspaceActions.this, membersOfRing, membersOfQuorumDcs, leave, join, quorumRf.getAsInt()));
                            break;
                            // if replication factor is 2, cannot perform safe replacements
                            // however can have operations that began earlier during RF=2
                            // so need to introduce some concept of barriers/ordering/sync points
                        }
                        case JOIN:
                        {
                            int join = prejoin.removeRandom(random, dc);
                            joined.add(join);
                            plan.add(new OnClusterJoin(KeyspaceActions.this, joined.toArray(), membersOfQuorumDcs, join, quorumRf.getAsInt()));
                            break;
                        }
                        case LEAVE:
                        {
                            int[] membersOfRing = joined.toArray();
                            int leave = joined.removeRandom(random, dc);
                            left.add(leave);
                            plan.add(new OnClusterLeave(KeyspaceActions.this, membersOfRing, membersOfQuorumDcs, leave, quorumRf.getAsInt()));
                            break;
                        }
                        case CHANGE_RF:
                            if (maxRf[dc] == minRf[dc]) {} // cannot perform RF changes at all
                            if (currentRf[dc] == minRf[dc] && joined.size(dc) == currentRf[dc]) {} // can do nothing until joined grows
                            else
                            {
                                int quorumRfBefore = quorumRf.getAsInt();
                                boolean increase;
                                if (currentRf[dc] == minRf[dc]) // can only grow
                                { ++currentRf[dc]; increase = true;}
                                else if (currentRf[dc] == joined.size(dc) || currentRf[dc] == maxRf[dc]) // can only shrink, and we know currentRf > minRf
                                { --currentRf[dc]; increase = false; }
                                else if (random.decide(0.5f)) // can do either
                                { --currentRf[dc]; increase = true; }
                                else
                                { ++currentRf[dc]; increase = false; }

                                long timestamp = time.futureTimestamp();
                                int coordinator = joined.selectRandom(random, dc);
                                int[] membersOfRing = joined.toArray();
                                int quorumRfAfter = quorumRf.getAsInt();
                                plan.add(new OnClusterChangeRf(KeyspaceActions.this, timestamp, coordinator, membersOfRing, membersOfQuorumDcs, increase, currentRf.clone(), quorumRfBefore, quorumRfAfter));
                            }
                    }
                }

                if (changePaxosVariantTo != null && !haveChangedVariant)
                    plan.add(new OnClusterSetPaxosVariant(KeyspaceActions.this, changePaxosVariantTo));

                plan.add(Actions.empty("Finished Cluster Actions"));
                return ActionList.of(plan);
            }
        }

        Generator generator = new Generator(options, snitch);
        ActionList pre = ActionList.of(generator.pre(createKeyspaceCql(keyspace), createTableCql));
        ActionSequence interleave = generator.plan().strictlySequential();
        ActionList post = ActionList.empty();
        debug.debug(CLUSTER, singletonList(interleave), cluster, keyspace, null);
        return new ActionPlan(pre, singletonList(interleave), post);
    }

    @SuppressWarnings("StringConcatenationInLoop")
    private String createKeyspaceCql(String keyspace)
    {
        String createKeyspaceCql = "CREATE KEYSPACE " + keyspace  + " WITH replication = {'class': 'NetworkTopologyStrategy'";
        for (int i = 0 ; i < options.initialRf.length ; ++i)
            createKeyspaceCql += ", '" + snitch.nameOfDc(i) + "': " + options.initialRf[i];
        createKeyspaceCql += "};";
        return createKeyspaceCql;
    }

}

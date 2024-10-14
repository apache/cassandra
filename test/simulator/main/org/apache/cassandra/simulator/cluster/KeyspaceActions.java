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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.ActionListener;
import org.apache.cassandra.simulator.ActionPlan;
import org.apache.cassandra.simulator.Actions;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.OrderOn.StrictSequential;
import org.apache.cassandra.simulator.systems.InterceptedExecution;
import org.apache.cassandra.simulator.systems.InterceptingExecutor;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.tcm.ClusterMetadataService;

import static java.util.Collections.singletonList;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.LOCAL_SERIAL;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE_NO_TIMEOUTS;
import static org.apache.cassandra.simulator.Debug.EventType.CLUSTER;
import static org.apache.cassandra.simulator.cluster.ClusterActions.TopologyChange.CHANGE_RF;
import static org.apache.cassandra.simulator.cluster.ClusterActions.TopologyChange.JOIN;
import static org.apache.cassandra.simulator.cluster.ClusterActions.TopologyChange.LEAVE;

public class KeyspaceActions extends ClusterActions
{
    final String keyspace;
    final String table;
    final String createTableCql;
    final ConsistencyLevel serialConsistency;
    final int[] primaryKeys;

    final EnumSet<TopologyChange> ops = EnumSet.noneOf(TopologyChange.class);
    final NodeLookup nodeLookup;
    final TokenPlacementModel.NodeFactory factory;
    final int[] minRf, initialRf, maxRf;
    final int[] membersOfQuorumDcs;

    // working state
    final NodesByDc all;
    final NodesByDc registered;
    final NodesByDc joined;
    final NodesByDc left;

    final int[] currentRf;
    Topology topology;
    boolean haveChangedVariant;
    int topologyChangeCount = 0;

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

        this.nodeLookup = simulated.snitch;

        this.factory = new TokenPlacementModel.NodeFactory(new SimulationLookup());
        int[] dcSizes = new int[options.initialRf.length];
        for (int dc : nodeLookup.nodeToDc)
            ++dcSizes[dc];

        this.all = new NodesByDc(nodeLookup, dcSizes);
        this.registered = new NodesByDc(nodeLookup, dcSizes);
        this.joined = new NodesByDc(nodeLookup, dcSizes);
        this.left = new NodesByDc(nodeLookup, dcSizes);

        for (int i = 1 ; i <= nodeLookup.nodeToDc.length ; ++i)
        {
            this.registered.add(i);
            this.all.add(i);
        }

        minRf = options.minRf;
        initialRf = options.initialRf;
        maxRf = options.maxRf;
        currentRf = initialRf.clone();
        membersOfQuorumDcs = serialConsistency == LOCAL_SERIAL ? all.dcs[0] : all.toArray();
        ops.addAll(Arrays.asList(options.allChoices.options));

    }

    public ActionPlan plan()
    {
        ActionList pre = ActionList.of(pre(createKeyspaceCql(keyspace), createTableCql));
        ActionList interleave = stream();
        ActionList post = ActionList.empty();
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

    private Action pre(String createKeyspaceCql, String createTableCql)
    {
        // randomise initial cluster, and return action to initialise it
        for (int dc = 0 ; dc < options.initialRf.length ; ++dc)
        {
            for (int i = 0 ; i < options.initialRf[dc] ; ++i)
            {
                int join = registered.removeRandom(random, dc);
                joined.add(join);
            }
        }

        updateTopology(recomputeTopology());
        int[] joined = this.joined.toArray();
        int[] prejoin = this.registered.toArray();
        return Actions.StrictAction.of("Initialize", () -> {
            List<Action> actions = new ArrayList<>();
            actions.add(initializeCluster(joined, prejoin));
            actions.add(schemaChange(1, createKeyspaceCql));
            actions.add(schemaChange(1, createTableCql));
            cluster.stream().forEach(i -> actions.add(invoke("Quiesce " + i.broadcastAddress(), RELIABLE_NO_TIMEOUTS, RELIABLE_NO_TIMEOUTS,
                                                             new InterceptedExecution.InterceptedRunnableExecution((InterceptingExecutor) i.executor(),
                                                                                                                   () -> i.runOnInstance(() -> ClusterMetadataService.instance().log().waitForHighestConsecutive())))));

            return ActionList.of(actions);
        });
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private ActionList stream()
    {
        ActionListener listener = debug.debug(CLUSTER, time, cluster, keyspace, null);
        if (listener == null)
            return ActionList.of(Actions.stream(new StrictSequential("Cluster Actions"), this::next));

        return ActionList.of(Actions.stream(new StrictSequential("Cluster Actions"), () -> {
            Action action = next();
            if (action != null)
                action.register(listener);
            return action;
        }));
    }

    private TokenPlacementModel.ReplicatedRanges placements(NodesByDc nodesByDc, int[] rfs)
    {
        List<TokenPlacementModel.Node> nodes = new ArrayList<>();
        for (int dcIdx = 0; dcIdx < nodesByDc.dcs.length; dcIdx++)
        {
            int[] nodesInDc = nodesByDc.dcs[dcIdx];
            for (int i = 0; i < nodesByDc.dcSizes[dcIdx]; i++)
            {
                int nodeIdx = nodesInDc[i];
                TokenPlacementModel.Node node = factory.make(nodeIdx,nodeIdx, 1);
                nodes.add(node);
                assert node.token() == tokenOf(nodeIdx);
            }
        }

        Map<String, Integer> rf = new HashMap<>();
        for (int i = 0; i < rfs.length; i++)
            rf.put(factory.lookup().dc(i + 1), rfs[i]);

        nodes.sort(TokenPlacementModel.Node::compareTo);
        return new TokenPlacementModel.NtsReplicationFactor(rfs).replicate(nodes);
    }

    private Topology recomputeTopology(TokenPlacementModel.ReplicatedRanges readPlacements,
                                       TokenPlacementModel.ReplicatedRanges writePlacements)
    {
        int[][] replicasForKey = new int[primaryKeys.length][];
        int[][] pendingReplicasForKey = new int[primaryKeys.length][];
        for (int i = 0 ; i < primaryKeys.length ; ++i)
        {
            int primaryKey = primaryKeys[i];
            LongToken token = new Murmur3Partitioner().getToken(Int32Type.instance.decompose(primaryKey));
            List<TokenPlacementModel.Replica> readReplicas = readPlacements.replicasFor(token.token);
            List<TokenPlacementModel.Replica> writeReplicas = writePlacements.replicasFor(token.token);

            replicasForKey[i] = readReplicas.stream().mapToInt(r -> r.node().idx()).toArray();
            Set<TokenPlacementModel.Replica> pendingReplicas = new HashSet<>(writeReplicas);
            pendingReplicas.removeAll(readReplicas);
            replicasForKey[i] = readReplicas.stream().mapToInt(r -> r.node().idx()).toArray();
            pendingReplicasForKey[i] = pendingReplicas.stream().mapToInt(r -> r.node().idx()).toArray();
        }

        int[] membersOfRing = joined.toArray();
        long[] membersOfRingTokens = IntStream.of(membersOfRing).mapToLong(nodeLookup::tokenOf).toArray();

        return new Topology(primaryKeys, membersOfRing, membersOfRingTokens, membersOfQuorum(), currentRf.clone(),
                            quorumRf(), replicasForKey, pendingReplicasForKey);
    }

    private Action next()
    {
        if (options.topologyChangeLimit >= 0 && topologyChangeCount++ > options.topologyChangeLimit)
            return null;

        while (!ops.isEmpty() && (!registered.isEmpty() || joined.size() > sum(minRf)))
        {
            if (options.changePaxosVariantTo != null && !haveChangedVariant && random.decide(1f / (1 + registered.size())))
            {
                haveChangedVariant = true;
                return schedule(new OnClusterSetPaxosVariant(KeyspaceActions.this, options.changePaxosVariantTo));
            }

            // pick a dc
            int dc = random.uniform(0, currentRf.length);

            // try to pick an action (and simply loop again if we cannot for this dc)
            TopologyChange next;
            if (registered.size(dc) > 0 && joined.size(dc) > currentRf[dc]) next = options.allChoices.choose(random);
            else if (registered.size(dc) > 0 && ops.contains(JOIN)) next = options.choicesNoLeave.choose(random);
            else if (joined.size(dc) > currentRf[dc] && ops.contains(LEAVE)) next = options.choicesNoJoin.choose(random);
            else if (joined.size(dc) > minRf[dc]) next = CHANGE_RF;
            else continue;

            // TODO (feature): introduce some time period between cluster actions
            switch (next)
            {
                case JOIN:
                {
                    Topology before = topology;
                    TokenPlacementModel.ReplicatedRanges placementsBefore = placements(joined, currentRf);
                    int join = registered.removeRandom(random, dc);
                    joined.add(join);
                    TokenPlacementModel.ReplicatedRanges placementsAfter = placements(joined, currentRf);
                    Topology during = recomputeTopology(placementsBefore, placementsAfter);
                    updateTopology(during);
                    Topology after = recomputeTopology(placementsAfter, placementsAfter);
                    Action action = new OnClusterJoin(KeyspaceActions.this, before, during, after, join);
                    return scheduleAndUpdateTopologyOnCompletion(action, after);
                }
                case REPLACE:
                {
                    Topology before = topology;
                    TokenPlacementModel.ReplicatedRanges placementsBefore = placements(joined, currentRf);
                    int join = registered.removeRandom(random, dc);
                    int leave = joined.selectRandom(random, dc);
                    joined.add(join);
                    joined.remove(leave);
                    left.add(leave);
                    nodeLookup.setTokenOf(join, nodeLookup.tokenOf(leave));
                    TokenPlacementModel.ReplicatedRanges placementsAfter = placements(joined, currentRf);
                    Topology during = recomputeTopology(placementsBefore, placementsAfter);
                    updateTopology(during);
                    Topology after = recomputeTopology(placementsAfter, placementsAfter);
                    Action action = new OnClusterReplace(KeyspaceActions.this, before, during, after, leave, join);
                    return scheduleAndUpdateTopologyOnCompletion(action, after);
                    // if replication factor is 2, cannot perform safe replacements
                    // however can have operations that began earlier during RF=2
                    // so need to introduce some concept of barriers/ordering/sync points
                }

                case LEAVE:
                {
                    Topology before = topology;
                    TokenPlacementModel.ReplicatedRanges placementsBefore = placements(joined, currentRf);
                    int leave = joined.removeRandom(random, dc);
                    left.add(leave);
                    TokenPlacementModel.ReplicatedRanges placementsAfter = placements(joined, currentRf);
                    Topology during = recomputeTopology(placementsBefore, placementsAfter);
                    updateTopology(during);
                    Topology after = recomputeTopology(placementsAfter, placementsAfter);
                    Action action = new OnClusterLeave(KeyspaceActions.this, before, during, after, leave);
                    return scheduleAndUpdateTopologyOnCompletion(action, after);
                }
                case CHANGE_RF:
                    if (maxRf[dc] == minRf[dc]) {} // cannot perform RF changes at all
                    if (currentRf[dc] == minRf[dc] && joined.size(dc) == currentRf[dc]) {} // can do nothing until joined grows
                    else
                    {
                        boolean increase;
                        if (currentRf[dc] == minRf[dc]) // can only grow
                        { ++currentRf[dc]; increase = true;}
                        else if (currentRf[dc] == joined.size(dc) || currentRf[dc] == maxRf[dc]) // can only shrink, and we know currentRf > minRf
                        { --currentRf[dc]; increase = false; }
                        else if (random.decide(0.5f)) // can do either
                        { --currentRf[dc]; increase = false; }
                        else
                        { ++currentRf[dc]; increase = true; }

                        // this isn't used on 4.0+ nodes, but no harm in supplying it anyway
                        long timestamp = time.nextGlobalMonotonicMicros();
                        int coordinator = joined.selectRandom(random, dc);
                        Topology before = topology;
                        Topology after = recomputeTopology();
                        return scheduleAndUpdateTopologyOnCompletion(new OnClusterChangeRf(KeyspaceActions.this, timestamp, coordinator, before, after, increase), after);
                    }
            }
        }

        if (options.changePaxosVariantTo != null && !haveChangedVariant)
        {
            haveChangedVariant = true;
            return schedule(new OnClusterSetPaxosVariant(KeyspaceActions.this, options.changePaxosVariantTo));
        }

        return null;
    }

    private Action schedule(Action action)
    {
        action.setDeadline(time, time.nanoTime() + options.topologyChangeInterval.get(random));
        return action;
    }

    private Action scheduleAndUpdateTopologyOnCompletion(Action action, Topology newTopology)
    {
        action.register(new ActionListener()
        {
            @Override
            public void before(Action action, Before before)
            {
                if (before == Before.EXECUTE)
                    time.forbidDiscontinuities();
            }

            @Override
            public void transitivelyAfter(Action finished)
            {
                updateTopology(newTopology);
                time.permitDiscontinuities();
            }
        });
        return schedule(action);
    }

    void updateTopology(Topology newTopology)
    {
        topology = newTopology;
        announce(topology);
    }

    private Topology recomputeTopology()
    {
        TokenPlacementModel.ReplicatedRanges ranges = placements(joined, currentRf);
        return recomputeTopology(ranges, ranges);
    }

    private int quorumRf()
    {
        if (serialConsistency == LOCAL_SERIAL)
            return currentRf[0];

        return sum(currentRf);
    }

    private int[] membersOfQuorum()
    {
        if (serialConsistency == LOCAL_SERIAL)
            return joined.toArray(0);

        return joined.toArray();
    }

    private static int sum(int[] vs)
    {
        int sum = 0;
        for (int v : vs)
            sum += v;
        return sum;
    }

    private long tokenOf(int node)
    {
        return Long.parseLong(cluster.get(nodeLookup.tokenOf(node)).config().getString("initial_token"));
    }

    public class SimulationLookup extends TokenPlacementModel.DefaultLookup
    {
        public String dc(int dcIdx)
        {
            return super.dc(nodeLookup.dcOf(dcIdx) + 1);
        }

        public String rack(int rackIdx)
        {
            return super.rack(1);
        }

        public long token(int tokenIdx)
        {
            return Long.parseLong(cluster.get(nodeLookup.tokenOf(tokenIdx)).config().getString("initial_token"));
        }

        public TokenPlacementModel.Lookup forceToken(int tokenIdx, long token)
        {
            SimulationLookup newLookup = new SimulationLookup();
            newLookup.tokenOverrides.putAll(tokenOverrides);
            newLookup.tokenOverrides.put(tokenIdx, token);
            return newLookup;
        }
    }
}

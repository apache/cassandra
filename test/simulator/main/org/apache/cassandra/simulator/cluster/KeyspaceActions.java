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
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.PendingRangeMaps;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.ActionListener;
import org.apache.cassandra.simulator.ActionPlan;
import org.apache.cassandra.simulator.Actions;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.OrderOn.StrictSequential;
import org.apache.cassandra.simulator.systems.SimulatedSystems;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.LOCAL_SERIAL;
import static org.apache.cassandra.simulator.Debug.EventType.CLUSTER;
import static org.apache.cassandra.simulator.cluster.ClusterActions.TopologyChange.CHANGE_RF;
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

    final EnumSet<TopologyChange> ops = EnumSet.noneOf(TopologyChange.class);
    final NodeLookup nodeLookup;
    final int[] minRf, initialRf, maxRf;
    final int[] membersOfQuorumDcs;

    // working state
    final NodesByDc all;
    final NodesByDc prejoin;
    final NodesByDc joined;
    final NodesByDc left;

    final int[] currentRf;
    final TokenMetadata tokenMetadata = new TokenMetadata(snitch.get());
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

        int[] dcSizes = new int[options.initialRf.length];
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
                int join = prejoin.removeRandom(random, dc);
                joined.add(join);
                tokenMetadata.updateNormalToken(tokenOf(join), inet(join));
            }
        }

        updateTopology(recomputeTopology());
        int[] joined = this.joined.toArray();
        int[] prejoin = this.prejoin.toArray();
        return Actions.StrictAction.of("Initialize", () -> {
            return ActionList.of(initializeCluster(joined, prejoin),
                                 schemaChange("Create Keyspace", KeyspaceActions.this, 1, createKeyspaceCql),
                                 schemaChange("Create Table", KeyspaceActions.this, 1, createTableCql));
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

    private Action next()
    {
        if (options.topologyChangeLimit >= 0 && topologyChangeCount++ > options.topologyChangeLimit)
            return null;

        while (!ops.isEmpty() && (!prejoin.isEmpty() || joined.size() > sum(minRf)))
        {
            if (options.changePaxosVariantTo != null && !haveChangedVariant && random.decide(1f / (1 + prejoin.size())))
            {
                haveChangedVariant = true;
                return schedule(new OnClusterSetPaxosVariant(KeyspaceActions.this, options.changePaxosVariantTo));
            }

            // pick a dc
            int dc = random.uniform(0, currentRf.length);

            // try to pick an action (and simply loop again if we cannot for this dc)
            TopologyChange next;
            if (prejoin.size(dc) > 0 && joined.size(dc) > currentRf[dc]) next = options.allChoices.choose(random);
            else if (prejoin.size(dc) > 0 && ops.contains(JOIN)) next = options.choicesNoLeave.choose(random);
            else if (joined.size(dc) > currentRf[dc] && ops.contains(LEAVE)) next = options.choicesNoJoin.choose(random);
            else if (joined.size(dc) > minRf[dc]) next = CHANGE_RF;
            else continue;

            // TODO (feature): introduce some time period between cluster actions
            switch (next)
            {
                case REPLACE:
                {
                    Topology before = topology;
                    int join = prejoin.removeRandom(random, dc);
                    int leave = joined.selectRandom(random, dc);
                    joined.add(join);
                    joined.remove(leave);
                    left.add(leave);
                    nodeLookup.setTokenOf(join, nodeLookup.tokenOf(leave));
                    Collection<Token> token = singleton(tokenOf(leave));
                    tokenMetadata.addReplaceTokens(token, inet(join), inet(leave));
                    tokenMetadata.unsafeCalculatePendingRanges(strategy(), keyspace);
                    Topology during = recomputeTopology();
                    updateTopology(during);
                    tokenMetadata.updateNormalTokens(token, inet(join));
                    tokenMetadata.unsafeCalculatePendingRanges(strategy(), keyspace);
                    Topology after = recomputeTopology();
                    Action action = new OnClusterReplace(KeyspaceActions.this, before, during, after, leave, join);
                    return scheduleAndUpdateTopologyOnCompletion(action, after);
                    // if replication factor is 2, cannot perform safe replacements
                    // however can have operations that began earlier during RF=2
                    // so need to introduce some concept of barriers/ordering/sync points
                }
                case JOIN:
                {
                    Topology before = topology;
                    int join = prejoin.removeRandom(random, dc);
                    joined.add(join);
                    Collection<Token> token = singleton(tokenOf(join));
                    tokenMetadata.addBootstrapTokens(token, inet(join));
                    tokenMetadata.unsafeCalculatePendingRanges(strategy(), keyspace);
                    Topology during = recomputeTopology();
                    updateTopology(during);
                    tokenMetadata.updateNormalTokens(token, inet(join));
                    tokenMetadata.unsafeCalculatePendingRanges(strategy(), keyspace);
                    Topology after = recomputeTopology();
                    Action action = new OnClusterJoin(KeyspaceActions.this, before, during, after, join);
                    return scheduleAndUpdateTopologyOnCompletion(action, after);
                }
                case LEAVE:
                {
                    Topology before = topology;
                    int leave = joined.removeRandom(random, dc);
                    left.add(leave);
                    tokenMetadata.addLeavingEndpoint(inet(leave));
                    tokenMetadata.unsafeCalculatePendingRanges(strategy(), keyspace);
                    Topology during = recomputeTopology();
                    updateTopology(during);
                    tokenMetadata.removeEndpoint(inet(leave));
                    tokenMetadata.unsafeCalculatePendingRanges(strategy(), keyspace);
                    Topology after = recomputeTopology();
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
        AbstractReplicationStrategy strategy = strategy();
        Map<InetSocketAddress, Integer> lookup = Cluster.getUniqueAddressLookup(cluster, i -> i.config().num());
        int[][] replicasForKey = new int[primaryKeys.length][];
        int[][] pendingReplicasForKey = new int[primaryKeys.length][];
        for (int i = 0 ; i < primaryKeys.length ; ++i)
        {
            int primaryKey = primaryKeys[i];
            Token token = new Murmur3Partitioner().getToken(Int32Type.instance.decompose(primaryKey));
            replicasForKey[i] = strategy.calculateNaturalReplicas(token, tokenMetadata)
                                        .endpointList().stream().mapToInt(lookup::get).toArray();
            PendingRangeMaps pendingRanges = tokenMetadata.getPendingRanges(keyspace);
            EndpointsForToken pendingEndpoints = pendingRanges == null ? null : pendingRanges.pendingEndpointsFor(token);
            if (pendingEndpoints == null) pendingReplicasForKey[i] = new int[0];
            else pendingReplicasForKey[i] = pendingEndpoints.endpointList().stream().mapToInt(lookup::get).toArray();
        }
        int[] membersOfRing = joined.toArray();
        long[] membersOfRingTokens = IntStream.of(membersOfRing).mapToLong(nodeLookup::tokenOf).toArray();
        return new Topology(primaryKeys, membersOfRing, membersOfRingTokens, membersOfQuorum(), currentRf.clone(),
                            quorumRf(), replicasForKey, pendingReplicasForKey);
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

    private InetAddressAndPort inet(int node)
    {
        return InetAddressAndPort.getByAddress(cluster.get(node).config().broadcastAddress());
    }

    AbstractReplicationStrategy strategy()
    {
        Map<String, String> rf = new HashMap<>();
        for (int i = 0 ; i < snitch.dcCount() ; ++i)
            rf.put(snitch.nameOfDc(i), Integer.toString(currentRf[i]));
        return new NetworkTopologyStrategy(keyspace, tokenMetadata, snitch.get(), rf);
    }

    private Token tokenOf(int node)
    {
        return new LongToken(Long.parseLong(cluster.get(nodeLookup.tokenOf(node)).config().getString("initial_token")));
    }

}

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config.PaxosVariant;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.Actions.ReliableAction;
import org.apache.cassandra.simulator.Actions.StrictAction;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.RandomSource.Choices;
import org.apache.cassandra.simulator.systems.InterceptedExecution;
import org.apache.cassandra.simulator.systems.InterceptingExecutor;
import org.apache.cassandra.simulator.systems.NonInterceptible;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.simulator.utils.KindOfSequence;

import static org.apache.cassandra.distributed.impl.UnsafeGossipHelper.addToRingNormalRunner;
import static org.apache.cassandra.simulator.Action.Modifiers.NO_TIMEOUTS;
import static org.apache.cassandra.simulator.Debug.EventType.CLUSTER;
import static org.apache.cassandra.simulator.cluster.ClusterActions.TopologyChange.JOIN;
import static org.apache.cassandra.simulator.cluster.ClusterActions.TopologyChange.LEAVE;
import static org.apache.cassandra.simulator.cluster.ClusterActions.TopologyChange.REPLACE;
import static org.apache.cassandra.simulator.systems.NonInterceptible.Permit.REQUIRED;
import static org.apache.cassandra.simulator.utils.KindOfSequence.UNIFORM;


// TODO (feature): add Gossip failures (up to some acceptable number)
// TODO (feature): add node down/up (need to coordinate bootstrap/repair execution around this)
// TODO (feature): add node stop/start (need to coordinate normal operation execution around this)
// TODO (feature): permit multiple topology actions in parallel, e.g. REPLACE and CHANGE_RF
// TODO (feature): support nodes rejoining cluster so we can leave running indefinitely
@SuppressWarnings("unused")
public class ClusterActions extends SimulatedSystems
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterActions.class);

    public enum TopologyChange
    {
        JOIN, LEAVE, REPLACE, CHANGE_RF
    }

    public static class Options
    {
        public final int topologyChangeLimit;
        public final KindOfSequence.Period topologyChangeInterval;
        public final Choices<TopologyChange> allChoices;
        public final Choices<TopologyChange> choicesNoLeave;
        public final Choices<TopologyChange> choicesNoJoin;

        public final int[] minRf, initialRf, maxRf;
        public final PaxosVariant changePaxosVariantTo;

        public Options(Options copy)
        {
            this(copy, copy.changePaxosVariantTo);
        }

        public Options(Options copy, PaxosVariant changePaxosVariantTo)
        {
            this.topologyChangeLimit = copy.topologyChangeLimit;
            this.topologyChangeInterval = copy.topologyChangeInterval;
            this.allChoices = copy.allChoices;
            this.choicesNoLeave = copy.choicesNoLeave;
            this.choicesNoJoin = copy.choicesNoJoin;
            this.minRf = copy.minRf;
            this.initialRf = copy.initialRf;
            this.maxRf = copy.maxRf;
            this.changePaxosVariantTo = changePaxosVariantTo;
        }

        public Options(int topologyChangeLimit, KindOfSequence.Period topologyChangeInterval, Choices<TopologyChange> choices, int[] minRf, int[] initialRf, int[] maxRf, PaxosVariant changePaxosVariantTo)
        {
            if (Arrays.equals(minRf, maxRf))
                choices = choices.without(TopologyChange.CHANGE_RF);

            this.topologyChangeInterval = topologyChangeInterval;
            this.topologyChangeLimit = topologyChangeLimit;
            this.minRf = minRf;
            this.initialRf = initialRf;
            this.maxRf = maxRf;
            this.allChoices = choices;
            this.choicesNoJoin = allChoices.without(JOIN).without(REPLACE);
            this.choicesNoLeave = allChoices.without(LEAVE);
            this.changePaxosVariantTo = changePaxosVariantTo;
        }

        public static Options noActions(int clusterSize)
        {
            int[] rf = new int[]{clusterSize};
            return new Options(0, UNIFORM.period(null, null), Choices.uniform(), rf, rf, rf, null);
        }

        public Options changePaxosVariantTo(PaxosVariant newVariant)
        {
            return new Options(this, newVariant);
        }
    }

    final Cluster cluster;
    final Options options;
    final ClusterActionListener listener;
    final Debug debug;

    public ClusterActions(SimulatedSystems simulated,
                          Cluster cluster,
                          Options options,
                          ClusterActionListener listener,
                          Debug debug)
    {
        super(simulated);
        Preconditions.checkNotNull(cluster);
        Preconditions.checkNotNull(options);
        Preconditions.checkNotNull(listener);
        Preconditions.checkNotNull(debug);
        this.cluster = cluster;
        this.options = options;
        this.listener = listener;
        this.debug = debug;
    }

    public static class InitialConfiguration
    {
        public static final int[] EMPTY = {};
        private final int[] joined;
        private final int[] prejoin;

        public InitialConfiguration(int[] joined, int[] prejoin)
        {
            this.joined = joined;
            this.prejoin = prejoin;
        }

        public static InitialConfiguration initializeAll(int nodes)
        {
            int[] joined = new int[nodes];
            for (int i = 0; i < nodes; i++)
                joined[i] = i + 1;
            return new InitialConfiguration(joined, EMPTY);
        }
    }

    public Action initializeCluster(InitialConfiguration config)
    {
        return this.initializeCluster(config.joined, config.prejoin);
    }

    public Action initializeCluster(int[] joined, int[] prejoin)
    {
        return StrictAction.of("Initialise Cluster", () -> {
            List<Action> actions = new ArrayList<>();

            cluster.stream().forEach(i -> actions.add(invoke("Startup " + i.broadcastAddress(), NO_TIMEOUTS, NO_TIMEOUTS,
                                                             new InterceptedExecution.InterceptedRunnableExecution((InterceptingExecutor) i.executor(), i::startup))));

            List<InetSocketAddress> endpoints = cluster.stream().map(IInstance::broadcastAddress).collect(Collectors.toList());
            cluster.forEach(i -> actions.add(resetGossipState(i, endpoints)));

            for (int add : joined)
            {
                actions.add(transitivelyReliable("Add " + add + " to ring", cluster.get(add), addToRingNormalRunner(cluster.get(add))));
                actions.addAll(sendLocalGossipStateToAll(add));
            }

            actions.add(ReliableAction.transitively("Sync Pending Ranges Executor", ClusterActions.this::syncPendingRanges));
            debug.debug(CLUSTER, time, cluster, null, null);
            return ActionList.of(actions);
        });
    }

    Action resetGossipState(IInvokableInstance i, List<InetSocketAddress> endpoints)
    {
        return transitivelyReliable("Reset Gossip", i, () -> Gossiper.runInGossipStageBlocking(Gossiper.instance::unsafeSetEnabled));
    }

    @SuppressWarnings("unchecked")
    void validateReplicasForKeys(IInvokableInstance on, String keyspace, String table, Topology topology)
    {
        int[] primaryKeys = topology.primaryKeys;
        int[][] validate = NonInterceptible.apply(REQUIRED, () -> {
            Map<InetSocketAddress, Integer> lookup = Cluster.getUniqueAddressLookup(cluster, i -> i.config().num());
            int[][] result = new int[primaryKeys.length][];
            for (int i = 0 ; i < primaryKeys.length ; ++i)
            {
                int primaryKey = primaryKeys[i];
                result[i] = on.unsafeApplyOnThisThread(ClusterActions::replicasForPrimaryKey, keyspace, table, primaryKey)
                              .stream()
                              .mapToInt(lookup::get)
                              .filter(r -> Arrays.binarySearch(topology.membersOfQuorum, r) >= 0)
                              .toArray();
            }
            return result;
        });
        for (int i = 0 ; i < primaryKeys.length ; ++i)
        {
            int[] vs1 = validate[i];
            int[] vs2 = topology.replicasForKeys[i].clone();
            Arrays.sort(vs1);
            Arrays.sort(vs2);
            if (!Arrays.equals(vs1, vs2))
                throw new AssertionError();
        }
    }

    // assumes every node knows the correct topology
    static List<InetSocketAddress> replicasForPrimaryKey(String keyspaceName, String table, int primaryKey)
    {
        Keyspace keyspace = Keyspace.open(keyspaceName);
        TableMetadata metadata = keyspace.getColumnFamilyStore(table).metadata.get();
        DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(primaryKey));
        // we return a Set because simulator can easily encounter point where nodes are both natural and pending
        return ReplicaLayout.forTokenWriteLiveAndDown(keyspace, key.getToken()).all().asList(Replica::endpoint);
    }

    private ActionList to(BiFunction<Integer, Integer, Action> action, int from, IntStream to)
    {
        return ActionList.of(to.filter(i -> i != from)
                .mapToObj(i -> action.apply(from, i)));
    }
    private ActionList toAll(BiFunction<Integer, Integer, Action> action, int from)
    {
        return to(action, from, IntStream.rangeClosed(1, cluster.size()));
    }
    private ActionList to(BiFunction<Integer, Integer, Action> action, int from, int[] to)
    {
        return to(action, from, IntStream.of(to));
    }

    ActionList on(Function<Integer, Action> action, IntStream on)
    {
        return ActionList.of(on.mapToObj(action::apply));
    }
    ActionList onAll(Function<Integer, Action> action)
    {
        return on(action, IntStream.rangeClosed(1, cluster.size()));
    }
    ActionList on(Function<Integer, Action> action, int[] on)
    {
        return on(action, IntStream.of(on));
    }

    ActionList syncPendingRanges() { return onAll(OnInstanceSyncPendingRanges.factory(this)); }
    ActionList gossipWithAll(int from) { return toAll(OnInstanceGossipWith.factory(this), from); }
    ActionList sendShutdownToAll(int from) { return toAll(OnInstanceSendShutdown.factory(this), from); }
    ActionList sendLocalGossipStateToAll(int from) { return toAll(OnInstanceSendLocalGossipState.factory(this), from); }
    ActionList flushAndCleanup(int[] on) { return on(OnInstanceFlushAndCleanup.factory(this), on); }
}

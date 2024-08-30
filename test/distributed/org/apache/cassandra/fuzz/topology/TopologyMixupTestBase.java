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

package org.apache.cassandra.fuzz.topology;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Invariants;
import accord.utils.Property;
import accord.utils.Property.Command;
import accord.utils.Property.SimpleCommand;
import accord.utils.RandomSource;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.IntHashSet;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.INodeProvisionStrategy;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.utils.ConfigGenBuilder;

import static accord.utils.Property.commands;
import static accord.utils.Property.ignoreCommand;
import static accord.utils.Property.multistep;
import static accord.utils.Property.stateful;

/**
 * These tests can create many instances, so mac users may need to run the following to avoid address bind failures
 * <p>
 * {@code for id in $(seq 0 15); do sudo ifconfig lo0 alias "127.0.0.$id"; done;}
 */
public abstract class TopologyMixupTestBase<S extends TopologyMixupTestBase.SchemaSpec> extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(TopologyMixupTestBase.class);

    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    private enum TopologyChange
    {
        AddNode,
        RemoveNode,
        HostReplace,
        //TODO (coverage): add the following states once supported
//        StopNode,
//        StartNode,
//        MoveToken
        //TODO (coverage): node migrate to another rack or dc (unsupported on trunk as of this writing, but planned work for TCM)
//        MoveNodeToNewRack,
//        MoveNodeToNewDC,
    }

    private enum RemoveType
    {Decommission, RemoveNode, Assassinate}

    private static final Gen.IntGen MURMUR_TOKEN_GEN = rs -> rs.nextInt(Integer.MIN_VALUE + 1, Integer.MAX_VALUE);
    private static final int TARGET_RF = 3;
    private static final Gen<Gen<RemoveType>> REMOVE_TYPE_DISTRIBUTION = Gens.enums().allMixedDistribution(RemoveType.class);
    private static final Gen<Map<String, Object>> CONF_GEN = new ConfigGenBuilder()
                                                             // jvm-dtest hard codes this partitioner in its APIs, so overriding will break the test
                                                             .withPartitionerGen(null)
                                                             .build();

    // common commands
    private Command<State<S>, Void, ?> repairCommand(int toCoordinate)
    {
        return new SimpleCommand<>(state -> "nodetool repair " + state.schemaSpec.keyspaceName() + ' ' + state.schemaSpec.name() + " from node" + toCoordinate + state.commandNamePostfix(),
                                   state -> state.cluster.get(toCoordinate).nodetoolResult("repair", state.schemaSpec.keyspaceName(), state.schemaSpec.name()).asserts().success());
    }

    private Command<State<S>, Void, ?> waitForCMSToQuiesce()
    {
        return new SimpleCommand<>(state -> "Waiting for CMS to Quiesce" + state.commandNamePostfix(),
                                   state -> ClusterUtils.waitForCMSToQuiesce(state.cluster, state.cmsGroup));
    }

    private Command<State<S>, Void, ?> stopInstance(int toRemove)
    {
        return new SimpleCommand<>(state -> "Stop Node" + toRemove + " for Assassinate" + state.commandNamePostfix(),
                                   state -> {
                                       IInvokableInstance inst = state.cluster.get(toRemove);
                                       TopologyHistory.Node node = state.topologyHistory.node(toRemove);
                                       ClusterUtils.stopUnchecked(inst);
                                       node.down();
                                   });
    }

    private Command<State<S>, Void, ?> addNode()
    {
        return new SimpleCommand<>(state -> "Add Node" + (state.topologyHistory.uniqueInstances + 1) + state.commandNamePostfix(),
                                   state -> {
                                       TopologyHistory.Node n = state.topologyHistory.addNode();
                                       IInvokableInstance newInstance = ClusterUtils.addInstance(state.cluster, n.dc, n.rack, c -> c.set("auto_bootstrap", true));
                                       newInstance.startup(state.cluster);
                                       n.up();
                                   });
    }

    private Command<State<S>, Void, ?> removeNodeDecommission(RandomSource rs, State<S> state)
    {
        int toRemove = rs.pickInt(state.topologyHistory.up());
        return new SimpleCommand<>("nodetool decommission node" + toRemove + state.commandNamePostfix(), s2 -> {
            IInvokableInstance inst = s2.cluster.get(toRemove);
            TopologyHistory.Node node = s2.topologyHistory.node(toRemove);
            node.status = TopologyHistory.Node.Status.BeingDecommissioned;
            inst.nodetoolResult("decommission").asserts().success();
            ClusterUtils.stopUnchecked(inst);
            node.removed();
        });
    }

    private Command<State<S>, Void, ?> removeNode(RandomSource rs, State<S> state)
    {
        int[] up = state.topologyHistory.up();
        int toRemove = rs.pickInt(up);
        int toCoordinate;
        {
            int picked;
            do
            {
                picked = rs.pickInt(up);
            }
            while (picked == toRemove);
            toCoordinate = picked;
        }
        return multistep(stopInstance(toRemove),
                         new SimpleCommand<>("nodetool removenode node" + toRemove + " from node" + toCoordinate + state.commandNamePostfix(), s2 -> {
                             TopologyHistory.Node node = s2.topologyHistory.node(toRemove);
                             node.status = TopologyHistory.Node.Status.BeingRemoved;
                             IInvokableInstance coordinator = s2.cluster.get(toCoordinate);
                             coordinator.nodetoolResult("removenode", Integer.toString(toRemove), "--force").asserts().success();
                             node.removed();
                             s2.currentEpoch.set(HackSerialization.tcmEpoch(coordinator));
                         }),
                         repairCommand(toCoordinate));
    }

    private Command<State<S>, Void, ?> removeNodeAssassinate(RandomSource rs, State<S> state)
    {
        //TODO (correctness): assassinate CMS member isn't allowed
        IntHashSet up = asSet(state.topologyHistory.up());
        IntHashSet cmsGroup = asSet(state.cmsGroup);
        Sets.SetView<Integer> upAndNotInCMS = Sets.difference(up, cmsGroup);
        if (upAndNotInCMS.isEmpty()) throw new AssertionError("Every node is a CMS member");
        List<Integer> allowed = new ArrayList<>(upAndNotInCMS);
        allowed.sort(Comparator.naturalOrder());
        int toRemove = rs.pick(allowed);
        int toCoordinate;
        {
            int[] upInt = state.topologyHistory.up();
            int picked;
            do
            {
                picked = rs.pickInt(upInt);
            }
            while (picked == toRemove);
            toCoordinate = picked;
        }
        return multistep(stopInstance(toRemove),
                         new SimpleCommand<>("nodetool assassinate node" + toRemove + " from node" + toCoordinate + state.commandNamePostfix(), s2 -> {
                             TopologyHistory.Node node = s2.topologyHistory.node(toRemove);
                             node.status = TopologyHistory.Node.Status.BeingAssassinated;
                             IInvokableInstance coordinator = s2.cluster.get(toCoordinate);
                             InetSocketAddress address = s2.cluster.get(toRemove).config().broadcastAddress();
                             coordinator.nodetoolResult("assassinate", address.getAddress().getHostAddress() + ":" + address.getPort()).asserts().success();
                             node.removed();
                             s2.currentEpoch.set(HackSerialization.tcmEpoch(coordinator));
                         }),
                         repairCommand(toCoordinate)
        );
    }

    private Command<State<S>, Void, ?> removeNodeRandomizedDispatch(RandomSource rs, State<S> state)
    {
        RemoveType type = state.removeTypeGen.next(rs);
        switch (type)
        {
            case Decommission:
                return removeNodeDecommission(rs, state);
            case RemoveNode:
                return removeNode(rs, state);
            case Assassinate:
                return removeNodeAssassinate(rs, state);
            default:
                throw new UnsupportedOperationException("Unknown remove type: " + type);
        }
    }

    private Command<State<S>, Void, ?> hostReplace(RandomSource rs, State<S> state)
    {
        int nodeToReplace = rs.pickInt(state.topologyHistory.up());
        IInvokableInstance toReplace = state.cluster.get(nodeToReplace);
        TopologyHistory.Node adding = state.topologyHistory.replace(nodeToReplace);
        TopologyHistory.Node removing = state.topologyHistory.nodes.get(nodeToReplace);

        return multistep(new SimpleCommand<>("Stop Node" + nodeToReplace + " for HostReplace; Node" + adding.id + state.commandNamePostfix(), s2 -> {
                             ClusterUtils.stopUnchecked(toReplace);
                             removing.down();
                         }),
                         new SimpleCommand<>("Host Replace Node" + nodeToReplace + "; Node" + adding.id + state.commandNamePostfix(), s2 -> {
                             logger.info("node{} starting host replacement; epoch={}", adding.id, HackSerialization.tcmEpochAndSync(s2.cluster.getFirstRunningInstance()));
                             removing.status = TopologyHistory.Node.Status.BeingReplaced;
                             IInvokableInstance inst = ClusterUtils.replaceHostAndStart(s2.cluster, toReplace);
                             s2.topologyHistory.replaced(removing, adding);
                             long epoch = HackSerialization.tcmEpoch(inst);
                             s2.currentEpoch.set(epoch);
                             logger.info("{} completed host replacement in epoch={}", inst, epoch);
                         }),
                         //TODO (remove after rebase to trunk): https://issues.apache.org/jira/browse/CASSANDRA-19705  After the rebase to trunk this is not needed.  The issue is that the CMS placement removes the node, it does not promote another node, this cases rf=3 to become rf=2
                         new SimpleCommand<>("CMS reconfigure on Node" + adding.id + state.commandNamePostfix(), s2 -> s2.cluster.get(adding.id).nodetoolResult("cms", "reconfigure", Integer.toString(TARGET_RF)).asserts().success())
        );
    }

    protected abstract Gen<State<S>> stateGen();

    protected void preCheck(Property.StatefulBuilder statefulBuilder)
    {

    }

    protected void destroyState(State<S> state, @Nullable Throwable cause) throws Throwable
    {

    }

    @Test
    public void test()
    {
        Property.StatefulBuilder statefulBuilder = stateful().withSteps(20).withStepTimeout(Duration.ofMinutes(2)).withExamples(1);
        preCheck(statefulBuilder);
        statefulBuilder.check(commands(this::stateGen)
                              .preCommands(state -> state.preActions.forEach(Runnable::run))
                              .add(2, (rs, state) -> {
                                  EnumSet<TopologyChange> possibleTopologyChanges = possibleTopologyChanges(state);
                                  if (possibleTopologyChanges.isEmpty()) return ignoreCommand();
                                  return topologyCommand(state, possibleTopologyChanges).next(rs);
                              })
                              .add(1, (rs, state) -> repairCommand(rs.pickInt(state.topologyHistory.up())))
                              .add(7, (rs, state) -> state.statementGen.apply(rs, state))
                              .destroyState((state, cause) -> {
                                  try (state)
                                  {
                                      TopologyMixupTestBase.this.destroyState(state, cause);
                                  }
                              })
                              .build());
    }

    private EnumSet<TopologyChange> possibleTopologyChanges(State<S> state)
    {
        EnumSet<TopologyChange> possibleTopologyChanges = EnumSet.noneOf(TopologyChange.class);
        // up or down is logically more correct, but since this runs sequentially and after the topology changes are complete, we don't have downed nodes at this point
        // so up is enough to know the topology size
        int size = state.topologyHistory.up().length;
        if (size < state.topologyHistory.maxNodes)
            possibleTopologyChanges.add(TopologyChange.AddNode);
        if (size > state.topologyHistory.quorum())
        {
            if (size > TARGET_RF)
                possibleTopologyChanges.add(TopologyChange.RemoveNode);
            possibleTopologyChanges.add(TopologyChange.HostReplace);
        }
        return possibleTopologyChanges;
    }

    private Gen<Command<State<S>, Void, ?>> topologyCommand(State<S> state, EnumSet<TopologyChange> possibleTopologyChanges)
    {
        Map<Gen<Command<State<S>, Void, ?>>, Integer> possible = new LinkedHashMap<>();
        for (TopologyChange task : possibleTopologyChanges)
        {
            switch (task)
            {
                case AddNode:
                    possible.put(ignore -> multistep(addNode(), waitForCMSToQuiesce()), 1);
                    break;
                case RemoveNode:
                    possible.put(rs -> multistep(removeNodeRandomizedDispatch(rs, state), waitForCMSToQuiesce()), 1);
                    break;
                case HostReplace:
                    possible.put(rs -> multistep(hostReplace(rs, state), waitForCMSToQuiesce()), 1);
                    break;
                default:
                    throw new UnsupportedOperationException(task.name());
            }
        }
        return Gens.oneOf(possible);
    }

    private static IntHashSet asSet(int[] array)
    {
        IntHashSet set = new IntHashSet(array.length);
        for (int i : array)
            set.add(i);
        return set;
    }

    public interface SchemaSpec
    {
        String name();

        String keyspaceName();
    }

    protected static class State<S extends SchemaSpec> implements AutoCloseable
    {
        final TopologyHistory topologyHistory;
        final Cluster cluster;
        final S schemaSpec;
        final List<Runnable> preActions = new CopyOnWriteArrayList<>();
        final AtomicLong currentEpoch = new AtomicLong();
        final BiFunction<RandomSource, State<S>, Command<State<S>, Void, ?>> statementGen;
        final Gen<RemoveType> removeTypeGen;
        private final Map<String, Object> yamlConfigOverrides;
        int[] cmsGroup = new int[0];

        public State(RandomSource rs, BiFunction<RandomSource, Cluster, S> schemaSpecGen, Function<S, BiFunction<RandomSource, State<S>, Command<State<S>, Void, ?>>> cqlOperationsGen)
        {
            this.topologyHistory = new TopologyHistory(rs.fork(), 2, 4);
            try
            {

                this.yamlConfigOverrides = CONF_GEN.next(rs);
                cluster = Cluster.build(topologyHistory.minNodes)
                                 .withTokenSupplier(topologyHistory)
                                 .withConfig(c -> {
                                     c.with(Feature.values())
                                      .set("write_request_timeout", "10s");
                                     //TODO (maintenance): where to put this?  Anything touching ConfigGenBuilder with jvm-dtest needs this...
                                     ((InstanceConfig) c).remove("commitlog_sync_period_in_ms");
                                     for (Map.Entry<String, Object> e : yamlConfigOverrides.entrySet())
                                         c.set(e.getKey(), e.getValue());
                                     onConfigure(c);
                                 })
                                 //TODO (maintenance): should TopologyHistory also be a INodeProvisionStrategy.Factory so address information is stored in the Node?
                                 //TODO (maintenance): AbstractCluster's Map<Integer, NetworkTopology.DcAndRack> nodeIdTopology makes playing with dc/rack annoying, if this becomes an interface then TopologyHistory could own
                                 .withNodeProvisionStrategy((subnet, portMap) -> new INodeProvisionStrategy.AbstractNodeProvisionStrategy(portMap)
                                 {
                                     {
                                         Invariants.checkArgument(subnet == 0, "Unexpected subnet detected: %d", subnet);
                                     }

                                     private final String ipPrefix = "127.0." + subnet + '.';

                                     @Override
                                     public int seedNodeNum()
                                     {
                                         int[] up = topologyHistory.up();
                                         if (Arrays.equals(up, new int[]{ 1, 2 }))
                                             return 1;
                                         return rs.pickInt(up);
                                     }

                                     @Override
                                     public String ipAddress(int nodeNum)
                                     {
                                         return ipPrefix + nodeNum;
                                     }
                                 })
                                 .start();
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
            fixDistributedSchemas(cluster);
            init(cluster, TARGET_RF);
            // fix TCM
            {
                NodeToolResult result = cluster.get(1).nodetoolResult("cms", "reconfigure", "2");
                result.asserts().success();
                logger.info("CMS reconfigure: {}", result.getStdout());
            }
            preActions.add(new Runnable()
            {
                // in order to remove this action, an anonymous class is needed so "this" works, lambda "this" is the parent class
                @Override
                public void run()
                {
                    if (topologyHistory.up().length == TARGET_RF)
                    {
                        NodeToolResult result = cluster.get(1).nodetoolResult("cms", "reconfigure", Integer.toString(TARGET_RF));
                        result.asserts().success();
                        logger.info("CMS reconfigure: {}", result.getStdout());
                        preActions.remove(this);
                    }
                }
            });
            preActions.add(() -> {
                int[] up = topologyHistory.up();
                // use the most recent node just in case the cluster isn't in-sync
                IInvokableInstance node = cluster.get(up[up.length - 1]);
                cmsGroup = HackSerialization.cmsGroup(node);
                currentEpoch.set(HackSerialization.tcmEpoch(node));
            });
            preActions.add(() -> cluster.checkAndResetUncaughtExceptions());
            this.schemaSpec = schemaSpecGen.apply(rs, cluster);
            statementGen = cqlOperationsGen.apply(schemaSpec);

            removeTypeGen = REMOVE_TYPE_DISTRIBUTION.next(rs);

            long waitForEpoch = HackSerialization.tcmEpoch(cluster.get(1));
            currentEpoch.set(waitForEpoch);
            onStartupComplete(waitForEpoch);
        }

        protected void onStartupComplete(long tcmEpoch)
        {

        }

        protected void onConfigure(IInstanceConfig config)
        {

        }

        protected String commandNamePostfix()
        {
            return "; epoch=" + currentEpoch.get() + ", cms=" + Arrays.toString(cmsGroup);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("Yaml Config:\n").append(YamlConfigurationLoader.toYaml(this.yamlConfigOverrides));
            sb.append("\nTopology:\n").append(topologyHistory);
            sb.append("\nCMS Voting Group: ").append(Arrays.toString(cmsGroup));
            return sb.toString();
        }

        @Override
        public void close() throws Exception
        {
            cluster.close();
        }
    }

    public static class TopologyHistory implements TokenSupplier
    {
        private final RandomSource rs;
        private final int tokensPerNode;
        private final int minNodes, maxNodes;

        private final Int2ObjectHashMap<Node> nodes = new Int2ObjectHashMap<>();
        private final Set<String> activeTokens = new HashSet<>();
        private int uniqueInstances = 0;
        /**
         * Tracks how many topology change events were performed
         */
        private int generation = 0;

        public TopologyHistory(RandomSource rs, int minNodes, int maxNodes)
        {
            this.rs = rs;
            this.minNodes = minNodes;
            this.maxNodes = maxNodes;
            this.tokensPerNode = Cluster.build(1).getTokenCount();
            for (int i = 0; i < minNodes; i++)
                addNode();
            for (Node n : nodes.values())
                n.status = Node.Status.Up;
        }

        public long generation()
        {
            return generation;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= nodes.size(); i++)
            {
                Node node = nodes.get(i);
                sb.append("\n\tNode").append(i).append(": status=").append(node.status).append(", tokens=").append(node.tokens);
            }
            return sb.toString();
        }

        public int quorum()
        {
            return (TARGET_RF / 2) + 1;
        }

        @Override
        public Collection<String> tokens(int i)
        {
            Node n = nodes.get(i);
            if (n == null)
                throw new IllegalArgumentException("Unknown node" + i);
            return n.tokens;
        }

        public int[] up()
        {
            IntArrayList up = new IntArrayList(nodes.size(), -1);
            for (Map.Entry<Integer, Node> n : nodes.entrySet())
            {
                if (n.getValue().status == Node.Status.Up)
                    up.add(n.getKey());
            }
            int[] ints = up.toIntArray();
            Arrays.sort(ints);
            return ints;
        }

        public int size()
        {
            return nodes.size();
        }

        public Node addNode()
        {
            int id = ++uniqueInstances;
            List<String> instTokens = Gens.lists(MURMUR_TOKEN_GEN
                                                 .filterAsInt(t -> !activeTokens.contains(Integer.toString(t))))
                                          .unique()
                                          .ofSize(tokensPerNode)
                                          .next(rs).stream()
                                          .map(Object::toString)
                                          .collect(Collectors.toList());
            activeTokens.addAll(instTokens);
            Node node = new Node(this, id, instTokens, "datacenter0", "rack0");
            node.status = Node.Status.Down;
            nodes.put(id, node);
            return node;
        }

        public Node replace(int toReplace)
        {
            int id = ++uniqueInstances;
            Node replacing = Objects.requireNonNull(nodes.get(toReplace));
            Node node = new Node(this, id, replacing.tokens, replacing.dc, replacing.rack);
            node.replacing = node.id;
            nodes.put(id, node);
            return node;
        }

        public void replaced(Node removing, Node adding)
        {
            adding.status = TopologyHistory.Node.Status.Up;
            removing.status = TopologyHistory.Node.Status.Removed;
            adding.replacing = null;
            generation++;
        }

        public Node node(int id)
        {
            if (!nodes.containsKey(id)) throw new NoSuchElementException("Unknown node" + id);
            return nodes.get(id);
        }

        private static class Node
        {
            enum Status
            {Up, Down, BeingReplaced, BeingDecommissioned, BeingRemoved, BeingAssassinated, Removed}

            final TopologyHistory parent;
            final int id;
            final List<String> tokens;
            final String dc, rack;
            Status status = Status.Down;
            Integer replacing = null;

            private Node(TopologyHistory parent, int id, List<String> tokens, String dc, String rack)
            {
                this.parent = parent;
                this.id = id;
                this.tokens = tokens;
                this.dc = dc;
                this.rack = rack;
            }

            public void up()
            {
                status = TopologyHistory.Node.Status.Up;
                parent.generation++;
            }

            public void down()
            {
                status = TopologyHistory.Node.Status.Down;
                parent.generation++;
            }

            public void removed()
            {
                status = Status.Removed;
                parent.activeTokens.removeAll(tokens);
                parent.generation++;
            }

            @Override
            public String toString()
            {
                return "Node{" +
                       "status=" + status +
                       (replacing == null ? "" : (", replacing=" + replacing)) +
                       ", tokens=" + tokens +
                       '}';
            }
        }
    }

    public static class HackSerialization
    {
        private static long tcmEpoch(IInvokableInstance inst)
        {
            return inst.callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());
        }

        private static long tcmEpochAndSync(IInvokableInstance inst)
        {
            return inst.callOnInstance(() -> ClusterMetadataService.instance().log().waitForHighestConsecutive().epoch.getEpoch());
        }

        public static int[] cmsGroup(IInvokableInstance inst)
        {
            return inst.callOnInstance(() -> {
                ClusterMetadata current = ClusterMetadata.current();
                Set<InetAddressAndPort> members = current.placements.get(ReplicationParams.meta(current)).writes.byEndpoint().keySet();
                // Why not just use 'current.fullCMSMembers()'?  That uses the "read" replicas, so "could" have less endpoints
                // It would be more consistent to use fullCMSMembers but thought process is knowing the full set is better
                // than the coordination set.
                int[] array = members.stream().mapToInt(HackSerialization::addressToNodeId).toArray();
                Arrays.sort(array);
                return array;
            });
        }

        private static int addressToNodeId(InetAddressAndPort addressAndPort)
        {
            String address = addressAndPort.getAddress().getHostAddress();
            String[] parts = address.split("\\.");
            Invariants.checkState(parts.length == 4, "Unable to parse address %s", address);
            return Integer.parseInt(parts[3]);
        }
    }
}

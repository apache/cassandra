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

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.IntHashSet;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.harry.sut.TokenPlacementModel.Range;
import org.apache.cassandra.harry.sut.TokenPlacementModel.Replica;
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
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.sut.injvm.InJVMTokenAwareVisitExecutor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ConfigGenBuilder;
import org.apache.cassandra.utils.Retry;

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
        StopNode,
        StartNode,
        //TODO (coverage): add the following states once supported
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
                                   state -> state.cluster.get(toCoordinate).nodetoolResult("repair", state.schemaSpec.keyspaceName(), state.schemaSpec.name(), "--force").asserts().success());
    }

    private static <S extends TopologyMixupTestBase.SchemaSpec> Command<State<S>, Void, ?> repairCommand(int toCoordinate, String ks, String... tables) {
        return new SimpleCommand<>(state -> "nodetool repair " + ks + (tables.length == 0 ? "" : " " + Arrays.asList(tables)) + " from node" + toCoordinate + state.commandNamePostfix(),
                state -> {
                    if (tables.length == 0) {
                        state.cluster.get(toCoordinate).nodetoolResult("repair", ks, "--force").asserts().success();
                        return;
                    }
                    List<String> args = new ArrayList<>(3 + tables.length);
                    args.add("repair");
                    args.add(ks);
                    args.addAll(Arrays.asList(tables));
                    args.add("--force");
                    state.cluster.get(toCoordinate).nodetoolResult(args.toArray(String[]::new)).asserts().success();
                });
    }

    private Command<State<S>, Void, ?> waitForCMSToQuiesce()
    {
        return new Property.StateOnlyCommand<>()
        {
            private Epoch maxEpoch = null;
            @Override
            public String detailed(State<S> state)
            {
                if (maxEpoch == null)
                    maxEpoch = ClusterUtils.maxEpoch(state.cluster, state.topologyHistory.up());
                return "Waiting for CMS to Quiesce on epoch " + maxEpoch.getEpoch() + state.commandNamePostfix();
            }

            @Override
            public void applyUnit(State<S> state)
            {
                Invariants.nonNull(maxEpoch, "detailed was not called before calling apply");
                ClusterUtils.waitForCMSToQuiesce(state.cluster, maxEpoch, true);
            }
        };
    }

    private Command<State<S>, Void, ?> waitForGossipToSettle()
    {
        return new SimpleCommand<>(state -> "Waiting for Ring to Settle" + state.commandNamePostfix(),
                                   state -> {
                                       int[] up = state.topologyHistory.up();
                                       for (int node : up)
                                       {
                                           IInvokableInstance instance = state.cluster.get(node);
                                           ClusterUtils.awaitRingJoin(state.cluster, up, instance);
                                       }
                                   });
    }

    private Command<State<S>, Void, ?> waitAllNodesInPeers()
    {
        return new SimpleCommand<>(state -> "Waiting for all alive nodes to be in peers" + state.commandNamePostfix(),
                                   state -> {
                                       int[] up = state.topologyHistory.up();
                                       for (int node : up)
                                       {
                                           IInvokableInstance instance = state.cluster.get(node);
                                           ClusterUtils.awaitInPeers(state.cluster, up, instance);
                                       }
                                   });
    }

    private Command<State<S>, Void, ?> stopInstance(RandomSource rs, State<S> state)
    {
        int toStop = rs.pickInt(state.upAndSafe());
        return stopInstance(toStop, "Normal Stop");
    }

    private Command<State<S>, Void, ?> startInstance(RandomSource rs, State<S> state)
    {
        int toStop = rs.pickInt(state.topologyHistory.down());
        return startInstance(toStop);
    }

    private Command<State<S>, Void, ?> startInstance(int toStart)
    {
        return new SimpleCommand<>(state -> "Start Node" + toStart + state.commandNamePostfix(),
                state -> {
                    IInvokableInstance inst = state.cluster.get(toStart);
                    TopologyHistory.Node node = state.topologyHistory.node(toStart);
                    inst.startup();
                    node.up();
                });
    }

    private Command<State<S>, Void, ?> stopInstance(int toRemove, String why)
    {
        return new SimpleCommand<>(state -> "Stop Node" + toRemove + " for " + why + state.commandNamePostfix(),
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
                                       ClusterUtils.assertModeJoined(newInstance);
                                       n.up();
                                   });
    }

    private Command<State<S>, Void, ?> removeNodeDecommission(RandomSource rs, State<S> state)
    {
        int toRemove = rs.pickInt(state.upAndSafe());
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
        int toRemove = rs.pickInt(state.upAndSafe());
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
        return multistep(stopInstance(toRemove, "nodetool removenode"),
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
        int toRemove = rs.pickInt(state.upAndSafe());
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
        return multistep(stopInstance(toRemove, "nodetool assassinate"),
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
        int nodeToReplace = rs.pickInt(state.upAndSafe());
        IInvokableInstance toReplace = state.cluster.get(nodeToReplace);
        TopologyHistory.Node adding = state.topologyHistory.replace(nodeToReplace);
        TopologyHistory.Node removing = state.topologyHistory.nodes.get(nodeToReplace);

        return multistep(stopInstance(nodeToReplace, "HostReplace; Node" + adding.id),
                         new SimpleCommand<>("Host Replace Node" + nodeToReplace + "; Node" + adding.id + state.commandNamePostfix(), s2 -> {
                             logger.info("node{} starting host replacement; epoch={}", adding.id, HackSerialization.tcmEpochAndSync(s2.cluster.getFirstRunningInstance()));
                             removing.status = TopologyHistory.Node.Status.BeingReplaced;
                             IInvokableInstance inst = ClusterUtils.replaceHostAndStart(s2.cluster, toReplace);
                             ClusterUtils.assertModeJoined(inst);
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
        Property.StatefulBuilder statefulBuilder = stateful().withSteps(20).withStepTimeout(Duration.ofMinutes(3)).withExamples(1);
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
                              .commandsTransformer((state, gen) -> {
                                  for (BiFunction<State<S>, Gen<Command<State<S>, Void, ?>>, Gen<Command<State<S>, Void, ?>>> fn : state.commandsTransformers)
                                      gen = fn.apply(state, gen);
                                  return gen;
                              })
                              .onSuccess((state, sut, history) -> logger.info("Successful for the following:\nState {}\nHistory:\n{}", state, Property.formatList("\t\t", history)))
                              .build());
    }

    private EnumSet<TopologyChange> possibleTopologyChanges(State<S> state)
    {
        EnumSet<TopologyChange> possibleTopologyChanges = EnumSet.noneOf(TopologyChange.class);
        // up or down is logically more correct, but since this runs sequentially and after the topology changes are complete, we don't have downed nodes at this point
        // so up is enough to know the topology size
        int up = state.topologyHistory.up().length;
        int down = state.topologyHistory.down().length;
        int[] upAndSafe = state.upAndSafe();
        int total = up + down;
        if (total < state.topologyHistory.maxNodes)
            possibleTopologyChanges.add(TopologyChange.AddNode);
        if (upAndSafe.length > 0)
        {
            // can't remove the node if all nodes are CMS nodes
            if (!Sets.difference(asSet(upAndSafe), asSet(state.cmsGroup)).isEmpty())
                possibleTopologyChanges.add(TopologyChange.RemoveNode);
            possibleTopologyChanges.add(TopologyChange.HostReplace);
            possibleTopologyChanges.add(TopologyChange.StopNode);
        }
        if (down > 0)
            possibleTopologyChanges.add(TopologyChange.StartNode);
        return possibleTopologyChanges;
    }

    private Command<State<S>, Void, ?> awaitClusterStable()
    {
        return multistep(waitForCMSToQuiesce(),
                         waitForGossipToSettle(),
                         waitAllNodesInPeers());
    }

    private Gen<Command<State<S>, Void, ?>> topologyCommand(State<S> state, EnumSet<TopologyChange> possibleTopologyChanges)
    {
        Map<Gen<Command<State<S>, Void, ?>>, Integer> possible = new LinkedHashMap<>();
        for (TopologyChange task : possibleTopologyChanges)
        {
            switch (task)
            {
                case AddNode:
                    possible.put(ignore -> multistep(addNode(), awaitClusterStable()), 1);
                    break;
                case RemoveNode:
                    possible.put(rs -> multistep(removeNodeRandomizedDispatch(rs, state), awaitClusterStable()), 1);
                    break;
                case HostReplace:
                    possible.put(rs -> multistep(hostReplace(rs, state), awaitClusterStable()), 1);
                    break;
                case StartNode:
                    possible.put(rs -> startInstance(rs, state), 1);
                    break;
                case StopNode:
                    possible.put(rs -> stopInstance(rs, state), 1);
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

    protected interface CommandGen<S extends TopologyMixupTestBase.SchemaSpec>
    {
        Command<State<S>, Void, ?> apply(RandomSource rs, State<S> state);
    }

    private static class LoggingCommand<State, SystemUnderTest, Result> extends Property.ForwardingCommand<State, SystemUnderTest, Result>
    {
        private static final Logger logger = LoggerFactory.getLogger(LoggingCommand.class);

        private LoggingCommand(Command<State, SystemUnderTest, Result> delegate)
        {
            super(delegate);
        }

        @Override
        public Result apply(State s) throws Throwable
        {
            String name = detailed(s);
            long startNanos = Clock.Global.nanoTime();
            try
            {
                logger.info("Starting command: {}", name);
                Result o = super.apply(s);
                logger.info("Command {} was success after {}", name, Duration.ofNanos(Clock.Global.nanoTime() - startNanos));
                return o;
            }
            catch (Throwable t)
            {
                logger.warn("Command {} failed after {}: {}", name, Duration.ofNanos(Clock.Global.nanoTime() - startNanos), t.toString()); // don't want stack trace, just type/msg
                throw t;
            }
        }
    }

    protected static class State<S extends SchemaSpec> implements AutoCloseable
    {
        final TopologyHistory topologyHistory;
        final Cluster cluster;
        final S schemaSpec;
        final List<BiFunction<State<S>, Gen<Command<State<S>, Void, ?>>, Gen<Command<State<S>, Void, ?>>>> commandsTransformers = new ArrayList<>();
        final List<Runnable> preActions = new CopyOnWriteArrayList<>();
        final AtomicLong currentEpoch = new AtomicLong();
        final CommandGen<S> statementGen;
        final Gen<RemoveType> removeTypeGen;
        private final Map<String, Object> yamlConfigOverrides;
        int[] cmsGroup = new int[0];
        private TokenPlacementModel.ReplicationFactor rf;
        private final RingModel ring = new RingModel();

        public State(RandomSource rs, BiFunction<RandomSource, Cluster, S> schemaSpecGen, Function<S, CommandGen<S>> cqlOperationsGen)
        {
            this.topologyHistory = new TopologyHistory(rs.fork(), 2, 4);
            rf = new TokenPlacementModel.SimpleReplicationFactor(2);
            try
            {

                this.yamlConfigOverrides = CONF_GEN.next(rs);
                cluster = Cluster.build(topologyHistory.minNodes)
                                 .withTokenSupplier(topologyHistory)
                                 .withConfig(c -> {
                                     c.with(Feature.values())
                                      .set("write_request_timeout", "10s")
                                      .set("read_request_timeout", "10s")
                                      .set("range_request_timeout", "20s")
                                      .set("request_timeout", "20s")
                                      .set("transaction_timeout", "15s")
                                      .set("native_transport_timeout", "30s")
                                      // bound startup to some value larger than the task timeout, this is to allow the
                                      // tests to stop blocking when a startup issue is detected.  The main reason for
                                      // this is that startup blocks forever, waiting for accord and streaming to
                                      // complete... but if there are bugs at these layers then the startup will never
                                      // exit, blocking the JVM from giving the needed information (logs/seed) to debug.
                                      .set(Constants.KEY_DTEST_STARTUP_TIMEOUT, "4m")
                                      .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false);
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
            cluster.setUncaughtExceptionsFilter((node, t) -> {
                // api is "ignore" so false means include,
                var rootCause = Throwables.getRootCause(t);
                if (rootCause.getMessage() != null)
                {
                    if (rootCause.getMessage().startsWith("Queried for epoch") && rootCause.getMessage().contains("but could not catch up. Current epoch:"))
                        return true;
                    if (rootCause.getMessage().startsWith("Operation timed out"))
                    {
                        // is this due to TCM fetching epochs? PaxosBackedProcessor.getLogState is costly and more likely to timeout... so ignore those
                        Optional<StackTraceElement> match = Stream.of(rootCause.getStackTrace())
                                                                  .filter(s -> s.getClassName().equals("org.apache.cassandra.tcm.PaxosBackedProcessor") && s.getMethodName().equals("getLogState"))
                                                                  .findFirst();
                        if (match.isPresent())
                            return true;
                    }
                }
                return false;
            });
            fixDistributedSchemas(cluster);
            init(cluster, TARGET_RF);
            // fix TCM
            {
                NodeToolResult result = cluster.get(1).nodetoolResult("cms", "reconfigure", "2");
                result.asserts().success();
                logger.info("CMS reconfigure: {}", result.getStdout());
            }
            commandsTransformers.add(new BiFunction<State<S>, Gen<Command<State<S>, Void, ?>>, Gen<Command<State<S>, Void, ?>>>() {
                // in order to remove this action, an anonymous class is needed so "this" works, lambda "this" is the parent class
                @Override
                public Gen<Command<State<S>, Void, ?>> apply(State<S> state, Gen<Command<State<S>, Void, ?>> commandGen) {
                    if (topologyHistory.up().length < TARGET_RF)
                        return commandGen;
                    SimpleCommand<State<S>> reconfig = new SimpleCommand<>("nodetool cms reconfigure " + TARGET_RF, ignore -> {
                        NodeToolResult result = cluster.get(1).nodetoolResult("cms", "reconfigure", Integer.toString(TARGET_RF));
                        result.asserts().success();
                        logger.info("CMS reconfigure: {}", result.getStdout());
                    });
                    SimpleCommand<State<S>> fixDistributedSchemas = new SimpleCommand<>("Set system distributed keyspaces to RF=" + TARGET_RF, ignore ->
                            fixDistributedSchemas(cluster));
                    SimpleCommand<State<S>> fixTestKeyspace = new SimpleCommand<>("Set " + KEYSPACE + " keyspace to RF=" + TARGET_RF, s -> {
                        cluster.schemaChange("ALTER KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + TARGET_RF + "}");
                        rf = new TokenPlacementModel.SimpleReplicationFactor(TARGET_RF);
                    });
                    var self = this;
                    return rs -> {
                        Command<State<S>, Void, ?> next = commandGen.next(rs);
                        if (next.checkPreconditions(state) == Property.PreCheckResult.Ignore)
                            return next;
                        commandsTransformers.remove(self);
                        int[] up = state.topologyHistory.up();
                        List<Command<State<S>, Void, ?>> commands = new ArrayList<>();
                        commands.add(fixDistributedSchemas);
                        for (String ks : Arrays.asList("system_auth", "system_traces"))
                        {
                            int coordinator = rs.pickInt(up);
                            commands.add(repairCommand(coordinator, ks));
                        }
                        commands.add(fixTestKeyspace);
                        {
                            int coordinator = rs.pickInt(up);
                            commands.add(repairCommand(coordinator, KEYSPACE));
                        }
                        commands.add(reconfig);
                        commands.add(next);
                        return multistep(commands);
                    };
                }
            });
            commandsTransformers.add((state, commandGen) -> rs2 -> {
                Command<State<S>, Void, ?> c = commandGen.next(rs2);
                if (!(c instanceof Property.MultistepCommand))
                    return new LoggingCommand<>(c);
                Property.MultistepCommand<State<S>, Void> multistep = (Property.MultistepCommand<State<S>, Void>) c;
                List<Command<State<S>, Void, ?>> subcommands = new ArrayList<>();
                for (var sub : multistep)
                    subcommands.add(new LoggingCommand<>(sub));
                return multistep(subcommands);
            });
            preActions.add(() -> {
                int[] up = topologyHistory.up();
                // use the most recent node just in case the cluster isn't in-sync
                IInvokableInstance node = cluster.get(up[up.length - 1]);
                cmsGroup = HackSerialization.cmsGroup(node);
                currentEpoch.set(HackSerialization.tcmEpoch(node));

                ring.rebuild(cluster.coordinator(up[0]), rf, up);
                // ring must know about the up nodes
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
            return "; epoch=" + currentEpoch.get() + ", cms=" + Arrays.toString(cmsGroup) + ", up=" + Arrays.toString(topologyHistory.up()) + ", down=" + Arrays.toString(topologyHistory.down());
        }

        public int[] upAndSafe()
        {
            IntHashSet up = asSet(topologyHistory.up());
            int quorum = topologyHistory.quorum();
            // find what ranges are able to handle 1 node loss
            Set<Range> safeRanges = new HashSet<>();
            ring.rangesToReplicas((range, replicas) -> {
                IntHashSet alive = new IntHashSet();
                for (int peer : replicas)
                {
                    if (up.contains(peer))
                        alive.add(peer);
                }
                if (quorum < alive.size())
                    safeRanges.add(range);
            });

            // filter nodes where 100% of their ranges are "safe"
            IntArrayList safeNodes = new IntArrayList();
            for (int id : up)
            {
                List<Range> ranges = ring.ranges(id);
                if (ranges.stream().allMatch(safeRanges::contains))
                    safeNodes.add(id);
            }

            int[] upAndSafe = safeNodes.toIntArray();
            Arrays.sort(upAndSafe);
            return upAndSafe;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("Yaml Config:\n").append(YamlConfigurationLoader.toYaml(this.yamlConfigOverrides));
            sb.append("\nTopology:\n").append(topologyHistory);
            sb.append("\nCMS Voting Group: ").append(Arrays.toString(cmsGroup));
            if (epochHistory != null)
                sb.append("\n").append(epochHistory);
            return sb.toString();
        }

        private String epochHistory = null;

        @Override
        public void close() throws Exception
        {
            var cmsNodesUp = Sets.intersection(asSet(cmsGroup), asSet(topologyHistory.up()));
            int cmsNode = Iterables.getFirst(cmsNodesUp, null);
            try
            {
                SimpleQueryResult qr = Retry.retryWithBackoffBlocking(5, () -> cluster.get(cmsNode).executeInternalWithResult("SELECT epoch, kind, transformation FROM system_views.cluster_metadata_log"));
                TableBuilder builder = new TableBuilder(" | ");
                builder.add(qr.names());
                while (qr.hasNext())
                {
                    Row next = qr.next();
                    builder.add(Stream.of(next.toObjectArray())
                                      .map(Objects::toString)
                                      .map(s -> s.length() > 100 ? s.substring(0, 100) + "..." : s)
                                      .collect(Collectors.toList()));
                }
                epochHistory = "Epochs:\n" + builder;
            }
            catch (Throwable t)
            {
                logger.warn("Unable to fetch epoch history on node{}", cmsNode, t);
            }
            logger.info("Shutting down clusters");
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
            return nodes(Node.Status.Up);
        }

        public int[] down()
        {
            return nodes(Node.Status.Down);
        }

        private int[] nodes(Node.Status target)
        {
            IntArrayList up = new IntArrayList(nodes.size(), -1);
            for (Map.Entry<Integer, Node> n : nodes.entrySet())
            {
                if (n.getValue().status == target)
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

    private static class RingModel
    {
        TokenPlacementModel.ReplicatedRanges ring = null;
        Int2ObjectHashMap<Replica> idToReplica = null;

        private void rebuild(ICoordinator coordinator, TokenPlacementModel.ReplicationFactor rf, int[] up)
        {
            ring = InJVMTokenAwareVisitExecutor.getRing(coordinator, rf);

            Int2ObjectHashMap<Replica> idToReplica = new Int2ObjectHashMap<>();
            for (Map.Entry<Range, List<Replica>> e : ring.asMap().entrySet())
            {
                for (var replica : e.getValue())
                    idToReplica.put(toNodeId(replica), replica);
            }
            this.idToReplica = idToReplica;

            IntHashSet upSet = asSet(up);
            if (!idToReplica.keySet().containsAll(upSet))
            {
                int coordinatorNode = coordinator.instance().config().num();
                Sets.SetView<Integer> diff = Sets.difference(upSet, idToReplica.keySet());
                throw new AssertionError("Unable to find nodes " + diff + " in the ring on node" + coordinatorNode);
            }
        }

        private static int toNodeId(Replica replica)
        {
            //TODO (fix test api): NodeId is in the API but is always null.  Cheapest way to get the id is to assume the address has it
            // same issue with address...
            // /127.0.0.2
            String harryId = replica.node().id();
            int index = harryId.lastIndexOf('.');
            int peer = Integer.parseInt(harryId.substring(index + 1));
            return peer;
        }

        List<Range> ranges(int node)
        {
            Replica replica = idToReplica.get(node);
            if (replica == null)
                throw new AssertionError("Unknown node" + node);
            List<Range> ranges = ring.ranges(replica);
            if (ranges == null)
                throw new AssertionError("node" + node + " some how does not have ranges...");
            return ranges;
        }

        private void rangesToReplicas(BiConsumer<Range, int[]> fn)
        {
            for (Map.Entry<Range, List<Replica>> e : ring.asMap().entrySet())
            {
                int[] replicas = e.getValue().stream().mapToInt(RingModel::toNodeId).toArray();
                Arrays.sort(replicas);
                fn.accept(e.getKey(), replicas);
            }
        }
    }
}

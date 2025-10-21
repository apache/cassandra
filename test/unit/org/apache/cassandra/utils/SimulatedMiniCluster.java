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

package org.apache.cassandra.utils;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.collect.Iterables;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.SequentialExecutorPlus;
import org.apache.cassandra.concurrent.SimulatedExecutorFactory;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.ICompactionManager;
import org.apache.cassandra.db.repair.CassandraTableRepairManager;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.IGossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Locator;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.SimulatedMessageDelivery;
import org.apache.cassandra.net.SimulatedMessageDelivery.ActionSupplier;
import org.apache.cassandra.repair.IValidationManager;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.repair.StreamExecutor;
import org.apache.cassandra.repair.TableRepairManager;
import org.apache.cassandra.repair.ValidationManager;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.paxos.cleanup.PaxosRepairState;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.mockito.Mockito;

import static org.apache.cassandra.utils.AccordGenerators.fromQT;

public class SimulatedMiniCluster
{
    private final RandomSource rs;
    private final Function<Node, IVerbHandler<?>> verbHandlerFactory;
    private final SimulatedExecutorFactory executorFactory;
    private final SequentialExecutorPlus orderedExecutor;
    private final ScheduledExecutorPlus unorderedScheduled;
    private final IFailureDetector failureDetector = Mockito.mock(IFailureDetector.class);
    private final Locator locator = Mockito.mock(Locator.class);
    private final MBeanWrapper mbean = Mockito.mock(MBeanWrapper.class);
    private final SimulatedGossip gossiper = new SimulatedGossip();
    private final List<Throwable> failures = new ArrayList<>();
    private final IPartitioner partitioner;
    private final Map<String, List<String>> dcsToRacks;
    private final List<String> dcs;
    private final int tokensPerInstance;
    private final Gen<Token> tokenGen;
    private ClusterMetadata current;
    private final Map<NodeId, Node> nodes = new LinkedHashMap<>();
    private final TreeSet<Token> knownTokens = new TreeSet<>(); // includes bootstraping nodes tokens (aka tokens not in the ring)

    private SimulatedMiniCluster(Builder builder)
    {
        this.rs = builder.rs;
        this.verbHandlerFactory = builder.verbHandlerFactory;
        this.executorFactory = new SimulatedExecutorFactory(rs, failures::add);
        this.orderedExecutor = executorFactory.configureSequential("ignore").build();
        this.unorderedScheduled = executorFactory.scheduled("ignored");
        this.partitioner = fromQT(CassandraGenerators.nonLocalPartitioners()).next(rs);
        this.dcsToRacks = createDcRackDetails(rs);
        this.dcs = new ArrayList<>(dcsToRacks.keySet());
        dcs.sort(Comparator.naturalOrder());
        this.tokensPerInstance = rs.nextBoolean() ? 1 : 4;
        this.tokenGen = fromQT(CassandraGenerators.token(partitioner)).filter(t -> !knownTokens.contains(t));
        // setup Directory with known dcs
        this.current = new ClusterMetadata(partitioner);
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting(current));
    }

    public Node node(int id)
    {
        return node(new NodeId(id));
    }

    public Node node(NodeId id)
    {
        Node node = nodes.get(id);
        if (node == null)
            throw new AssertionError("Unable to find node for id " + id);
        return node;
    }

    public Node node(InetAddressAndPort address)
    {
        //TODO (performance): don't walk, keep index?
        for (Node node : nodes.values())
        {
            if (node.broadcastAddressAndPort.equals(address))
                return node;
        }
        throw new AssertionError("Unable to find node for address " + address);
    }

    private Collection<Token> nextUnknownTokens()
    {
        if (tokensPerInstance == 1) return Collections.singleton(tokenGen.next(rs));
        return Gens.lists(tokenGen).unique().ofSize(tokensPerInstance).next(rs);
    }

    public Node createNode()
    {
        if (nodes.isEmpty())
            return createFirstNode();

        NodeId id = new NodeId(nodes.size() + 1);
        UUID hostId = id.toUUID();
        Collection<Token> tokens = nextUnknownTokens();
        String dc = rs.pick(dcs);
        String rack = rs.pick(dcsToRacks.get(dc));
        Node node = new Node(id, hostId, address(id), tokens, dc, rack);
        register(node);
        return node;
    }

    public Node createNodeAndJoin()
    {
        if (nodes.isEmpty())
            return createFirstNode();

        NodeId id = new NodeId(nodes.size() + 1);
        UUID hostId = id.toUUID();
        Collection<Token> tokens = nextUnknownTokens();
        String dc = rs.pick(dcs);
        String rack = rs.pick(dcsToRacks.get(dc));
        Node node = new Node(id, hostId, address(id), tokens, dc, rack);
        registerAndJoin(node);
        return node;
    }

    private Node createFirstNode()
    {
        NodeId id = new NodeId(nodes.size() + 1);
        UUID hostId = id.toUUID();
        Collection<Token> tokens = nextUnknownTokens();
        String dc = dcs.get(0);
        String rack = dcsToRacks.get(dc).get(0);
        Node node = new Node(id, hostId, address(id), tokens, dc, rack);
        registerAndJoin(node);
        return node;
    }

    private void registerAndJoin(Node node)
    {
        register(node);
        prepareJoin(node.id);
        while (!current.inProgressSequences.isEmpty())
            bumpInProgress();
    }

    private void register(Node node)
    {
        nodes.put(node.id, node);
        knownTokens.addAll(node.tokens);
        registerWithSnitch(node);
        registerWithGossip(node);
        registerWithCMS(node);
    }

    private void registerWithCMS(Node node)
    {
        if (node.id.id() == 1)
        {
            // rebuild metadata from scratch
            Directory directory = Directory.EMPTY.with(new NodeAddresses(node.hostId, node.broadcastAddressAndPort, node.broadcastAddressAndPort, node.broadcastAddressAndPort), new Location(node.dc, node.rack));
            notifyMetadataChange(new ClusterMetadata(partitioner, directory));
        }
        else
        {
            notifyMetadataChange(current.transformer().register(new NodeAddresses(node.hostId, node.broadcastAddressAndPort, node.broadcastAddressAndPort, node.broadcastAddressAndPort),
                                                                new Location(node.dc, node.rack),
                                                                NodeVersion.CURRENT)
                                        .build().metadata);
        }
    }

    private void prepareJoin(NodeId id)
    {
        Node node = nodes.get(id);
        if (node == null)
            throw new IllegalArgumentException("Unknown " + id);
        PrepareJoin task = new PrepareJoin(id, new HashSet<>(node.tokens), new UniformRangePlacement(), true, false);
        notifyMetadataChange(process(task).metadata);
    }

    private void bumpInProgress()
    {
        if (current.inProgressSequences.isEmpty())
            throw new IllegalStateException("Attempted to bump epoch when nothing was pending");
        Iterator<MultiStepOperation<?>> it = current.inProgressSequences.iterator();
        Invariants.require(it.hasNext());
        notifyMetadataChange(process(it.next()).metadata);
    }

    protected void notifyMetadataChange(ClusterMetadata current)
    {
        this.current = current;
        ((StubClusterMetadataService) ClusterMetadataService.instance()).setMetadata(current);
    }

    private Transformation.Success process(Transformation transformation)
    {
        Transformation.Result result = transformation.execute(current);
        if (result.isRejected())
            throw new IllegalStateException("Unable to make TCM transition");
        return result.success();
    }

    private Transformation.Success process(MultiStepOperation<?> transformation)
    {
        Transformation.Result result = transformation.applyTo(current);
        if (result.isRejected())
            throw new IllegalStateException("Unable to make TCM transition");
        return result.success();
    }

    private static InetAddressAndPort address(NodeId id)
    {
        try
        {
            return InetAddressAndPort.getByAddress(ByteArrayUtil.bytes(id.id()));
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError("Unable to create address for id " + id, e);
        }
    }

    private static Map<String, List<String>> createDcRackDetails(RandomSource rs)
    {
        int numDCs = rs.nextInt(1, 4);
        Map<String, List<String>> map = new LinkedHashMap<>();
        for (int i = 0; i < numDCs; i++)
        {
            String name = "DC" + (i + 1);
            int numRacks = rs.nextInt(1, 10);
            List<String> racks = Gens.lists(Gens.strings().ascii().ofLength(5).map(s -> "R" + s)).unique().ofSize(numRacks).next(rs);
            racks.sort(Comparator.naturalOrder());
            map.put(name, racks);
        }
        return map;
    }

    public boolean hasWork()
    {
        return executorFactory.hasWork();
    }

    public boolean processAny()
    {
        return executorFactory.processAny();
    }

    public boolean processOne()
    {
        return executorFactory.processOne();
    }

    public void processAll()
    {
        executorFactory.processAll();;
    }

    public void simulateStages(Stage... stages)
    {
        for (Stage stage : stages)
        {
            switch (stage)
            {
                case GOSSIP:
                case ANTI_ENTROPY:
                case MIGRATION:
                case MISC:
                case TRACING:
                case FETCH_METADATA:
                    stage.unsafeSetExecutor(orderedExecutor);
                    break;
                default:
                    stage.unsafeSetExecutor(unorderedScheduled);
            }
        }
    }

    private void registerWithSnitch(Node node)
    {
        Mockito.when(locator.location(Mockito.eq(node.broadcastAddressAndPort))).thenReturn(new Location(node.dc, node.rack));
    }

    private void registerWithGossip(Node node)
    {
        VersionedValue.VersionedValueFactory valueFactory = node.valueFactory;
        EndpointState state = new EndpointState(new HeartBeatState(42, 42));
        state.addApplicationState(ApplicationState.STATUS, valueFactory.normal(node.tokens));
        state.addApplicationState(ApplicationState.STATUS_WITH_PORT, valueFactory.normal(node.tokens));
        state.addApplicationState(ApplicationState.HOST_ID, valueFactory.hostId(node.hostId));
        state.addApplicationState(ApplicationState.TOKENS, valueFactory.tokens(node.tokens));
        state.addApplicationState(ApplicationState.DC, valueFactory.datacenter(node.dc));
        state.addApplicationState(ApplicationState.RACK, valueFactory.rack(node.rack));
        state.addApplicationState(ApplicationState.RELEASE_VERSION, valueFactory.releaseVersion());

        gossiper.endpoints.put(node.broadcastAddressAndPort, state);
    }

    public static class Builder
    {
        private final RandomSource rs;
        private final Function<Node, IVerbHandler<?>> verbHandlerFactory;

        public Builder(RandomSource rs, Function<Node, IVerbHandler<?>> verbHandlerFactory)
        {
            this.rs = rs;
            this.verbHandlerFactory = verbHandlerFactory;
        }

        public SimulatedMiniCluster build()
        {
            return new SimulatedMiniCluster(this);
        }
    }

     private enum NodeStatus { Init, Registered, Joining, Joined, Leaving, Removed}

    public class Node implements SharedContext
    {
        private final ICompactionManager compactionManager = Mockito.mock(ICompactionManager.class);
        private final NodeId id;
        private final UUID hostId;
        private final InetAddressAndPort broadcastAddressAndPort;
        private final Collection<Token> tokens;
        private final String dc, rack;
        private final VersionedValue.VersionedValueFactory valueFactory;
        private final SimulatedMessageDelivery messaging;
        private final SimulatedMessageDelivery.SimulatedMessageReceiver receiver;
        private final ActiveRepairService activeRepairService;
        private final PaxosRepairState paxosRepairState;
        private final IValidationManager validationManager;
        private final StreamExecutor streamExecutor;
        private NodeStatus status = NodeStatus.Init;
        private ActionSupplier messagingActions = (self, msg, to) -> SimulatedMessageDelivery.Action.DELIVER;

        public Node(NodeId id, UUID hostId, InetAddressAndPort broadcastAddressAndPort, Collection<Token> tokens, String dc, String rack)
        {
            this.id = id;
            this.hostId = hostId;
            this.broadcastAddressAndPort = broadcastAddressAndPort;
            this.tokens = tokens;
            this.dc = dc;
            this.rack = rack;

            IPartitioner partitioner = Iterables.getFirst(tokens, null).getPartitioner();
            this.valueFactory = new VersionedValue.VersionedValueFactory(partitioner);
            this.messaging = new SimulatedMessageDelivery(broadcastAddressAndPort,
                                                          messagingActions::get,
                                                          SimulatedMessageDelivery.randomDelay(rs),
                                                          (to, msg) -> unorderedScheduled.submit(() -> node(to).receiver.recieve(msg)),
                                                          (action, to, msg) -> {},
                                                          unorderedScheduled::schedule,
                                                          failures::add);
            this.activeRepairService = new ActiveRepairService(this);
            this.paxosRepairState = new PaxosRepairState(this);
            this.validationManager = (cfs, validator) -> unorderedScheduled.submit(() -> {
                try
                {
                    ValidationManager.doValidation(cfs, validator);
                }
                catch (Throwable e)
                {
                    validator.fail(e);
                }
            });
            this.streamExecutor = plan -> {
                long delayNanos = rs.nextLong(TimeUnit.SECONDS.toNanos(5), TimeUnit.MINUTES.toNanos(10));
                unorderedScheduled.schedule(() -> {
                    StreamState success = new StreamState(plan.planId(), plan.streamOperation(), Collections.emptySet());
                    for (StreamEventHandler handler : plan.handlers())
                        handler.onSuccess(success);
                }, delayNanos, TimeUnit.NANOSECONDS);
                return null;
            };

            // setup last as "this" is leaking, so make sure all final fields are defined first
            this.receiver = messaging.receiver(verbHandlerFactory.apply(this));
        }

        public NodeId id()
        {
            return id;
        }

        public UUID hostId()
        {
            return hostId;
        }

        public void messagingActions(ActionSupplier messagingActions)
        {
            this.messagingActions = Objects.requireNonNull(messagingActions);
        }

        @Override
        public InetAddressAndPort broadcastAddressAndPort()
        {
            return broadcastAddressAndPort;
        }

        @Override
        public Supplier<Random> random()
        {
            return () -> rs.fork().asJdkRandom();
        }

        @Override
        public Clock clock()
        {
            return executorFactory;
        }

        @Override
        public ExecutorFactory executorFactory()
        {
            return executorFactory;
        }

        @Override
        public MBeanWrapper mbean()
        {
            return mbean;
        }

        @Override
        public ScheduledExecutorPlus optionalTasks()
        {
            return unorderedScheduled;
        }

        @Override
        public ScheduledExecutorPlus nonPeriodicTasks()
        {
            return unorderedScheduled;
        }

        @Override
        public ScheduledExecutorPlus scheduledTasks()
        {
            return unorderedScheduled;
        }


        @Override
        public IFailureDetector failureDetector()
        {
            return failureDetector;
        }

        @Override
        public Locator locator()
        {
            return locator;
        }

        @Override
        public IGossiper gossiper()
        {
            return gossiper;
        }

        @Override
        public MessageDelivery messaging()
        {
            return messaging;
        }

        @Override
        public ActiveRepairService repair()
        {
            return activeRepairService;
        }

        @Override
        public PaxosRepairState paxosRepairState()
        {
            return paxosRepairState;
        }

        @Override
        public ICompactionManager compactionManager()
        {
            return compactionManager;
        }

        @Override
        public IValidationManager validationManager()
        {
            return validationManager;
        }

        @Override
        public TableRepairManager repairManager(ColumnFamilyStore store)
        {
            return new CassandraTableRepairManager(store, this)
            {
                @Override
                public void snapshot(String name, Collection<Range<Token>> ranges, boolean force)
                {
                    // no-op
                }
            };
        }

        @Override
        public StreamExecutor streamExecutor()
        {
            return streamExecutor;
        }
    }

    private class SimulatedGossip implements IGossiper
    {
        private final Map<InetAddressAndPort, EndpointState> endpoints = new HashMap<>();

        @Override
        public void register(IEndpointStateChangeSubscriber subscriber)
        {

        }

        @Override
        public void unregister(IEndpointStateChangeSubscriber subscriber)
        {

        }

        @Nullable
        @Override
        public EndpointState getEndpointStateForEndpoint(InetAddressAndPort ep)
        {
            return endpoints.get(ep);
        }

        @Override
        public void notifyFailureDetector(Map<InetAddressAndPort, EndpointState> remoteEpStateMap)
        {

        }

        @Override
        public void applyStateLocally(Map<InetAddressAndPort, EndpointState> epStateMap)
        {
            // If we were testing paxos this would be wrong...
            // CASSANDRA-18917 added support for simulating Gossip, but gossip issues were found so couldn't merge that patch...
            // For the paxos repair, since we don't care about paxos messages, this is ok to no-op for now, but if paxos cleanup
            // ever was to be tested this logic would need to be implemented
        }
    }
}

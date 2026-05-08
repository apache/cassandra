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

package org.apache.cassandra.service.accord;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.collect.Sets;

import org.assertj.core.api.Assertions;
import org.assertj.core.description.Description;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.Utils;
import accord.api.MessageSink;
import accord.impl.DefaultTimeouts;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TestAgent;
import accord.impl.mock.MockCluster;
import accord.local.Node;
import accord.local.ShardDistributor;
import accord.local.TimeService;
import accord.primitives.Ranges;
import accord.topology.EpochReady;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import accord.utils.Gen;
import accord.utils.Invariants;
import accord.utils.Property.SimpleCommand;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.Cancellable;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.SimulatedExecutorFactory;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.SimulatedMessageDelivery;
import org.apache.cassandra.net.SimulatedMessageDelivery.Action;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.ValidatingClusterMetadataService;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.sequences.LeaveStreams;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.Pair;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordCommandStoreShardCount;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;

public class EpochSyncTest
{
    private static final Logger logger = LoggerFactory.getLogger(EpochSyncTest.class);

    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting());
    }

    @Test
    public void test()
    {
        stateful().withSeed(152472217520379L).withExamples(50).withSteps(500).check(commands(() -> Cluster::new)
                .destroyState(cluster -> {
                    finishPendingWork(cluster);
                    cluster.processAll();
                    cluster.validate(true);
                })
                .addAllIf(Cluster::hasPendingWork, b -> {
                    b.addIf(c -> !c.status(s -> s == Cluster.Status.Registered).isEmpty(),
                            (rs, state) -> {
                                long epoch = state.cms.metadata().epoch.getEpoch() + 1;
                                Node.Id pick = rs.pick(state.status(s -> s == Cluster.Status.Registered));
                                return new SimpleCommand<>(String.format("%s Start Joining; epoch=%d", pick, epoch),
                                                           c -> c.increment(pick));
                            })
                     .addIf(c -> !c.cms.metadata().inProgressSequences.isEmpty(),
                            (rs, state) -> new SimpleCommand<>(String.format("Next Epoch Step; epoch=%d", state.cms.metadata().epoch.getEpoch() + 1),
                                                               Cluster::incrementInProgressSequences));
                })
                .addAllIf(Cluster::hasNoPendingWork, b -> {
                    b.addIf(cluster -> cluster.joined().size() <= cluster.maxNodes, EpochSyncTest::addNode)
                     .addIf(cluster -> cluster.joined().size() > cluster.minNodes, EpochSyncTest::removeNode);
                })
                .addIf(Cluster::hasWork, EpochSyncTest::processSome)
                .add(rs -> new SimpleCommand<>("Validate", c -> c.validate(false)))
                .add((rs, cluster) -> new SimpleCommand<>("Bump Epoch " + (cluster.cms.metadata().epoch.getEpoch() + 1), Cluster::bumpEpoch))
                .build());
    }

    private static void finishPendingWork(Cluster cluster)
    {
        List<Node.Id> registered = cluster.status(s -> s == Cluster.Status.Registered);
        if (!registered.isEmpty())
            registered.forEach(cluster::increment);
        while (!cluster.cms.metadata().inProgressSequences.isEmpty())
            cluster.incrementInProgressSequences();
    }

    private static SimpleCommand<Cluster> addNode(RandomSource rs, Cluster cluster)
    {
        Node.Id id = new Node.Id(++cluster.nodeCounter);
        long token = cluster.tokenGen.nextLong(rs);
        while (cluster.tokens.contains(token))
            token = cluster.tokenGen.nextLong(rs);
        long epoch = cluster.cms.metadata().epoch.getEpoch() + 1;
        long finalToken = token;
        return new SimpleCommand<>("Start Node " + id + "; token=" + token + ", epoch=" + epoch,
                                   c -> c.registerNode(id, finalToken));
    }

    private static SimpleCommand<Cluster> removeNode(RandomSource rs, Cluster cluster)
    {
        List<Node.Id> alive = cluster.joined();
        Node.Id pick = rs.pick(alive);
        long token = cluster.instances.get(pick).token;
        long epoch = cluster.cms.metadata().epoch.getEpoch() + 1;
        return new SimpleCommand<>("Remove Node " + pick + "; token=" + token + "; epoch=" + epoch, c -> c.removeNode(pick));
    }

    private static SimpleCommand<Cluster> processSome(RandomSource rs) {
        return new SimpleCommand<>("Process Some",
                c -> {//noinspection StatementWithEmptyBody
                    for (int i = 0, attempts = rs.nextInt(1, 100); i < attempts && c.processOne(); i++) {}
                });
    }

    private static class Cluster
    {
        private static final int rf = 3;
        private static final ReplicationParams replication_params = ReplicationParams.simple(rf);

        private final RandomSource rs;
        private final int minNodes, maxNodes;
        private final Gen.LongGen tokenGen;
        private final SortedSet<Long> tokens = new TreeSet<>();
        private final Map<Node.Id, Instance> instances = new HashMap<>();
        private final Set<Node.Id> removed = new HashSet<>();
        private final List<Throwable> failures = new ArrayList<>();
        private final SimulatedExecutorFactory globalExecutor;
        private final ScheduledExecutorPlus scheduler;
        private int nodeCounter = 0;
        private final ValidatingClusterMetadataService cms = ValidatingClusterMetadataService.createAndRegister(NodeVersion.CURRENT_METADATA_VERSION);

        class Mapper implements AccordEndpointMapper
        {
            @Override
            public Node.Id mappedIdOrNull(InetAddressAndPort endpoint, @Nullable Object logIdentityIfUnmapped)
            {
                return nodeId(endpoint);
            }

            @Override
            public InetAddressAndPort mappedEndpointOrNull(Node.Id id, @Nullable Object logIdentityIfUnmapped)
            {
                return address(id);
            }

            @Override
            public Map<Node.Id, Long> removedNodes()
            {
                return Map.of();
            }

            @Override
            public NodeStatus nodeStatus(Node.Id id)
            {
                return instances.get(id).status != Status.Removed ? NodeStatus.HEALTHY : NodeStatus.UNKNOWN;
            }
        }

        public Cluster(RandomSource rs)
        {
            // add the test keyspace
            createTestKeyspaceAndTable();
            this.rs = rs;
            this.minNodes = 3;
            this.maxNodes = 10;
            this.tokenGen = rs2 -> rs2.nextLong(Long.MIN_VALUE + 1, Long.MAX_VALUE);

            this.globalExecutor = new SimulatedExecutorFactory(rs, failures::add);
            this.scheduler = globalExecutor.scheduled("ignored");
            Stage.MISC.unsafeSetExecutor(scheduler);

            scheduler.scheduleWithFixedDelay(() -> {
                if (aliveCount() < 2) return;
                if (!partitions.isEmpty() && rs.nextBoolean())
                {
                    // remove partition
                    if (partitions.size() == 1)
                    {
                        partitions.clear();
                        return;
                    }
                    partitions.remove(rs.pickOrderedSet(partitions));
                }
                else
                {
                    // add partition
                    List<Node.Id> alive = notRemoved();
                    InetAddressAndPort a = address(rs.pick(alive));
                    InetAddressAndPort b = address(rs.pick(alive));
                    while (a.equals(b))
                        b = address(rs.pick(alive));
                    partitions.add(new Connection(a, b));
                }
            }, 1, 1, TimeUnit.MINUTES);
        }

        private static InetAddressAndPort address(Node.Id id)
        {
            try
            {
                return InetAddressAndPort.getByAddress(ByteArrayUtil.bytes(id.id));
            }
            catch (UnknownHostException e)
            {
                throw new AssertionError("Unable to create address for id " + id, e);
            }
        }

        private boolean hasPendingWork()
        {
            return !status(s -> s == Cluster.Status.Registered).isEmpty()
                    || !cms.metadata().inProgressSequences.isEmpty()
                   || globalExecutor.hasWork();
        }

        private boolean hasNoPendingWork()
        {
            return !hasPendingWork();
        }

        private Transformation.Success process(Transformation transformation)
        {
            Transformation.Result result = transformation.execute(cms.metadata());
            if (result.isRejected())
                throw new IllegalStateException("Unable to make TCM transition: " + result.rejected());
            return result.success();
        }

        private Transformation.Success process(MultiStepOperation<?> transformation)
        {
            Transformation.Result result = transformation.applyTo(cms.metadata());
            if (result.isRejected())
                throw new IllegalStateException("Unable to make TCM transition");
            return result.success();
        }

        public void incrementInProgressSequences()
        {
            if (cms.metadata().inProgressSequences.isEmpty())
                throw new IllegalStateException("Attempted to bump epoch when nothing was pending");
            Iterator<MultiStepOperation<?>> it = cms.metadata().inProgressSequences.iterator();
            Invariants.require(it.hasNext());
            notify(process(it.next()).metadata);
        }

        private static boolean left(ClusterMetadata metadata, Node.Id id)
        {
            return metadata.directory.peerState(new NodeId(id.id)) == NodeState.LEFT;
        }

        private static boolean joined(ClusterMetadata metadata, Node.Id id)
        {
            NodeAddresses address = metadata.directory.getNodeAddresses(new NodeId(id.id));
            return metadata.placements.get(replication_params).reads.byEndpoint().keySet().contains(address.broadcastAddress);
        }

        public enum EpochTracker { topologyManager, accordSyncPropagator }

        Set<EpochTracker> globalSynced(long epoch)
        {
            return notRemoved().stream()
                               .filter(n -> instances.get(n).epoch.getEpoch() <= epoch)
                               .map(n -> instances.get(n).synced(epoch))
                               .reduce(EnumSet.allOf(EpochTracker.class), Sets::intersection);
        }

        boolean allSynced(long epoch)
        {
            Set<EpochTracker> done = globalSynced(epoch);
            return done.contains(EpochTracker.topologyManager);
        }

        private static Node.Id nodeId(InetAddressAndPort address)
        {
            return new Node.Id(ByteArrayUtil.getInt(address.addressBytes));
        }

        private void createTestKeyspaceAndTable()
        {
            ClusterMetadata current = cms.metadata();
            Tables tables = Tables.of(TableMetadata.minimal("test", "tb1").unbuild()
                                                   .partitioner(Murmur3Partitioner.instance)
                                                   .params(TableParams.builder().transactionalMode(TransactionalMode.full).build())
                                                   .build());
            KeyspaceMetadata ks = KeyspaceMetadata.create("test", KeyspaceParams.simple(rf), tables);

            cms.setMetadata(current.transformer()
                                   .with(new DistributedSchema(current.schema.getKeyspaces().with(ks)))
                                   .build()
                            .metadata);
        }

        void validate(boolean isDone)
        {
            for (Node.Id id : notRemoved())
            {
                Instance inst = instances.get(id);
                if (removed.contains(id)) continue; // ignore removed nodes
                TopologyManager tm = inst.topology;
                for (long epoch = Math.max(tm.firstNonEmpty(), inst.epoch.getEpoch()); epoch <= cms.metadata().epoch.getEpoch(); epoch++)
                {
                    // validate config
                    if (isDone)
                    {
                        Assertions.assertThat(inst.topologyService.syncPropagator().hasPending(epoch))
                                  .describedAs("node%s has pending notifications in AccordSyncPropagator for epoch %d", id, epoch)
                                  .isFalse();

                        // validate topology manager
                        Ranges ranges = tm.active().getKnown(epoch).all().ranges().mergeTouching();
                        Ranges actual = tm.unsafeQuorumReady(epoch).mergeTouching();
                        Assertions.assertThat(actual)
                                  .describedAs("node%s does not have all expected sync ranges for epoch %d; missing %s", id, epoch, ranges.without(actual))
                                  .isEqualTo(ranges);
                    }
                    else
                    {
                        if (!allSynced(epoch))
                            continue;

                        Assertions.assertThat(tm.active().hasEpoch(epoch)).describedAs("node%s does not have epoch %d", id, epoch).isTrue();
                        Topology topology = tm.active().getKnown(epoch).all();
                        Ranges ranges = topology.ranges().mergeTouching();
                        Ranges actual = tm.unsafeQuorumReady(epoch).mergeTouching();
                        // TopologyManager defines syncComplete for an epoch as (epoch - 1).syncComplete.  This means that an epoch has reached quorum, but will still miss ranges as previous epochs have not
                        if (!ranges.equals(actual) && tm.minEpoch() != epoch && !ranges.equals(tm.unsafeQuorumReady(epoch - 1).mergeTouching()))
                            continue;
                        long epoch_ = epoch;
                        Assertions.assertThat(actual)
                                  .describedAs(new Description()
                                  {
                                      public String value()
                                      {
                                          return String.format("node%s does not have all expected sync ranges for epoch %d; missing %s; peers=%s",
                                                               id, epoch_, ranges.without(actual), topology.nodes());

                                      }
                                  })
                                  .isEqualTo(ranges);
                    }
                }
            }
        }

        String displayTopology()
        {
            class Hold {
                final Cluster.Status status;
                final long token;

                Hold(Status status, long token)
                {
                    this.status = status;
                    this.token = token;
                }

                @Override
                public String toString()
                {
                    return status + "\t" + (status == Status.Registered ? "?" : Long.toString(token));
                }
            }
            List<Node.Id> notRemoved = notRemoved();
            List<Pair<Node.Id, Hold>> list = new ArrayList<>(notRemoved.size());
            for (Node.Id n : notRemoved)
            {
                Instance instance = instances.get(n);
                list.add(Pair.create(n, new Hold(instance.status, instance.token)));
            }
            list.sort(Comparator.comparing(a -> a.right.token));
            StringBuilder sb = new StringBuilder();
            for (var p : list)
                sb.append(p.left).append('\t').append(p.right).append('\n');
            return sb.toString();
        }

        @Override
        public String toString()
        {
            return "Topology:\n" + displayTopology();
        }

        boolean hasWork()
        {
            return globalExecutor.hasWork();
        }

        boolean processOne()
        {
            boolean result = globalExecutor.processOne();
            checkFailures();
            return result;
        }

        @SuppressWarnings("StatementWithEmptyBody")
        void processAll()
        {
            while (processOne())
            {
            }
        }

        public void checkFailures()
        {
            if (Thread.interrupted())
                failures.add(new InterruptedException());
            if (failures.isEmpty()) return;
            AssertionError error = new AssertionError("Unexpected exceptions found");
            failures.forEach(error::addSuppressed);
            failures.clear();
            throw error;
        }

        List<Node.Id> joined()
        {
            return status(s -> s == Status.Joined);
        }

        List<Node.Id> status(Predicate<Status> fn)
        {
            List<Node.Id> ids = new ArrayList<>(instances.size());
            for (Instance i : instances.values())
            {
                if (fn.test(i.status))
                    ids.add(i.id);
            }
            ids.sort(Comparator.naturalOrder());
            return ids;
        }

        List<Node.Id> notRemoved()
        {
            ArrayList<Node.Id> ids = new ArrayList<>(Sets.difference(instances.keySet(), removed));
            ids.sort(Comparator.naturalOrder());
            return ids;
        }

        int aliveCount()
        {
            return instances.size() - removed.size();
        }

        private final NavigableSet<Connection> partitions = new TreeSet<>();

        private boolean partitioned(InetAddressAndPort self, InetAddressAndPort to)
        {
            return partitions.contains(new Connection(self, to));
        }

        private SimulatedMessageDelivery createMessaging(Node.Id id)
        {
            InetAddressAndPort address = address(id);
            return new SimulatedMessageDelivery(address,
                                                (self, msg, to) -> {
                                                    if (removed.contains(nodeId(self)) || removed.contains(nodeId(to)))
                                                        return Action.DROP;
                                                    if (!self.equals(to) && partitioned(self, to))
                                                        return Action.DROP_PARTITIONED;
                                                    if (rs.decide(.01))
                                                        return rs.nextBoolean() ? Action.DELIVER_WITH_FAILURE : Action.FAILURE;
                                                    return Action.DELIVER;
                                                },
                                                SimulatedMessageDelivery.randomDelay(rs.fork()),
                                                (to, msg) -> instances.get(nodeId(to)).receiver.recieve(msg),
                                                (action, to, msg) -> logger.trace("{} message {}", action, msg),
                                                scheduler::schedule,
                                                failures::add);
        }

        void registerNode(Node.Id id, long token)
        {
            Invariants.require(!tokens.contains(token), "Attempted to add token %d for node %s but token is already taken", token, id);
            Invariants.require(!instances.containsKey(id), "Attempted to add node %s; but already exists", id);

            ClusterMetadata.Transformer builder = cms.metadata().transformer();

            Instance instance = new Instance(id, token, builder.epoch(), createMessaging(id), new Mapper());
            instances.put(id, instance);
            tokens.add(token);

            builder.register(new NodeAddresses(address(id)), new Location("dc1", "r1"), NodeVersion.CURRENT);
            notify(builder.build().metadata);
        }

        void increment(Node.Id pick)
        {
            Instance inst = Objects.requireNonNull(instances.get(pick), "Unknown id " + pick);

            switch (inst.status)
            {
                case Init:
                case Joined:
                case Removed:
                    throw new IllegalStateException("Unexpected status: " + inst.status);
                case Registered:
                    inst.status = Status.Joining;
                    PrepareJoin task = new PrepareJoin(new NodeId(pick.id), Collections.singleton(new LongToken(inst.token)), new UniformRangePlacement(), true, false);
                    notify(process(task).metadata);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown status: " + inst.status);
            }
        }

        void removeNode(Node.Id pick)
        {
            Instance inst = Objects.requireNonNull(instances.get(pick), "Unknown id " + pick);
            Invariants.require(!removed.contains(pick), "Can not remove node twice; node " + pick);
            removed.add(pick);
            Assertions.assertThat(inst.topologyService.syncPropagator().hasPending()).isFalse();
            inst.status = Status.Leaving;
            PrepareLeave prepareLeave = new PrepareLeave(new NodeId(pick.id), false, new UniformRangePlacement(), LeaveStreams.Kind.REMOVENODE);
            notify(process(prepareLeave).metadata);
        }

        void bumpEpoch()
        {
            notify(cms.metadata().forceEpoch(Epoch.create(cms.metadata().epoch.getEpoch() + 1)));
        }

        private void notify(ClusterMetadata current)
        {
            Topology t = AccordTopology.createAccordTopology(current);
            Ranges ranges = t.ranges().mergeTouching();
            if (!current.placements.get(replication_params).reads.isEmpty())
                Assertions.assertThat(ranges).hasSize(1);
            cms.setMetadata(current);
            for (Node.Id id : status(s -> s != Status.Removed))
            {
                Instance inst = instances.get(id);
                inst.maybeTransition(current, t);
                Topology topology = AccordTopology.createAccordTopology(current);
                inst.topology.reportTopology(topology);
            }
        }

        @SuppressWarnings("SameParameterValue")
        private <T> AsyncChain<T> schedule(long time, TimeUnit unit, Callable<T> task)
        {
            return new AsyncChains.Head<>()
            {
                @Override
                protected Cancellable start(BiConsumer<? super T, Throwable> callback)
                {
                    Future<?> future = scheduler.schedule(() -> {
                        T value;
                        try
                        {
                            value = task.call();
                        }
                        catch (Throwable t)
                        {
                            callback.accept(null, t);
                            return;
                        }
                        callback.accept(value, null);
                    }, time, unit);
                    return () -> future.cancel(true);
                }
            };
        }

        private enum Status { Init, Registered, Joining, Joined, Leaving, Removed}
        private class Instance
        {
            private final Node.Id id;
            private final long token;
            private final AccordTopologyService topologyService;
            private final SimulatedMessageDelivery messaging;
            private final SimulatedMessageDelivery.SimulatedMessageReceiver receiver;
            private final TopologyManager topology;
            private final Epoch epoch;
            private Status status = Status.Init;

            Instance(Node.Id id, long token, Epoch epoch, SimulatedMessageDelivery messagingService, AccordEndpointMapper mapper)
            {
                this.id = id;
                this.token = token;
                this.epoch = epoch;
                MockCluster.Clock clock = new MockCluster.Clock(0);
                Node node = Utils.createNode(id, ignore -> AsyncResults.settable(), new MessageSink.NoOpSink(), clock, new TestAgent(clock), new TokenKey.KeyspaceSplitter(new ShardDistributor.EvenSplit<>(getAccordCommandStoreShardCount(), getPartitioner().accordSplitter())));
                // TODO (review): Should there be a real scheduler here? Is it possible to adapt the Scheduler interface to scheduler used in this test?
                TimeService time = TimeService.ofNonMonotonic(globalExecutor::currentTimeMillis, TimeUnit.MILLISECONDS);
                this.topologyService = new AccordTopologyService(id, mapper, messagingService, scheduler);
                this.topology = new TopologyManager(SizeOfIntersectionSorter.SUPPLIER, node, topologyService, time, new DefaultTimeouts(time))
                {
                    @Override
                    protected EpochReady bootstrap(Supplier<EpochReady> bootstrap)
                    {
                        bootstrap.get();
                        AsyncResult<Void> metadata = schedule(rs.nextInt(1, 10), TimeUnit.SECONDS, (Callable<Void>) () -> null).beginAsResult();
                        AsyncResult<Void> coordination = metadata.flatMap(ignore -> schedule(rs.nextInt(1, 10), TimeUnit.SECONDS, (Callable<Void>) () -> null).beginAsResult());
                        AsyncResult<Void> data = coordination.flatMap(ignore -> schedule(rs.nextInt(1, 10), TimeUnit.SECONDS, (Callable<Void>) () -> null).beginAsResult());
                        AsyncResult<Void> reads = data.flatMap(ignore -> schedule(rs.nextInt(1, 10), TimeUnit.SECONDS, (Callable<Void>) () -> null).beginAsResult());
                        return new EpochReady(topology.epoch(), metadata, coordination, data, reads);
                    }
                };
                this.topology.addListener(topologyService.syncPropagator());
                this.topology.addListener(topologyService.watermarkCollector());

                Map<Verb, IVerbHandler<?>> handlers = new EnumMap<>(Verb.class);
                //noinspection unchecked
                handlers.put(Verb.ACCORD_SYNC_NOTIFY_REQ, (messaging, msg) -> AccordService.receive(messaging, topology, (Message<AccordSyncPropagator.Notification>) (Message<?>) msg));
                this.messaging = messagingService;
                this.receiver = messagingService.receiver(new SimulatedMessageDelivery.SimpleVerbHandler(handlers));
            }

            @Override
            public String toString()
            {
                return "Instance{" +
                       "id=" + id +
                       ", token=" + token +
                       ", epoch=" + epoch +
                       ", status=" + status +
                       '}';
            }

            void maybeTransition(ClusterMetadata current, Topology t)
            {
                switch (status)
                {
                    case Init:
                        Invariants.require(!t.nodes().contains(id), "Node was in Init state but present in the Topology!");
                        Invariants.require(current.directory.peerId(address(id)) != null, "Node exists but not in TCM");
                        status = Status.Registered;
                        break;
                    case Registered:
                        Invariants.require(!t.nodes().contains(id), "Node was in Init state but present in the Topology!");
                        Invariants.require(current.directory.peerId(address(id)) != null, "Node exists but not in TCM");
                        if (current.placements.get(replication_params).writes.byEndpoint().keySet().contains(address(id)))
                            status = Status.Joining;
                        break;
                    case Joining:
                        Invariants.require(current.directory.peerId(address(id)) != null, "Node exists but not in TCM");
                        if (joined(current, id))
                            status = Status.Joined;
                    case Removed:
                    case Joined:
                        // nothing to do
                        break;
                    case Leaving:
                        if (left(current, id))
                            stop();
                        break;
                    default:
                        throw new UnsupportedOperationException("Unknown status: " + status);
                }
            }

            Set<EpochTracker> synced(long epoch)
            {
                if (epoch < this.epoch.getEpoch()) throw new IllegalArgumentException("Asked for epoch before this instance existed");
                EnumSet<EpochTracker> done = EnumSet.noneOf(EpochTracker.class);
                if (topology.unsafeIsQuorumReady(epoch))
                    done.add(EpochTracker.topologyManager);
                if (!topologyService.syncPropagator().hasPending(epoch))
                    done.add(EpochTracker.accordSyncPropagator);
                return done;
            }

            void stop()
            {
                status = Status.Removed;
                tokens.remove(token);
                messaging.stop();
            }
        }
    }

    private static class Connection implements Comparable<Connection>
    {
        final InetAddressAndPort from, to;

        private Connection(InetAddressAndPort from, InetAddressAndPort to)
        {
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Connection that = (Connection) o;
            return from.equals(that.from) && to.equals(that.to);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(from, to);
        }

        @Override
        public String toString()
        {
            return "Connection{" + "from=" + from + ", to=" + to + '}';
        }

        @Override
        public int compareTo(Connection o)
        {
            int rc = from.compareTo(o.from);
            if (rc == 0)
                rc = to.compareTo(o.to);
            return rc;
        }
    }
}

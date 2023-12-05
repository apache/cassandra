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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.Agent;
import accord.impl.AbstractConfigurationService;
import accord.impl.TestAgent;
import accord.impl.basic.PendingQueue;
import accord.impl.basic.PropagatingPendingQueue;
import accord.impl.basic.RandomDelayQueue;
import accord.impl.basic.SimulatedDelayedExecutorService;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.concurrent.AdaptingScheduledExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.Future;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;
import static org.apache.cassandra.simulator.RandomSource.Choices.choose;

public class AccordSyncPropagatorTest
{
    @BeforeClass
    public static void setup() throws NoSuchFieldException, IllegalAccessException
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting());
    }

    @Test
    public void burnTest()
    {
        Gen<Ranges> rangesGen = AccordGenerators.ranges().filter(r -> !r.isEmpty());
        Gen<List<Node.Id>> nodesGen = Gens.lists(AccordGens.nodes()).unique().ofSizeBetween(1, 40);
        qt().withExamples(100).check(rs -> {
            List<Node.Id> nodes = nodesGen.next(rs);
            Set<Node.Id> nodesAsSet = ImmutableSet.copyOf(nodes);

            List<Throwable> failures = new ArrayList<>();
            RandomDelayQueue delayQueue = new RandomDelayQueue.Factory(rs).get();
            PendingQueue queue = new PropagatingPendingQueue(failures, delayQueue);
            Agent agent = new TestAgent.RethrowAgent();
            SimulatedDelayedExecutorService globalExecutor = new SimulatedDelayedExecutorService(queue, agent);
            ScheduledExecutorPlus scheduler = new AdaptingScheduledExecutorPlus(globalExecutor);

            Cluster cluster = new Cluster(nodes, rs, scheduler);

            long epochOffset = rs.nextLong(1, 1024);
            int numEpochs = rs.nextInt(1, 10);
            Map<Long, Ranges> allRanges = new HashMap<>();
            for (int i = 0; i < numEpochs; i++)
            {
                long epoch = epochOffset + i;
                Ranges ranges = rangesGen.next(rs);
                allRanges.put(epoch, ranges);
                scheduler.schedule(() -> {
                    for (Node.Id nodeId : nodes)
                        cluster.node(nodeId).propagator.reportSyncComplete(epoch, nodes, nodeId);

                    for (int j = 0, attempts = rs.nextInt(1, 4); j < attempts; j++)
                    {
                        for (Range range : ranges)
                        {
                            Cluster.Instace inst = cluster.node(choose(rs, nodes));
                            scheduler.schedule(() -> {
                                Ranges subrange = Ranges.of(range);
                                inst.propagator.reportClosed(epoch, nodes, subrange);
                                scheduler.schedule(() -> inst.propagator.reportRedundant(epoch, nodes, subrange), 1, TimeUnit.MINUTES);
                            }, rs.nextInt(30, 300), TimeUnit.SECONDS);
                        }
                    }
                }, rs.nextInt(30, 300), TimeUnit.SECONDS);
            }

            while (queue.size() > 0)
            {
                Runnable next = (Runnable) queue.poll();
                if (next == null)
                    break;
                next.run();
                if (!failures.isEmpty())
                {
                    RuntimeException e = new RuntimeException("Failures detected");
                    failures.forEach(e::addSuppressed);
                    throw e;
                }
            }
            if (hasPending(cluster))
                throw new AssertionError("Unable to make progress: pending syncs on \n" + cluster.instances.values().stream().filter(i -> i.propagator.hasPending()).map(i -> i.propagator.toString()).collect(Collectors.joining("\n")));

            for (Cluster.Instace inst : cluster.instances.values())
            {
                Cluster.ConfigService cs = inst.configurationService;
                assertSetsEqual(cs.completedEpochs, allRanges.keySet(), "completedEpochs %s", inst.id);
                assertSetsEqual(cs.syncCompletes.keySet(), allRanges.keySet(), "syncCompletes %s", inst.id);
                for (Map.Entry<Long, Set<Node.Id>> e : cs.syncCompletes.entrySet())
                    assertSetsEqual(e.getValue(), nodesAsSet, "syncCompletes values on %s", inst.id);

                assertMapEquals(cs.closed, allRanges, "Unexpected state for closed on %s", inst.id);
                assertMapEquals(cs.redundant, allRanges, "Unexpected state for redundant on %s", inst.id);
            }
        });
    }

    private static  <T> void assertSetsEqual(Set<T> actual, Set<T> expected, String msg, Object... args)
    {
        Set<T> notExpected = Sets.difference(actual, expected);
        Assertions.assertThat(notExpected).describedAs("Unexpected values detected; " + msg, args).isEmpty();
        Set<T> missing = Sets.difference(expected, actual);
        Assertions.assertThat(missing).describedAs("Missing values detected; " + msg, args).isEmpty();
    }

    private static <K, V> void assertMapEquals(Map<K, V> actual, Map<K, V> expected, String msg, Object... args)
    {
        assertSetsEqual(actual.keySet(), expected.keySet(), msg, args);
        List<String> errors = new ArrayList<>();
        for (Map.Entry<K, V> e : actual.entrySet())
        {
            V value = e.getValue();
            V other = expected.get(e.getKey());
            if (!Objects.equals(value, other))
                errors.add(String.format("Missmatch at key %s: expected %s but given %s", e.getKey(), other, value));
        }
        if (!errors.isEmpty())
            throw new AssertionError(String.join("\n", errors));
    }

    private static boolean hasPending(Cluster cluster)
    {
        return cluster.instances.values().stream().anyMatch(i -> i.propagator.hasPending());
    }

    private static class Cluster implements AccordEndpointMapper
    {
        private final ImmutableBiMap<Node.Id, InetAddressAndPort> nodeToAddress;
        private final ImmutableMap<Node.Id, Instace> instances;
        private final RandomSource rs;
        private final ScheduledExecutorPlus scheduler;

        private Cluster(List<Node.Id> nodes,
                        RandomSource rs,
                        ScheduledExecutorPlus scheduler)
        {
            this.rs = rs;
            this.scheduler = scheduler;
            ImmutableBiMap.Builder<Node.Id, InetAddressAndPort> nodeToAddress = ImmutableBiMap.builder();
            ImmutableMap.Builder<Node.Id, Instace> instances = ImmutableMap.builder();
            for (Node.Id id : nodes)
            {
                InetAddressAndPort address = addressFromInt(id.id);
                nodeToAddress.put(id, address);
                ConfigService cs = new ConfigService(id);
                Sink sink = new Sink(id);
                IFailureDetector fd = new FailureDetector(address);
                instances.put(id, new Instace(id, address, cs, sink, fd, cs, new AccordSyncPropagator(id, Cluster.this, sink, fd, scheduler, cs)));
            }
            this.nodeToAddress = nodeToAddress.build();
            this.instances = instances.build();
        }

        private InetAddressAndPort addressFromInt(int value)
        {
            byte[] array = ByteBufferUtil.bytes(value).array();
            try
            {
                InetAddress address = InetAddress.getByAddress(array);
                return InetAddressAndPort.getByAddressOverrideDefaults(address, 1);
            }
            catch (UnknownHostException e)
            {
                throw new AssertionError(e);
            }
        }

        public Cluster.Instace node(Node.Id id)
        {
            Instace instace = instances.get(id);
            if (instace == null)
                throw new NullPointerException("Unknown id: " + id);
            return instace;
        }

        public Cluster.Instace node(InetAddressAndPort address)
        {
            return node(mappedId(address));
        }

        @Override
        public Node.Id mappedId(InetAddressAndPort endpoint)
        {
            Node.Id id = nodeToAddress.inverse().get(endpoint);
            if (id == null)
                throw new NullPointerException("Unable to map endpoint: " + endpoint);
            return id;
        }

        @Override
        public InetAddressAndPort mappedEndpoint(Node.Id id)
        {
            return nodeToAddress.get(id);
        }

        private enum Action
        {
            DELIVER, TIMEOUT, ERROR
        }

        private class Sink implements MessageDelivery
        {
            private final Node.Id from;
            private final Map<Long, RequestCallback<?>> callbacks = new HashMap<>();
            private final Map<InetAddressAndPort, Gen<Action>> nodeActions = new HashMap<>();

            private Sink(Node.Id from)
            {
                this.from = from;
            }

            @Override
            public <REQ> void send(Message<REQ> message, InetAddressAndPort to)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb)
            {
                Action action = action(to);
                switch (action)
                {
                    case ERROR:
                        cb.onFailure(to, RequestFailure.UNKNOWN);
                        return;
                    case TIMEOUT:
                        cb.onFailure(to, RequestFailure.TIMEOUT);
                        return;
                    case DELIVER:
                        break;
                    default:
                        throw new IllegalStateException("Unknown action: " + action);
                }
                callbacks.put(message.id(), cb);
                scheduler.schedule(() -> AccordService.receive(this, node(to).configurationService, (Message<List<AccordSyncPropagator.Notification>>) message.withFrom(mappedEndpoint(from))), 500, TimeUnit.MILLISECONDS);
                scheduler.schedule(() -> {
                    RequestCallback<?> removed = callbacks.remove(message.id());
                    if (removed != null)
                        removed.onFailure(to, RequestFailure.TIMEOUT);
                }, 1, TimeUnit.MINUTES);
            }

            @Override
            public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public <V> void respond(V response, Message<?> message)
            {
                Action action = action(message.respondTo());
                switch (action)
                {
                    case ERROR:
                    case TIMEOUT:
                        return;
                    case DELIVER:
                        break;
                    default:
                        throw new IllegalStateException("Unknown action: " + action);
                }

                RequestCallback cb = node(message.respondTo()).messagingService.callbacks.remove(message.id());
                if (cb != null)
                    cb.onResponse(message.responseWith(response));
            }

            private Action action(InetAddressAndPort to)
            {
                return nodeActions.computeIfAbsent(to, ignore -> Gens.enums().allWithWeights(Action.class, 81, 10, 1)).next(rs);
            }
        }

        private class FailureDetector implements IFailureDetector
        {
            private final InetAddressAndPort self;
            private final Map<InetAddressAndPort, Gen<Boolean>> nodeRuns = new HashMap<>();

            private FailureDetector(InetAddressAndPort self)
            {
                this.self = self;
            }

            @Override
            public boolean isAlive(InetAddressAndPort ep)
            {
                if (self.equals(ep)) return true;

                return !nodeRuns.computeIfAbsent(ep, ignore -> Gens.bools().biasedRepeatingRuns(.01, rs.nextInt(3, 15))).next(rs);
            }

            @Override
            public void interpret(InetAddressAndPort ep)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void report(InetAddressAndPort ep)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void remove(InetAddressAndPort ep)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void forceConviction(InetAddressAndPort ep)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener)
            {
                throw new UnsupportedOperationException();
            }
        }

        private class ConfigService extends AbstractConfigurationService.Minimal implements AccordSyncPropagator.Listener
        {
            private final Map<Long, Set<Node.Id>> syncCompletes = new HashMap<>();
            private final Map<Long, Set<Node.Id>> endpointAcks = new HashMap<>();
            private final NavigableSet<Long> completedEpochs = Collections.synchronizedNavigableSet(new TreeSet<>());
            private final Map<Long, Ranges> closed = new HashMap<>();
            private final Map<Long, Ranges> redundant = new HashMap<>();

            private ConfigService(Node.Id node)
            {
                super(node);
            }

            @Override
            protected void receiveRemoteSyncCompletePreListenerNotify(Node.Id node, long epoch)
            {
                syncCompletes.computeIfAbsent(epoch, ignore -> new HashSet<>()).add(node);
            }

            @Override
            protected void fetchTopologyInternal(long epoch)
            {
                // TODO
            }

            @Override
            protected void localSyncComplete(Topology topology, boolean startSync)
            {
                Set<Node.Id> notify = topology.nodes().stream().filter(i -> !localId.equals(i)).collect(Collectors.toSet());
                instances.get(localId).propagator.reportSyncComplete(topology.epoch(), notify, localId);
            }

            @Override
            public void reportEpochClosed(Ranges ranges, long epoch)
            {
                Topology topology = getTopologyForEpoch(epoch);
                instances.get(localId).propagator.reportClosed(epoch, topology.nodes(), ranges);
            }

            @Override
            public void reportEpochRedundant(Ranges ranges, long epoch)
            {
                Topology topology = getTopologyForEpoch(epoch);
                instances.get(localId).propagator.reportRedundant(epoch, topology.nodes(), ranges);
            }

            @Override
            public void onEndpointAck(Node.Id id, long epoch)
            {
                endpointAcks.computeIfAbsent(epoch, ignore -> new HashSet<>()).add(id);
            }

            @Override
            public void onComplete(long epoch)
            {
                completedEpochs.add(epoch);
                // TODO why do we see multiple calls?
//                if (!completedEpochs.add(epoch))
//                    throw new IllegalStateException("Completed epoch " + epoch + " multiple times");
            }

            @Override
            public synchronized void receiveClosed(Ranges ranges, long epoch)
            {
                super.receiveClosed(ranges, epoch);
                closed.merge(epoch, ranges, Ranges::with);
            }

            @Override
            public synchronized void receiveRedundant(Ranges ranges, long epoch)
            {
                super.receiveRedundant(ranges, epoch);
                redundant.merge(epoch, ranges, Ranges::with);
            }
        }

        public class Instace
        {
            private final Node.Id id;
            private final InetAddressAndPort address;
            private final ConfigService configurationService;
            private final Sink messagingService;
            private final IFailureDetector failureDetector;
            private final AccordSyncPropagator.Listener listener;
            private final AccordSyncPropagator propagator;

            private Instace(Node.Id id,
                            InetAddressAndPort address,
                            ConfigService configurationService,
                            Sink messagingService,
                            IFailureDetector failureDetector,
                            AccordSyncPropagator.Listener listener,
                            AccordSyncPropagator propagator)
            {
                this.id = id;
                this.address = address;
                this.configurationService = configurationService;
                this.messagingService = messagingService;
                this.failureDetector = failureDetector;
                this.listener = listener;
                this.propagator = propagator;
            }
        }
    }
}

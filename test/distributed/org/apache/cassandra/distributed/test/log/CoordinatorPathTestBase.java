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

package org.apache.cassandra.distributed.test.log;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.util.ByteUtils;
import harry.util.TestRunner;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.log.PlacementSimulator.Transformations;
import org.apache.cassandra.gms.GossipDigestAck;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleSeedProvider;
import org.apache.cassandra.tcm.*;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.log.LogStorage;
import org.apache.cassandra.tcm.log.Replication;
import org.apache.cassandra.tcm.transformations.cms.Initialize;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.utils.AssertUtil;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.distributed.test.log.PlacementSimulator.Node;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.RefSimulatedPlacementHolder;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.SimulatedPlacementHolder;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.SimulatedPlacements;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.bootstrap_diffBased;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.leave_diffBased;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.replace_directly;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.replicate;
import static org.apache.cassandra.net.Verb.GOSSIP_DIGEST_ACK;
import static org.apache.cassandra.net.Verb.TCM_REPLICATION;

public abstract class CoordinatorPathTestBase extends FuzzTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorPathTestBase.class);

    public void coordinatorPathTest(TestRunner.ThrowingBiConsumer<Cluster, SimulatedCluster> test) throws Throwable
    {
        coordinatorPathTest(test, true);
    }

    public void coordinatorPathTest(TestRunner.ThrowingBiConsumer<Cluster, SimulatedCluster> test, boolean startCluster) throws Throwable
    {
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        String nodeUnderTest = "127.0.0.1";
        InetAddressAndPort nodeUnderTestAddr = InetAddressAndPort.getByName(nodeUnderTest + ":7012");

        String cmsNodeStr = "127.0.0.10";
        InetAddressAndPort cmsAddr = InetAddressAndPort.getByName(cmsNodeStr + ":7012");
        FBUtilities.setBroadcastInetAddressAndPort(cmsAddr);

        TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(10);
        try (Cluster cluster = builder().withNodes(1)
                                        .withConfig(cfg -> cfg.set("seed_provider", new ParameterizedClass(SimpleSeedProvider.class.getName(),
                                                                                                           Collections.singletonMap("seeds", cmsNodeStr + ":7012"))))
                                        .withTokenSupplier(tokenSupplier)
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(10, "dc0", "rack0"))
                                        .createWithoutStarting();
             SimulatedCluster simulatedCluster = new SimulatedCluster(cluster, tokenSupplier))
        {
            simulatedCluster.initWithFakeCms(cmsAddr, nodeUnderTestAddr);
            if  (startCluster)
                cluster.startup();
            test.accept(cluster, simulatedCluster);
        }
        finally
        {
            ClusterMetadataService.unsetInstance();
        }
    }

    public void cmsNodeTest(TestRunner.ThrowingBiConsumer<Cluster, SimulatedCluster> test) throws Throwable
    {
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(10);
        try (Cluster cluster = builder().withNodes(1)
                                        .withTokenSupplier(tokenSupplier)
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(10, "dc0", "rack0"))
                                        .createWithoutStarting();
             SimulatedCluster simulatedCluster = new SimulatedCluster(cluster, tokenSupplier))
        {
            // Note that in case of a CMS node teset we first start a cluster, and then initialize a simulated cluster.
            cluster.startup();
            simulatedCluster.init();
            test.accept(cluster, simulatedCluster);
        }
        finally
        {
            ClusterMetadataService.unsetInstance();
        }
    }


    @Override
    public Cluster.Builder builder() {
        return Cluster.build()
                      .withConfig(cfg -> cfg.set("request_timeout", String.format("%dms", TimeUnit.MINUTES.toMillis(10))));
    }

    public static BooleanSupplier atMostResponses(int maxResponses)
    {
        AtomicInteger responsesSoFar = new AtomicInteger();
        return () -> {
            int responses = responsesSoFar.incrementAndGet();
            return responses <= maxResponses;
        };
    }

    public static Function<Integer, BooleanSupplier> respondFrom(int... nodeIds)
    {
        Set<Integer> nodes = new HashSet<>();
        for (int idx : nodeIds)
            nodes.add(idx);

        return (Integer idx) -> () -> nodes.contains(idx);
    }

    public static <T> Future<T> async(AssertUtil.ThrowingSupplier<T> supplier)
    {
        AsyncPromise<T> future = new AsyncPromise<>();
        new Thread(() -> {
            try
            {
                future.setSuccess(supplier.get());
            }
            catch (Throwable t)
            {
                future.setFailure(t);
            }
        }).start();
        return future;
    }

    public static final Map<Class<?>, Verb> classToVerb = new HashMap<>();
    static
    {
        classToVerb.put(Replay.class, Verb.TCM_REPLAY_REQ);
        classToVerb.put(Commit.class, Verb.TCM_COMMIT_REQ);
    }

    protected static class RealSimulatedNode extends VirtualSimulatedNode implements Predicate<Message<?>>
    {
        protected final Supplier<Entry.Id> entryIdGen;
        protected final EnumMap<Verb, SimulatedAction<?, ?>> actions;
        protected final SimulatedCluster cluster;

        private RealSimulatedNode(SimulatedCluster simulatedCluster, Node node)
        {
            super(simulatedCluster.state, node);

            this.entryIdGen = new Entry.DefaultEntryIdGen();
            this.actions = new EnumMap<>(Verb.class);
            this.cluster = simulatedCluster;
        }

        public void initializeDefaultHandlers()
        {
            on(Verb.GOSSIP_DIGEST_SYN, new GossipSynAction(this));
            on(Verb.READ_REQ, new ReadAction(this));
            on(Verb.RANGE_REQ, new RangeReadAction(this));
            on(Verb.MUTATION_REQ, new MutationAction(this));
            on(Verb.TCM_NOTIFY_REQ, new LogNotifyAction(this));
            on(Verb.TCM_REPLICATION, new EmptyAction(this, Verb.TCM_REPLICATION));
            on(Verb.TCM_COMMIT_RSP, new EmptyAction(this, Verb.TCM_COMMIT_RSP));
            on(Verb.GOSSIP_DIGEST_ACK2, new EmptyAction(this, Verb.GOSSIP_DIGEST_ACK2));
            on(Verb.GOSSIP_SHUTDOWN, new EmptyAction(this, Verb.GOSSIP_SHUTDOWN));
            on(Verb.READ_REPAIR_REQ, new MutationAction(this));
        }

        public String toString()
        {
            return "RealSimulatedNode{" +
                   "id=" + node.idx() +
                   ", token=" + node.token() +
                   '}';
        }

        @Override
        public void register()
        {
            super.register();
            ClusterMetadataTestHelper.register(node.idx());
        }

        @Override
        public void join()
        {
            super.join();
            ClusterMetadataTestHelper.join(node.idx(), node.token());
        }

        @Override
        public void leave()
        {
            super.leave();
            ClusterMetadataTestHelper.leave(node.idx());
        }

        public void replace(int replaced)
        {
            super.replace();
            ClusterMetadataTestHelper.replace(replaced, node.idx());
        }

        @Override
        public ClusterMetadataTestHelper.JoinProcess lazyJoin()
        {
            ClusterMetadataTestHelper.JoinProcess virtual = super.lazyJoin();
            ClusterMetadataTestHelper.JoinProcess real = ClusterMetadataTestHelper.lazyJoin(node.idx(), node.token());
            return new ClusterMetadataTestHelper.JoinProcess()
            {
                public ClusterMetadataTestHelper.JoinProcess prepareJoin()
                {
                    virtual.prepareJoin();
                    real.prepareJoin();
                    return this;
                }

                public ClusterMetadataTestHelper.JoinProcess startJoin()
                {
                    virtual.startJoin();
                    real.startJoin();
                    return this;
                }

                public ClusterMetadataTestHelper.JoinProcess midJoin()
                {
                    virtual.midJoin();
                    real.midJoin();
                    return this;
                }

                public ClusterMetadataTestHelper.JoinProcess finishJoin()
                {
                    virtual.finishJoin();
                    real.finishJoin();
                    return this;
                }
            };
        }

        @Override
        public ClusterMetadataTestHelper.LeaveProcess lazyLeave()
        {
            ClusterMetadataTestHelper.LeaveProcess virtual = super.lazyLeave();
            ClusterMetadataTestHelper.LeaveProcess real = ClusterMetadataTestHelper.lazyLeave(node.idx());
            return new ClusterMetadataTestHelper.LeaveProcess()
            {
                public ClusterMetadataTestHelper.LeaveProcess prepareLeave()
                {
                    virtual.prepareLeave();
                    real.prepareLeave();
                    return this;
                }

                public ClusterMetadataTestHelper.LeaveProcess startLeave()
                {
                    virtual.startLeave();
                    real.startLeave();
                    return this;
                }

                public ClusterMetadataTestHelper.LeaveProcess midLeave()
                {
                    virtual.midLeave();
                    real.midLeave();
                    return this;
                }

                public ClusterMetadataTestHelper.LeaveProcess finishLeave()
                {
                    virtual.finishLeave();
                    real.finishLeave();
                    return this;
                }
            };
        }

        public void on(Verb verb, SimulatedAction<?,?> action)
        {
            SimulatedAction<?, ?> prev = actions.put(verb, action);
            assert prev == null : String.format("Already had a subscribed handler for " + verb);
        }

        /**
         * Serialize and send a single message from this (simulated) node to the real node in the simulated cluster,
         * and block/wait for its response.
         */
        public <IN, OUT> OUT requestResponse(IN payload)
        {
            AsyncPromise<OUT> resFuture = new AsyncPromise<>();
            Verb verb = classToVerb.get(payload.getClass()).responseVerb;
            return withTemporaryHandler(verb,
                                        new AbstractSimulatedAction<OUT, Void>(this)
                                        {
                                            public Message<Void> respondTo(Message<OUT> rsp)
                                            {
                                                resFuture.setSuccess(rsp.payload);
                                                return null;
                                            }

                                            @Override
                                            public Verb verb()
                                            {
                                                return verb;
                                            }
                                        },
                                        () -> {
                                            sendFrom(node.idx(), payload);
                                            try
                                            {
                                                return resFuture.get(10, TimeUnit.SECONDS);
                                            }
                                            catch (Throwable e)
                                            {
                                                throw new RuntimeException(e);
                                            }
                                        });
        }

        public <T> T withTemporaryHandler(Verb verb, SimulatedAction<?,?> action, Callable<T> r)
        {
            SimulatedAction<?, ?> prev = actions.put(verb, action);
            T res = null;
            try
            {
                res = r.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            if (prev != null)
            {
                SimulatedAction<?,?> tmp = actions.put(verb, prev);
                assert tmp == action;
            }
            else
            {
                actions.remove(verb);
            }
            return res;
        }

        public void clean(Verb verb)
        {
            actions.remove(verb);
        }

        /**
         * Deliver a single message from this (simulated) node to the real node in the simulated cluster. Communication
         * between the simulated nodes is transparent for the purposes of this test, so it was not implemented.
         */
        public void sendFrom(int from, Message<?> message)
        {
            try
            {
                cluster.realCluster.deliverMessage(cluster.realCluster.get(1).broadcastAddress(),
                                                   // todo: use addr from the node!
                                                   Instance.serializeMessage(ClusterMetadataTestHelper.addr(from),
                                                                             ClusterMetadataTestHelper.addr(1),
                                                                             message));
            }
            catch (Throwable t)
            {
                throw new IllegalStateException(String.format("Caught an error while delivering %s to %d", message, from),
                                                t);
            }
        }

        /**
         * Serialize and deliver a single payload from this (simulated) node to the the real node in the simulated cluster.
         * Communication between the simulated nodes is transparent for the purposes of this test, so it was not implemented.
         */
        public <T> void sendFrom(int from, T payload)
        {
            sendFrom(from,
                     Message.out(classToVerb.get(payload.getClass()), payload, Long.MAX_VALUE));
        }

        /**
         * Try delivering to one of the internally subscribed handlers
         */
        @SuppressWarnings("unchecked, rawtypes")
        public boolean test(Message<?> message)
        {
            SimulatedAction action = actions.get(message.verb());
            Assert.assertNotNull(String.format("Can't find an action that corresponds to verb %s", message.verb()), action);
            action.validate(message);
            Message<?> response = action.respondTo(message);
            if (response != null)
                sendFrom(response.from().addressBytes[3], response);

            return false;
        }

        /**
         * Executes {@param request},
         */
        public <IN, OUT> WaitingAction<IN, OUT> blockOnReplica(Function<RealSimulatedNode, SimulatedAction<IN,OUT>> factory)
        {
            WaitingAction<IN, OUT> waitingAction = new WaitingAction<>(() -> factory.apply(this));
            actions.put(waitingAction.verb(), waitingAction);
            return waitingAction;
        }

    }

    /**
     * A virtual simulated node, that can only be used to make predictions about the cluster state, and doesn't
     * add any handlers to the cluster state.
     */
    protected static class VirtualSimulatedNode implements ClusterMetadataTestHelper.NodeOperations
    {
        public final Node node;
        public final Predicate<Node> matcher;
        protected final SimulatedPlacementHolder ref;

        public VirtualSimulatedNode(SimulatedPlacementHolder ref, Node node)
        {
            this.ref = ref;
            this.node = node;
            this.matcher = (o) -> node.idx() == o.idx();
        }

        @Override
        public void register()
        {
            // TODO: mark that we've registered somewhere, it'd be good to validate that at some point
        }

        @Override
        public void join()
        {
            lazyJoin().prepareJoin()
                      .startJoin()
                      .midJoin()
                      .finishJoin();
        }

        @Override
        public void leave()
        {
            lazyLeave().prepareLeave()
                       .startLeave()
                       .midLeave()
                       .finishLeave();
        }

        public void replace()
        {
            lazyReplace().prepareReplace()
                         .startReplace()
                         .midReplace()
                         .finishReplace();
        }

        @Override
        public ClusterMetadataTestHelper.JoinProcess lazyJoin()
        {
            return new ClusterMetadataTestHelper.JoinProcess()
            {
                PlacementSimulator.Transformations steps = null;
                int idx = -1;
                public ClusterMetadataTestHelper.JoinProcess prepareJoin()
                {
                    assert idx == -1;
                    assert steps == null;

                    ModelChecker.Pair<SimulatedPlacements, PlacementSimulator.Transformations> res = bootstrap_diffBased(ref.get(), node.idx(), node.token());
                    ref.set(res.l);
                    steps = res.r;
                    ref.apply(steps);
                    idx++;
                    return this;
                }

                public ClusterMetadataTestHelper.JoinProcess startJoin()
                {
                    assert idx == 0;
                    ref.apply(steps);
                    idx++;
                    return this;
                }

                public ClusterMetadataTestHelper.JoinProcess midJoin()
                {
                    assert idx == 1;
                    ref.apply(steps);
                    idx++;
                    return this;
                }

                public ClusterMetadataTestHelper.JoinProcess finishJoin()
                {
                    assert idx == 2;
                    ref.apply(steps);
                    idx++;
                    assert !steps.hasNext();
                    return this;
                }
            };
        }

        @Override
        public ClusterMetadataTestHelper.LeaveProcess lazyLeave()
        {
            return new ClusterMetadataTestHelper.LeaveProcess()
            {
                Transformations steps = null;
                int idx = -1;
                public ClusterMetadataTestHelper.LeaveProcess prepareLeave()
                {
                    assert idx == -1;
                    assert steps == null;

                    ModelChecker.Pair<SimulatedPlacements, Transformations> res = leave_diffBased(ref.get(), node.token());
                    ref.set(res.l);
                    steps = res.r;
                    idx++;
                    return this;
                }

                public ClusterMetadataTestHelper.LeaveProcess startLeave()
                {
                    assert idx == 0;
                    ref.apply(steps);
                    idx++;
                    return this;
                }

                public ClusterMetadataTestHelper.LeaveProcess midLeave()
                {
                    assert idx == 1;
                    ref.apply(steps);
                    idx++;
                    return this;
                }

                public ClusterMetadataTestHelper.LeaveProcess finishLeave()
                {
                    assert idx == 2;
                    ref.apply(steps);
                    idx++;
                    return this;
                }
            };
        }

        public ClusterMetadataTestHelper.ReplaceProcess lazyReplace()
        {
            return new ClusterMetadataTestHelper.ReplaceProcess()
            {
                PlacementSimulator.Transformations steps = null;
                int idx = -1;
                public ClusterMetadataTestHelper.ReplaceProcess prepareReplace()
                {
                    assert idx == -1;
                    assert steps == null;

                    ModelChecker.Pair<SimulatedPlacements, PlacementSimulator.Transformations> res = replace_directly(ref.get(), node.token(), node.idx());
                    ref.set(res.l);
                    steps = res.r;
                    ref.apply(steps);
                    idx++;
                    return this;
                }

                public ClusterMetadataTestHelper.ReplaceProcess startReplace()
                {
                    assert idx == 0;
                    ref.apply(steps);
                    idx++;
                    return this;
                }

                public ClusterMetadataTestHelper.ReplaceProcess midReplace()
                {
                    assert idx == 1;
                    ref.apply(steps);
                    idx++;
                    return this;
                }

                public ClusterMetadataTestHelper.ReplaceProcess finishReplace()
                {
                    assert idx == 2;
                    ref.apply(steps);
                    idx++;
                    assert !steps.hasNext();
                    return this;
                }
            };
        }
    }

    public static class SimulatedCluster implements Closeable
    {
        protected final ICluster<IInvokableInstance> realCluster;
        protected final SimulatedPlacementHolder state;
        protected final Map<InetAddressAndPort, RealSimulatedNode> nodes;
        protected final TokenSupplier tokenSupplier;
        protected final IPartitioner partitioner = Murmur3Partitioner.instance;
        protected ExecutorPlus executor;

        public SimulatedCluster(ICluster<IInvokableInstance> realCluster, TokenSupplier tokenSupplier)
        {
            int rf = 3;
            this.tokenSupplier = tokenSupplier;

            InetAddressAndPort nodeUnderTestAddr = ClusterMetadataTestHelper.addr(1);
            Node nodeUnderTest = new Node(tokenSupplier.token(1), nodeUnderTestAddr.addressBytes[3]);
            List<Node> orig = Collections.singletonList(nodeUnderTest);
            this.state = new RefSimulatedPlacementHolder(new SimulatedPlacements(3,
                                                                                 Collections.singletonList(nodeUnderTest),
                                                                                 replicate(orig, rf),
                                                                                 replicate(orig, rf),
                                                                                 Collections.emptyList()));
            this.realCluster = realCluster;
            this.nodes = new HashMap<>();

            // We would like all messages directed to the node under test to be delivered it.
            this.nodes.put(nodeUnderTestAddr, new RealSimulatedNode(this, new Node(tokenSupplier.token(1), 1)) {
                @Override
                public boolean test(Message<?> message)
                {
                    realCluster.get(1).receiveMessage(Instance.serializeMessage(message.from(), nodeUnderTestAddr, message));
                    return true;
                }
            });
        }

        public void initWithFakeCms(InetAddressAndPort cms, InetAddressAndPort nodeUnderTest)
        {
            assert executor == null;
            LogStorage logStorage = new AtomicLongBackedProcessor.InMemoryStorage();
            LocalLog log = LocalLog.sync(new ClusterMetadata(partitioner), logStorage, false);

            // Replicator only replicates to the node under test, as there are no other nodes in reality
            Commit.Replicator replicator = (result, source) -> {
                realCluster.deliverMessage(realCluster.get(1).broadcastAddress(),
                                           Instance.serializeMessage(cms, nodeUnderTest,
                                                                     Message.out(Verb.TCM_REPLICATION, result.success().replication)));
            };

            AtomicLongBackedProcessor processor = new AtomicLongBackedProcessor(log);

            ClusterMetadataService service = new ClusterMetadataService(new UniformRangePlacement(),
                                                                        new AtomicLongBackedProcessor.InMemoryMetadataSnapshots(),
                                                                        log,
                                                                        processor,
                                                                        replicator,
                                                                        true);
            ClusterMetadataService.setInstance(service);

            log.bootstrap(cms);
            service.commit(new Initialize(log.metadata()));
            service.commit(new Register(new NodeAddresses(cms, cms, cms), new Location("dc0", "rack0"), NodeVersion.CURRENT));

            IVerbHandler<Commit> commitRequestHandler = Commit.handlerForTests(processor,
                                                                               replicator,
                                                                               (msg, to) -> realCluster.deliverMessage(to, Instance.serializeMessage(cms, to, msg)));
            executor = ExecutorFactory.Global.executorFactory().pooled("FakeMessaging", 10);

            realCluster.setMessageSink((target, msg) -> executor.submit(() -> {
                try
                {
                    Message<?> message = Instance.deserializeMessage(msg);
                    // Catch the messages from the node under test and forward them to the CMS
                    if (target.equals(cms))
                    {
                        switch (message.verb())
                        {
                            case TCM_DISCOVER_REQ:
                                Message<?> rsp = message.responseWith(new Discovery.DiscoveredNodes(Collections.singleton(cms), Discovery.DiscoveredNodes.Kind.CMS_ONLY));
                                realCluster.deliverMessage(message.from(),
                                                           Instance.serializeMessage(cms, message.from(), rsp));
                                return;
                            case TCM_COMMIT_REQ:
                            {
                                commitRequestHandler.doVerb((Message<Commit>) message);
                                return;
                            }
                            case TCM_REPLAY_REQ:
                            {
                                Replay request = (Replay) message.payload;
                                LogState logState = logStorage.getLogState(request.start);
                                realCluster.deliverMessage(message.from(),
                                                           Instance.serializeMessage(cms, message.from(), message.responseWith(logState)));
                                return;
                            }
                            default:
                                logger.error("Mocked CMS node has received message with {} verb: {}", msg.verb(), msg);
                        }
                    }
                    else
                    {
                        nodes.get(target).test(message);
                    }
                }
                catch (Throwable t)
                {
                    logger.error(String.format("Caught an exception while trying to deliver %s to %s", msg, target), t);
                }
            }));
        }

        public void init()
        {
            assert executor == null;

            // We need to create a second node to be able to send and receive messages.
            RealSimulatedNode driver = createNode();
            LocalLog log = LocalLog.sync(new ClusterMetadata(DatabaseDescriptor.getPartitioner()), LogStorage.None, false);

            ClusterMetadataService metadataService =
            new ClusterMetadataService(new UniformRangePlacement(),
                                       MetadataSnapshots.NO_OP,
                                       log,
                                       new Processor()
                                       {
                                           public Commit.Result commit(Entry.Id entryId, Transformation event, Epoch lastKnown)
                                           {
                                               if (lastKnown == null)
                                                   lastKnown = log.waitForHighestConsecutive().epoch;
                                               Commit.Result result = driver.requestResponse(new Commit(entryId, event, lastKnown));
                                               if (result.isSuccess())
                                               {
                                                   log.append(result.success().replication.entries());
                                                   log.waitForHighestConsecutive();
                                               }
                                               return result;
                                           }

                                           public ClusterMetadata replayAndWait()
                                           {
                                               Epoch since = log.waitForHighestConsecutive().epoch;
                                               LogState logState = driver.requestResponse(new Replay(since, true));
                                               log.append(logState);
                                               return log.waitForHighestConsecutive();
                                           }
                                       },
                                       (a,b) -> {},
                                       false);

            ClusterMetadataService.setInstance(metadataService);
            driver.clean(TCM_REPLICATION);
            driver.on(Verb.TCM_REPLICATION, new SimulatedAction<Replication, NoPayload>()
            {
                public Verb verb()
                {
                    return TCM_REPLICATION;
                }

                public void validate(Message<Replication> request)
                {
                    // no-op
                }

                public Message<NoPayload> respondTo(Message<Replication> request)
                {
                    for (Entry entry : request.payload.entries())
                        log.append(entry);
                    log.waitForHighestConsecutive();
                    return null;
                }
            });

            // We're using executor for blocking messages perpetually (at least for now). It is possible
            // to have a more efficient way of blocking, namely one with a queue and continuation or a singal,
            // but implementing this is just not feasible until the test harness proves itself useful.
            executor = ExecutorFactory.Global.executorFactory().pooled("FakeMessaging", 10);
            realCluster.setMessageSink((target, msg) -> executor.submit(() -> {
                try
                {
                    nodes.get(target).test(Instance.deserializeMessage(msg));
                }
                catch (Throwable t)
                {
                    logger.error(String.format("Caught an exception while trying to deliver %s to %s", msg, target), t);
                }
            }));
            driver.register();
        }

        public RealSimulatedNode createNode()
        {
            int idx = this.nodes.size() + 1;
            RealSimulatedNode node = new RealSimulatedNode(this, new Node(tokenSupplier.token(idx), idx));
            node.initializeDefaultHandlers();
            nodes.put(node.node.addr(), node);
            return node;
        }

        public VirtualSimulatedCluster asVirtual()
        {
            return new VirtualSimulatedCluster(state.fork(), tokenSupplier);
        }

        public RealSimulatedNode node(int i)
        {
            return nodes.get(ClusterMetadataTestHelper.addr(i));
        }

        public Optional<RealSimulatedNode> find(Predicate<RealSimulatedNode> predicate)
        {
            return nodes.values().stream().filter(predicate).findFirst();
        }

        public Stream<RealSimulatedNode> filter(Predicate<RealSimulatedNode> predicate)
        {
            return nodes.values().stream().filter(predicate);
        }

        public void waitForQuiescense()
        {
            Epoch waitFor = ClusterMetadataService.instance().log().waitForHighestConsecutive().epoch;
            realCluster.get(1).acceptsOnInstance((Long epoch) -> {
                try
                {
                    ClusterMetadataService.instance().awaitAtLeast(Epoch.create(epoch));
                }
                catch (InterruptedException | TimeoutException e)
                {
                    throw new RuntimeException(e);
                }
            }).accept(waitFor.getEpoch());
        }

        public void close()
        {
            executor.shutdownNow();
        }
    }

    public static class VirtualSimulatedCluster
    {
        protected final SimulatedPlacementHolder state;
        protected final List<VirtualSimulatedNode> nodes;
        protected final TokenSupplier tokenSupplier;

        public VirtualSimulatedCluster(SimulatedPlacementHolder state, TokenSupplier tokenSupplier)
        {
            this.state = state;
            this.tokenSupplier = tokenSupplier;
            this.nodes = new ArrayList<>();
            for (Node node : state.get().nodes)
                this.nodes.add(new VirtualSimulatedNode(state, node));
        }

        public VirtualSimulatedNode createNode()
        {
            int idx = nodes.size() + 1;
            VirtualSimulatedNode node = new VirtualSimulatedNode(state, new Node(tokenSupplier.token(idx), idx));
            nodes.add(node);
            return node;
        }

        public VirtualSimulatedNode node(int idx)
        {
            return nodes.get(idx - 1);
        }
    }

    public interface SimulatedAction<IN, OUT>
    {
        Verb verb();
        void validate(Message<IN> request);
        // Null returned by this method means we don't want to send a response
        Message<OUT> respondTo(Message<IN> request);
    }

    public static abstract class AbstractSimulatedAction<IN, OUT> implements SimulatedAction<IN, OUT>
    {
        protected final RealSimulatedNode node;

        public AbstractSimulatedAction(RealSimulatedNode node)
        {
            this.node = node;
        }

        public void validate(Message<IN> request) {}
        public abstract Message<OUT> respondTo(Message<IN> request);
    }


    public static class GossipSynAction extends AbstractSimulatedAction<GossipDigestSyn, GossipDigestAck>
    {
        public GossipSynAction(RealSimulatedNode node)
        {
            super(node);
        }

        @Override
        public Verb verb()
        {
            return Verb.GOSSIP_DIGEST_SYN;
        }

        @Override
        public void validate(Message<GossipDigestSyn> request)
        {
            // no-op
        }

        @Override
        public Message<GossipDigestAck> respondTo(Message<GossipDigestSyn> request)
        {
            return Message.out(GOSSIP_DIGEST_ACK, new GossipDigestAck(Collections.emptyList(), Collections.emptyMap()));
        }
    }

    public static class EmptyAction extends AbstractSimulatedAction<Object, Object>
    {
        private final Verb verb;
        public EmptyAction(RealSimulatedNode node, Verb verb)
        {
            super(node);
            this.verb = verb;
        }

        @Override
        public Verb verb()
        {
            return verb;
        }

        @Override
        public void validate(Message<Object> request)
        {
        }

        @Override
        public Message<Object> respondTo(Message<Object> request)
        {
            return null;
        }
    }

    public static class LogNotifyAction extends AbstractSimulatedAction<LogState, NoPayload>
    {
        public LogNotifyAction(RealSimulatedNode node)
        {
            super(node);
        }

        @Override
        public Verb verb()
        {
            return Verb.TCM_NOTIFY_REQ;
        }

        @Override
        public Message<NoPayload> respondTo(Message<LogState> request)
        {
            return request.emptyResponse();
        }
    }

    public static class ReadAction extends AbstractSimulatedAction<ReadCommand, ReadResponse>
    {
        private BooleanSupplier shouldRespond;

        public ReadAction(RealSimulatedNode node)
        {
            this(node, () -> true);
        }

        public ReadAction(RealSimulatedNode node, BooleanSupplier shouldRespond)
        {
            super(node);
            this.shouldRespond = shouldRespond;
        }

        @Override
        public Verb verb()
        {
            return Verb.READ_REQ;
        }

        @Override
        public void validate(Message<ReadCommand> request)
        {
            ReadCommand command = request.payload;
            assert command instanceof SinglePartitionReadCommand;
            SinglePartitionReadCommand sprc = (SinglePartitionReadCommand) command;

            Murmur3Partitioner.LongToken requestToken = (Murmur3Partitioner.LongToken) sprc.partitionKey().getToken();
            assert node.cluster.state.get().isReadReplicaFor(requestToken.token, node.matcher) :
            String.format("Node %s is not a read replica for %s. Replicas: %s",
                          node, requestToken.token, node.cluster.state.get().readReplicasFor(requestToken.token));
        }

        @Override
        public Message<ReadResponse> respondTo(Message<ReadCommand> request)
        {
            if (shouldRespond.getAsBoolean())
            {
                ReadCommand command = request.payload;
                return Message.remoteResponseForTests(request.id(),
                                                      node.node.addr(),
                                                      request.verb().responseVerb,
                                                      ReadResponse.createDataResponse(EmptyIterators.unfilteredPartition(command.metadata()),
                                                                                      command));
            }
            return null;
        }
    }

    public static class RangeReadAction extends AbstractSimulatedAction<ReadCommand, ReadResponse>
    {
        public RangeReadAction(RealSimulatedNode node)
        {
            super(node);
        }

        @Override
        public Verb verb()
        {
            return Verb.RANGE_REQ;
        }

        @Override
        public void validate(Message<ReadCommand> request)
        {
            ReadCommand command = request.payload;
            assert command instanceof PartitionRangeReadCommand;
            PartitionRangeReadCommand prrc = (PartitionRangeReadCommand) command;

            Murmur3Partitioner.LongToken leftToken = (Murmur3Partitioner.LongToken) prrc.dataRange().keyRange().left.getToken();
            Murmur3Partitioner.LongToken rightToken = (Murmur3Partitioner.LongToken) prrc.dataRange().keyRange().right.getToken();
            assert node.cluster.state.get().isReadReplicaFor(leftToken.token, rightToken.token, node.matcher);
        }

        @Override
        public Message<ReadResponse> respondTo(Message<ReadCommand> request)
        {
            ReadCommand command = request.payload;
            return request.responseWith(ReadResponse.createDataResponse(EmptyIterators.unfilteredPartition(command.metadata()),
                                                                        command));
        }
    }

    public static class MutationAction extends AbstractSimulatedAction<Mutation, NoPayload>
    {
        private BooleanSupplier shouldRespond;

        public MutationAction(RealSimulatedNode node)
        {
            this(node, () -> true);
        }

        @Override
        public Verb verb()
        {
            return Verb.MUTATION_REQ;
        }

        public MutationAction(RealSimulatedNode node, BooleanSupplier shouldRespond)
        {
            super(node);
            this.shouldRespond = shouldRespond;
        }

        public void validate(Message<Mutation> request)
        {
            Mutation command = request.payload;
            Murmur3Partitioner.LongToken requestToken = (Murmur3Partitioner.LongToken) command.key().getToken();
            assert node.cluster.state.get().isWriteTargetFor(requestToken.token, node.matcher) : String.format("Node %s is not a write target for %s. Write placements: %s",
                                                                                                               node.node.idx(), requestToken, node.cluster.state.get().writePlacementsFor(requestToken.token));
        }

        public Message<NoPayload> respondTo(Message<Mutation> request)
        {
            if (shouldRespond.getAsBoolean())
                return Message.remoteResponseForTests(request.id(), node.node.addr(), request.verb().responseVerb, NoPayload.noPayload);
            return null;
        }
    }

    /**
     * Waiting action is a way to block replicas right before responding to the coordinator.
     */
    public static class WaitingAction<IN, OUT> implements SimulatedAction<IN, OUT>
    {
        // TODO: switch from latches to signals
        private final CountDownLatch gotAtLeastOneMessage;
        private final CountDownLatch releaseWaitingThread;
        private final SimulatedAction<IN,OUT> action;

        public WaitingAction(Supplier<SimulatedAction<IN,OUT>> factory)
        {
            this.action = factory.get();
            this.gotAtLeastOneMessage = CountDownLatch.newCountDownLatch(1);
            this.releaseWaitingThread = CountDownLatch.newCountDownLatch(1);
        }

        @Override
        public Verb verb()
        {
            return action.verb();
        }

        public void validate(Message<IN> request)
        {
            action.validate(request);
        }

        public Message<OUT> respondTo(Message<IN> request)
        {
            gotAtLeastOneMessage.decrement();
            releaseWaitingThread.awaitUninterruptibly();
            return action.respondTo(request);
        }

        public void waitForMessage()
        {
            gotAtLeastOneMessage.awaitUninterruptibly();
        }

        public void resume()
        {
            releaseWaitingThread.decrement();
        }
    }

    public static long token(Object... pk)
    {
        return ((Murmur3Partitioner.LongToken) DatabaseDescriptor.getPartitioner().decorateKey(toBytes(pk)).getToken()).token;
    }

    public static ByteBuffer toBytes(Object... pk)
    {
        assert pk.length > 0;
        if (pk.length == 1)
            return ByteUtils.objectToBytes(pk[0]);
        ByteBuffer[] bbs = new ByteBuffer[pk.length];
        for (int i = 0; i < pk.length; i++)
        {
            bbs[i] = ByteUtils.objectToBytes(pk[i]);
        }
        return ByteUtils.compose(bbs);
    }
}
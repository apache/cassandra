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

package org.apache.cassandra.gms;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.SimulatedExecutorFactory;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.UnitConfigOverride;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.ICompactionManager;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.IValidationManager;
import org.apache.cassandra.repair.MockMessaging;
import org.apache.cassandra.repair.StreamExecutor;
import org.apache.cassandra.repair.TableRepairManager;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.EchoVerbHandler;
import org.apache.cassandra.service.SharedContext;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.mockito.Mockito;
import org.quicktheories.impl.JavaRandom;

import static accord.utils.Property.qt;
import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIP_DISABLE_THREAD_VALIDATION;
import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;

public class FuzzTest extends CQLTester.InMemory
{
    private static final Logger logger = LoggerFactory.getLogger(FuzzTest.class);

    static
    {
        // need to do a static block as later static fields will depend on this block, so @BeforeClass won't work
        ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);
        // when running in CI an external actor will replace the test configs based off the test type (such as trie, cdc, etc.), this could then have failing tests
        // that do not repo with the same seed!  To fix that, go to UnitConfigOverride and update the config type to match the one that failed in CI, this should then
        // use the same config, so the seed should not reproduce.
        UnitConfigOverride.maybeOverrideConfig();

        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance); // TOOD (coverage): random select
        DatabaseDescriptor.setLocalDataCenter("test");
        // messages are processed in a simulated executor, so these checks all fail
        GOSSIP_DISABLE_THREAD_VALIDATION.setBoolean(true);
    }

    @BeforeClass
    public static void setUpClass()
    {
        // junit will call CQLTester.InMemory.setUpClass BEFORE the static block, so by overriding it the setup gets
        // delayed until after the static block
        CQLTester.InMemory.setUpClass();
    }

    private static final Gen<Token> tokenGen = fromQT(CassandraGenerators.token(DatabaseDescriptor.getPartitioner()));
    private static final Gen<String> IDENTIFIER_GEN = fromQT(Generators.IDENTIFIER_GEN);
    private static final Gen<InetAddressAndPort> ADDRESS_W_PORT = fromQT(CassandraGenerators.INET_ADDRESS_AND_PORT_GEN);
    private static final Gen<UUID> hostIdGen = fromQT(Generators.UUID_RANDOM_GEN);

    enum Action { JOIN, LEAVE, RESTART, EXPAND, SHRINK, HOST_REPLACE, DONE }

    @Test
    public void test()
    {
        qt().withSeed(6899622177214304648L).withExamples(1).check(rs -> {
            int maxExpandNodes = 3;
            int maxShrinkNodes = 3;
            int maxRestartNodes = 3;
            int maxExpands = 5;
            int maxShrinks = 5;
            int maxHostReplacements = 5;
            int maxRestarts = 5;
//            int numNodes = rs.nextInt(50, 100);
            int numNodes = 3;
            Cluster cluster = new Cluster(rs, numNodes);
            Gen<Action> actionGen = new Gen<>()
            {
                int numHostReplacements = 0;
                int numExpands = 0;
                int numShrinks = 0;
                int numRestarts = 0;
                @Override
                public Action next(RandomSource r)
                {
                    if (!cluster.pendingJoin.isEmpty())
                        return Action.JOIN;
                    if (!cluster.pendingLeaving.isEmpty())
                        return Action.LEAVE;
                    int joinedSize = cluster.joinedSize();
                    List<Action> allowed = new ArrayList<>(3);
                    if (numExpands < maxExpands)
                        allowed.add(Action.EXPAND);
                    if (numShrinks < maxShrinks && joinedSize > 2)
                        allowed.add(Action.SHRINK);
                    if (numRestarts < maxRestarts && joinedSize > 2)
                        allowed.add(Action.RESTART);
                    if (numHostReplacements < maxHostReplacements)
                        allowed.add(Action.HOST_REPLACE);
                    if (allowed.isEmpty())
                        return Action.DONE;
                    Action action = rs.pick(allowed);
                    switch (action)
                    {
                        case EXPAND:
                            numExpands++;
                            return Action.EXPAND;
                        case SHRINK:
                            numShrinks++;
                            return Action.SHRINK;
                        case RESTART:
                            numRestarts++;
                            return Action.RESTART;
                        case HOST_REPLACE:
                            numHostReplacements++;
                            return Action.HOST_REPLACE;
                        default:
                            throw new AssertionError("Unexpected action: " + action);
                    }
                }
            };
            for (Cluster.Node node : cluster.nodes.values())
                node.setSeeds();
            int gen = cluster.nextGeneration();
            List<InetAddressAndPort> addresses = new ArrayList<>(cluster.nodes.keySet());
            addresses.sort(Comparator.naturalOrder());
            addresses.forEach(a -> cluster.nodes.get(a).start(gen));
            while (cluster.globalExecutor.processOne())
                cluster.checkFailures();

            int operation = 0; // mostly here for debugging, can add a break point and stop where things are acting up
            for (; true; operation++)
            {
                switch (actionGen.next(rs))
                {
                    case JOIN:
                        cluster.nodes.get(rs.pick(cluster.pendingJoin)).join();
//                        cluster.processSome();
                        cluster.awaitGossipSettles();
                        break;
                    case LEAVE:
                        cluster.nodes.get(rs.pick(cluster.pendingLeaving)).leave();
                        cluster.awaitGossipSettles();
//                        cluster.processSome();
                        break;
                    case EXPAND:
                        cluster.expand(rs.nextInt(1, Math.min(cluster.joinedSize(), maxExpandNodes) + 1));
                        cluster.awaitGossipSettles();
                        break;
                    case SHRINK:
                        cluster.shrink(rs.nextInt(1, Math.min(cluster.joinedSize(), maxShrinkNodes) - 1));
                        cluster.awaitGossipSettles();
                        break;
                    case RESTART:
                        cluster.restart(rs.nextInt(1, Math.min(cluster.joinedSize(), maxRestartNodes) - 1));
                        cluster.awaitGossipSettles();
                    case HOST_REPLACE:
                        Cluster.Node replaceNode = cluster.selectNodeToRemove();
                        replaceNode.stop();
                        Cluster.Node adding = cluster.nodeForHostReplacement(replaceNode);
                        adding.hostReplace(replaceNode);
                        adding.start(cluster.nextGeneration());
//                        cluster.processSome();
                        cluster.awaitGossipSettles();
                        break;
                    case DONE:
                        cluster.processAll();
                        return;
                }
            }
        });
    }

    private static String normalize(InetAddressAndPort a)
    {
        return a.getAddress().getHostAddress() + ":" + a.getPort();
    }

    static class Cluster
    {
        private enum Status
        {INIT, STARTING, JOINED, STOPPED, BEING_REPLACED, HOST_REPLACEMENT, LEAVING, LEFT, RESTARTING}

        class Node implements SharedContext
        {
            final int id = ++nodeCounter;
            final UUID hostId;
            private final InetAddressAndPort addressAndPort;
            private final Collection<Token> tokens;
            final MockMessaging<Node> messaging;
            Gossiper gossiper;
            StorageService storageService; // replaced on restart
            boolean joined = false;
            List<InetAddressAndPort> seeds = null;
            private Future<Future<?>> leavingFuture;
            private Status status;
            private final List<Status> statusChanges = new ArrayList<>();

            public Node(UUID hostId, InetAddressAndPort addressAndPort, Collection<Token> tokens)
            {
                this.hostId = hostId;
                this.addressAndPort = addressAndPort;
                this.tokens = tokens;
                this.messaging = new MockMessaging<>(this, failures::add, this::handler, a -> {
                    Node node = nodes.get(a);
                    return node == null ? null : node.messaging;
                });
                // TODO (now): mock
                // ctx.failureDetector().registerFailureDetectionEventListener(this);
                // JVMStabilityInspector.inspectThrowable(t);
                // GossiperDiagnostics
                this.gossiper = new Gossiper(this, false);
                // this is partially mocked... SS doesn't support mocking atm...
                // org.apache.cassandra.gms.Gossiper.isGossipOnlyMember -> StorageService.instance.getTokenMetadata()
                this.storageService = new StorageService(this);
                set(Status.INIT);
            }

            private Cluster cluster()
            {
                return Cluster.this;
            }

            private void set(Status status)
            {
                this.status = status;
                statusChanges.add(status);
            }

            private void setSeeds()
            {
                int size = rs.nextInt(1, Math.min(nodes.size(), 10));
                Gen<InetAddressAndPort> nodeGen = Gens.pick(nodes.keySet());
                seeds = Gens.lists(nodeGen.filter(a -> !addressAndPort.equals(a))).unique().ofSize(size).next(rs);
            }

            void join()
            {
                if (joined)
                    throw new AssertionError("Already joined");

                List<Pair<ApplicationState, VersionedValue>> states = new ArrayList<Pair<ApplicationState, VersionedValue>>();
                states.add(Pair.create(ApplicationState.TOKENS, storageService.valueFactory.tokens(tokens)));
                states.add(Pair.create(ApplicationState.STATUS_WITH_PORT, storageService.valueFactory.normal(tokens)));
                states.add(Pair.create(ApplicationState.STATUS, storageService.valueFactory.normal(tokens)));
                gossiper.addLocalApplicationStates(states);
                storageService.getTokenMetadata().updateNormalTokens(tokens, addressAndPort);
                // TODO (now): needed for host replacement
                if (replacing != null)
                {
                    Gossiper.runInGossipStageBlocking(() -> gossiper.replacedEndpoint(replacing.addressAndPort));
                    hostsBeingReplaced.remove(replacing.addressAndPort);
                    nodes.remove(replacing.addressAndPort);
                    replacing = null;
                }
                joined = true;
                set(Status.JOINED);
                pendingJoin.remove(addressAndPort);
            }

            void start(int generationNbr)
            {
                setSeedProvider();

                Map<ApplicationState, VersionedValue> appStates = new EnumMap<>(ApplicationState.class);
                storageService.getTokenMetadata().updateHostId(hostId, addressAndPort);
                VersionedValue.VersionedValueFactory valueFactory = storageService.valueFactory;
                if (replacing != null)
                    appStates.put(ApplicationState.TOKENS, storageService.valueFactory.tokens(tokens));
                appStates.put(ApplicationState.NET_VERSION, valueFactory.networkVersion());
                appStates.put(ApplicationState.HOST_ID, valueFactory.hostId(hostId));
                appStates.put(ApplicationState.NATIVE_ADDRESS_AND_PORT, valueFactory.nativeaddressAndPort(addressAndPort));
                // TODO (now, mock): this isn't mocked correctly...
                appStates.put(ApplicationState.RPC_ADDRESS, valueFactory.rpcaddress(FBUtilities.getJustBroadcastNativeAddress()));
                appStates.put(ApplicationState.RELEASE_VERSION, valueFactory.releaseVersion());
                appStates.put(ApplicationState.SSTABLE_VERSIONS, valueFactory.sstableVersions(storageService.sstablesTracker.versionsInUse()));
                // TODO (coverage): support replacing
                // TODO (coverage): support bootstrap complete
                // TODO (coverage): empty without status
                set(Status.STARTING);
                messaging.listen();
                gossiper.register(storageService);
                gossiper.start(generationNbr, appStates);
            }

            private void setSeedProvider()
            {
                if (seeds == null)
                    throw new AssertionError("Unknown seeds");
                DatabaseDescriptor.setSeedProvider(() -> seeds);
            }

            void stop()
            {
                set(Status.STOPPED);
                this.gossiper.stop();
                messaging.shutdown();
            }

            private final Map<Verb, IVerbHandler<?>> handlers = ImmutableMap.of(
            Verb.GOSSIP_DIGEST_SYN, new GossipDigestSynVerbHandler(this),
            Verb.GOSSIP_DIGEST_ACK, new GossipDigestAckVerbHandler(this),
            Verb.GOSSIP_DIGEST_ACK2, new GossipDigestAck2VerbHandler(this),
            Verb.ECHO_REQ, new EchoVerbHandler(this),
            Verb.GOSSIP_SHUTDOWN, new GossipShutdownVerbHandler(this)
            );

            IVerbHandler handler(Verb verb)
            {
                IVerbHandler<?> handler = handlers.get(verb);
                if (handler == null)
                    throw new IllegalStateException("Unknown handler for verb: " + verb);
                return handler;
            }

            @Override
            public InetAddressAndPort broadcastAddressAndPort()
            {
                return addressAndPort;
            }

            @Override
            public Supplier<Random> random()
            {
                return () -> rs.fork().asJdkRandom();
            }

            @Override
            public Clock clock()
            {
                return globalExecutor;
            }

            @Override
            public ExecutorFactory executorFactory()
            {
                return globalExecutor;
            }

            @Override
            public MBeanWrapper mbean()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ScheduledExecutorPlus optionalTasks()
            {
                return unorderedScheduled;
            }

            @Override
            public MessageDelivery messaging()
            {
                return messaging;
            }

            @Override
            public IFailureDetector failureDetector()
            {
                return failureDetector;
            }

            @Override
            public IEndpointSnitch snitch()
            {
                return snitch;
            }

            @Override
            public Gossiper gossiper()
            {
                return gossiper;
            }

            @Override
            public ICompactionManager compactionManager()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ActiveRepairService repair()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public IValidationManager validationManager()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public TableRepairManager repairManager(ColumnFamilyStore store)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public StreamExecutor streamExecutor()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public StorageService storageService()
            {
                return storageService;
            }

            private Node replacing = null;
            private Node replacedBy = null;

            public boolean isBeingReplaced()
            {
                return replacedBy != null;
            }

            public void hostReplace(Node replaceNode)
            {
                setSeedProvider();
                replaceNode.set(Status.BEING_REPLACED);
                set(Status.HOST_REPLACEMENT);
                replacing = replaceNode;
                replaceNode.replacedBy = this;
                Future<Map<InetAddressAndPort, EndpointState>> future = gossiper.doShadowRoundAsyncUnsafe();
                do
                {
                    processSome();
                }
                while (!future.isDone());
                Map<InetAddressAndPort, EndpointState> shadowRound;
                try
                {
                    shadowRound = future.get();
                }
                catch (InterruptedException e)
                {
                    throw new UncheckedInterruptedException(e);
                }
                catch (ExecutionException e)
                {
                    failures.add(e.getCause());
                    checkFailures();
                    throw new AssertionError("Unreachable...");
                }
                EndpointState state = shadowRound.get(replaceNode.addressAndPort);
                if (state == null)
                    throw new IllegalStateException("Unknown addres: " + replaceNode.addressAndPort);
                if (state.getApplicationState(ApplicationState.TOKENS) == null)
                    throw new IllegalStateException("Unable to find tokens for " + replaceNode.addressAndPort);
                // TODO (now): support `state.isEmptyWithoutStatus() && REPLACEMENT_ALLOW_EMPTY.getBoolean()`
            }

            public void startLeaving()
            {
                set(Status.LEAVING);
                pendingLeaving.add(addressAndPort);
                // we don't currently share the lock, so simulate gaps
                this.leavingFuture = unorderedScheduled.submit(() -> gossiper.addLocalApplicationState(ApplicationState.SEVERITY, storageService.valueFactory.severity(42)))
                                                       .flatMap(ignore -> unorderedScheduled.submit(() -> gossiper.addLocalApplicationState(ApplicationState.STATUS_WITH_PORT, storageService.valueFactory.leaving(tokens))))
                                                       .flatMap(ignore -> unorderedScheduled.submit(() -> gossiper.addLocalApplicationState(ApplicationState.STATUS, storageService.valueFactory.leaving(tokens))))
                                                       .map(ignore -> unorderedScheduled.submit(() -> storageService.getTokenMetadata().addLeavingEndpoint(addressAndPort)));
            }

            public void leave()
            {
                // duplicate leave... ignore
                if (leavingFuture == null)
                    return;
                leavingFuture.addCallback((s, f) -> {
                    set(Status.LEFT);
                    gossiper.stop();
                    nodes.remove(addressAndPort);
                    pendingLeaving.remove(addressAndPort);
                });
                leavingFuture = null;
            }

            public void restart()
            {
                restarting.add(addressAndPort);
                stop();
                set(Status.RESTARTING);
                int seconds = rs.nextInt(30, Math.toIntExact(TimeUnit.MINUTES.toSeconds(5)));
                unorderedScheduled.schedule(this::startupFromRestart, seconds, TimeUnit.SECONDS);
            }

            private void startupFromRestart()
            {
                setSeedProvider();

                restarting.remove(addressAndPort);
                TokenMetadata tm = storageService.getTokenMetadata();
                storageService = new StorageService(this);
                storageService.setTokenMetadataUnsafe(tm);
                gossiper = new Gossiper(this, false);
                for (InetAddressAndPort address : tm.getAllEndpoints())
                    gossiper.addSavedEndpoint(address);

                VersionedValue.VersionedValueFactory valueFactory = storageService.valueFactory;
                Map<ApplicationState, VersionedValue> appStates = new EnumMap<>(ApplicationState.class);
                appStates.put(ApplicationState.TOKENS, valueFactory.tokens(tokens));
                appStates.put(ApplicationState.STATUS_WITH_PORT, valueFactory.hibernate(true));
                appStates.put(ApplicationState.STATUS, valueFactory.hibernate(true));
                appStates.put(ApplicationState.NET_VERSION, valueFactory.networkVersion());
                appStates.put(ApplicationState.HOST_ID, valueFactory.hostId(hostId));
                appStates.put(ApplicationState.NATIVE_ADDRESS_AND_PORT, valueFactory.nativeaddressAndPort(addressAndPort));
                // TODO (now, mock): this isn't mocked correctly...
                appStates.put(ApplicationState.RPC_ADDRESS, valueFactory.rpcaddress(FBUtilities.getJustBroadcastNativeAddress()));
                appStates.put(ApplicationState.RELEASE_VERSION, valueFactory.releaseVersion());
                appStates.put(ApplicationState.SSTABLE_VERSIONS, valueFactory.sstableVersions(storageService.sstablesTracker.versionsInUse()));

                set(Status.JOINED);
                messaging.listen();
                gossiper.register(storageService);
                gossiper.start(nextGeneration(), appStates);
            }

            public String status()
            {
                return status.name() + " " + statusChanges;
            }

            @Override
            public String toString()
            {
                return "Node{" +
                       "id=" + id +
                       ", hostId=" + hostId +
                       ", address=" + normalize(addressAndPort) +
                       ", status=" + status() +
                       '}';
            }
        }


        private final IEndpointSnitch snitch = Mockito.mock(IEndpointSnitch.class);
        private final IFailureDetector failureDetector = Mockito.mock(IFailureDetector.class);
        private final List<Throwable> failures = new CopyOnWriteArrayList<>();
        final Map<InetAddressAndPort, Node> nodes;
        private final SimulatedExecutorFactory globalExecutor;
        final ScheduledExecutorPlus unorderedScheduled;
        final ExecutorPlus orderedExecutor;
        private final RandomSource rs;
        private final Set<InetAddressAndPort> pendingJoin = new HashSet<>();
        private final Set<InetAddressAndPort> pendingLeaving = new HashSet<>();
        private final Set<InetAddressAndPort> restarting = new HashSet<>();
        private final List<String> dcs;
        private final Set<Token> tokens = new HashSet<>();
        private final Set<UUID> hostIds = new HashSet<>();
        private final Set<UUID> hostsBeingReplaced = new HashSet<>();
        private int nodeCounter = 0;

        Cluster(RandomSource rs, int numNodes)
        {
            this.rs = rs;
            globalExecutor = new SimulatedExecutorFactory(rs, fromQT(Generators.TIMESTAMP_GEN.map(Timestamp::getTime)).mapToLong(TimeUnit.MILLISECONDS::toNanos).next(rs));
            orderedExecutor = globalExecutor.configureSequential("ignore").build();
            unorderedScheduled = globalExecutor.scheduled("ignored");

            // We run tests in an isolated JVM per class, so not cleaing up is safe... but if that assumption ever changes, will need to cleanup
//            Stage.GOSSIP.unsafeSetExecutor(orderedExecutor); // we some times block on this stage... so can not mock... its single threaded, so we get similar results...
            Stage.INTERNAL_RESPONSE.unsafeSetExecutor(unorderedScheduled);

            JVMStabilityInspector.setOnUnknown(failures::add);

            dcs = Gens.lists(IDENTIFIER_GEN).unique().ofSizeBetween(1, Math.min(10, numNodes)).next(rs);
            this.nodes = Maps.newHashMapWithExpectedSize(numNodes);
            for (int i = 0; i < numNodes; i++)
                expandOne();
        }

        public void processSome()
        {
            for (int i = 0; i < 20 && globalExecutor.processAny(); i++)
                checkFailures();
        }

        public Node selectNodeToRemove()
        {
            Node replaceNode;
            Set<InetAddressAndPort> alive = aliveAddresses();
            do
            {
                InetAddressAndPort replaceAddress = rs.pick(alive);
                replaceNode = nodes.get(replaceAddress);
            } while (replaceNode.isBeingReplaced());
            return replaceNode;
        }

        public Set<InetAddressAndPort> aliveAddresses()
        {
            Set<InetAddressAndPort> result = nodes.keySet();
            for (Set<InetAddressAndPort> other : Arrays.asList(pendingJoin, pendingLeaving, restarting))
                result = Sets.difference(result, other);
            return new TreeSet<>(result); // copy so that the returned value has a consistent ordering
        }

        public Collection<Node> alive()
        {
            return aliveAddresses().stream().map(nodes::get).collect(Collectors.toList());
        }

        public int joinedSize()
        {
            return nodes.size() - pendingJoin.size();
        }

        public void expand(int size)
        {
            for (int i = 0; i < size; i++)
            {
                Node node = expandOne();
                node.setSeeds();
                node.start(nextGeneration());
            }
        }

        public void shrink(int size)
        {
            for (int i = 0; i < size; i++)
            {
                Node node = selectNodeToRemove();
                node.startLeaving();
            }
        }

        public void restart(int size)
        {
            for (int i = 0; i < size; i++)
            {
                Node node = selectNodeToRemove();
                node.restart();
            }
        }

        private int nextGeneration()
        {
            return (int) (globalExecutor.currentTimeMillis() / 1000);
        }

        private Node expandOne()
        {
            InetAddressAndPort addressAndPort;
            while (nodes.containsKey(addressAndPort = ADDRESS_W_PORT.next(rs)))
            {
            }
            Token token;
            while (!tokens.add(token = tokenGen.next(rs)))
            {
            }
            UUID hostId;
            while (!hostIds.add(hostId = hostIdGen.next(rs)))
            {
            }

            String dc = rs.pick(dcs);
            String rack = "rack";
            Mockito.when(snitch.getDatacenter(Mockito.eq(addressAndPort))).thenReturn(dc);
            Mockito.when(snitch.getRack(Mockito.eq(addressAndPort))).thenReturn(rack);

            Node node = new Node(hostId, addressAndPort, Collections.singleton(token));
            nodes.put(addressAndPort, node);
            pendingJoin.add(addressAndPort);
            return node;
        }

        public void awaitGossipSettles()
        {
            processAll(); // make sure the queue is dealt with before checking; only scheduled tasks should be remaining
            Duration duration = Duration.ofMinutes(30);
            long deadline = duration.toNanos() + globalExecutor.nanoTime();
            Map<GossipInfo, List<Node>> view;
            // size=0 nodes are trying to startup most likely... so the ring is empty
            // size=1 all nodes agree
            while (!(view = gossipDistinctView()).isEmpty() && view.size() != 1)
            {
                long nowNanos = globalExecutor.nanoTime();
                if (nowNanos > deadline)
                    throw new AssertionError("Gossip did not settle within " + duration + ";\n" + gossipDetailedDiff(view));
                processSome();
            }
        }

        private void processAll()
        {
            while (globalExecutor.processOne())
                checkFailures();
        }

        private String gossipDetailedDiff(Map<GossipInfo, List<Node>> view)
        {
            if (view.size() == 1)
                return "";
            List<GossipInfo> infos = new ArrayList<>(view.keySet());
            GossipInfo first = infos.remove(0);
            StringBuilder sb = new StringBuilder();
            infos.forEach(i -> diff(first, i, sb));
            return sb.toString();
        }

        private void diff(GossipInfo left, GossipInfo right, StringBuilder sb)
        {
            Sets.SetView<InetAddressAndPort> notInRight = Sets.difference(left.instances.keySet(), right.instances.keySet());
            Sets.SetView<InetAddressAndPort> notInLeft = Sets.difference(right.instances.keySet(), left.instances.keySet());
            if (!notInRight.isEmpty())
                sb.append("Left has the following address not found in Right: ").append(notInRight.stream().map(a -> normalize(a) + " " + status(a)).collect(Collectors.toList())).append('\n');
            if (!notInLeft.isEmpty())
                sb.append("Right has the following address not found in Left: ").append(notInLeft.stream().map(a -> normalize(a) + " " + status(a)).collect(Collectors.toList())).append('\n');

            for (InetAddressAndPort address : Sets.intersection(left.instances.keySet(), right.instances.keySet()))
            {
                GossipInfo.Details l = left.instances.get(address);
                GossipInfo.Details r = right.instances.get(address);
                diff(address, l, r, sb);
            }
        }

        private String status(InetAddressAndPort addressAndPort)
        {
            Node node = nodes.get(addressAndPort);
            return node == null ? "removed" : node.status();
        }

        private void diff(InetAddressAndPort address, GossipInfo.Details left, GossipInfo.Details right, StringBuilder sb)
        {
            if (left.states.isEmpty() && !right.states.isEmpty())
            {
                sb.append("[address=").append(normalize(address)).append(", status=").append(status(address)).append("] ").append("Left is empty\n");
                return;
            }
            if (right.states.isEmpty() && !left.states.isEmpty())
            {
                sb.append("[address=").append(normalize(address)).append(", status=").append(status(address)).append("] ").append("Right is empty\n");
                return;
            }
            Sets.SetView<ApplicationState> notInRight = Sets.difference(left.states.keySet(), right.states.keySet());
            Sets.SetView<ApplicationState> notInLeft = Sets.difference(right.states.keySet(), left.states.keySet());
            if (!notInRight.isEmpty())
                sb.append("[address=").append(normalize(address)).append(", status=").append(status(address)).append("] ").append("Left has the following states not found in Right: ").append(notInRight).append('\n');
            if (!notInLeft.isEmpty())
                sb.append("[address=").append(normalize(address)).append(", status=").append(status(address)).append("] ").append("Right has the following states not found in Left: ").append(notInLeft).append('\n');

            for (ApplicationState key : Sets.intersection(left.states.keySet(), right.states.keySet()))
            {
                String l = left.states.get(key);
                String r = right.states.get(key);
                if (!l.equals(r))
                    sb.append("[address=").append(normalize(address)).append(", status=").append(status(address)).append(", state=").append(key).append("] ").append("Left and Right do not match: ").append(StringUtils.difference(l, r)).append('\n');
            }
        }

        private Map<GossipInfo, List<Node>> gossipDistinctView()
        {
            Map<GossipInfo, List<Node>> view = new LinkedHashMap<>();
            for (Node node : alive())
            {
                if (!node.gossiper.isEnabled())
                    continue;
                view.computeIfAbsent(create(node), ignore -> new ArrayList<>()).add(node);
            }
            return view;
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

        private Cluster.Node nodeForHostReplacement(Cluster.Node replaceNode)
        {
            InetAddressAndPort addressAndPort;
            while (nodes.containsKey(addressAndPort = ADDRESS_W_PORT.next(rs)))
            {

            }
            UUID hostId;
            while (!hostIds.add(hostId = hostIdGen.next(rs)))
            {
            }

            String dc = snitch.getDatacenter(replaceNode.addressAndPort);
            String rack = snitch.getRack(replaceNode.addressAndPort);
            Mockito.when(snitch.getDatacenter(Mockito.eq(addressAndPort))).thenReturn(dc);
            Mockito.when(snitch.getRack(Mockito.eq(addressAndPort))).thenReturn(rack);

            Node node = new Node(hostId, addressAndPort, replaceNode.tokens);
            node.setSeeds();
            nodes.put(addressAndPort, node);
            pendingJoin.add(addressAndPort);
            hostsBeingReplaced.add(replaceNode.hostId);
            return node;
        }
    }

    // TODO (now): remove copy/paste
    private static <T> Gen<T> fromQT(org.quicktheories.core.Gen<T> qt)
    {
        return rs -> {
            JavaRandom r = new JavaRandom(rs.asJdkRandom());
            return qt.generate(r);
        };
    }

    private static class GossipInfo
    {
        private final ImmutableMap<InetAddressAndPort, Details> instances;

        public GossipInfo(ImmutableMap<InetAddressAndPort, Details> map)
        {
            this.instances = map;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GossipInfo that = (GossipInfo) o;
            return Objects.equals(instances, that.instances);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(instances);
        }

        private static class Details
        {
            private final ImmutableMap<ApplicationState, String> states;

            public Details(ImmutableMap<ApplicationState, String> map)
            {
                this.states = map;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Details details = (Details) o;
                return Objects.equals(states, details.states);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(states);
            }
        }
    }

    private static GossipInfo create(Cluster.Node node)
    {
        ImmutableMap.Builder<InetAddressAndPort, GossipInfo.Details> builder = ImmutableMap.builder();
        for (Map.Entry<InetAddressAndPort, EndpointState> e : node.gossiper.endpointStateMap.entrySet())
        {
            // if removed, we don't care
            if (!node.cluster().nodes.containsKey(node.addressAndPort))
                continue;;
            GossipInfo.Details details = create(e.getValue());
            // TODO (now): should empty members be excluded?  we shouldn't gossip them...
            if (details.states.isEmpty())
                continue;
            builder.put(e.getKey(), details);
        }
        return new GossipInfo(builder.build());
    }

    private static GossipInfo.Details create(EndpointState state)
    {
        ImmutableMap.Builder<ApplicationState, String> builder = ImmutableMap.builder();
        Map<ApplicationState, VersionedValue> states = EndpointState.filterMajorVersion3LegacyApplicationStates(state.unsafeGetStates());
        for (Map.Entry<ApplicationState, VersionedValue> e : states.entrySet())
        {
            String value = e.getValue().value;
            if (e.getKey() == ApplicationState.TOKENS)
                value = "TOKENS";
            // TODO (now): remove this hack... we should fix shutdown
            if (VersionedValue.SHUTDOWN.equals(state.getStatus()))
                continue; // this is a hack to work around an issue where shutdown misses the join event, so not all nodes will ever agree...
            builder.put(e.getKey(), value);
        }
        return new GossipInfo.Details(builder.build());
    }
}

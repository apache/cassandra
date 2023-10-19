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

package org.apache.cassandra.simulator.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.ColumnSpec;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.generators.Surjections;
import harry.model.clock.OffsetClock;
import harry.model.sut.SystemUnderTest;
import harry.model.sut.TokenPlacementModel;
import harry.operations.Query;
import harry.runner.DefaultDataTracker;
import harry.visitors.GeneratingVisitor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.fuzz.HarryHelper;
import org.apache.cassandra.distributed.fuzz.InJvmSut;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.simulator.*;
import org.apache.cassandra.simulator.cluster.ClusterActionListener.NoOpListener;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.cluster.ClusterActions.Options;
import org.apache.cassandra.simulator.harry.HarryValidatingQuery;
import org.apache.cassandra.simulator.systems.Failures;
import org.apache.cassandra.simulator.systems.InterceptedExecution;
import org.apache.cassandra.simulator.systems.InterceptingExecutor;
import org.apache.cassandra.simulator.systems.SimulatedActionTask;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.simulator.systems.SimulatedTime;
import org.apache.cassandra.simulator.utils.KindOfSequence;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.sequences.AddToCMS;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.utils.CloseableIterator;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.net.Verb.*;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.TIME_LIMITED;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.UNLIMITED;
import static org.apache.cassandra.simulator.cluster.ClusterActions.Options.noActions;

public class HarrySimulatorTest
{
    private static final Logger logger = LoggerFactory.getLogger(HarrySimulatorTest.class);

    public static void main(String... args) throws Throwable
    {
        new HarrySimulatorTest().harryTest();
        System.exit(0);
    }

    @Test
    public void harryTest() throws Exception
    {
        int bootstrapNode1 = 4;
        int bootstrapNode2 = 8;
        int bootstrapNode3 = 12;
        int rowsPerPhase = 1000;
        int nodesPerDc = 3;
        StringBuilder rfString = new StringBuilder();
        Map<String, Integer> rfMap = new HashMap<>();
        for (int i = 0; i < 3; i++)
        {
            String dc = "dc" + i;
            rfMap.put(dc, 3);
            if (i > 0)
                rfString.append(", ");
            rfString.append("'").append(dc).append("'").append(" : ").append(nodesPerDc);
        }

        TokenPlacementModel.NtsReplicationFactor rf = new TokenPlacementModel.NtsReplicationFactor(rfMap);

        ConsistencyLevel cl = ALL;

        simulate(HarryHelper.defaultConfiguration()
                            .setSchemaProvider(new Configuration.SchemaProviderConfiguration()
                            {
                                private final Surjections.Surjection<SchemaSpec> schema = schemaSpecGen("harry", "tbl");
                                public SchemaSpec make(long l, SystemUnderTest systemUnderTest)
                                {
                                    return schema.inflate(l);
                                }
                            })

                            .setPartitionDescriptorSelector(new Configuration.DefaultPDSelectorConfiguration(2, 1))
                            .setClusteringDescriptorSelector(HarryHelper.singleRowPerModification().setMaxPartitionSize(100).build()),
                 (simulation) -> {
                     simulation.cluster.stream().forEach((IInvokableInstance i) -> {
                         simulation.simulated.failureDetector.markUp(i.config().broadcastAddress());
                     });

                     List<ActionSchedule.Work> work = new ArrayList<>();
                     work.add(work(run(() -> {
                         for (Map.Entry<String, List<TokenPlacementModel.Node>> e : simulation.nodeState.nodesByDc.entrySet())
                         {
                             List<TokenPlacementModel.Node> nodesInDc = e.getValue();
                             for (int i = 0; i < 3; i++)
                             {
                                 TokenPlacementModel.Node node = nodesInDc.get(i);
                                 simulation.nodeState.unsafeBootstrap(node);
                             }
                         }
                     })));
                     work.add(work(lazy(() -> simulation.clusterActions.initializeCluster(new ClusterActions.InitialConfiguration(simulation.nodeState.joined(), new int[0])))));
                     work.add(work(addToCMS(simulation.simulated, simulation.cluster, 5),
                                   addToCMS(simulation.simulated, simulation.cluster, 9)));
                     work.add(work(simulation.clusterActions.schemaChange(1,
                                                                          String.format("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', " + rfString + "};",
                                                                                        simulation.harryRun.schemaSpec.keyspace))));
                     work.add(work(simulation.clusterActions.schemaChange(1,
                                                                          simulation.harryRun.schemaSpec.compile().cql())));
                     work.add(work(HarrySimulatorTest.generate(rowsPerPhase, simulation, cl)));


                     simulation.cluster.stream().forEach(i -> {
                         work.add(work(lazy(simulation.simulated, i, () -> logger.info(ClusterMetadata.current().epoch.toString()))));
                     });
                     work.add(work(lazy(() -> validateAllLocal(simulation, simulation.nodeState.ring, rf))));

                     List<Integer> registeredNodes = new ArrayList<>(Arrays.asList(bootstrapNode1, bootstrapNode2, bootstrapNode3));
                     List<Integer> bootstrappedNodes = new ArrayList<>();
                     while (!registeredNodes.isEmpty() || !bootstrappedNodes.isEmpty())
                     {
                         boolean shouldBootstrap = simulation.simulated.random.decide(0.5f);
                         if (shouldBootstrap && registeredNodes.isEmpty())
                             shouldBootstrap = false;
                         if (!shouldBootstrap && bootstrappedNodes.isEmpty())
                             shouldBootstrap = true;

                         int node;
                         if (shouldBootstrap)
                         {
                             node = registeredNodes.remove(0);
                             long token = simulation.simulated.random.uniform(Long.MIN_VALUE, Long.MAX_VALUE);
                             work.add(work(addToCMS(simulation.simulated, simulation.cluster, node)));
                             work.add(work(ActionList.of(bootstrap(simulation.simulated, simulation.cluster, token, node)),
                                           generate(rowsPerPhase, simulation, cl)
                             ));
                             simulation.cluster.stream().forEach(i -> {
                                 work.add(work(lazy(simulation.simulated, i, () -> logger.info(ClusterMetadata.current().epoch.toString()))));
                             });
                             work.add(work(run(() -> simulation.nodeState.bootstrap(node, token))));
                             work.add(work(assertNodeState(simulation.simulated, simulation.cluster, node,NodeState.JOINED)));
                             bootstrappedNodes.add(node);
                         }
                         else
                         {
                             assert !bootstrappedNodes.isEmpty();
                             node = bootstrappedNodes.remove(0);
                             work.add(work(ActionList.of(decommission(simulation.simulated, simulation.cluster, node)),
                                           generate(rowsPerPhase, simulation, cl)
                             ));
                             simulation.cluster.stream().forEach(i -> {
                                 work.add(work(lazy(simulation.simulated, i, () -> logger.info(ClusterMetadata.current().epoch.toString()))));
                             });
                             work.add(work(run(() -> simulation.nodeState.decommission(node))));
                             work.add(work(assertNodeState(simulation.simulated, simulation.cluster, node,NodeState.LEFT)));
                         }
                         work.add(work(lazy(() -> validateAllLocal(simulation, simulation.nodeState.ring, rf))));
                         boolean tmp = shouldBootstrap;
                         work.add(work(run(() -> System.out.printf("Finished %s of %d and data validation!\n", tmp ? "bootstrap" : "decomission", node))));
                     }
                     work.add(work(run(() -> System.out.println("Finished!"))));

                     return arr(work.toArray(new ActionSchedule.Work[0]));
                 },
                 (config) -> config.set("cms.default_max_retries", 10)
                                   .set("cms.progress_barrier.min_consistency_level", ALL)
                                   .set("cms.progress_barrier.default_consistency_level", ALL)
                                   .set("cms.progress_barrier.timeout", "600000ms")
                                   // Backoff should be larger than read timeout, since otherwise we will simply saturate the stage with retries
                                   .set("cms.progress_barrier.backoff", "11000ms")
                                   .set("cms.await_timeout", "20s"),
                 arr(),
                 (config) -> config
                             .failures(new HaltOnError())
                             .threadCount(1000)
                             .readTimeoutNanos(SECONDS.toNanos(5))
                             .writeTimeoutNanos(SECONDS.toNanos(5))
                             .readTimeoutNanos(SECONDS.toNanos(10))
                             .nodes(12, 12)
                             .dcs(3, 3)
        );
    }

    /**
     * Harry simulation. Assumes no scheduler delays and has ideal networking conditions.
     *
     * Since this is generally used to test _different_ subsystems, we plug in failures into the component
     * we are testing to both reduce the noise and the surface for potential investigations. This also
     * has a nice side effect of making simulations slightly faster.
     */
    static abstract class HarrySimulation implements Simulation
    {
        protected final Configuration.ConfigurationBuilder harryConfig;
        protected final ClusterActions clusterActions;
        protected final SimulatedNodeState nodeState;
        protected final Run harryRun;

        protected final SimulatedSystems simulated;
        protected final RunnableActionScheduler scheduler;
        protected final Cluster cluster;

        public HarrySimulation(SimulatedSystems simulated, RunnableActionScheduler scheduler, Cluster cluster, Configuration.ConfigurationBuilder harryConfig)
        {
            this.simulated = simulated;
            this.scheduler = scheduler;
            this.cluster = cluster;

            this.harryConfig = harryConfig;
            Options options = noActions(cluster.size());
            this.clusterActions = new ClusterActions(simulated, cluster,
                                                     options, new NoOpListener(), new Debug(new EnumMap<>(Debug.Info.class), new int[0]));

            InJvmSut sut = new InJvmSut(cluster) {
                public void shutdown()
                {
                    // Let simulation shut down the cluster, as it uses `nanoTime`
                }
            };

            Configuration configuration = harryConfig.setClock(() -> new OffsetClock(1000)) // todo: potentially integrate approximate clock with Simulator
                                                     .setSUT(() -> sut)
                                                     .build();
            this.harryRun = configuration.createRun();
            this.nodeState = new SimulatedNodeState(this);
        }

        public void run()
        {
            try (CloseableIterator<?> iter = iterator())
            {
                while (iter.hasNext())
                    iter.next();
            }
        }

        public void close() throws Exception
        {
        }
    }

    static class HarrySimulationBuilder extends ClusterSimulation.Builder<HarrySimulation>
    {
        protected final Configuration.ConfigurationBuilder harryConfig;
        protected final Consumer<IInstanceConfig> configUpdater;

        protected final Function<HarrySimulation, ActionSchedule.Work[]> work;

        HarrySimulationBuilder(Configuration.ConfigurationBuilder harryConfig,
                               Consumer<IInstanceConfig> configUpdater,
                               Function<HarrySimulation, ActionSchedule.Work[]> work
        )
        {
            this.harryConfig = harryConfig;
            this.configUpdater = configUpdater;
            this.work = work;
        }

        public Map<Verb, FutureActionScheduler> perVerbFutureActionSchedulers(int nodeCount, SimulatedTime time, RandomSource random)
        {
            return HarrySimulatorTest.networkSchedulers(nodeCount, time, random);
        }

        public FutureActionScheduler futureActionScheduler(int nodeCount, SimulatedTime time, RandomSource random)
        {
            return new AlwaysDeliverNetworkScheduler(time, random);
        }

        @Override
        public ClusterSimulation<HarrySimulation> create(long seed) throws IOException
        {
            RandomSource random = new RandomSource.Default();
            random.reset(seed);
            this.harryConfig.setSeed(seed);

            return new ClusterSimulation<>(random, seed, 1, this,
                                           configUpdater,
                                           (simulated, scheduler, cluster, options) -> new HarrySimulation(simulated, scheduler, cluster, harryConfig)
                                           {

                                               @Override
                                               public CloseableIterator<?> iterator()
                                               {
                                                   // ok; and scheduler jitter is here
                                                   return new ActionSchedule(simulated.time, simulated.futureScheduler, () -> 0L, scheduler, work.apply(this));
                                               }
                                           });
        }
    }

    /**
     * Simulation entrypoint; syntax sugar for creating a simulation.
     */
    public static void simulate(Configuration.ConfigurationBuilder harryConfig,
                                Function<HarrySimulation, ActionSchedule.Work[]> work,
                                Consumer<IInstanceConfig> instanceConfigUpdater,
                                String[] properties,
                                Consumer<ClusterSimulation.Builder<HarrySimulation>> configure) throws IOException
    {
        try (WithProperties p = new WithProperties().with(properties))
        {
            SimulationTestBase.simulate(new HarrySimulationBuilder(harryConfig, instanceConfigUpdater, work),
                                        configure);
        };
    }

    /**
     * Custom network scheduler for testing TCM.
     */
    public static Map<Verb, FutureActionScheduler> networkSchedulers(int nodes, SimulatedTime time, RandomSource random)
    {
        Set<Verb> extremelyLossy = new HashSet<>(Arrays.asList(TCM_COMMIT_REQ, TCM_REPLICATION, TCM_NOTIFY_REQ,
                                                               TCM_CURRENT_EPOCH_REQ, TCM_FETCH_CMS_LOG_REQ, TCM_FETCH_PEER_LOG_REQ,
                                                               TCM_INIT_MIG_RSP, TCM_INIT_MIG_REQ, TCM_ABORT_MIG,
                                                               TCM_DISCOVER_RSP, TCM_DISCOVER_REQ));

        Set<Verb> somewhatSlow = new HashSet<>(Arrays.asList(BATCH_STORE_REQ, BATCH_STORE_RSP));

        Set<Verb> somewhatLossy = new HashSet<>(Arrays.asList(PAXOS2_COMMIT_REMOTE_REQ, PAXOS2_COMMIT_REMOTE_RSP, PAXOS2_PREPARE_RSP, PAXOS2_PREPARE_REQ, PAXOS2_PROPOSE_RSP, PAXOS2_PROPOSE_REQ,
                                                              PAXOS_PREPARE_RSP, PAXOS_PREPARE_REQ, PAXOS_PROPOSE_RSP, PAXOS_PROPOSE_REQ, PAXOS_COMMIT_RSP, PAXOS_COMMIT_REQ,
                                                              TCM_NOTIFY_RSP, TCM_FETCH_CMS_LOG_RSP, TCM_FETCH_PEER_LOG_RSP, TCM_COMMIT_RSP));

        Map<Verb, FutureActionScheduler> schedulers = new HashMap<>();
        for (Verb verb : Verb.values())
        {
            if (extremelyLossy.contains(verb))
                schedulers.put(verb, new FixedLossNetworkScheduler(nodes, random, time, KindOfSequence.UNIFORM, .2f, .3f));
            else if (somewhatLossy.contains(verb))
                schedulers.put(verb, new FixedLossNetworkScheduler(nodes, random, time, KindOfSequence.UNIFORM, .1f, .15f));
            else if (somewhatSlow.contains(verb))
                schedulers.put(verb, new AlwaysDeliverNetworkScheduler(time, random, TimeUnit.MILLISECONDS.toNanos(100)));
        }
        return schedulers;
    }

    public Action addToCMS(SimulatedSystems simulated, Cluster cluster, int node)
    {
        return new SimulatedActionTask("", Action.Modifiers.RELIABLE_NO_TIMEOUTS, Action.Modifiers.RELIABLE_NO_TIMEOUTS, null, simulated,
                                       new InterceptedExecution.InterceptedRunnableExecution((InterceptingExecutor) cluster.get(node).executor(),
                                                                                             cluster.get(node).transfer((IIsolatedExecutor.SerializableRunnable) () -> AddToCMS.initiate())));
    }

    /**
     * Creates an action that is equivalent to starting bootstrap on the node via nodetool bootstrap, or
     * simply starting a fresh node with a pre-configured token.
     */
    public static Action bootstrap(SimulatedSystems simulated, Cluster cluster, long token, int node)
    {
        IIsolatedExecutor.SerializableRunnable runnable = () -> {
            try
            {
                StorageService.instance.startup(() -> new PrepareJoin(ClusterMetadata.current().myNodeId(),
                                                                      Collections.singleton(new Murmur3Partitioner.LongToken(token)),
                                                                      ClusterMetadataService.instance().placementProvider(),
                                                                      true,
                                                                      true),
                                                true);
            }
            catch (Throwable t)
            {
                logger.error("Could not bootstrap. Interrupting simulation.", t);
                SimulatorUtils.failWithOOM();
            }
        };

        return new SimulatedActionTask("Bootstrap Node",
                                       Action.Modifiers.NONE,
                                       Action.Modifiers.NONE,
                                       null,
                                       simulated,
                                       new InterceptedExecution.InterceptedRunnableExecution((InterceptingExecutor) cluster.get(node).executor(),
                                                                                             cluster.get(node).transfer(runnable)));
    }

    public Action decommission(SimulatedSystems simulated, Cluster cluster, int node)
    {
        IIsolatedExecutor.SerializableRunnable runnable = () -> {
            try
            {
            StorageService.instance.decommission(false, false);
            }
            catch (Throwable t)
            {
                logger.error("Could not decommission. Interrupting simulation.", t);
                SimulatorUtils.failWithOOM();
            }
        };

        return new SimulatedActionTask("Decomission Node",
                                       Action.Modifiers.NONE,
                                       Action.Modifiers.NONE,
                                       null,
                                       simulated,
                                       new InterceptedExecution.InterceptedRunnableExecution((InterceptingExecutor) cluster.get(node).executor(),
                                                                                             cluster.get(node).transfer(runnable)));
    }

    public static Action run(Runnable run)
    {
        return new Actions.LambdaAction("", Action.Modifiers.RELIABLE_NO_TIMEOUTS, () -> {
            run.run();
            return ActionList.empty();
        });
    }

    public static Action lazy(SimulatedSystems simulated, IInvokableInstance instance, IIsolatedExecutor.SerializableRunnable runnable)
    {
        return new SimulatedActionTask("", Action.Modifiers.RELIABLE_NO_TIMEOUTS, Action.Modifiers.RELIABLE_NO_TIMEOUTS, null, simulated,
                                       new InterceptedExecution.InterceptedRunnableExecution((InterceptingExecutor) instance.executor(), instance.transfer(runnable)));
    }

    public static Action lazy(Supplier<Action> run)
    {
        return new Actions.LambdaAction("", Action.Modifiers.RELIABLE_NO_TIMEOUTS, () -> ActionList.of(run.get()));
    }

    private static Action assertNodeState(SimulatedSystems simulated, Cluster cluster, int i, NodeState expected)
    {
        return lazy(simulated, cluster.get(i),
                    () -> {
                        NodeState actual = ClusterMetadata.current().myNodeState();
                        if (!actual.toString().equals(expected.toString()))
                        {
                            logger.info("Node {} state ({}) is not as expected {}", i, actual, expected);
                            SimulatorUtils.failWithOOM();
                        }
                    });
    }

    /**
     * Creates an action list with a fixed number of data-generating operations that conform to the given Harry configuration.
     */
    public static ActionList generate(int ops, HarrySimulation simulation, ConsistencyLevel cl)
    {
        Action[] actions = new Action[ops];
        OrderOn orderOn = new OrderOn.Strict(actions, 2);
        generate(ops, simulation, new Consumer<Action>()
                 {
                     int i = 0;

                     public void accept(Action action)
                     {
                         actions[i++] = action;
                     }
                 },
                 cl);
        return ActionList.of(actions).orderOn(orderOn);
    }

    public static void generate(int ops, HarrySimulation simulation, Consumer<Action> add, org.apache.cassandra.distributed.api.ConsistencyLevel cl)
    {
        SimulatedVisitExectuor visitExectuor = new SimulatedVisitExectuor(simulation,
                                                                          simulation.harryRun,
                                                                          cl);
        GeneratingVisitor generatingVisitor = new GeneratingVisitor(simulation.harryRun, visitExectuor);

        for (int i = 0; i < ops; i++)
        {
            generatingVisitor.visit(simulation.harryRun.clock.nextLts());
            // A tiny chance of executing a multi-partition batch
            if (ops % 10 == 0)
                generatingVisitor.visit(simulation.harryRun.clock.nextLts());
            add.accept(visitExectuor.build());
        }

    }

    /**
     * Create an infinite stream to generate data.
     */
    public static Supplier<Action> generate(HarrySimulation simulation, org.apache.cassandra.distributed.api.ConsistencyLevel cl)
    {
        SimulatedVisitExectuor visitExectuor = new SimulatedVisitExectuor(simulation,
                                                                          simulation.harryRun,
                                                                          cl);
        GeneratingVisitor generatingVisitor = new GeneratingVisitor(simulation.harryRun, visitExectuor);

        DefaultDataTracker tracker = (DefaultDataTracker) simulation.harryRun.tracker;
        return new Supplier<Action>()
        {
            public Action get()
            {
                // Limit how many queries can be in-flight simultaneously to reduce noise
                if (tracker.maxStarted() - tracker.maxConsecutiveFinished() == 0)
                {
                    generatingVisitor.visit();
                    return visitExectuor.build();
                }
                else
                {
                    // No-op
                    return run(() -> {});
                }
            }

            public String toString()
            {
                return "Query Generator";
            }
        };
    }

    /**
     * Given you have used `generate` methods to generate data with Harry, you can use this method to check whether all
     * data has been propagated everywhere it should be, be it via streaming, read repairs, or regular writes.
     */
    public static Action validateAllLocal(HarrySimulation simulation, List<TokenPlacementModel.Node> owernship, TokenPlacementModel.ReplicationFactor rf)
    {
        return new Actions.LambdaAction("Validate", Action.Modifiers.RELIABLE_NO_TIMEOUTS,
                                        () -> {
                                            if (!simulation.harryRun.tracker.isFinished(simulation.harryRun.tracker.maxStarted()))
                                                throw new IllegalStateException("Can not begin validation, as writing has not quiesced yet: " + simulation.harryRun.tracker);
                                            List<Action> actions = new ArrayList<>();
                                            long maxLts = simulation.harryRun.tracker.maxStarted();
                                            long maxPosition = simulation.harryRun.pdSelector.maxPosition(maxLts);
                                            logger.info("Starting validation of {} written partitions. Highest LTS is {}. Ring view: {}", maxPosition, maxLts, simulation.nodeState);
                                            for (int position = 0; position < maxPosition; position++)
                                            {
                                                long minLts = simulation.harryRun.pdSelector.minLtsAt(position);
                                                long pd = simulation.harryRun.pdSelector.pd(minLts, simulation.harryRun.schemaSpec);
                                                Query query = Query.selectPartition(simulation.harryRun.schemaSpec, pd, false);
                                                actions.add(new HarryValidatingQuery(simulation.simulated, simulation.cluster, rf,
                                                                                     simulation.harryRun, owernship, query));
                                            }
                                            return ActionList.of(actions).setStrictlySequential();
                                        });
    }

    // todo rename
    private static ActionSchedule.Work work(Action... actions)
    {
        return new ActionSchedule.Work(UNLIMITED, Collections.singletonList(ActionList.of(actions).setStrictlySequential()));
    }

    private static ActionSchedule.Work work(ActionList... actions)
    {
        return new ActionSchedule.Work(UNLIMITED, Arrays.asList(actions));
    }

    private static ActionSchedule.Work interleave(long nanos, Action... actions)
    {
        return new ActionSchedule.Work(TIME_LIMITED, nanos, Collections.singletonList(ActionList.of(actions)));
    }

    private static ActionSchedule.Work interleave(Action... actions)
    {
        return new ActionSchedule.Work(UNLIMITED, Collections.singletonList(ActionList.of(actions)));
    }


    /**
     * Simple simulated node state. Used to closely track what is going on in the cluster and
     * model placements for node-local validation.
     */
    public static class SimulatedNodeState
    {
        public final TokenPlacementModel.Node[] nodesLookup;
        public final List<TokenPlacementModel.Node> ring;
        public final Map<String, List<TokenPlacementModel.Node>> nodesByDc;
        public final Map<String, Integer> idByAddr;

        public SimulatedNodeState(HarrySimulation simulation)
        {
            this.nodesLookup = new TokenPlacementModel.Node[simulation.cluster.size()];
            this.nodesByDc = new HashMap<>();
            this.idByAddr = new HashMap<>();
            for (int i = 0; i < simulation.cluster.size(); i++)
            {
                int nodeId = i + 1;
                IInstanceConfig config = simulation.cluster.get(nodeId).config();

                InetAddressAndPort addr = InetAddressAndPort.getByAddress(config.broadcastAddress());
                TokenPlacementModel.Node node = new TokenPlacementModel.Node(Long.parseLong(config.getString("initial_token")),
                                                                             addr.toString(),
                                                                             new TokenPlacementModel.Location(simulation.clusterActions.snitch.get().getDatacenter(addr),
                                                                                                              simulation.clusterActions.snitch.get().getRack(addr)));
                nodesLookup[i] = node;
                nodesByDc.computeIfAbsent(node.location.dc, (k) -> new ArrayList<>()).add(node);
                idByAddr.put(addr.toString(), config.num());
            }
            this.ring = new ArrayList<>();
        }

        public int[] joined()
        {
            int[] joined = new int[ring.size()];
            for (int i = 0; i < ring.size(); i++)
            {
                joined[i] = idByAddr.get(ring.get(i).id);
            }
            return joined;
        }

        public void unsafeBootstrap(int nodeId)
        {
            TokenPlacementModel.Node n = nodesLookup[nodeId - 1];
            int idx = Collections.binarySearch(ring, n);
            if (idx < 0)
                ring.add(-idx - 1, n);
            else
                ring.set(idx, n);
        }

        public void unsafeBootstrap(TokenPlacementModel.Node n)
        {
            int idx = Collections.binarySearch(ring, n);
            if (idx < 0)
                ring.add(-idx - 1, n);
            else
                ring.set(idx, n);
        }

        public void bootstrap(int nodeId, long token)
        {
            System.out.printf("Marking %d as bootstrapped%n", nodeId);
            TokenPlacementModel.Node n = nodesLookup[nodeId - 1];
            // Update token in the lookup. Do it before search as we need ring sorted
            n = withToken(n, token);
            nodesLookup[nodeId - 1] = n;

            int idx = Collections.binarySearch(ring, n);

            if (idx < 0)
                ring.add(-idx - 1, n);
            else
                ring.set(idx, n);

            // Assert sorted
            TokenPlacementModel.Node prev = null;
            for (TokenPlacementModel.Node node : ring)
            {
                if (prev != null)
                    assert node.token > prev.token : "Ring doesn't seem to be sorted: " + ring;
                prev = node;
            }
        }

        public void decommission(int nodeId)
        {
            System.out.printf("Marking %d as decomissioned%n", nodeId);
            TokenPlacementModel.Node n = nodesLookup[nodeId - 1];
            int idx = Collections.binarySearch(ring, n);
            ring.remove(idx);
            assertSorted(ring);
        }

        private static void assertSorted(List<TokenPlacementModel.Node> ring)
        {
            // Assert sorted
            TokenPlacementModel.Node prev = null;
            for (TokenPlacementModel.Node node : ring)
            {
                if (prev != null)
                    assert node.token > prev.token : "Ring doesn't seem to be sorted: " + ring;
                prev = node;
            }

        }
        private static TokenPlacementModel.Node withToken(TokenPlacementModel.Node node, long token)
        {
            return new TokenPlacementModel.Node(token, node.id, node.location);
        }

        public String toString()
        {
            return "SimulatedNodeState{" +
                   "ring=" + ring +
                   '}';
        }
    }

    public static <T> T[] arr(T... arr)
    {
        return arr;
    }

    // Use only types that can guarantee we will et 64 bits of entropy here, at least for now.
    public static Surjections.Surjection<SchemaSpec> schemaSpecGen(String keyspace, String prefix)
    {
        AtomicInteger counter = new AtomicInteger();
        return new SchemaGenerators.Builder(keyspace, () -> prefix + counter.getAndIncrement())
               .partitionKeySpec(1, 2,
                                 ColumnSpec.int64Type,
                                 ColumnSpec.asciiType,
                                 ColumnSpec.textType)
               .clusteringKeySpec(1, 1,
                                  ColumnSpec.int64Type,
                                  ColumnSpec.asciiType,
                                  ColumnSpec.textType,
                                  ColumnSpec.ReversedType.getInstance(ColumnSpec.int64Type),
                                  ColumnSpec.ReversedType.getInstance(ColumnSpec.asciiType),
                                  ColumnSpec.ReversedType.getInstance(ColumnSpec.textType))
               .regularColumnSpec(5, 5,
                                  ColumnSpec.int64Type,
                                  ColumnSpec.asciiType(4, 128))
               .staticColumnSpec(5, 5,
                                 ColumnSpec.int64Type,
                                 ColumnSpec.asciiType(4, 128))
               .surjection();
    }

    public static class HaltOnError extends Failures
    {
        @Override
        public void onFailure(Throwable t)
        {
            super.onFailure(t);
            t.printStackTrace();
            logger.error("Caught an exception, going to halt", t);
            //while (true) { }
        }
    }
}

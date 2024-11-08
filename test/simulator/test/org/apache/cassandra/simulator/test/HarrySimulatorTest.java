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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.inject.Inject;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.impl.Query;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.gen.OperationsGenerators;
import org.apache.cassandra.harry.execution.CompiledStatement;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.execution.QueryBuildingVisitExecutor;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.model.TokenPlacementModel;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.ActionSchedule;
import org.apache.cassandra.simulator.Actions;
import org.apache.cassandra.simulator.AlwaysDeliverNetworkScheduler;
import org.apache.cassandra.simulator.ClusterSimulation;
import org.apache.cassandra.simulator.Debug;
import org.apache.cassandra.simulator.FixedLossNetworkScheduler;
import org.apache.cassandra.simulator.FutureActionScheduler;
import org.apache.cassandra.simulator.OrderOn;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.RunnableActionScheduler;
import org.apache.cassandra.simulator.Simulation;
import org.apache.cassandra.simulator.SimulationException;
import org.apache.cassandra.simulator.SimulationRunner;
import org.apache.cassandra.simulator.SimulatorUtils;
import org.apache.cassandra.simulator.cluster.ClusterActionListener.NoOpListener;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.cluster.ClusterActions.Options;
import org.apache.cassandra.simulator.systems.Failures;
import org.apache.cassandra.simulator.systems.InterceptedExecution;
import org.apache.cassandra.simulator.systems.InterceptingExecutor;
import org.apache.cassandra.simulator.systems.SimulatedActionCallable;
import org.apache.cassandra.simulator.systems.SimulatedActionTask;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.simulator.systems.SimulatedTime;
import org.apache.cassandra.simulator.utils.KindOfSequence;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Startup;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.sequences.SingleNodeSequences;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.harry.model.TokenPlacementModel.constantLookup;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.UNLIMITED;
import static org.apache.cassandra.simulator.cluster.ClusterActions.Options.noActions;


/**
 * In order to run these tests in your IDE, you need to first build a simulator jara
 *
 *    ant simulator-jars
 *
 * And then run your test using the following settings (omit add-* if you are running on jdk8):
 *
        -Dstorage-config=/Users/ifesdjeen/p/java/cassandra-accord/test/conf
        -Djava.awt.headless=true
        -javaagent:/Users/ifesdjeen/p/java/cassandra-accord/lib/jamm-0.4.0.jar
        -ea
        -Dcassandra.debugrefcount=true
        -Xss384k
        -XX:SoftRefLRUPolicyMSPerMB=0
        -XX:ActiveProcessorCount=2
        -XX:HeapDumpPath=build/test
        -Dcassandra.test.driver.connection_timeout_ms=10000
        -Dcassandra.test.driver.read_timeout_ms=24000
        -Dcassandra.memtable_row_overhead_computation_step=100
        -Dcassandra.test.use_prepared=true
        -Dcassandra.test.sstableformatdevelopment=true
        -Djava.security.egd=file:/dev/urandom
        -Dcassandra.testtag=.jdk11
        -Dcassandra.keepBriefBrief=true
        -Dcassandra.allow_simplestrategy=true
        -Dcassandra.strict.runtime.checks=true
        -Dcassandra.reads.thresholds.coordinator.defensive_checks_enabled=true
        -Dcassandra.test.flush_local_schema_changes=false
        -Dcassandra.test.messagingService.nonGracefulShutdown=true
        -Dcassandra.use_nix_recursive_delete=true
        -Dcie-cassandra.disable_schema_drop_log=true
        -Dlogback.configurationFile=file:///Users/ifesdjeen/p/java/cassandra-accord/test/conf/logback-simulator.xml
        -Dcassandra.ring_delay_ms=10000
        -Dcassandra.tolerate_sstable_size=true
        -Dcassandra.skip_sync=true
        -Dcassandra.debugrefcount=false
        -Dcassandra.test.simulator.determinismcheck=strict
        -Dcassandra.test.simulator.print_asm=none
        -javaagent:/Users/ifesdjeen/p/java/cassandra-accord/build/test/lib/jars/simulator-asm.jar
        -Xbootclasspath/a:/Users/ifesdjeen/p/java/cassandra-accord/build/test/lib/jars/simulator-bootstrap.jar
        -XX:ActiveProcessorCount=4
        -XX:-TieredCompilation
        -XX:-BackgroundCompilation
        -XX:CICompilerCount=1
        -XX:Tier4CompileThreshold=1000
        -XX:ReservedCodeCacheSize=256M
        -Xmx16G
        -Xmx4G
        --add-exports java.base/jdk.internal.misc=ALL-UNNAMED
        --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
        --add-exports java.base/sun.nio.ch=ALL-UNNAMED
        --add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED
        --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED
        --add-exports java.rmi/sun.rmi.server=ALL-UNNAMED
        --add-exports java.sql/java.sql=ALL-UNNAMED
        --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED
        --add-opens java.base/java.lang.module=ALL-UNNAMED
        --add-opens java.base/java.net=ALL-UNNAMED
        --add-opens java.base/jdk.internal.loader=ALL-UNNAMED
        --add-opens java.base/jdk.internal.ref=ALL-UNNAMED
        --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED
        --add-opens java.base/jdk.internal.math=ALL-UNNAMED
        --add-opens java.base/jdk.internal.module=ALL-UNNAMED
        --add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED
        --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED
        --add-opens jdk.management.jfr/jdk.management.jfr=ALL-UNNAMED
        --add-opens java.desktop/com.sun.beans.introspect=ALL-UNNAMED
 */
@Command(name = "harry", description = "Harry simulation test")
public class HarrySimulatorTest
{
    private static final Logger logger = LoggerFactory.getLogger(HarrySimulatorTest.class);

    @Inject
    public HelpOption helpOption;
    @Option(name = { "-r", "--rows-per-phase"}, description = "Number of rows to check at each phase of the test")
    public int rowsPerPhase = 10;
    @Option(name = {"--nodes-per-dc"}, description = "How many nodes per dc for replication")
    public int nodesPerDc = 3;
    @Option(name = {"-s", "--seed"}, title = "0x", description = "What seed to run with; in hex format... example: 0x190e6ff01d6")
    public String seed = null;

    public static void main(String... args) throws Throwable
    {
        HarrySimulatorTest test = SingleCommand.singleCommand(HarrySimulatorTest.class).parse(args);
        if (test.helpOption.showHelpIfRequested())
            return;
        test.harryTest();
        System.exit(1);
    }

    @Test
    public void test() throws Exception
    {
        // To rerun a failing test for a given seed, uncomment the below and set the seed
//        this.seed = "<your seed here>";
        this.seed = "0xdd3bb3793a6b925a";
        harryTest();
    }

    private void harryTest() throws Exception
    {
        int bootstrapNode1 = 4;
        int bootstrapNode2 = 8;
        int bootstrapNode3 = 12;

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

        simulate((config) -> config
                             .failures(new HaltOnError())
                             .threadCount(1000)
                             .readTimeoutNanos(SECONDS.toNanos(5))
                             .writeTimeoutNanos(SECONDS.toNanos(5))
                             .readTimeoutNanos(SECONDS.toNanos(10))
                             .nodes(12, 12)
                             .dcs(3, 3),
                 (config) -> config.set("cms_default_max_retries", 100)
                                   .set("request_timeout", "10000ms")
                                   .set("progress_barrier_min_consistency_level", ALL)
                                   .set("progress_barrier_default_consistency_level", ALL)
                                   .set("progress_barrier_timeout", "600000ms")
                                   // Backoff should be larger than read timeout, since otherwise we will simply saturate the stage with retries
                                   .set("progress_barrier_backoff", "1000ms")
                                   .set("cms_await_timeout", "600000ms"),
                 arr(),
                 (simulation) -> {
                     simulation.cluster.stream().forEach((IInvokableInstance i) -> {
                         simulation.simulated.failureDetector.markUp(i.config().broadcastAddress());
                     });

                     List<ActionSchedule.Work> work = new ArrayList<>();
                     work.add(work("Set up", run(() -> {
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
                     work.add(work("Initial configuration",
                                   lazy(() -> simulation.clusterActions.initializeCluster(new ClusterActions.InitialConfiguration(simulation.nodeState.joined(), new int[0])))));

                     work.add(work("Reconfigure CMS",
                                   reconfigureCMS(simulation.simulated, simulation.cluster, 2, true)));
                     work.add(work("Create Keyspace",
                                   simulation.clusterActions.schemaChange(1,
                                                                          String.format("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', " + rfString + "};",
                                                                                        simulation.schema.keyspace))));
                     work.add(work("Create table",
                                   simulation.clusterActions.schemaChange(1,
                                                                          simulation.schema.compile())));
                     simulation.cluster.stream().forEach(i -> {
                         work.add(work("Output epoch",
                                       lazy(simulation.simulated, i, () -> logger.warn(ClusterMetadata.current().epoch.toString()))));
                     });

                     work.add(interleave("Start generating", HarrySimulatorTest.generateWrites(rowsPerPhase, simulation, cl)));
                     work.add(work("Validate all data locally",
                                   lazy(() -> validateAllLocal(simulation, simulation.nodeState.ring, rf))));

                     return arr(work.toArray(new ActionSchedule.Work[0]));
                 },
                 (simulation) -> {
                     List<ActionSchedule.Work> work = new ArrayList<>();
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
                             work.add(interleave("Bootstrap and generate data",
                                                 ActionList.of(bootstrap(simulation.simulated, simulation.cluster, token, node)),
                                                 generateWrites(rowsPerPhase, simulation, cl)
                             ));
                             simulation.cluster.stream().forEach(i -> {
                                 work.add(work("Output epoch",
                                               lazy(simulation.simulated, i, () -> logger.warn(ClusterMetadata.current().epoch.toString()))));
                             });
                             work.add(work("Bootstrap",
                                           run(() -> simulation.nodeState.bootstrap(node, token))));
                             work.add(work("Check node state",
                                           assertNodeState(simulation.simulated, simulation.cluster, node, NodeState.JOINED)));
                             bootstrappedNodes.add(node);
                         }
                         else
                         {
                             assert !bootstrappedNodes.isEmpty();
                             node = bootstrappedNodes.remove(0);
                             work.add(interleave("Decommission and generate data",
                                                 ActionList.of(decommission(simulation.simulated, simulation.cluster, node)),
                                                 generateWrites(rowsPerPhase, simulation, cl)
                             ));
                             simulation.cluster.stream().forEach(i -> {
                                 work.add(work("Output epoch",
                                               lazy(simulation.simulated, i, () -> logger.warn(ClusterMetadata.current().epoch.toString()))));
                             });
                             work.add(work("Decommission",
                                           run(() -> simulation.nodeState.decommission(node))));
                             work.add(work("Check node state", assertNodeState(simulation.simulated, simulation.cluster, node, NodeState.LEFT)));
                         }
                         work.add(work("Validate data locally",
                                       lazy(() -> validateAllLocal(simulation, simulation.nodeState.ring, rf))));
                         boolean tmp = shouldBootstrap;
                         work.add(work("Output message",
                                       run(() -> logger.warn("Finished {} of {} and data validation!\n", tmp ? "bootstrap" : "decommission", node))));
                     }
                     work.add(work("Output message",
                                   run(() -> logger.warn("Finished!"))));

                     return arr(work.toArray(new ActionSchedule.Work[0]));
                 }
        );
    }

    /**
     * Harry simulation. Assumes no scheduler delays and has ideal networking conditions.
     *
     * Since this is generally used to test _different_ subsystems, we plug in failures into the component
     * we are testing to both reduce the noise and the surface for potential investigations. This also
     * has a nice side effect of making simulations slightly faster.
     */
    static class HarrySimulation implements Simulation
    {
        protected final ClusterActions clusterActions;
        protected final SimulatedNodeState nodeState;

        protected final SimulatedSystems simulated;
        protected final RunnableActionScheduler scheduler;
        protected final Cluster cluster;
        protected final Function<HarrySimulation, ActionSchedule.Work[]> schedule;

        protected final EntropySource rng;
        protected final SchemaSpec schema;
        protected final Generator<OperationsGenerators.ToOp> insertGen;
        protected final QueryBuildingVisitExecutor queryBuilder;
        protected final QuiescentChecker model;

        protected final Map<Long, Visit> log;
        protected final Generator<Long> ltsGen;
        protected final DataTracker tracker;

        private HarrySimulation(SchemaSpec schema,
                                EntropySource rng,
                                SimulatedSystems simulated,
                                RunnableActionScheduler scheduler,
                                Cluster cluster,
                                Function<HarrySimulation, SimulatedNodeState> nodeState,
                                Function<HarrySimulation, ActionSchedule.Work[]> schedule,
                                Map<Long, Visit> log,
                                Generator<Long> ltsGen,
                                DataTracker tracker)
        {
            this.rng = rng;
            this.schema = schema;
            this.insertGen = OperationsGenerators.writeOp(schema);
            this.queryBuilder = new QueryBuildingVisitExecutor(schema, QueryBuildingVisitExecutor.WrapQueries.UNLOGGED_BATCH);
            this.model = new QuiescentChecker(schema.valueGenerators, tracker, new Model.Replay()
            {
                @Override
                public Visit replay(long lts)
                {
                    return log.get(lts);
                }

                @Override
                public Operations.Operation replay(long lts, int opId)
                {
                    return log.get(lts).operations[opId];
                }

                @Override
                public Iterator<Visit> iterator()
                {
                    List<Long> visited = new ArrayList<>(log.keySet());
                    visited.sort(Long::compare);
                    return new Iterator<>()
                    {
                        int idx = 0;

                        @Override
                        public boolean hasNext()
                        {
                            return idx < visited.size();
                        }

                        @Override
                        public Visit next()
                        {
                            return replay(visited.get(idx++));
                        }
                    };
                }
            });

            this.simulated = simulated;
            this.scheduler = scheduler;
            this.cluster = cluster;

            Options options = noActions(cluster.size());
            this.clusterActions = new ClusterActions(simulated, cluster,
                                                     options, new NoOpListener(), new Debug(new EnumMap<>(Debug.Info.class), new int[0]));

            this.nodeState = nodeState.apply(this);
            this.schedule = schedule;

            this.log = log;
            this.ltsGen = ltsGen;
            this.tracker = tracker;
        }

        public HarrySimulation withScheduler(RunnableActionScheduler scheduler)
        {
            return new HarrySimulation(schema, rng, simulated, scheduler, cluster, (ignore) -> nodeState, schedule, log, ltsGen, tracker);
        }

        public HarrySimulation withSchedulers(Function<HarrySimulation, Map<Verb, FutureActionScheduler>> schedulers)
        {
            Map<Verb, FutureActionScheduler> perVerbFutureActionScheduler = schedulers.apply(this);
            SimulatedSystems simulated = new SimulatedSystems(this.simulated.random,
                                                              this.simulated.time,
                                                              this.simulated.delivery,
                                                              this.simulated.execution,
                                                              this.simulated.ballots,
                                                              this.simulated.failureDetector,
                                                              this.simulated.snitch,
                                                              this.simulated.futureScheduler,
                                                              perVerbFutureActionScheduler,
                                                              this.simulated.debug,
                                                              this.simulated.failures);
            return new HarrySimulation(schema, rng, simulated, scheduler, cluster, (ignore) -> nodeState, schedule, log, ltsGen, tracker);
        }

        public HarrySimulation withSchedule(Function<HarrySimulation, ActionSchedule.Work[]> schedule)
        {
            return new HarrySimulation(schema, rng, simulated, scheduler, cluster, (ignore) -> nodeState, schedule, log, ltsGen, tracker);
        }

        @Override
        public CloseableIterator<?> iterator()
        {
            return new ActionSchedule(simulated.time, simulated.futureScheduler, () -> 0L, scheduler, schedule.apply(this));
        }

        @Override
        public void run()
        {
            try (CloseableIterator<?> iter = iterator())
            {
                while (iter.hasNext())
                {
                    checkForErrors();
                    iter.next();
                }
                checkForErrors();
            }
        }

        private void checkForErrors()
        {
            if (simulated.failures.hasFailure())
            {
                AssertionError error = new AssertionError("Errors detected during simulation");
                // don't care about the stack trace... the issue is the errors found and not what part of the scheduler we stopped
                error.setStackTrace(new StackTraceElement[0]);
                simulated.failures.get().forEach(error::addSuppressed);
                throw error;
            }
        }

        public void close() throws Exception
        {
        }
    }

    static class HarrySimulationBuilder extends ClusterSimulation.Builder<HarrySimulation>
    {
        protected final Consumer<IInstanceConfig> configUpdater;

        HarrySimulationBuilder(Consumer<IInstanceConfig> configUpdater)
        {
            this.configUpdater = configUpdater;
        }

        @Override
        public Map<Verb, FutureActionScheduler> perVerbFutureActionSchedulers(int nodeCount, SimulatedTime time, RandomSource random)
        {
            return HarrySimulatorTest.networkSchedulers(nodeCount, time, random);
        }

        @Override
        public FutureActionScheduler futureActionScheduler(int nodeCount, SimulatedTime time, RandomSource random)
        {
            return new AlwaysDeliverNetworkScheduler(time);
        }

        @Override
        public ClusterSimulation<HarrySimulation> create(long seed) throws IOException
        {
            RandomSource random = new RandomSource.Default();
            random.reset(seed);

            return new ClusterSimulation<>(random, seed, 1, this, configUpdater,
                                           (simulated, scheduler, cluster, options) -> {
                                               EntropySource rng = new JdkRandomEntropySource(seed);
                                               SchemaSpec schema = schemaSpecGen("harry", "tbl").generate(rng);
                                               return new HarrySimulation(schema,
                                                                          rng,
                                                                          simulated,
                                                                          scheduler,
                                                                          cluster,
                                                                          SimulatedNodeState::new,
                                                                          // No work initially
                                                                          (sim) -> new ActionSchedule.Work[0],
                                                                          new HashMap<>(),
                                                                          OperationsGenerators.lts(),
                                                                          new DataTracker.SimpleDataTracker());
                                           });
        }
    }

    /**
     * Simulation entrypoint; syntax sugar for creating a simulation.
     */
    void simulate(Consumer<ClusterSimulation.Builder<HarrySimulation>> configure,
                  Consumer<IInstanceConfig> instanceConfigUpdater,
                  String[] properties,
                  Function<HarrySimulation, ActionSchedule.Work[]>... phases) throws IOException
    {
        try (WithProperties p = new WithProperties().with(properties))
        {
            HarrySimulationBuilder factory = new HarrySimulationBuilder(instanceConfigUpdater);

            SimulationRunner.beforeAll();
            long seed = SimulationRunner.parseHex(Optional.ofNullable(this.seed)).orElseGet(() -> new Random().nextLong());
            logger.info("Seed 0x{}", Long.toHexString(seed));
            configure.accept(factory);
            try (ClusterSimulation<HarrySimulation> clusterSimulation = factory.create(seed))
            {
                HarrySimulation simulation = clusterSimulation.simulation();

                // For better determinism during startup, we allow instances to fully start (including daemon work)
                for (int i = 0; i < phases.length; i++)
                {
                    HarrySimulation current = simulation;
                    if (i == 0)
                        current = current.withScheduler(new RunnableActionScheduler.Immediate()).withSchedulers((s) -> Collections.emptyMap());
                    current.withSchedule(phases[i]).run();
                }
            }
            catch (Throwable t)
            {
                if (t instanceof SimulationException) throw t;
                throw new SimulationException(seed, "Failure creating the simulation", t);
            }
        }
    }

    /**
     * Custom network scheduler for testing TCM.
     */
    public static Map<Verb, FutureActionScheduler> networkSchedulers(int nodes, SimulatedTime time, RandomSource random)
    {
        Set<Verb> extremelyLossy = new HashSet<>(Arrays.asList(Verb.TCM_ABORT_MIG, Verb.TCM_REPLICATION,
                                                               Verb.TCM_COMMIT_REQ, Verb.TCM_NOTIFY_REQ,
                                                               Verb.TCM_INIT_MIG_REQ, Verb.TCM_INIT_MIG_RSP,
                                                               Verb.TCM_DISCOVER_REQ, Verb.TCM_DISCOVER_RSP));

        Set<Verb> somewhatSlow = new HashSet<>(Arrays.asList(Verb.BATCH_STORE_REQ, Verb.BATCH_STORE_RSP));

        Set<Verb> somewhatLossy = new HashSet<>(Arrays.asList(Verb.TCM_CURRENT_EPOCH_REQ,
                                                              Verb.TCM_NOTIFY_RSP, Verb.TCM_FETCH_CMS_LOG_RSP,
                                                              Verb.TCM_FETCH_PEER_LOG_RSP, Verb.TCM_COMMIT_RSP,
                                                              Verb.TCM_FETCH_CMS_LOG_REQ, Verb.TCM_FETCH_PEER_LOG_REQ,
                                                              Verb.PAXOS2_COMMIT_REMOTE_REQ, Verb.PAXOS2_COMMIT_REMOTE_RSP,
                                                              Verb.PAXOS2_PREPARE_REQ, Verb.PAXOS2_PREPARE_RSP,
                                                              Verb.PAXOS2_PROPOSE_REQ, Verb.PAXOS2_PROPOSE_RSP,
                                                              Verb.PAXOS_PREPARE_REQ, Verb.PAXOS_PREPARE_RSP,
                                                              Verb.PAXOS_PROPOSE_RSP, Verb.PAXOS_PROPOSE_REQ,
                                                              Verb.PAXOS_COMMIT_REQ, Verb.PAXOS_COMMIT_RSP));

        Map<Verb, FutureActionScheduler> schedulers = new HashMap<>();
        for (Verb verb : Verb.values())
        {
            if (extremelyLossy.contains(verb))
                schedulers.put(verb, new FixedLossNetworkScheduler(nodes, random, time, KindOfSequence.UNIFORM, .15f, .20f));
            else if (somewhatLossy.contains(verb))
                schedulers.put(verb, new FixedLossNetworkScheduler(nodes, random, time, KindOfSequence.UNIFORM, .1f, .15f));
            else if (somewhatSlow.contains(verb))
                schedulers.put(verb, new AlwaysDeliverNetworkScheduler(time, TimeUnit.MILLISECONDS.toNanos(100)));
        }
        return schedulers;
    }

    public Action reconfigureCMS(SimulatedSystems simulated, Cluster cluster, int rf, boolean inEachDc)
    {
        return new SimulatedActionTask("", Action.Modifiers.RELIABLE_NO_TIMEOUTS, Action.Modifiers.RELIABLE_NO_TIMEOUTS, null, simulated,
                                       new InterceptedExecution.InterceptedRunnableExecution((InterceptingExecutor) cluster.get(1).executor(),
                                                                                             cluster.get(1).transfer((IIsolatedExecutor.SerializableRunnable) () -> {
                                                                                                 ReplicationParams params;
                                                                                                 if (inEachDc)
                                                                                                 {
                                                                                                     Map<String, Integer> rfs = new HashMap<>();
                                                                                                     for (String dc : ClusterMetadata.current().directory.knownDatacenters())
                                                                                                     {
                                                                                                         rfs.put(dc, rf);
                                                                                                     }
                                                                                                     params = ReplicationParams.ntsMeta(rfs);
                                                                                                 }
                                                                                                 else
                                                                                                 {
                                                                                                     params = ReplicationParams.simpleMeta(rf, ClusterMetadata.current().directory.knownDatacenters());
                                                                                                 }
                                                                                                 ClusterMetadataService.instance().reconfigureCMS(params);
                                                                                             })));
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
                Startup.startup(() -> new PrepareJoin(ClusterMetadata.current().myNodeId(),
                                                      Collections.singleton(new Murmur3Partitioner.LongToken(token)),
                                                      ClusterMetadataService.instance().placementProvider(),
                                                      true,
                                                      true),
                                true,
                                true,
                                false);
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
                SingleNodeSequences.decommission(false, false);
            }
            catch (Throwable t)
            {
                logger.error("Could not decommission. Interrupting simulation.", t);
                SimulatorUtils.failWithOOM();
            }
        };

        return new SimulatedActionTask("Decommission Node",
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
                            logger.error("Node {} state ({}) is not as expected {}", i, actual, expected);
                            SimulatorUtils.failWithOOM();
                        }
                    });
    }

    /**
     * Creates an action list with a fixed number of data-generating operations that conform to the given Harry configuration.
     */
    public static ActionList generateWrites(int ops, HarrySimulation simulation, ConsistencyLevel cl)
    {
        Action[] actions = new Action[ops];
        OrderOn orderOn = new OrderOn.Strict(actions, 2);
        for (int i = 0; i < ops; i++)
        {
            long lts = simulation.ltsGen.generate(simulation.rng);

            Visit visit = new Visit(lts, new Operations.Operation[]{ simulation.insertGen.generate(simulation.rng).toOp(lts) });
            Visit prev_ = simulation.log.put(lts, visit);
            Invariants.checkState(prev_ == null);

            actions[i] = new Actions.LambdaAction("", Action.Modifiers.RELIABLE_NO_TIMEOUTS, () -> {
                CompiledStatement compiledStatement = simulation.queryBuilder.compile(visit);
                DataTracker tracker = simulation.tracker;

                RetryingQuery query = new RetryingQuery(compiledStatement.cql(), cl, compiledStatement.bindings());
                Action wrapper = new SimulatedActionCallable<>("Query",
                                                               Action.Modifiers.RELIABLE_NO_TIMEOUTS,
                                                               Action.Modifiers.RELIABLE_NO_TIMEOUTS,
                                                               simulation.simulated,
                                                               simulation.cluster.get((int) ((lts % simulation.cluster.size()) + 1)),
                                                               query)
                {
                    @Override
                    protected InterceptedExecution.InterceptedTaskExecution task()
                    {
                        return new InterceptedExecution.InterceptedTaskExecution((InterceptingExecutor) on.executor())
                        {
                            public void run()
                            {
                                tracker.begin(visit);
                                System.out.println("Started visit = " + visit);
                                // we'll be invoked on the node's executor, but we need to ensure the task is loaded on its classloader
                                try
                                {
                                    accept(on.unsafeCallOnThisThread(execute), null);
                                }
                                catch (Throwable t)
                                {
                                    accept(null, t);
                                }
                                finally
                                {
                                    execute = null;
                                }
                            }
                        };
                    }

                    @Override
                    public void accept(SimpleQueryResult result, Throwable failure)
                    {
                        if (failure != null)
                            simulated.failures.accept(failure);
                        else
                        {
                            System.out.println("Finished visit = " + visit);
                            tracker.end(visit);
                        }
                    }
                };

                return ActionList.of(wrapper);
            });
        }
        return ActionList.of(actions).orderOn(orderOn);
    }

    public static class RetryingQuery extends Query
    {
        public RetryingQuery(String query, ConsistencyLevel cl, Object[] boundValues)
        {
            super(query, -1, cl, null, boundValues);
        }

        @Override
        public SimpleQueryResult call()
        {
            while (true)
            {
                try
                {
                    return super.call();
                }
                catch (UncheckedInterruptedException e)
                {
                    throw new RuntimeException(e);
                }
                catch (Throwable t)
                {
                    logger.error("Caught error while executing query. Will ignore and retry: " + t.getMessage());
                }
            }
        }
    }

    /**
     * Given you have used `generate` methods to generate data with Harry, you can use this method to check whether all
     * data has been propagated everywhere it should be, be it via streaming, read repairs, or regular writes.
     */
    public static Action validateAllLocal(HarrySimulation simulation, List<TokenPlacementModel.Node> owernship, TokenPlacementModel.ReplicationFactor rf)
    {
        return new Actions.LambdaAction("Validate", Action.Modifiers.RELIABLE_NO_TIMEOUTS,
                                        () -> {
                                            if (!simulation.tracker.allFinished())
                                                throw new IllegalStateException("Can not begin validation, as writing has not quiesced yet: " + simulation.tracker);

                                            logger.warn("Starting validation. Ring view: {}", simulation.nodeState);
                                            Set<Long> pds = visitedPds(simulation);
                                            List<Action> actions = new ArrayList<>();
                                            for (Long pd : pds)
                                            {
                                                Operations.SelectPartition select = new Operations.SelectPartition(Long.MAX_VALUE, pd);
                                                actions.add(new HarryValidatingQuery(simulation, simulation.cluster, rf,
                                                                                     owernship, new Visit(Long.MAX_VALUE, new Operations.Operation[]{ select }),
                                                                                     simulation.queryBuilder));
                                            }
                                            return ActionList.of(actions).setStrictlySequential();
                                        });
    }

    private static Set<Long> visitedPds(HarrySimulation simulation)
    {
        Set<Long> pds = new HashSet<>();
        for (Visit visit : simulation.log.values())
        {
            for (Operations.Operation operation : visit.operations)
            {
                if (operation instanceof Operations.PartitionOperation)
                    pds.add(((Operations.PartitionOperation) operation).pd);
            }
        }
        return pds;
    }

    private static ActionSchedule.Work work(String toString, Action... actions)
    {
        return new ActionSchedule.Work(UNLIMITED, Collections.singletonList(ActionList.of(actions).setStrictlySequential())) {
            @Override
            public String toString()
            {
                return toString;
            }
        };
    }

    private static ActionSchedule.Work interleave(String toString, ActionList... actions)
    {
        return new ActionSchedule.Work(UNLIMITED, Arrays.asList(actions)) {
            @Override
            public String toString()
            {
                return toString;
            }
        };
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

                TokenPlacementModel.Node node = new TokenPlacementModel.Node(0, 0, 0, 0,
                                                                             constantLookup(addr.toString(),
                                                                                            Long.parseLong(config.getString("initial_token")),
                                                                                            simulation.clusterActions.snitch.get().getDatacenter(addr),
                                                                                            simulation.clusterActions.snitch.get().getRack(addr)));
                nodesLookup[i] = node;
                nodesByDc.computeIfAbsent(node.dc(), (k) -> new ArrayList<>()).add(node);
                idByAddr.put(addr.toString(), config.num());
            }
            this.ring = new ArrayList<>();
        }

        public int[] joined()
        {
            int[] joined = new int[ring.size()];
            for (int i = 0; i < ring.size(); i++)
            {
                joined[i] = idByAddr.get(ring.get(i).id());
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
                    assert node.token() > prev.token() : "Ring doesn't seem to be sorted: " + ring;
                prev = node;
            }
        }

        public void decommission(int nodeId)
        {
            System.out.printf("Marking %d as decommissioned%n", nodeId);
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
                    assert node.token() > prev.token() : "Ring doesn't seem to be sorted: " + ring;
                prev = node;
            }

        }
        private static TokenPlacementModel.Node withToken(TokenPlacementModel.Node node, long token)
        {
            return new TokenPlacementModel.Node(0, 0, 0, 0,
                                                constantLookup(node.id(),
                                                               token,
                                                               node.dc(),
                                                               node.rack()));
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

    public static Generator<SchemaSpec> schemaSpecGen(String keyspace, String prefix)
    {
        return SchemaGenerators.schemaSpecGen(keyspace, prefix, 1000);
    }

    public static class HaltOnError extends Failures
    {
        @Override
        public void onFailure(Throwable t)
        {
            super.onFailure(t);
            logger.error("Caught an exception, going to halt", t);
        }
    }
}

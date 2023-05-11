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

package org.apache.cassandra.simulator;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.ToDoubleFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.airlift.airline.Command;
import io.airlift.airline.Help;
import io.airlift.airline.Option;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.simulator.Debug.Info;
import org.apache.cassandra.simulator.Debug.Levels;
import org.apache.cassandra.simulator.cluster.ClusterActions.TopologyChange;
import org.apache.cassandra.simulator.debug.SelfReconcile;
import org.apache.cassandra.simulator.systems.InterceptedWait;
import org.apache.cassandra.simulator.systems.InterceptedWait.CaptureSites.Capture;
import org.apache.cassandra.simulator.systems.InterceptibleThread;
import org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods;
import org.apache.cassandra.simulator.utils.ChanceRange;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.util.Arrays.stream;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_ALTER_RF_DURING_RANGE_MOVEMENT;
import static org.apache.cassandra.config.CassandraRelevantProperties.BATCH_COMMIT_LOG_SYNC_INTERVAL;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_REMOTE_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.CLOCK_GLOBAL;
import static org.apache.cassandra.config.CassandraRelevantProperties.CLOCK_MONOTONIC_APPROX;
import static org.apache.cassandra.config.CassandraRelevantProperties.CLOCK_MONOTONIC_PRECISE;
import static org.apache.cassandra.config.CassandraRelevantProperties.CONSISTENT_DIRECTORY_LISTINGS;
import static org.apache.cassandra.config.CassandraRelevantProperties.DETERMINISM_UNSAFE_UUID_NODE;
import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_SSTABLE_ACTIVITY_TRACKING;
import static org.apache.cassandra.config.CassandraRelevantProperties.DETERMINISM_SSTABLE_COMPRESSION_DEFAULT;
import static org.apache.cassandra.config.CassandraRelevantProperties.DTEST_API_LOG_TOPOLOGY;
import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIPER_SKIP_WAITING_TO_SETTLE;
import static org.apache.cassandra.config.CassandraRelevantProperties.IGNORE_MISSING_NATIVE_FILE_HINTS;
import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;
import static org.apache.cassandra.config.CassandraRelevantProperties.LIBJEMALLOC;
import static org.apache.cassandra.config.CassandraRelevantProperties.MEMTABLE_OVERHEAD_SIZE;
import static org.apache.cassandra.config.CassandraRelevantProperties.MIGRATION_DELAY;
import static org.apache.cassandra.config.CassandraRelevantProperties.PAXOS_REPAIR_RETRY_TIMEOUT_IN_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.RING_DELAY;
import static org.apache.cassandra.config.CassandraRelevantProperties.SHUTDOWN_ANNOUNCE_DELAY_IN_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_AUTH_DEFAULT_RF;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_IGNORE_SIGAR;
import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_GOSSIP_ENDPOINT_REMOVAL;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_JVM_DTEST_DISABLE_SSL;
import static org.apache.cassandra.simulator.debug.Reconcile.reconcileWith;
import static org.apache.cassandra.simulator.debug.Record.record;
import static org.apache.cassandra.simulator.debug.SelfReconcile.reconcileWithSelf;
import static org.apache.cassandra.simulator.utils.IntRange.parseRange;
import static org.apache.cassandra.simulator.utils.LongRange.parseNanosRange;

@SuppressWarnings({ "ZeroLengthArrayAllocation", "CodeBlock2Expr", "SameParameterValue", "DynamicRegexReplaceableByCompiledPattern", "CallToSystemGC" })
public class SimulationRunner
{
    private static final Logger logger = LoggerFactory.getLogger(SimulationRunner.class);

    public enum RecordOption { NONE, VALUE, WITH_CALLSITES }

    @BeforeClass
    public static void beforeAll()
    {
        // setup system properties for our instances to behave correctly and consistently/deterministically

        // Disallow time on the bootstrap classloader
        for (CassandraRelevantProperties property : Arrays.asList(CLOCK_GLOBAL, CLOCK_MONOTONIC_APPROX, CLOCK_MONOTONIC_PRECISE))
            property.setString("org.apache.cassandra.simulator.systems.SimulatedTime$Delegating");
        try { Clock.Global.nanoTime(); } catch (IllegalStateException e) {} // make sure static initializer gets called

        // TODO (cleanup): disable unnecessary things like compaction logger threads etc
        LIBJEMALLOC.setString("-");
        DTEST_API_LOG_TOPOLOGY.setBoolean(false);

        // this property is used to allow non-members of the ring to exist in gossip without breaking RF changes
        // it would be nice not to rely on this, but hopefully we'll have consistent range movements before it matters
        ALLOW_ALTER_RF_DURING_RANGE_MOVEMENT.setBoolean(true);

        for (CassandraRelevantProperties property : Arrays.asList(CLOCK_GLOBAL, CLOCK_MONOTONIC_APPROX, CLOCK_MONOTONIC_PRECISE))
            property.setString("org.apache.cassandra.simulator.systems.SimulatedTime$Global");

        CASSANDRA_JMX_REMOTE_PORT.setString("");
        RING_DELAY.setInt(0);
        PAXOS_REPAIR_RETRY_TIMEOUT_IN_MS.setLong(NANOSECONDS.toMillis(Long.MAX_VALUE));
        SHUTDOWN_ANNOUNCE_DELAY_IN_MS.setInt(0);
        DETERMINISM_UNSAFE_UUID_NODE.setBoolean(true);
        GOSSIPER_SKIP_WAITING_TO_SETTLE.setInt(0);
        BATCH_COMMIT_LOG_SYNC_INTERVAL.setInt(-1);
        DISABLE_SSTABLE_ACTIVITY_TRACKING.setBoolean(false);
        DETERMINISM_SSTABLE_COMPRESSION_DEFAULT.setBoolean(false); // compression causes variation in file size for e.g. UUIDs, IP addresses, random file paths
        CONSISTENT_DIRECTORY_LISTINGS.setBoolean(true);
        TEST_IGNORE_SIGAR.setBoolean(true);
        SYSTEM_AUTH_DEFAULT_RF.setInt(3);
        MIGRATION_DELAY.setInt(Integer.MAX_VALUE);
        DISABLE_GOSSIP_ENDPOINT_REMOVAL.setBoolean(true);
        MEMTABLE_OVERHEAD_SIZE.setInt(100);
        IGNORE_MISSING_NATIVE_FILE_HINTS.setBoolean(true);
        ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);
        TEST_JVM_DTEST_DISABLE_SSL.setBoolean(true); // to support easily running without netty from dtest-jar

        if (Thread.currentThread() instanceof InterceptibleThread); // load InterceptibleThread class to avoid infinite loop in InterceptorOfGlobalMethods
        new InterceptedWait.CaptureSites(Thread.currentThread())
        .toString(ste -> !ste.getClassName().equals(SelfReconcile.class.getName())); // ensure self reconcile verify can work without infinite looping
        InterceptorOfGlobalMethods.Global.unsafeReset();
        ThreadLocalRandom.current();
    }

    protected interface ICommand<B extends ClusterSimulation.Builder<?>>
    {
        public void run(B builder) throws IOException;
    }

    protected abstract static class BasicCommand<B extends ClusterSimulation.Builder<?>> implements ICommand<B>
    {
        @Option(name = { "--seed" } , title = "0x", description = "Specify the first seed to test (each simulation will increment the seed by 1)")
        protected String seed;

        @Option(name = { "--simulations"} , title = "int", description = "The number of simulations to run")
        protected int simulationCount = 1;

        @Option(name = { "-t", "--threads" }, title = "int", description = "The number of threads to split between node thread pools. Each ongoing action also requires its own thread.")
        protected int threadCount = 1000;

        @Option(name = { "-n", "--nodes" } , title = "int...int", description = "Cluster size range, lb..ub (default 4..16)")
        protected String nodeCount = "4..16";

        @Option(name = { "--dcs" }, title = "int...int", description = "Cluster DC count, lb..ub (default 1..2)")
        protected String dcCount = "1..2";

        @Option(name = { "-o", "--within-key-concurrency" }, title = "int..int", description = "Number of simultaneous paxos operations per key, lb..ub (default 2..5)")
        protected String withinKeyConcurrency = "2..5";

        @Option(name = { "-c", "--concurrency" }, title = "int", description = "Number of keys to operate on simultaneously (default 10)")
        protected int concurrency = 10;
        @Option(name = { "-k", "--keys" }, title = "int", description = "Number of unique partition keys to operate over (default to 2* concurrency)")
        protected int primaryKeyCount = -1;
        @Option(name = { "--key-seconds" }, title = "int...int", description = "Number of seconds to simulate a partition key for before selecting another (default 5..30)")
        protected String primaryKeySeconds = "5..30";

        @Option(name = { "--cluster-actions" }, title = "JOIN,LEAVE,REPLACE,CHANGE_RF", description = "Cluster actions to select from, comma delimited (JOIN, LEAVE, REPLACE, CHANGE_RF)")
        protected String topologyChanges = stream(TopologyChange.values()).map(Object::toString).collect(Collectors.joining(","));
        @Option(name = { "--cluster-action-interval" }, title = "int...int(s|ms|us|ns)", description = "The period of time between two cluster actions (default 5..15s)")
        protected String topologyChangeInterval = "5..15s";
        @Option(name = { "--cluster-action-limit" }, title = "int", description = "The maximum number of topology change events to perform (default 0)")
        protected String topologyChangeLimit = "0";

        @Option(name = { "-s", "--run-time" }, title = "int", description = "Length of simulated time to run in seconds (default -1)")
        protected int secondsToSimulate = -1;

        @Option(name = { "--reads" }, title = "[distribution:]float...float", description = "Proportion of actions that are reads (default: 0.05..0.95)")
        protected String readChance;
        @Option(name = { "--nemesis" }, title = "[distribution:]float...float", description = "Proportion of nemesis points that are intercepted (default: 0..0.01)")
        protected String nemesisChance;

        @Option(name = { "--priority" }, title = "uniform|randomwalk|seq", description = "Priority assignment for actions that may overlap their execution", allowedValues = { "uniform", "randomwalk", "seq" })
        protected String priority;

        // TODO (feature): simulate GC pauses

        @Option(name = { "--network-flaky-chance" }, title = "[distribution:]float...float", description = "Chance of some minority of nodes experiencing flaky connections (default: qlog:0.01..0.1)")
        protected String networkFlakyChance = "qlog:0.01..0.1";
        @Option(name = { "--network-partition-chance" }, title = "[distribution:]float...float", description = "Chance of some minority of nodes being isolated (default: qlog:0.01..0.1)")
        protected String networkPartitionChance = "qlog:0.01..0.1";
        @Option(name = { "--network-reconfigure-interval" }, title = "int...int(s|ms|us|ns)", description = "Period of time for which a flaky or catastrophic network partition may be in force")
        protected String networkReconfigureInterval = "1..10s";
        @Option(name = { "--network-drop-chance" }, title = "[distribution:]float...float", description = "Chance of dropping a message under normal circumstances (default: qlog:0..0.001)")
        protected String networkDropChance = "qlog:0..0.001";
        // TODO (feature): TDP vs UDP simulation (right now we have no head of line blocking so we deliver in a UDP fashion which is not how the cluster operates)
        @Option(name = { "--network-delay-chance" }, title = "[distribution:]float...float", description = "Chance of delaying a message under normal circumstances (default: qlog:0..0.1)")
        protected String networkDelayChance = "qlog:0..0.01";
        @Option(name = { "--network-latency" }, title = "int...int(s|ms|us|ns)", description = "Range of possible latencies messages can be simulated to experience (default 1..2ms)")
        protected String networkLatency = "1..2ms";
        @Option(name = { "--network-delay" }, title = "int...int(s|ms|us|ns)", description = "Range of possible latencies messages can be simulated to experience when delayed (default 2..20ms)")
        protected String networkDelay = "2..20ms";
        @Option(name = { "--network-flaky-drop-chance" }, title = "[distribution:]float...float", description = "Chance of dropping a message on a flaky connection (default: qlog:0.01..0.1)")
        protected String flakyNetworkDropChance = "qlog:0.01..0.1";
        @Option(name = { "--network-flaky-delay-chance" }, title = "[distribution:]float...float", description = "Chance of delaying a message on a flaky connection (default: qlog:0.01..0.2)")
        protected String flakyNetworkDelayChance = "qlog:0.01..0.2";
        @Option(name = { "--network-flaky-latency" }, title = "int...int(s|ms|us|ns)", description = "Range of possible latencies messages can be simulated to experience on a flaky connection (default 2..4ms)")
        protected String flakyNetworkLatency = "2..4ms";
        @Option(name = { "--network-flaky-delay" }, title = "int...int(s|ms|us|ns)", description = "Range of possible latencies messages can be simulated to experience when delayed on a flaky connection (default 4..100ms)")
        protected String flakyNetworkDelay = "4..100ms";

        @Option(name = { "--clock-drift" }, title = "int...int(s|ms|us|ns)", description = "The range of clock skews to experience (default 10..1000ms)")
        protected String clockDrift = "10..1000ms";
        @Option(name = { "--clock-discontinuity-interval" }, title = "int...int(s|ms|us|ns)", description = "The period of clock continuity (a discontinuity is a large jump of the global clock to introduce additional chaos for event scheduling) (default 10..60s)")
        protected String clockDiscontinuityInterval = "10..60s";

        @Option(name = { "--scheduler-jitter" }, title = "int...int(s|ms|us|ns)", description = "The scheduler will randomly prioritise all tasks scheduled to run within this interval (default 10..1500us)")
        protected String schedulerJitter = "10..1500us";
        @Option(name = { "--scheduler-delay-chance" }, title = "[distribution:]float...float", description = "Chance of delaying the consequence of an action (default: 0..0.1)")
        protected String schedulerDelayChance = "qlog:0..0.1";
        @Option(name = { "--scheduler-delay" }, title = "int...int(s|ms|us|ns)", description = "Range of possible additional latencies thread execution can be simulated to experience when delayed (default 1..10000us)")
        protected String schedulerDelayMicros = "1..10000us";
        @Option(name = { "--scheduler-long-delay" }, title = "int...int(s|ms|us|ns)", description = "Range of possible additional latencies thread execution can be simulated to experience when delayed (default 1..10000us)")
        protected String schedulerLongDelayMicros = "1..10000us";

        @Option(name = { "--log" }, title = "level", description = "<partition> <cluster> level events, between 0 and 2", arity = 2)
        protected List<Integer> log;

        @Option(name = { "--debug-keys" }, title = "level", description = "Print debug info only for these keys (comma delimited)")
        protected String debugKeys;

        @Option(name = { "--debug-rf" }, title = "level", description = "Print RF on <partition> <cluster> events; level 0 to 2", arity = 2, allowedValues = { "0", "1", "2" })
        protected List<Integer> debugRf;

        @Option(name = { "--debug-ownership" }, title = "level", description = "Print ownership on <partition> <cluster> events; level 0 to 2", arity = 2, allowedValues = { "0", "1", "2" })
        protected List<Integer> debugOwnership;

        @Option(name = { "--debug-ring" }, title = "level", description = "Print ring state on <partition> <cluster> events; level 0 to 2", arity = 2, allowedValues = { "0", "1", "2" })
        protected List<Integer> debugRing;

        @Option(name = { "--debug-gossip" }, title = "level", description = "Debug gossip at <partition> <cluster> events; level 0 to 2", arity = 2, allowedValues = { "0", "1", "2" })
        protected List<Integer> debugGossip;

        @Option(name = { "--debug-paxos" }, title = "level", description = "Print paxos state on <partition> <cluster> events; level 0 to 2", arity = 2, allowedValues = { "0", "1", "2" })
        protected List<Integer> debugPaxos;

        @Option(name = { "--capture" }, title = "wait,wake,now", description = "Capture thread stack traces alongside events, choose from (wait,wake,now)")
        protected String capture;

        protected void propagate(B builder)
        {
            builder.threadCount(threadCount);
            builder.concurrency(concurrency);
            if (primaryKeyCount >= 0) builder.primaryKeyCount(primaryKeyCount);
            else builder.primaryKeyCount(2 * concurrency);
            builder.secondsToSimulate(secondsToSimulate);
            parseChanceRange(Optional.ofNullable(networkPartitionChance)).ifPresent(builder::networkPartitionChance);
            parseChanceRange(Optional.ofNullable(networkFlakyChance)).ifPresent(builder::networkFlakyChance);
            parseNanosRange(Optional.ofNullable(networkReconfigureInterval)).ifPresent(builder::networkReconfigureInterval);
            parseChanceRange(Optional.ofNullable(networkDropChance)).ifPresent(builder::networkDropChance);
            parseChanceRange(Optional.ofNullable(networkDelayChance)).ifPresent(builder::networkDelayChance);
            parseNanosRange(Optional.ofNullable(networkLatency)).ifPresent(builder::networkLatencyNanos);
            parseNanosRange(Optional.ofNullable(networkDelay)).ifPresent(builder::networkDelayNanos);
            parseChanceRange(Optional.ofNullable(flakyNetworkDropChance)).ifPresent(builder::flakyNetworkDropChance);
            parseChanceRange(Optional.ofNullable(flakyNetworkDelayChance)).ifPresent(builder::flakyNetworkDelayChance);
            parseNanosRange(Optional.ofNullable(flakyNetworkLatency)).ifPresent(builder::flakyNetworkLatencyNanos);
            parseNanosRange(Optional.ofNullable(flakyNetworkDelay)).ifPresent(builder::flakyNetworkDelayNanos);
            parseChanceRange(Optional.ofNullable(schedulerDelayChance)).ifPresent(builder::schedulerDelayChance);
            parseNanosRange(Optional.ofNullable(clockDrift)).ifPresent(builder::clockDriftNanos);
            parseNanosRange(Optional.ofNullable(clockDiscontinuityInterval)).ifPresent(builder::clockDiscontinuityIntervalNanos);
            parseNanosRange(Optional.ofNullable(schedulerJitter)).ifPresent(builder::schedulerJitterNanos);
            parseNanosRange(Optional.ofNullable(schedulerDelayMicros)).ifPresent(builder::schedulerDelayNanos);
            parseNanosRange(Optional.ofNullable(schedulerLongDelayMicros)).ifPresent(builder::schedulerLongDelayNanos);
            parseChanceRange(Optional.ofNullable(readChance)).ifPresent(builder::readChance);
            parseChanceRange(Optional.ofNullable(nemesisChance)).ifPresent(builder::nemesisChance);
            parseRange(Optional.ofNullable(nodeCount)).ifPresent(builder::nodes);
            parseRange(Optional.ofNullable(dcCount)).ifPresent(builder::dcs);
            parseRange(Optional.ofNullable(primaryKeySeconds)).ifPresent(builder::primaryKeySeconds);
            parseRange(Optional.ofNullable(withinKeyConcurrency)).ifPresent(builder::withinKeyConcurrency);
            Optional.ofNullable(topologyChanges).ifPresent(topologyChanges -> {
                builder.topologyChanges(stream(topologyChanges.split(","))
                                        .filter(v -> !v.isEmpty())
                                        .map(v -> TopologyChange.valueOf(v.toUpperCase()))
                                        .toArray(TopologyChange[]::new));
            });
            parseNanosRange(Optional.ofNullable(topologyChangeInterval)).ifPresent(builder::topologyChangeIntervalNanos);
            builder.topologyChangeLimit(Integer.parseInt(topologyChangeLimit));
            Optional.ofNullable(priority).ifPresent(kinds -> {
                builder.scheduler(stream(kinds.split(","))
                                  .filter(v -> !v.isEmpty())
                                  .map(v -> RunnableActionScheduler.Kind.valueOf(v.toUpperCase()))
                                  .toArray(RunnableActionScheduler.Kind[]::new));
            });

            Optional.ofNullable(this.capture)
                    .map(s -> s.split(","))
                    .map(s -> new Capture(
                        stream(s).anyMatch(s2 -> s2.equalsIgnoreCase("wait")),
                        stream(s).anyMatch(s2 -> s2.equalsIgnoreCase("wake")),
                        stream(s).anyMatch(s2 -> s2.equalsIgnoreCase("now"))
                    ))
                    .ifPresent(builder::capture);

            EnumMap<Info, Levels> debugLevels = new EnumMap<>(Info.class);
            Optional.ofNullable(log).ifPresent(list -> debugLevels.put(Info.LOG, new Levels(list.get(0), list.get(1))));
            Optional.ofNullable(debugRf).ifPresent(list -> debugLevels.put(Info.RF, new Levels(list.get(0), list.get(1))));
            Optional.ofNullable(debugOwnership).ifPresent(list -> debugLevels.put(Info.OWNERSHIP, new Levels(list.get(0), list.get(1))));
            Optional.ofNullable(debugRing).ifPresent(list -> debugLevels.put(Info.RING, new Levels(list.get(0), list.get(1))));
            Optional.ofNullable(debugGossip).ifPresent(list -> debugLevels.put(Info.GOSSIP, new Levels(list.get(0), list.get(1))));
            Optional.ofNullable(debugPaxos).ifPresent(list -> debugLevels.put(Info.PAXOS, new Levels(list.get(0), list.get(1))));
            if (!debugLevels.isEmpty())
            {
                int[] debugPrimaryKeys = Optional.ofNullable(debugKeys)
                                                 .map(pks -> stream(pks.split(",")).mapToInt(Integer::parseInt).sorted().toArray())
                                                 .orElse(new int[0]);
                builder.debug(debugLevels, debugPrimaryKeys);
            }
        }

        public void run(B builder) throws IOException
        {
            beforeAll();
            Thread.setDefaultUncaughtExceptionHandler((th, e) -> {
                boolean isInterrupt = false;
                Throwable t = e;
                while (!isInterrupt && t != null)
                {
                    isInterrupt = t instanceof InterruptedException || t instanceof UncheckedInterruptedException;
                    t = t.getCause();
                }
                if (!isInterrupt)
                    logger.error("Uncaught exception on {}", th, e);
                if (e instanceof Error)
                    throw (Error) e;
            });

            propagate(builder);

            long seed = parseHex(Optional.ofNullable(this.seed)).orElse(new Random(System.nanoTime()).nextLong());
            for (int i = 0 ; i < simulationCount ; ++i)
            {
                cleanup();
                run(seed, builder);
                ++seed;
            }
        }

        protected abstract void run(long seed, B builder) throws IOException;
    }

    @Command(name = "run")
    protected static class Run<B extends ClusterSimulation.Builder<?>> extends BasicCommand<B>
    {
        protected void run(long seed, B builder) throws IOException
        {
            logger.error("Seed 0x{}", Long.toHexString(seed));

            try (ClusterSimulation<?> cluster = builder.create(seed))
            {
                try
                {
                    cluster.simulation.run();
                }
                catch (Throwable t)
                {
                    throw new SimulationException(seed, t);
                }
            }
            catch (Throwable t)
            {
                if (t instanceof SimulationException) throw t;
                throw new SimulationException(seed, "Failure creating the simulation", t);
            }
        }
    }

    @Command(name = "record")
    protected static class Record<B extends ClusterSimulation.Builder<?>> extends BasicCommand<B>
    {
        @Option(name = {"--to"}, description = "Directory of recordings to reconcile with for the seed", required = true)
        private String dir;

        @Option(name = {"--with-rng"}, title = "0|1", description = "Record RNG values (with or without call sites)", allowedValues = {"0", "1"})
        private int rng = -1;

        @Option(name = {"--with-time"}, title = "0|1", description = "Record time values (with or without call sites)", allowedValues = {"0", "1"})
        private int time = -1;

        @Override
        protected void run(long seed, B builder) throws IOException
        {
            record(dir, seed, RecordOption.values()[rng + 1], RecordOption.values()[time + 1], builder);
        }
    }

    @Command(name = "reconcile")
    protected static class Reconcile<B extends ClusterSimulation.Builder<?>> extends BasicCommand<B>
    {
        @Option(name = {"--with"}, description = "Directory of recordings to reconcile with for the seed")
        private String dir;

        @Option(name = {"--with-rng"}, title = "0|1", description = "Reconcile RNG values (if present in source)", allowedValues = {"0", "1"})
        private int rng = -1;

        @Option(name = {"--with-time"}, title = "0|1", description = "Reconcile time values (if present in source)", allowedValues = {"0", "1"})
        private int time = -1;

        @Option(name = {"--with-allocations"}, description = "Reconcile memtable allocations (only with --with-self)", arity = 0)
        private boolean allocations;

        @Option(name = {"--with-self"}, description = "Reconcile with self", arity = 0)
        private boolean withSelf;

        @Override
        protected void run(long seed, B builder) throws IOException
        {
            RecordOption withRng = RecordOption.values()[rng + 1];
            RecordOption withTime = RecordOption.values()[time + 1];
            if (withSelf) reconcileWithSelf(seed, withRng, withTime, allocations, builder);
            else if (allocations) throw new IllegalArgumentException("--with-allocations is only compatible with --with-self");
            else reconcileWith(dir, seed, withRng, withTime, builder);
        }
    }

    protected static class HelpCommand<B extends ClusterSimulation.Builder<?>> extends Help implements ICommand<B>
    {
        @Override
        public void run(B builder) throws IOException
        {
            super.run();
        }
    }


    private static Optional<Long> parseHex(Optional<String> value)
    {
        return value.map(s -> {
            if (s.startsWith("0x"))
                return Hex.parseLong(s, 2, s.length());
            throw new IllegalArgumentException("Invalid hex string: " + s);
        });
    }

    private static final Pattern CHANCE_PATTERN = Pattern.compile("(uniform|(?<qlog>qlog(\\((?<quantizations>[0-9]+)\\))?):)?(?<min>0(\\.[0-9]+)?)(..(?<max>0\\.[0-9]+))?", Pattern.CASE_INSENSITIVE);
    private static Optional<ChanceRange> parseChanceRange(Optional<String> chance)
    {
        return chance.map(s -> {
            ToDoubleFunction<RandomSource> chanceSelector = RandomSource::uniformFloat;
            Matcher m = CHANCE_PATTERN.matcher(s);
            if (!m.matches()) throw new IllegalArgumentException("Invalid chance specification: " + s);
            if (m.group("qlog") != null)
            {
                int quantizations = m.group("quantizations") == null ? 4 : Integer.parseInt(m.group("quantizations"));
                chanceSelector = randomSource -> randomSource.qlog2uniformFloat(quantizations);
            }
            float min = Float.parseFloat(m.group("min"));
            float max = m.group("max") == null ? min : Float.parseFloat(m.group("max"));
            return new ChanceRange(chanceSelector, min, max);
        });
    }

    private static void cleanup()
    {
        FastThreadLocal.destroy();
        for (int i = 0 ; i < 10 ; ++i)
            System.gc();
    }

}

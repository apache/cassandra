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
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;
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
import org.apache.cassandra.simulator.ClusterSimulation.Builder.ChanceRange;
import org.apache.cassandra.simulator.ClusterSimulation.Builder.Range;
import org.apache.cassandra.simulator.Debug.Info;
import org.apache.cassandra.simulator.Debug.Levels;
import org.apache.cassandra.simulator.cluster.ClusterActions.TopologyChange;
import org.apache.cassandra.simulator.debug.Capture;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.util.Arrays.stream;
import static org.apache.cassandra.simulator.debug.Reconcile.reconcileWith;
import static org.apache.cassandra.simulator.debug.Record.record;
import static org.apache.cassandra.simulator.debug.SelfReconcile.reconcileWithSelf;

//
// TODO (now): print (more?) summary info about run
// TODO (now): runs are not repeatable: investigate
@SuppressWarnings({ "ZeroLengthArrayAllocation", "CodeBlock2Expr", "SameParameterValue", "DynamicRegexReplaceableByCompiledPattern", "CallToSystemGC" })
public class SimulationRunner
{
    private static final Logger logger = LoggerFactory.getLogger(SimulationRunner.class);

    @BeforeClass
    public static void beforeAll()
    {
        // Disallow time on the bootstrap classloader
        System.setProperty("cassandra.clock", "org.apache.cassandra.simulator.systems.SimulatedTime$Throwing");
        System.setProperty("cassandra.monotonic_clock.approx", "org.apache.cassandra.simulator.systems.SimulatedTime$Throwing");
        System.setProperty("cassandra.monotonic_clock.precise", "org.apache.cassandra.simulator.systems.SimulatedTime$Throwing");
        try { Clock.Global.nanoTime(); } catch (IllegalStateException e) {} // make sure static initializer gets called

        // setup system properties for our instances to behave correctly and consistently/deterministically
        // TODO (future): disable unnecessary things like compaction logger threads etc
        System.setProperty("cassandra.test.ignore_sigar", "true");
        System.setProperty("cassandra.libjemalloc", "-");
        System.setProperty("cassandra.jmx.remote.port", "");
        System.setProperty("cassandra.dtest.api.log.topology", "false");
        System.setProperty("cassandra.shutdown_announce_in_ms", "0");
        System.setProperty("cassandra.ring_delay_ms", "0");
        System.setProperty("cassandra.broadcast_interval_ms", "0");
        System.setProperty("cassandra.skip_wait_for_gossip_to_settle", "0");
        System.setProperty("cassandra.allow_alter_rf_during_range_movement", "true");
        System.setProperty("cassandra.batch_commitlog_sync_interval_millis", "-1");
        System.setProperty("cassandra.clock", "org.apache.cassandra.simulator.systems.SimulatedTime$Wrapped");
        System.setProperty("cassandra.monotonic_clock.approx", "org.apache.cassandra.simulator.systems.SimulatedTime$Wrapped");
        System.setProperty("cassandra.monotonic_clock.precise", "org.apache.cassandra.simulator.systems.SimulatedTime$Wrapped");
        System.setProperty("cassandra.unsafe.deterministicuuidnode", "true");
        System.setProperty("cassandra.sstable_compression_default", "false"); // compression causes variation in file size for e.g. UUIDs, IP addresses, random file paths
        System.setProperty("cassandra.track_sstable_activity", "false"); // perhaps we should have a general purpose disable all unnecessary logging etc
        System.setProperty("cassandra.require_native_file_hints", "false");
        System.setProperty("cassandra.system_auth.rf", "3"); // permit range movements
        System.setProperty("cassandra.consistent_directory_listings", "true");
        System.setProperty("cassandra.migration_delay_ms", "" + Integer.MAX_VALUE);
        System.setProperty("cassandra.gossip.disable_endpoint_removal", "true");
        System.setProperty("cassandra.memtable_row_overhead_fixed", "100"); // determinism
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
    }

    protected interface ICommand<B extends ClusterSimulation.Builder<?>>
    {
        public void run(B builder) throws IOException;
    }

    protected abstract static class BasicCommand<B extends ClusterSimulation.Builder<?>> implements ICommand<B>
    {
        @Option(name = { "-s", "--seed"} , title = "0x|int", description = "Specify the first seed to test (each simulation will increment the seed by 1)")
        protected String seed;

        @Option(name = { "-c", "--simulations"} , title = "int", description = "The number of simulations to run")
        protected int simulationCount = 1;

        @Option(name = { "-t", "--threads" }, title = "int", description = "The number of threads to split between node thread pools. Each ongoing action also requires its own thread.")
        protected int threadCount = 1000;

        @Option(name = { "-n", "--nodes" } , title = "int..int", description = "Cluster size range, lb..ub (default 4..16)")
        protected String nodeCount = "4..16";

        @Option(name = { "--dcs" }, title = "int..int", description = "Cluster DC count, lb..ub (default 1..2)")
        protected String dcCount = "1..2";

        @Option(name = { "--clusterActions" }, title = "JOIN,LEAVE,REPLACE,CHANGE_RF", description = "Cluster actions to select from, comma delimited (JOIN, LEAVE, REPLACE, CHANGE_RF)")
        protected String topologyChanges = stream(TopologyChange.values()).map(Object::toString).collect(Collectors.joining(","));

        @Option(name = {"-p", "--partitions"}, title = "int", description = "Number of partitions / primary keys to operate over")
        protected int primaryKeyCount = 1;

        @Option(name = {"-a", "--actions"}, title = "int", description = "Number of actions to initiate per partition / primary key")
        protected int actionsPerPrimaryKey = 1000;

        @Option(name = { "--drop" }, title = "[distribution:]float..float", description = "Chance of dropping the consequence of an action (default: 0..0.1)")
        protected String dropChance;
        @Option(name = { "--delay" }, title = "[distribution:]float..float", description = "Chance of delaying the consequence of an action (default: 0..0.1)")
        protected String delayChance;
        @Option(name = { "--timeout" }, title = "[distribution:]float..float", description = "Chance of permitting a timeout consequence to be scheduled (default: 0..0.1)")
        protected String timeoutChance;
        @Option(name = { "--reads" }, title = "[distribution:]float..float", description = "Proportion of actions that are reads (default: 0.05..0.95)")
        protected String readChance;
        @Option(name = { "--nemesis" }, title = "[distribution:]float..float", description = "Proportion of nemesis points that are intercepted (default: 0..0.01)")
        protected String nemesisChance;

        @Option(name = { "--scheduler" }, title = "uniform|randomwalk", description = "Type of scheduler to use", allowedValues = { "uniform", "randomwalk" })
        protected String scheduler;

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
            builder.primaryKeyCount(primaryKeyCount);
            builder.actionsPerPrimaryKey(actionsPerPrimaryKey);
            parseChanceRange(Optional.ofNullable(dropChance)).ifPresent(builder::dropChance);
            parseChanceRange(Optional.ofNullable(delayChance)).ifPresent(builder::delayChance);
            parseChanceRange(Optional.ofNullable(timeoutChance)).ifPresent(builder::timeoutChance);
            parseChanceRange(Optional.ofNullable(readChance)).ifPresent(builder::readChance);
            parseChanceRange(Optional.ofNullable(nemesisChance)).ifPresent(builder::nemesisChance);
            parseRange(Optional.ofNullable(nodeCount)).ifPresent(builder::nodes);
            parseRange(Optional.ofNullable(dcCount)).ifPresent(builder::dcs);
            Optional.ofNullable(topologyChanges).ifPresent(topologyChanges -> {
                builder.topologyChanges(stream(topologyChanges.split(","))
                                        .filter(v -> !v.isEmpty())
                                        .map(v -> TopologyChange.valueOf(v.toUpperCase()))
                                        .toArray(TopologyChange[]::new));
            });
            Optional.ofNullable(scheduler).ifPresent(kinds -> {
                builder.scheduler(stream(kinds.split(","))
                                  .filter(v -> !v.isEmpty())
                                  .map(v -> ActionScheduler.Kind.valueOf(v.toUpperCase()))
                                  .toArray(ActionScheduler.Kind[]::new));
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

            long seed = parseLong(Optional.ofNullable(this.seed)).orElse(new Random().nextLong());
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
                    logger.error("Failed on seed {}", Long.toHexString(seed), t);
                }
            }
        }
    }

    @Command(name = "record")
    protected static class Record<B extends ClusterSimulation.Builder<?>> extends BasicCommand<B>
    {
        @Option(name = {"--to"}, description = "Directory of recordings to reconcile with for the seed", required = true)
        private String dir;

        @Option(name = {"--with-rng"}, description = "Record RNG values", arity = 0)
        private boolean rng;

        @Option(name = {"--with-rng-callsites"}, description = "Record RNG call sites", arity = 0)
        private boolean rngCallSites;

        @Override
        protected void run(long seed, B builder) throws IOException
        {
            record(dir, seed, rng, rngCallSites, builder);
        }
    }

    @Command(name = "reconcile")
    protected static class Reconcile<B extends ClusterSimulation.Builder<?>> extends BasicCommand<B>
    {
        @Option(name = {"--with"}, description = "Directory of recordings to reconcile with for the seed")
        private String dir;

        @Option(name = {"--with-rng"}, description = "Reconcile RNG values (if present in source)", arity = 0)
        private boolean rng;

        @Option(name = {"--with-rng-callsites"}, description = "Reconcile RNG call sites (if present in source)", arity = 0)
        private boolean rngCallSites;

        @Option(name = {"--with-self"}, description = "Reconcile with self", arity = 0)
        private boolean withSelf;

        @Override
        protected void run(long seed, B builder) throws IOException
        {
            if (withSelf) reconcileWithSelf(seed, rng, rngCallSites, builder);
            else reconcileWith(dir, seed, rng, rngCallSites, builder);
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


    private static Optional<Long> parseLong(Optional<String> value)
    {
        return value.map(s -> s.startsWith("0x")
                              ? Hex.parseLong(s, 2, s.length())
                              : Long.parseLong(s));
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

    private static Optional<Range> parseRange(Optional<String> chance)
    {
        return chance.map(s -> new Range(Integer.parseInt(s.replaceFirst("\\.\\.+[0-9]+", "")),
                                         Integer.parseInt(s.replaceFirst("[0-9]+\\.\\.+", ""))));
    }

    private static void cleanup()
    {
        FastThreadLocal.destroy();
        for (int i = 0 ; i < 10 ; ++i)
            System.gc();
    }

}

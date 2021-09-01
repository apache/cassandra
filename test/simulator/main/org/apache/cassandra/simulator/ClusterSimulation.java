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
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.FileSystem;
import java.util.EnumMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInstanceInitializer;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableConsumer;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableTriConsumer;
import org.apache.cassandra.distributed.impl.DirectStreamingConnectionFactory;
import org.apache.cassandra.distributed.impl.IsolatedExecutor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.service.paxos.BallotGenerator;
import org.apache.cassandra.simulator.ActionSchedulersRandom.SplitRange;
import org.apache.cassandra.simulator.ClusterSimulation.Builder.ChanceRange;
import org.apache.cassandra.simulator.RandomSource.Choices;
import org.apache.cassandra.simulator.asm.ChanceSupplier;
import org.apache.cassandra.simulator.asm.NemesisFieldSelectors;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.cluster.ClusterActions.TopologyChange;
import org.apache.cassandra.simulator.debug.Capture;
import org.apache.cassandra.simulator.asm.InterceptClasses;
import org.apache.cassandra.simulator.systems.Failures;
import org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods;
import org.apache.cassandra.simulator.systems.InterceptingExecutorFactory;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.simulator.systems.SimulatedBallots;
import org.apache.cassandra.simulator.systems.SimulatedExecution;
import org.apache.cassandra.simulator.systems.SimulatedFailureDetector;
import org.apache.cassandra.simulator.systems.SimulatedMessageDelivery;
import org.apache.cassandra.simulator.systems.SimulatedSnitch;
import org.apache.cassandra.simulator.systems.SimulatedTime;
import org.apache.cassandra.simulator.systems.SimulatedWaits;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;

import static java.lang.Integer.min;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.distributed.impl.AbstractCluster.getSharedClassPredicate;
import static org.apache.cassandra.utils.Shared.Scope.ANY;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * Wraps a Cluster and a Simulation to run upon it
 */
@SuppressWarnings("RedundantCast")
public class ClusterSimulation<S extends Simulation> implements AutoCloseable
{
    public static final Class<?>[] SHARE = new Class[]
                                        {
                                            AsyncFunction.class,
                                            FutureCallback.class,
                                            io.netty.util.concurrent.GenericFutureListener.class,
                                            io.netty.channel.FileRegion.class,
                                            io.netty.util.ReferenceCounted.class
                                        };

    public static final Class<?>[] ISOLATE = new Class<?>[0];

    static final FileSystem jimfs = Jimfs.newFileSystem();

    public interface SimulationFactory<S extends Simulation>
    {
        S create(SimulatedSystems simulated, ActionSchedulers scheduler, Cluster cluster, ClusterActions.Options options);
    }

    public interface SchedulerFactory
    {
        ActionSchedulers create(RandomSource random, ChanceRange delayChance, ChanceRange dropChance, ChanceRange timeoutChance);
    }

    @SuppressWarnings("UnusedReturnValue")
    public static abstract class Builder<S extends Simulation>
    {
        public static class Range
        {
            public final int min;
            public final int max;

            Range(int min, int max)
            {
                this.min = min;
                this.max = max;
            }

            int select(RandomSource random)
            {
                if (min == max) return min;
                return random.uniform(min, 1 + max);
            }

            int select(RandomSource random, int minlb, int maxub)
            {
                int min = Math.max(this.min, minlb);
                int max = Math.min(this.max, maxub);
                if (min >= max) return min;
                return random.uniform(min, 1 + max);
            }
        }

        public static class ChanceRange
        {
            public final ToDoubleFunction<RandomSource> distribution;
            public final float min;
            public final float max;

            ChanceRange(ToDoubleFunction<RandomSource> distribution, float min, float max)
            {
                this.distribution = distribution;
                assert min >= 0 && max <= 1.0;
                this.min = min;
                this.max = max;
            }

            public float select(RandomSource random)
            {
                if (min >= max) return min;
                return (float) ((distribution.applyAsDouble(random) * (max - min)) + min);
            }

            ChanceSupplier asSupplier(RandomSource random)
            {
                return () -> select(random);
            }
        }

        protected Supplier<RandomSource> randomSupplier = RandomSource.Default::new;
        protected int uniqueNum = 0;
        protected int threadCount;

        protected Range nodeCount = new Range(4, 16), dcCount = new Range(1, 2);
        protected TopologyChange[] topologyChanges = TopologyChange.values();

        protected int primaryKeyCount, actionsPerPrimaryKey;

        protected ChanceRange dropChance  =   new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4),  0.01f, 0.1f),
                              delayChance =   new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4),  0.01f, 0.1f),
                              timeoutChance = new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4),  0.01f, 0.1f),
                              readChance  =   new ChanceRange(RandomSource::uniformFloat,                         0.05f, 0.95f),
                              nemesisChance = new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4), 0.001f, 0.01f);

        protected SchedulerFactory schedulerFactory = schedulerFactory(ActionScheduler.Kind.values());

        protected Debug debug = new Debug();
        protected Capture capture = new Capture(false, false, false);

        public Debug debug()
        {
            return debug;
        }

        public Builder<S> debug(EnumMap<Debug.Info, Debug.Levels> debug, int[] primaryKeys)
        {
            this.debug = new Debug(debug, primaryKeys);
            return this;
        }

        public Builder<S> unique(int num)
        {
            this.uniqueNum = num;
            return this;
        }

        public Builder<S> threadCount(int count)
        {
            this.threadCount = count;
            return this;
        }

        public Builder<S> nodes(Range range)
        {
            this.nodeCount = range;
            return this;
        }

        public Builder<S> nodes(int min, int max)
        {
            this.nodeCount = new Range(min, max);
            return this;
        }

        public Builder<S> dcs(Range range)
        {
            this.dcCount = range;
            return this;
        }

        public Builder<S> dcs(int min, int max)
        {
            this.dcCount = new Range(min, max);
            return this;
        }

        public Builder<S> topologyChanges(TopologyChange[] topologyChanges)
        {
            this.topologyChanges = topologyChanges;
            return this;
        }

        public int primaryKeyCount()
        {
            return primaryKeyCount;
        }

        public Builder<S> primaryKeyCount(int count)
        {
            this.primaryKeyCount = count;
            return this;
        }

        public int actionsPerPrimaryKey()
        {
            return actionsPerPrimaryKey;
        }

        public Builder<S> actionsPerPrimaryKey(int count)
        {
            this.actionsPerPrimaryKey = count;
            return this;
        }

        public Builder<S> dropChance(ChanceRange dropChance)
        {
            this.dropChance = dropChance;
            return this;
        }

        public Builder<S> delayChance(ChanceRange delayChance)
        {
            this.delayChance = delayChance;
            return this;
        }

        public Builder<S> timeoutChance(ChanceRange timeoutChance)
        {
            this.timeoutChance = timeoutChance;
            return this;
        }

        public ChanceRange readChance()
        {
            return readChance;
        }

        public Builder<S> readChance(ChanceRange readChance)
        {
            this.readChance = readChance;
            return this;
        }

        public Builder<S> nemesisChance(ChanceRange nemesisChance)
        {
            this.nemesisChance = nemesisChance;
            return this;
        }

        public Builder<S> scheduler(ActionScheduler.Kind... kinds)
        {
            this.schedulerFactory = schedulerFactory(kinds);
            return this;
        }

        static SchedulerFactory schedulerFactory(ActionScheduler.Kind... kinds)
        {
            return (random, delayChance, dropChance, timeoutChance) -> {
                float delay = delayChance.select(random);
                float drop = dropChance.select(random);
                float timeout = timeoutChance.select(random);
                SplitRange splitRange =  Choices.random(random, SplitRange.values()).choose(random);
                switch (Choices.random(random, kinds).choose(random))
                {
                    default: throw new AssertionError();
                    case UNIFORM: return new ActionSchedulersUniform(random, delay, drop, timeout, splitRange);
                    case RANDOM_WALK: return new ActionSchedulersRandomWalk(random, delay, drop, timeout, splitRange);
                }
            };
        }

        public Builder<S> scheduler(SchedulerFactory schedulerFactory)
        {
            this.schedulerFactory = schedulerFactory;
            return this;
        }

        public Builder<S> random(Supplier<RandomSource> randomSupplier)
        {
            this.randomSupplier = randomSupplier;
            return this;
        }

        public Builder<S> capture(Capture capture)
        {
            this.capture = capture;
            return this;
        }

        public Capture capture()
        {
            return capture;
        }

        public abstract ClusterSimulation<S> create(long seed) throws IOException;
    }

    static class ThreadAllocator
    {
        final RandomSource random;
        int clusterPool; // number of threads we have left for the whole cluster
        int remainingNodes; // number of nodes we still need to allocate them to
        int allocationPool; //  threads to allocate for the node we're currently processing
        int remainingAllocations; // number of _remaining_ allocations take() assumes we want to evenly allocate threads over

        public ThreadAllocator(RandomSource random, int threadsToAllocate, int betweenNodes)
        {
            this.random = random;
            this.clusterPool = threadsToAllocate;
            this.remainingNodes = betweenNodes;
        }

        // randomly set the number of threads in various thread pools
        IInstanceConfig update(IInstanceConfig config)
        {
            cycle();
            // allocate in ascending order of max, for take() correctness
            return config
                   .set("memtable_flush_writers", take(1, 1, 2))
                   .set("concurrent_compactors", take(1, 1, 4))
                   .set("concurrent_writes", take(1, 4))
                   .set("concurrent_counter_writes", take(1, 4))
                   .set("concurrent_materialized_view_writes", take(1, 4))
                   .set("concurrent_reads", take(1, 4))
                   .forceSet("available_processors", take(3, 4));
        }

        // begin allocating for a new node
        void cycle()
        {
            assert remainingNodes > 0;
            // return unallocated items to the outerPool
            clusterPool += allocationPool;
            // set the curPool to allocate the next allocationPool size
            allocationPool = clusterPool;
            remainingAllocations = remainingNodes;
            // randomly select the next pool size, subtracting it from the outer pool
            allocationPool = take(1, 1);
            clusterPool -= allocationPool;
            // this is hard-coded to match the sum of the first arguments above
            remainingAllocations = 9;
            --remainingNodes;
        }

        /**
         * See {@link #take(int, int, int)}
         */
        int take(int times, int min)
        {
            return take(times, min, allocationPool);
        }

        /**
         * Allocate a random number of threads between [min..max)
         * The allocation is suitable for multiple users of the value, i.e.
         * {@code times} multiple of the result are deducted from the pool.
         *
         * If there are adequate supplies we aim to allocate threads "equally" between pools,
         * selecting a uniform value between 0.5x and 2x the fair split of the remaining pool
         * on each allocation. If the min/max bounds override that, they are preferred.
         *
         * The minimum is always honoured, regardless of available pool size.
         */
        int take(int times, int min, int max)
        {
            int remaining = remainingAllocations;
            assert remaining >= times;
            remainingAllocations -= times;
            if (remaining * min <= allocationPool)
                return min;
            if (times == remaining)
                return allocationPool / remaining;
            if (times + 1 == remaining)
                return random.uniform(Math.max(min, (allocationPool - max) / times), Math.min(max, (allocationPool - min) / times));

            int median = allocationPool / remaining;
            min = Math.max(min, Math.min(max, median) / 2);
            max = Math.min(max, median * 2);
            return min >= max ? min : random.uniform(min, max);
        }
    }


    public final RandomSource random;
    public final SimulatedSystems simulated;
    public final Cluster cluster;
    public final S simulation;
    protected final Map<Integer, InterceptingExecutorFactory> factories = new TreeMap<>();

    public ClusterSimulation(RandomSource random, long seed, int uniqueNum,
                             Builder<?> builder,
                             Consumer<IInstanceConfig> configUpdater,
                             SimulationFactory<S> factory) throws IOException
    {
        this.random = random;

        final SimulatedMessageDelivery delivery;
        final SimulatedWaits waits;
        final SimulatedExecution execution;
        final SimulatedBallots ballots;
        final SimulatedSnitch snitch;
        final SimulatedTime time;
        final SimulatedFailureDetector failureDetector;

        int numOfNodes = builder.nodeCount.select(random);
        int numOfDcs = builder.dcCount.select(random, 0, numOfNodes / 3);
        int[] numInDcs = new int[numOfDcs];
        int[] nodeToDc = new int[numOfNodes];

        int[] minRf = new int[numOfDcs], initialRf = new int[numOfDcs], maxRf = new int[numOfDcs];
        {
            // TODO (future): split unevenly
            int n = 0, nc = 0;
            for (int i = 0; i < numOfDcs; ++i)
            {
                int numInDc = (numOfNodes / numOfDcs) + (numOfNodes % numOfDcs > i ? 1 : 0);
                numInDcs[i] = numInDc;
                minRf[i] = 3;
                maxRf[i] = min(numInDc, 9);
                initialRf[i] = random.uniform(minRf[i], 1 + maxRf[i]);
                nc += numInDc;
                while (n < nc)
                    nodeToDc[n++] = i;
            }
        }
        snitch = new SimulatedSnitch(nodeToDc, numInDcs);

        waits = new SimulatedWaits(random);
        waits.captureStackTraces(builder.capture.waitSites, builder.capture.wakeSites, builder.capture.nowSites);
        execution = new SimulatedExecution();

        time = new SimulatedTime(random, 1577836800000L /*Jan 1st UTC*/, MILLISECONDS.toNanos(10L));
        ballots = new SimulatedBallots(random, () -> {
            long max = random.uniform(2, 16);
            return () -> random.uniform(1, max);
        });

        ThreadAllocator threadAllocator = new ThreadAllocator(random, builder.threadCount, numOfNodes);
        cluster = snitch.setup(Cluster.build(numOfNodes)
                         .withRoot(jimfs.getPath(seed + "-" + uniqueNum))
                         .withSharedClasses(getSharedClassPredicate(ISOLATE, SHARE, ANY, SIMULATION))
                         .withConfig(config -> configUpdater.accept(threadAllocator.update(config
                             .set("read_request_timeout_in_ms", 10000000L)
                             .set("write_request_timeout_in_ms", 10000000L)
                             .set("cas_contention_timeout_in_ms", 10000000L)
                             .set("request_timeout_in_ms", 10000000L)
    //                            .set("memtable_cleanup_threshold", 0.05f)
                             .set("memtable_heap_space_in_mb", 1)
                             .set("file_cache_size_in_mb", 16)
                             .set("use_deterministic_table_id", true)
                             .set("disk_access_mode", "standard")
                             .set("failure_detector", SimulatedFailureDetector.Instance.class.getName())
                             .set("commitlog_compression", new ParameterizedClass(LZ4Compressor.class.getName(), emptyMap()))
                         )))
                         .withInstanceInitializer(new IInstanceInitializer()
                         {
                             @Override
                             public void initialise(ClassLoader classLoader, ThreadGroup threadGroup, int num, int generation)
                             {
                                 InterceptingExecutorFactory factory = execution.factory(classLoader, threadGroup);
                                 IsolatedExecutor.transferAdhoc((SerializableConsumer<ExecutorFactory>) ExecutorFactory.Global::unsafeSet, classLoader)
                                                 .accept(factory);
                                 IsolatedExecutor.transferAdhoc((SerializableTriConsumer<InterceptorOfGlobalMethods, Integer, Integer>) InterceptorOfGlobalMethods.Global::unsafeSet, classLoader)
                                                 .accept(waits.interceptGlobalMethods(classLoader), random.uniform(0, Integer.MAX_VALUE), random.uniform(0, Integer.MAX_VALUE));

                                 time.setup(classLoader);
                                 factories.put(num, factory);
                             }

                             @Override
                             public void beforeStartup(IInstance i)
                             {
                                 ((IInvokableInstance) i).unsafeAcceptOnThisThread(FBUtilities::setAvailableProcessors, i.config().getInt("available_processors"));
                             }

                             @Override
                             public void afterStartup(IInstance i)
                             {
                                 ((IInvokableInstance) i).unsafeAcceptOnThisThread(BallotGenerator.Global::unsafeSet, (BallotGenerator) ballots.get());
                             }

                         }).withClassTransformer(new InterceptClasses(builder.delayChance.asSupplier(random), builder.nemesisChance.asSupplier(random), NemesisFieldSelectors.get())::apply)
        ).createWithoutStarting();

        snitch.setup(cluster);
        DirectStreamingConnectionFactory.setup(cluster);
        delivery = new SimulatedMessageDelivery(cluster);
        failureDetector = new SimulatedFailureDetector(cluster);
        simulated = new SimulatedSystems(random, time, waits, delivery, execution, ballots, failureDetector, snitch, builder.debug, new Failures());

        ActionSchedulers scheduler = builder.schedulerFactory.create(random, builder.delayChance, builder.dropChance, builder.timeoutChance);
        ClusterActions.Options options = new ClusterActions.Options(Choices.random(random, builder.topologyChanges), minRf, initialRf, maxRf, null);
        simulation = factory.create(simulated, scheduler, cluster, options);
    }

    public void close() throws IOException
    {
        // Re-enable time on shutdown
        try
        {
            Field field = Clock.Global.class.getDeclaredField("instance");
            field.setAccessible(true);

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            field.set(null, new Clock.Default());
        }
        catch (NoSuchFieldException|IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }

        simulated.waits.stop();
        simulated.execution.forceStop();
        factories.values().forEach(InterceptingExecutorFactory::close);
        Throwable fail = null;
        try
        {
            simulation.close();
        }
        catch (Throwable t)
        {
            fail = t;
        }
        try
        {
            cluster.close();
        }
        catch (Throwable t)
        {
            fail = Throwables.merge(fail, t);
        }
        Throwables.maybeFail(fail, IOException.class);
    }
}

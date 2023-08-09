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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInstanceInitializer;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableBiConsumer;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableConsumer;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableRunnable;
import org.apache.cassandra.distributed.impl.DirectStreamingConnectionFactory;
import org.apache.cassandra.distributed.impl.IsolatedExecutor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.util.FileSystems;
import org.apache.cassandra.service.paxos.BallotGenerator;
import org.apache.cassandra.service.paxos.PaxosPrepare;
import org.apache.cassandra.simulator.RandomSource.Choices;
import org.apache.cassandra.simulator.asm.InterceptAsClassTransformer;
import org.apache.cassandra.simulator.asm.NemesisFieldSelectors;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.cluster.ClusterActions.TopologyChange;
import org.apache.cassandra.io.filesystem.ListenableFileSystem;
import org.apache.cassandra.simulator.systems.Failures;
import org.apache.cassandra.simulator.systems.InterceptedWait.CaptureSites.Capture;
import org.apache.cassandra.simulator.systems.InterceptibleThread;
import org.apache.cassandra.simulator.systems.InterceptingGlobalMethods;
import org.apache.cassandra.simulator.systems.InterceptingGlobalMethods.ThreadLocalRandomCheck;
import org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods;
import org.apache.cassandra.simulator.systems.InterceptingExecutorFactory;
import org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods.IfInterceptibleThread;
import org.apache.cassandra.simulator.systems.NetworkConfig;
import org.apache.cassandra.simulator.systems.NetworkConfig.PhaseConfig;
import org.apache.cassandra.simulator.systems.SchedulerConfig;
import org.apache.cassandra.simulator.systems.SimulatedFutureActionScheduler;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.simulator.systems.SimulatedBallots;
import org.apache.cassandra.simulator.systems.SimulatedExecution;
import org.apache.cassandra.simulator.systems.SimulatedFailureDetector;
import org.apache.cassandra.simulator.systems.SimulatedMessageDelivery;
import org.apache.cassandra.simulator.systems.SimulatedSnitch;
import org.apache.cassandra.simulator.systems.SimulatedTime;
import org.apache.cassandra.simulator.utils.ChanceRange;
import org.apache.cassandra.simulator.utils.IntRange;
import org.apache.cassandra.simulator.utils.KindOfSequence;
import org.apache.cassandra.simulator.utils.LongRange;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;
import org.apache.cassandra.utils.memory.HeapPool;

import static java.lang.Integer.min;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.impl.AbstractCluster.getSharedClassPredicate;
import static org.apache.cassandra.simulator.SimulatorUtils.failWithOOM;
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
                                            io.netty.util.ReferenceCounted.class,
                                            io.netty.util.concurrent.FastThreadLocal.class
                                        };

    public static final Class<?>[] ISOLATE = new Class<?>[0];

    public interface SimulationFactory<S extends Simulation>
    {
        S create(SimulatedSystems simulated, RunnableActionScheduler scheduler, Cluster cluster, ClusterActions.Options options);
    }

    public interface SchedulerFactory
    {
        RunnableActionScheduler create(RandomSource random);
    }

    @SuppressWarnings("UnusedReturnValue")
    public static abstract class Builder<S extends Simulation>
    {
        protected Supplier<RandomSource> randomSupplier = RandomSource.Default::new;
        protected int uniqueNum = 0;
        protected int threadCount;

        protected int concurrency = 10;
        protected IntRange nodeCount = new IntRange(4, 16), dcCount = new IntRange(1, 2),
                        primaryKeySeconds = new IntRange(5, 30), withinKeyConcurrency = new IntRange(2, 5);
        protected TopologyChange[] topologyChanges = TopologyChange.values();
        protected int topologyChangeLimit = -1;

        protected int primaryKeyCount;
        protected int secondsToSimulate;

        protected ChanceRange normalNetworkDropChance  = new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4), 0f, 0.001f),
                              normalNetworkDelayChance = new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4), 0.01f, 0.1f),
                                flakyNetworkDropChance = new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4), 0.01f, 0.1f),
                               flakyNetworkDelayChance = new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4), 0.01f, 0.1f),
                                networkPartitionChance = new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4), 0.0f, 0.1f),
                                    networkFlakyChance = new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4), 0.0f, 0.1f),
                                    monitorDelayChance = new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4), 0.01f, 0.1f),
                                  schedulerDelayChance = new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4), 0.01f, 0.1f),
                                         timeoutChance = new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4), 0.01f, 0.1f),
                                            readChance = new ChanceRange(RandomSource::uniformFloat,                        0.05f, 0.95f),
                                         nemesisChance = new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4), 0.001f, 0.01f);

        protected LongRange normalNetworkLatencyNanos = new LongRange(1, 2, MILLISECONDS, NANOSECONDS),
                              normalNetworkDelayNanos = new LongRange(2, 100, MILLISECONDS, NANOSECONDS),
                             flakyNetworkLatencyNanos = new LongRange(2, 100, MILLISECONDS, NANOSECONDS),
                               flakyNetworkDelayNanos = new LongRange(2, 100, MILLISECONDS, NANOSECONDS),
                           networkReconfigureInterval = new LongRange(50, 5000, MICROSECONDS, NANOSECONDS),
                                 schedulerJitterNanos = new LongRange(100, 2000, MICROSECONDS, NANOSECONDS),
                                  schedulerDelayNanos = new LongRange(0, 50, MICROSECONDS, NANOSECONDS),
                              schedulerLongDelayNanos = new LongRange(50, 5000, MICROSECONDS, NANOSECONDS),
                                      clockDriftNanos = new LongRange(1, 5000, MILLISECONDS, NANOSECONDS),
                       clockDiscontinuitIntervalNanos = new LongRange(10, 60, SECONDS, NANOSECONDS),
                          topologyChangeIntervalNanos = new LongRange(5, 15, SECONDS, NANOSECONDS);



        protected long contentionTimeoutNanos = MILLISECONDS.toNanos(500L),
                            writeTimeoutNanos = SECONDS.toNanos(1L),
                             readTimeoutNanos = SECONDS.toNanos(2L),
                          requestTimeoutNanos = SECONDS.toNanos(2L);

        protected SchedulerFactory schedulerFactory = schedulerFactory(RunnableActionScheduler.Kind.values());

        protected Debug debug = new Debug();
        protected Capture capture = new Capture(false, false, false);
        protected HeapPool.Logged.Listener memoryListener;
        protected SimulatedTime.Listener timeListener = (i1, i2) -> {};
        protected LongConsumer onThreadLocalRandomCheck;

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

        public Builder<S> nodes(IntRange range)
        {
            this.nodeCount = range;
            return this;
        }

        public Builder<S> nodes(int min, int max)
        {
            this.nodeCount = new IntRange(min, max);
            return this;
        }

        public Builder<S> dcs(IntRange range)
        {
            this.dcCount = range;
            return this;
        }

        public Builder<S> dcs(int min, int max)
        {
            this.dcCount = new IntRange(min, max);
            return this;
        }

        public Builder<S> concurrency(int concurrency)
        {
            this.concurrency = concurrency;
            return this;
        }

        public IntRange primaryKeySeconds()
        {
            return primaryKeySeconds;
        }

        public Builder<S> primaryKeySeconds(IntRange range)
        {
            this.primaryKeySeconds = range;
            return this;
        }

        public Builder<S> withinKeyConcurrency(IntRange range)
        {
            this.withinKeyConcurrency = range;
            return this;
        }

        public Builder<S> withinKeyConcurrency(int min, int max)
        {
            this.withinKeyConcurrency = new IntRange(min, max);
            return this;
        }

        public Builder<S> topologyChanges(TopologyChange[] topologyChanges)
        {
            this.topologyChanges = topologyChanges;
            return this;
        }

        public Builder<S> topologyChangeIntervalNanos(LongRange topologyChangeIntervalNanos)
        {
            this.topologyChangeIntervalNanos = topologyChangeIntervalNanos;
            return this;
        }

        public Builder<S> topologyChangeLimit(int topologyChangeLimit)
        {
            this.topologyChangeLimit = topologyChangeLimit;
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

        public int secondsToSimulate()
        {
            return secondsToSimulate;
        }

        public Builder<S> secondsToSimulate(int seconds)
        {
            this.secondsToSimulate = seconds;
            return this;
        }

        public Builder<S> networkPartitionChance(ChanceRange partitionChance)
        {
            this.networkPartitionChance = partitionChance;
            return this;
        }

        public Builder<S> networkFlakyChance(ChanceRange flakyChance)
        {
            this.networkFlakyChance = flakyChance;
            return this;
        }

        public Builder<S> networkReconfigureInterval(LongRange reconfigureIntervalNanos)
        {
            this.networkReconfigureInterval = reconfigureIntervalNanos;
            return this;
        }

        public Builder<S> networkDropChance(ChanceRange dropChance)
        {
            this.normalNetworkDropChance = dropChance;
            return this;
        }

        public Builder<S> networkDelayChance(ChanceRange delayChance)
        {
            this.normalNetworkDelayChance = delayChance;
            return this;
        }

        public Builder<S> networkLatencyNanos(LongRange networkLatencyNanos)
        {
            this.normalNetworkLatencyNanos = networkLatencyNanos;
            return this;
        }

        public Builder<S> networkDelayNanos(LongRange networkDelayNanos)
        {
            this.normalNetworkDelayNanos = networkDelayNanos;
            return this;
        }

        public Builder<S> flakyNetworkDropChance(ChanceRange dropChance)
        {
            this.flakyNetworkDropChance = dropChance;
            return this;
        }

        public Builder<S> flakyNetworkDelayChance(ChanceRange delayChance)
        {
            this.flakyNetworkDelayChance = delayChance;
            return this;
        }

        public Builder<S> flakyNetworkLatencyNanos(LongRange networkLatencyNanos)
        {
            this.flakyNetworkLatencyNanos = networkLatencyNanos;
            return this;
        }

        public Builder<S> flakyNetworkDelayNanos(LongRange networkDelayNanos)
        {
            this.flakyNetworkDelayNanos = networkDelayNanos;
            return this;
        }

        public Builder<S> clockDriftNanos(LongRange clockDriftNanos)
        {
            this.clockDriftNanos = clockDriftNanos;
            return this;
        }

        public Builder<S> clockDiscontinuityIntervalNanos(LongRange clockDiscontinuityIntervalNanos)
        {
            this.clockDiscontinuitIntervalNanos = clockDiscontinuityIntervalNanos;
            return this;
        }

        public Builder<S> schedulerDelayChance(ChanceRange delayChance)
        {
            this.schedulerDelayChance = delayChance;
            return this;
        }

        public Builder<S> schedulerJitterNanos(LongRange schedulerJitterNanos)
        {
            this.schedulerJitterNanos = schedulerJitterNanos;
            return this;
        }

        public LongRange schedulerJitterNanos()
        {
            return schedulerJitterNanos;
        }

        public Builder<S> schedulerDelayNanos(LongRange schedulerDelayNanos)
        {
            this.schedulerDelayNanos = schedulerDelayNanos;
            return this;
        }

        public Builder<S> schedulerLongDelayNanos(LongRange schedulerLongDelayNanos)
        {
            this.schedulerLongDelayNanos = schedulerLongDelayNanos;
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

        public IntRange withinKeyConcurrency()
        {
            return withinKeyConcurrency;
        }

        public int concurrency()
        {
            return concurrency;
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

        public Builder<S> scheduler(RunnableActionScheduler.Kind... kinds)
        {
            this.schedulerFactory = schedulerFactory(kinds);
            return this;
        }

        public SimulatedFutureActionScheduler futureActionScheduler(int nodeCount, SimulatedTime time, RandomSource random)
        {
            KindOfSequence kind = Choices.random(random, KindOfSequence.values())
                                         .choose(random);
            return new SimulatedFutureActionScheduler(kind, nodeCount, random, time,
                                                      new NetworkConfig(new PhaseConfig(normalNetworkDropChance, normalNetworkDelayChance, normalNetworkLatencyNanos, normalNetworkDelayNanos),
                                                                        new PhaseConfig(flakyNetworkDropChance, flakyNetworkDelayChance, flakyNetworkLatencyNanos, flakyNetworkDelayNanos),
                                                                        networkPartitionChance, networkFlakyChance, networkReconfigureInterval),
                                                      new SchedulerConfig(schedulerDelayChance, schedulerDelayNanos, schedulerLongDelayNanos));
        }

        static SchedulerFactory schedulerFactory(RunnableActionScheduler.Kind... kinds)
        {
            return (random) -> {
                switch (Choices.random(random, kinds).choose(random))
                {
                    default: throw new AssertionError();
                    case SEQUENTIAL: return new RunnableActionScheduler.Sequential();
                    case UNIFORM: return new RunnableActionScheduler.RandomUniform(random);
                    case RANDOM_WALK: return new RunnableActionScheduler.RandomWalk(random);
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

        public Builder<S> memoryListener(HeapPool.Logged.Listener memoryListener)
        {
            this.memoryListener = memoryListener;
            return this;
        }

        public Builder<S> timeListener(SimulatedTime.Listener timeListener)
        {
            this.timeListener = timeListener;
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

        public Builder<S> onThreadLocalRandomCheck(LongConsumer runnable)
        {
            this.onThreadLocalRandomCheck = runnable;
            return this;
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
    private final ListenableFileSystem fs;
    protected final Map<Integer, List<Closeable>> onUnexpectedShutdown = new TreeMap<>();
    protected final List<Callable<Void>> onShutdown = new CopyOnWriteArrayList<>();
    protected final ThreadLocalRandomCheck threadLocalRandomCheck;

    public ClusterSimulation(RandomSource random, long seed, int uniqueNum,
                             Builder<?> builder,
                             Consumer<IInstanceConfig> configUpdater,
                             SimulationFactory<S> factory) throws IOException
    {
        this.random = random;
        this.fs = new ListenableFileSystem(FileSystems.jimfs(Long.toHexString(seed) + 'x' + uniqueNum));

        final SimulatedMessageDelivery delivery;
        final SimulatedExecution execution;
        final SimulatedBallots ballots;
        final SimulatedSnitch snitch;
        final SimulatedTime time;
        final SimulatedFailureDetector failureDetector;

        int numOfNodes = builder.nodeCount.select(random);
        int numOfDcs = builder.dcCount.select(random, 0, numOfNodes / 4);
        int[] numInDcs = new int[numOfDcs];
        int[] nodeToDc = new int[numOfNodes];

        int[] minRf = new int[numOfDcs], initialRf = new int[numOfDcs], maxRf = new int[numOfDcs];
        {
            // TODO (feature): split unevenly
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

        execution = new SimulatedExecution();

        KindOfSequence kindOfDriftSequence = Choices.uniform(KindOfSequence.values()).choose(random);
        KindOfSequence kindOfDiscontinuitySequence = Choices.uniform(KindOfSequence.values()).choose(random);
        time = new SimulatedTime(numOfNodes, random, 1577836800000L /*Jan 1st UTC*/, builder.clockDriftNanos, kindOfDriftSequence,
                                 kindOfDiscontinuitySequence.period(builder.clockDiscontinuitIntervalNanos, random),
                                 builder.timeListener);
        ballots = new SimulatedBallots(random, () -> {
            long max = random.uniform(2, 16);
            return () -> random.uniform(1, max);
        });

        Predicate<String> sharedClassPredicate = getSharedClassPredicate(ISOLATE, SHARE, ANY, SIMULATION);
        InterceptAsClassTransformer interceptClasses = new InterceptAsClassTransformer(builder.monitorDelayChance.asSupplier(random), builder.nemesisChance.asSupplier(random), NemesisFieldSelectors.get(), ClassLoader.getSystemClassLoader(), sharedClassPredicate.negate());
        threadLocalRandomCheck = new ThreadLocalRandomCheck(builder.onThreadLocalRandomCheck);

        Failures failures = new Failures();
        ThreadAllocator threadAllocator = new ThreadAllocator(random, builder.threadCount, numOfNodes);
        List<String> allowedDiskAccessModes = Arrays.asList("mmap", "mmap_index_only", "standard");
        String disk_access_mode = allowedDiskAccessModes.get(random.uniform(0, allowedDiskAccessModes.size() - 1));
        boolean commitlogCompressed = random.decide(.5f);
        cluster = snitch.setup(Cluster.build(numOfNodes)
                         .withRoot(fs.getPath("/cassandra"))
                         .withSharedClasses(sharedClassPredicate)
                         .withConfig(config -> {
                             config.with(Feature.BLANK_GOSSIP)
                                   .set("read_request_timeout", String.format("%dms", NANOSECONDS.toMillis(builder.readTimeoutNanos)))
                                   .set("write_request_timeout", String.format("%dms", NANOSECONDS.toMillis(builder.writeTimeoutNanos)))
                                   .set("cas_contention_timeout", String.format("%dms", NANOSECONDS.toMillis(builder.contentionTimeoutNanos)))
                                   .set("request_timeout", String.format("%dms", NANOSECONDS.toMillis(builder.requestTimeoutNanos)))
                                   .set("memtable_heap_space", "1MiB")
                                   .set("memtable_allocation_type", builder.memoryListener != null ? "unslabbed_heap_buffers_logged" : "heap_buffers")
                                   .set("file_cache_size", "16MiB")
                                   .set("use_deterministic_table_id", true)
                                   .set("disk_access_mode", disk_access_mode)
                                   .set("failure_detector", SimulatedFailureDetector.Instance.class.getName());
                             if (commitlogCompressed)
                                 config.set("commitlog_compression", new ParameterizedClass(LZ4Compressor.class.getName(), emptyMap()));
                             configUpdater.accept(threadAllocator.update(config));
                         })
                         .withInstanceInitializer(new IInstanceInitializer()
                         {
                             @Override
                             public void initialise(ClassLoader classLoader, ThreadGroup threadGroup, int num, int generation)
                             {
                                 List<Closeable> onShutdown = new ArrayList<>();
                                 InterceptorOfGlobalMethods interceptorOfGlobalMethods = IsolatedExecutor.transferAdhoc((IIsolatedExecutor.SerializableQuadFunction<Capture, LongConsumer, Consumer<Throwable>, RandomSource, InterceptorOfGlobalMethods>) InterceptingGlobalMethods::new, classLoader)
                                                                                                         .apply(builder.capture, builder.onThreadLocalRandomCheck, failures, random);
                                 onShutdown.add(interceptorOfGlobalMethods);

                                 InterceptingExecutorFactory factory = execution.factory(interceptorOfGlobalMethods, classLoader, threadGroup);
                                 IsolatedExecutor.transferAdhoc((SerializableConsumer<ExecutorFactory>) ExecutorFactory.Global::unsafeSet, classLoader)
                                                 .accept(factory);
                                 onShutdown.add(factory);

                                 IsolatedExecutor.transferAdhoc((SerializableBiConsumer<InterceptorOfGlobalMethods, IntSupplier>) InterceptorOfGlobalMethods.Global::unsafeSet, classLoader)
                                                 .accept(interceptorOfGlobalMethods, () -> {
                                                     if (InterceptibleThread.isDeterministic())
                                                         throw failWithOOM();
                                                     return random.uniform(Integer.MIN_VALUE, Integer.MAX_VALUE);
                                                 });
                                 onShutdown.add(IsolatedExecutor.transferAdhoc((SerializableRunnable)InterceptorOfGlobalMethods.Global::unsafeReset, classLoader)::run);
                                 onShutdown.add(time.setup(num, classLoader));

                                 onUnexpectedShutdown.put(num, onShutdown);
                             }

                             @Override
                             public void beforeStartup(IInstance i)
                             {
                                 ((IInvokableInstance) i).unsafeAcceptOnThisThread(FBUtilities::setAvailableProcessors, i.config().getInt("available_processors"));
                                 ((IInvokableInstance) i).unsafeAcceptOnThisThread(IfInterceptibleThread::setThreadLocalRandomCheck, (LongConsumer) threadLocalRandomCheck);

                                 int num = i.config().num();
                                 if (builder.memoryListener != null)
                                 {
                                    ((IInvokableInstance) i).unsafeAcceptOnThisThread(HeapPool.Logged::setListener, builder.memoryListener);
                                     onUnexpectedShutdown.get(num).add(() -> ((IInvokableInstance) i).unsafeAcceptOnThisThread(HeapPool.Logged::setListener, (ignore1, ignore2) -> {}));
                                 }

                                 ((IInvokableInstance) i).unsafeAcceptOnThisThread(PaxosPrepare::setOnLinearizabilityViolation, SimulatorUtils::failWithOOM);
                                 onUnexpectedShutdown.get(num).add(() -> ((IInvokableInstance) i).unsafeRunOnThisThread(() -> PaxosPrepare.setOnLinearizabilityViolation(null)));
                             }

                             @Override
                             public void afterStartup(IInstance i)
                             {
                                 int num = i.config().num();
                                 ((IInvokableInstance) i).unsafeAcceptOnThisThread(BallotGenerator.Global::unsafeSet, (BallotGenerator) ballots.get());
                                 onUnexpectedShutdown.get(num).add(() -> ((IInvokableInstance) i).unsafeRunOnThisThread(() -> BallotGenerator.Global.unsafeSet(new BallotGenerator.Default())));

                                 ((IInvokableInstance) i).unsafeAcceptOnThisThread((SerializableConsumer<BufferPool.DebugLeaks>) debug -> BufferPools.forChunkCache().debug(null, debug), failures);
                                 onUnexpectedShutdown.get(num).add(() -> ((IInvokableInstance) i).unsafeRunOnThisThread(() -> BufferPools.forChunkCache().debug(null, null)));

                                 ((IInvokableInstance) i).unsafeAcceptOnThisThread((SerializableConsumer<BufferPool.DebugLeaks>) debug -> BufferPools.forNetworking().debug(null, debug), failures);
                                 onUnexpectedShutdown.get(num).add(() -> ((IInvokableInstance) i).unsafeRunOnThisThread(() -> BufferPools.forNetworking().debug(null, null)));

                                 ((IInvokableInstance) i).unsafeAcceptOnThisThread((SerializableConsumer<Ref.OnLeak>) Ref::setOnLeak, failures);
                                 onUnexpectedShutdown.get(num).add(() -> ((IInvokableInstance) i).unsafeRunOnThisThread(() -> Ref.setOnLeak(null)));
                             }
                         }).withClassTransformer(interceptClasses)
                           .withShutdownExecutor((name, classLoader, shuttingDown, call) -> {
                               onShutdown.add(call);
                               return null;
                           })
        ).createWithoutStarting();

        IfInterceptibleThread.setThreadLocalRandomCheck(threadLocalRandomCheck);
        snitch.setup(cluster);
        DirectStreamingConnectionFactory.setup(cluster);
        delivery = new SimulatedMessageDelivery(cluster);
        failureDetector = new SimulatedFailureDetector(cluster);
        SimulatedFutureActionScheduler futureActionScheduler = builder.futureActionScheduler(numOfNodes, time, random);
        simulated = new SimulatedSystems(random, time, delivery, execution, ballots, failureDetector, snitch, futureActionScheduler, builder.debug, failures);
        simulated.register(futureActionScheduler);

        RunnableActionScheduler scheduler = builder.schedulerFactory.create(random);
        ClusterActions.Options options = new ClusterActions.Options(builder.topologyChangeLimit, Choices.uniform(KindOfSequence.values()).choose(random).period(builder.topologyChangeIntervalNanos, random),
                                                                    Choices.random(random, builder.topologyChanges),
                                                                    minRf, initialRf, maxRf, null);
        simulation = factory.create(simulated, scheduler, cluster, options);
    }

    public synchronized void close() throws IOException
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

        threadLocalRandomCheck.stop();
        simulated.execution.forceStop();
        SimulatedTime.Global.disable();

        Throwable fail = null;
        for (int num = 1 ; num <= cluster.size() ; ++num)
        {
            if (!cluster.get(num).isShutdown())
            {
                fail = Throwables.close(fail, onUnexpectedShutdown.get(num));
            }
        }

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
        for (Callable<Void> call : onShutdown)
        {
            try
            {
                call.call();
            }
            catch (Throwable t)
            {
                fail = Throwables.merge(fail, t);
            }
        }
        Throwables.maybeFail(fail, IOException.class);
    }
}

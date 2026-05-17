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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntSupplier;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.RateLimiter;

import org.agrona.collections.IntArrayList;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.CommandStore;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.primitives.PartialDeps;
import accord.primitives.TxnId;
import accord.utils.Functions;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.shared.DistributedTestBase;
import org.apache.cassandra.metrics.AccordCoordinatorMetrics;
import org.apache.cassandra.metrics.AccordExecutorMetrics;
import org.apache.cassandra.metrics.ShardedDecayingHistograms.ShardedDecayingHistogram;
import org.apache.cassandra.metrics.ShardedHistogram;
import org.apache.cassandra.metrics.SnapshottingTimer;
import org.apache.cassandra.net.ArtificialLatency;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.debug.AccordTracing;
import org.apache.cassandra.service.accord.debug.AccordTracing.Message;
import org.apache.cassandra.service.accord.debug.CoordinationKinds;
import org.apache.cassandra.service.accord.debug.TxnKindsAndDomains;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static accord.coordinate.Coordination.CoordinationKind.Client;
import static accord.coordinate.Coordination.CoordinationKind.Execute;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.apache.cassandra.service.accord.debug.AccordTracing.BucketMode.LEAKY;
import static org.apache.cassandra.service.accord.debug.AccordTracing.BucketMode.RING;
import static org.apache.cassandra.service.accord.debug.AccordTracing.BucketMode.SLOWEST;

public class AccordLoadTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordLoadTest.class);

    @BeforeClass
    public static void setUp() throws IOException
    {
        CassandraRelevantProperties.SIMULATOR_STARTED.setString(Long.toString(MILLISECONDS.toSeconds(currentTimeMillis())));
        int nodeCount = 5;
        AccordTestBase.setupCluster(builder -> builder.withDCs(nodeCount).withConfig(config -> {
            config.with(Feature.NETWORK, Feature.GOSSIP)
                  .set("accord.shard_durability_target_splits", "8")
                  .set("accord.shard_durability_max_splits", "16")
                  .set("accord.shard_durability_cycle", "1m")
                  .set("accord.queue_submission_model", "SIGNAL")
//                  .set("accord.queue_submission_model", "SEMI_SYNC")
                  .set("accord.command_store_shard_count", "8")
                  .set("accord.queue_thread_count", "4")
                  .set("accord.queue_shard_count", "1")
                  .set("accord.replica_execution", "ALL")
                  .set("accord.send_stable", "TO_ALL_REPLICA_EXECUTABLE_ELSE_FOR_READS")
                  .set("accord.send_minimal", "false")
//                  .set("accord.permit_fast_quorum_medium_path", "false")
                  .set("accord.catchup_on_start_fail_latency", "2m");
        }), nodeCount);
    }

    public static class Settings
    {
        final int repairInterval;
        final int compactionInterval;
        final int journalFlushInterval;
        final int cfkFlushInterval;
        final int cfkCompactionPeriodSeconds;
        final int dataFlushInterval;
        final int restartInterval;
        final int restartDecay;
        final int batchSize;
        final long batchPeriodNanos;
        final int clientConcurrency;
        final int clients;
        final int ratePerSecond;
        final int minRatePerSecond;
        final int increaseRatePerSecondInterval;
        final int keysPerOperation;
        final float readRatio;
        final IntSupplier keySelector;
        final boolean readBeforeWrite;
        final float traceSlowest;
        final int traceLast;
        final int[][] artificialLatencies;

        Settings(SettingsBuilder builder)
        {
            this.repairInterval = builder.repairInterval;
            this.compactionInterval = builder.compactionInterval;
            this.journalFlushInterval = builder.journalFlushInterval;
            this.cfkFlushInterval = builder.cfkFlushInterval;
            this.cfkCompactionPeriodSeconds = builder.cfkCompactionPeriodSeconds;
            this.dataFlushInterval = builder.dataFlushInterval;
            this.restartInterval = builder.restartInterval;
            this.restartDecay = builder.restartDecay;
            this.batchSize = builder.batchSize;
            this.batchPeriodNanos = builder.batchPeriodNanos;
            this.clientConcurrency = builder.clientConcurrency;
            this.clients = builder.clients;
            this.ratePerSecond = builder.ratePerSecond;
            this.minRatePerSecond = builder.minRatePerSecond;
            this.increaseRatePerSecondInterval = builder.increaseRatePerSecondInterval;
            this.keysPerOperation = builder.keysPerOperation;
            this.readRatio = builder.readRatio;
            this.keySelector = builder.keySelector;
            this.readBeforeWrite = builder.readBeforeWrite;
            this.artificialLatencies = builder.artificialLatencies;
            this.traceSlowest = builder.traceSlowest;
            this.traceLast = builder.traceLast;
        }
    }

    // interval is measured in terms of *operations* unless otherwise specified
    public static class SettingsBuilder
    {
        int repairInterval = Integer.MAX_VALUE;
        int compactionInterval = Integer.MAX_VALUE;
        int journalFlushInterval = Integer.MAX_VALUE;
        int cfkFlushInterval = Integer.MAX_VALUE;
        int cfkCompactionPeriodSeconds = 0;
        int dataFlushInterval = Integer.MAX_VALUE;
        int restartInterval = Integer.MAX_VALUE;
        int restartDecay = 2;
        int batchSize = 1000;
        long batchPeriodNanos = SECONDS.toNanos(10);
        int clientConcurrency = 50;
        int clients = -1;
        int ratePerSecond = 1000;
        int minRatePerSecond = 50;
        int increaseRatePerSecondInterval = 1000;
        int keysPerOperation = 1;
        float readRatio = 0.5f;
        IntSupplier keySelector;
        boolean readBeforeWrite;
        float traceSlowest;
        int traceLast;
        int[][] artificialLatencies;

        public SettingsBuilder setRepairInterval(int repairInterval)
        {
            this.repairInterval = repairInterval;
            return this;
        }

        public SettingsBuilder setCompactionInterval(int compactionInterval)
        {
            this.compactionInterval = compactionInterval;
            return this;
        }

        public SettingsBuilder setJournalFlushInterval(int journalFlushInterval)
        {
            this.journalFlushInterval = journalFlushInterval;
            return this;
        }

        public SettingsBuilder setCfkFlushInterval(int cfkFlushInterval)
        {
            this.cfkFlushInterval = cfkFlushInterval;
            return this;
        }

        public SettingsBuilder setCfkCompactionPeriodSeconds(int cfkCompactionPeriodSeconds)
        {
            this.cfkCompactionPeriodSeconds = cfkCompactionPeriodSeconds;
            return this;
        }

        public SettingsBuilder setDataFlushInterval(int dataFlushInterval)
        {
            this.dataFlushInterval = dataFlushInterval;
            return this;
        }

        public SettingsBuilder setRestartInterval(int restartInterval)
        {
            this.restartInterval = restartInterval;
            return this;
        }

        public SettingsBuilder setRestartDecay(int restartDecay)
        {
            this.restartDecay = restartDecay;
            return this;
        }

        public SettingsBuilder setBatchSize(int batchSize)
        {
            this.batchSize = batchSize;
            return this;
        }

        public SettingsBuilder setBatchPeriodNanos(long batchPeriodNanos)
        {
            this.batchPeriodNanos = batchPeriodNanos;
            return this;
        }

        public SettingsBuilder setClientConcurrency(int clientConcurrency)
        {
            this.clientConcurrency = clientConcurrency;
            return this;
        }

        public SettingsBuilder setClients(int clients)
        {
            this.clients = clients;
            return this;
        }

        public SettingsBuilder setRatePerSecond(int ratePerSecond)
        {
            this.ratePerSecond = ratePerSecond;
            return this;
        }

        public SettingsBuilder setMinRatePerSecond(int minRatePerSecond)
        {
            this.minRatePerSecond = minRatePerSecond;
            return this;
        }

        public SettingsBuilder setIncreaseRatePerSecondInterval(int increaseRatePerSecondInterval)
        {
            this.increaseRatePerSecondInterval = increaseRatePerSecondInterval;
            return this;
        }

        public SettingsBuilder setKeysPerOperation(int keysPerOperation)
        {
            this.keysPerOperation = keysPerOperation;
            return this;
        }

        public SettingsBuilder setReadRatio(float readRatio)
        {
            this.readRatio = readRatio;
            return this;
        }

        public SettingsBuilder setReadBeforeWrite(boolean readBeforeWrite)
        {
            this.readBeforeWrite = readBeforeWrite;
            return this;
        }

        public SettingsBuilder setTraceSlowest(float traceSlowest)
        {
            this.traceSlowest = traceSlowest;
            return this;
        }

        public SettingsBuilder setTraceLast(int traceLast)
        {
            this.traceLast = traceLast;
            return this;
        }

        public SettingsBuilder setKeySelector(IntSupplier keySelector)
        {
            this.keySelector = keySelector;
            return this;
        }

        public SettingsBuilder setArtificialLatencies(int[][] artificialLatencies)
        {
            this.artificialLatencies = artificialLatencies;
            return this;
        }

        public Settings build()
        {
            return new Settings(this);
        }
    }

    private static final int[][] LATENCIES = new int[][] {
        new int[] {  0, 44, 64, 43, 84 },
        new int[] { 44,  0, 30,  3, 45 },
        new int[] { 64, 30,  0, 28, 37 },
        new int[] { 43,  3, 28,  0, 49 },
        new int[] { 84, 45, 37, 49,  0 }
    };

    private static SettingsBuilder withArtificialLatencies(SettingsBuilder builder)
    {
        return builder.setArtificialLatencies(LATENCIES);
    }

    private static SettingsBuilder ycsbA(SettingsBuilder builder, int keyCount)
    {
        return builder.setKeySelector(ycsbZipfian(keyCount))
                      .setReadRatio(0.5f);
    }

    private static SettingsBuilder ycsbB(SettingsBuilder builder, int keyCount)
    {
        return builder.setKeySelector(ycsbZipfian(keyCount))
                      .setReadRatio(0.95f);
    }

    private static SettingsBuilder ycsbC(SettingsBuilder builder, int keyCount)
    {
        return builder.setKeySelector(ycsbZipfian(keyCount))
                      .setReadRatio(1.0f);

    }

    private static IntSupplier ycsbZipfian(int keyCount)
    {
        ZipfDistribution distribution = new ZipfDistribution(new JDKRandomGenerator(), keyCount, 0.99);
        int count = distribution.inverseCumulativeProbability(0.65f);
        float[] probs = new float[count];
        for (int i = 0 ; i < probs.length ; ++i)
            probs[i] = (float) distribution.cumulativeProbability(i);
        // zipf is slow to compute, so we cache the first 65% of the distribution then use uniform probability; this is good enough for our purposes
        float max = probs[probs.length - 1];
        float inv_incr = probs.length >= keyCount ? 0f : 1f / ((1f-max)/(keyCount - probs.length));
        Random random = new Random();
        return () -> {
            float v = random.nextFloat();
            if (v < max)
            {
                int i = Arrays.binarySearch(probs, v);
                if (i < 0) i = -1 - i;
                return i;
            }
            else
            {
                return (int)((v - max)*inv_incr);
            }
        };
    }

    private static IntSupplier roundrobin(int keyCount)
    {
        AtomicInteger next = new AtomicInteger();
        return () -> {
            int v = next.incrementAndGet();
            if (v < keyCount)
                return v;
            return next.updateAndGet(i -> i > keyCount ? 0 : i + 1);
        };
    }

    private static IntSupplier uniform(int keyCount)
    {
        Random random = new Random();
        return () -> random.nextInt(keyCount);
    }

    public void testLoad(final Settings settings) throws Exception
    {
        Cluster cluster = SHARED_CLUSTER;
        cluster.schemaChange("CREATE TABLE " + qualifiedAccordTableName + " (k int, v int, PRIMARY KEY(k)) WITH transactional_mode = 'full'");

        try
        {
            final ConcurrentHashMap<Verb, AtomicInteger> verbs = new ConcurrentHashMap<>();
            cluster.filters().outbound().messagesMatching(new IMessageFilters.Matcher()
            {
                @Override
                public boolean matches(int i, int i1, IMessage iMessage)
                {
                    verbs.computeIfAbsent(Verb.fromId(iMessage.verb()), ignore -> new AtomicInteger()).incrementAndGet();
                    return false;
                }
            }).drop();

            int clientCount = settings.clients < 0 ? cluster.size() : settings.clients;
            long nextRepairAt = settings.repairInterval;
            long nextCompactionAt = settings.compactionInterval;
            long nextJournalFlushAt = settings.journalFlushInterval;
            long nextDataFlushAt = settings.dataFlushInterval;
            long nextCfkFlushAt = settings.cfkFlushInterval;
            long nextRestartAt = settings.restartInterval;
            final ExecutorService restartExecutor = Executors.newSingleThreadExecutor();
            final ExecutorService clientExecutor = Executors.newFixedThreadPool(clientCount);
            final BitSet initialised = new BitSet();

            java.util.concurrent.Future<?> restarting = null;
            cluster.get(1).nodetoolResult("cms", "reconfigure", "datacenter1:1", "datacenter2:1", "datacenter3:1").asserts().success();
            if (settings.cfkCompactionPeriodSeconds < Integer.MAX_VALUE && settings.cfkCompactionPeriodSeconds > 0)
            {
                cluster.forEach(i -> i.acceptOnInstance(period -> {
                    ((AccordService) AccordService.instance()).journal().compactor().updateCompactionPeriod(period, SECONDS);
                }, settings.cfkCompactionPeriodSeconds));
            }

            if (settings.artificialLatencies != null)
            {
                for (int i = 0 ; i < cluster.size() ; ++i)
                {
                    StringBuilder str = new StringBuilder();
                    for (int j = 0 ; j < settings.artificialLatencies[i].length ; ++j)
                    {
                        if (j > 0)
                            str.append(",");
                        str.append("datacenter")
                           .append(j + 1)
                           .append(':')
                           .append(settings.artificialLatencies[i][j])
                           .append("ms");
                    }
                    cluster.get(i + 1).acceptOnInstance(latencies -> {
                        ArtificialLatency.setArtificialLatencies(latencies);
                        ArtificialLatency.setArtificialLatencyOnlyPermittedConsistencyLevels(false);
                        ArtificialLatency.setArtificialLatencyVerbs(ArtificialLatency.recommendedVerbs());
                        ArtificialLatency.setEnabled(true);
                    }, str.toString());
                }
            }

            if (settings.traceSlowest > 0f)
            {
                float traceSlowest = settings.traceSlowest;
                for (int i = 0 ; i < cluster.size() ; ++i)
                {
                    cluster.get(i + 1).runOnInstance(() -> {
                        AccordTracing tracing = ((AccordAgent) AccordService.unsafeInstance().agent()).tracing();
                        tracing.setPattern(1, pattern -> pattern.withChance(traceSlowest)
                                                                .withKinds(TxnKindsAndDomains.parse("{K*}"))
                                                                .withTraceNew(CoordinationKinds.ALL),
                                           SLOWEST, -1, 2, LEAKY, 10, 1, CoordinationKinds.ALL);
                    });
                }
            }

            if (settings.traceLast > 0)
            {
                int traceLast = settings.traceLast;
                for (int i = 0 ; i < cluster.size() ; ++i)
                {
                    cluster.get(i + 1).runOnInstance(() -> {
                        AccordTracing tracing = ((AccordAgent) AccordService.unsafeInstance().agent()).tracing();
                        tracing.setPattern(2, pattern -> pattern.withKinds(TxnKindsAndDomains.parse("{KW}"))
                                                                    .withTraceNew(CoordinationKinds.ALL),
                                           RING, -1, traceLast, LEAKY, 10, 1, CoordinationKinds.ALL);
                    });
                }
            }

            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicBoolean pauseOrStop = new AtomicBoolean();
            final WaitQueue waitQueue = WaitQueue.newWaitQueue();
            Random random = new Random();
            Semaphore completed = new Semaphore(0);
            AtomicIntegerArray coordinatorIndexes = new AtomicIntegerArray(clientCount);
            final List<java.util.concurrent.Future<?>> clients = new ArrayList<>();
            final AtomicReferenceArray<RateLimiter> rateLimiters = new AtomicReferenceArray<>(clientCount);
            final ConcurrentHashMap<TxnId, Boolean> debugLatency = new ConcurrentHashMap<>();
            final AtomicReference<EstimatedHistogram> readHistogram = new AtomicReference<>(new EstimatedHistogram(200));
            final AtomicReference<EstimatedHistogram> writeHistogram = new AtomicReference<>(new EstimatedHistogram(200));
            if (settings.clients >= cluster.size())
                throw new IllegalArgumentException("Cannot have more clients than nodes");
            if (settings.restartInterval < Integer.MAX_VALUE && settings.clients + 1 >= cluster.size())
                throw new IllegalArgumentException("If restarting, cannot have as many clients as nodes, as must reroute client requests during restart");

            int clientRatePerSecond = Math.min(settings.ratePerSecond, settings.minRatePerSecond) / clientCount;
            for (int client = 0 ; client < clientCount ; ++client)
            {
                rateLimiters.set(client, RateLimiter.create(clientRatePerSecond));
                final int clientIndex = client;
                coordinatorIndexes.set(client, client + 1);
                clients.add(clientExecutor.submit(() -> {
                    final Semaphore inFlight = new Semaphore(settings.clientConcurrency);
                    while (true)
                    {
                        while (pauseOrStop.get())
                        {
                            if (stop.get())
                                break;

                            WaitQueue.Signal signal = waitQueue.register();
                            if (pauseOrStop.get()) signal.awaitThrowUncheckedOnInterrupt();
                            else signal.cancel();
                        }

                        int coordinatorIdx = coordinatorIndexes.get(clientIndex);
                        ICoordinator coordinator = cluster.coordinator(coordinatorIdx);
                        try
                        {
                            rateLimiters.get(clientIndex).acquire();
                            inFlight.acquire();
                            long commandStart = System.nanoTime();
                            IntArrayList keys = new IntArrayList(settings.keysPerOperation, -1);
                            for (int i = 0 ; i < settings.keysPerOperation ; ++i)
                            {
                                int k = settings.keySelector.getAsInt();
                                if (!keys.containsInt(k))
                                    keys.add(k);
                            }
                            if (!keys.intStream().allMatch(initialised::get))
                            {
                                coordinator.executeWithResult((success, fail) -> {
                                    inFlight.release();
                                    completed.release();
                                    if (fail == null)
                                    {
                                        long elapsed = System.nanoTime() - commandStart;
                                        writeHistogram.get().add(NANOSECONDS.toMicros(elapsed));
                                        synchronized (initialised)
                                        {
                                            keys.forEachInt(initialised::set);
                                        }
                                    }
                                    else
                                    {
                                        logger.error("{}", fail.toString());
                                    }
                                }, "UPDATE " + qualifiedAccordTableName + " SET v = 0 WHERE k IN ?", ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, keys);
                            }
                            else if (random.nextFloat() < settings.readRatio)
                            {
                                coordinator.executeWithResult((success, fail) -> {
                                    inFlight.release();
                                    completed.release();
                                    if (fail == null)
                                        readHistogram.get().add(NANOSECONDS.toMicros(System.nanoTime() - commandStart));
                                }, "BEGIN TRANSACTION\n" +
                                   "SELECT * FROM " + qualifiedAccordTableName + " WHERE k IN ?;\n" +
                                   "COMMIT TRANSACTION;", ConsistencyLevel.SERIAL, keys
                                );
                            }
                            else
                            {
                                coordinator.executeWithResult((success, fail) -> {
                                    inFlight.release();
                                    completed.release();
                                    if (fail == null)
                                    {
                                        long elapsed = System.nanoTime() - commandStart;
                                        writeHistogram.get().add(NANOSECONDS.toMicros(elapsed));
                                    }
                                    else
                                        logger.error("{}", fail.toString());
                                }, "BEGIN TRANSACTION\n" +
                                   //                               "UPDATE " + qualifiedAccordTableName + " SET v = ? WHERE k = ?;\n" +
                                   "UPDATE " + qualifiedAccordTableName + " SET v += ? WHERE k IN ?;\n" +
                                   "COMMIT TRANSACTION;", ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, random.nextInt(100), keys);
                            }
                        }
                        catch (RejectedExecutionException e)
                        {
                            inFlight.release();
                        }
                        catch (InterruptedException e)
                        {
                            throw new UncheckedInterruptedException(e);
                        }
                    }
                }));
            }

            int targetClientRatePerSecond = settings.ratePerSecond / clientCount;
            int nextRateLimitIncrease = settings.increaseRatePerSecondInterval;
            while (true)
            {
                long batchStart = System.nanoTime();
                int batchSize = 0;

                if (completed.tryAcquire(settings.batchSize, settings.batchPeriodNanos, NANOSECONDS))
                    batchSize = settings.batchSize;
                batchSize += completed.drainPermits();

                if (clientRatePerSecond < targetClientRatePerSecond)
                {
                    if ((nextRateLimitIncrease -= batchSize) <= 0)
                    {
                        clientRatePerSecond = Math.min(clientRatePerSecond * 2, targetClientRatePerSecond);
                        for (int i = 0 ; i < clientCount ; ++i)
                            rateLimiters.set(i, RateLimiter.create(clientRatePerSecond));
                        nextRateLimitIncrease = settings.increaseRatePerSecondInterval;
                    }
                }

                if ((nextRepairAt -= batchSize) <= 0)
                {
                    nextRepairAt += settings.repairInterval;
                    System.out.println("repairing...");
                    cluster.coordinator(1).instance().nodetool("repair", qualifiedAccordTableName);
                }

                if ((nextCompactionAt -= batchSize) <= 0)
                {
                    nextCompactionAt += settings.compactionInterval;
                    System.out.println("compacting accord...");
                    cluster.forEach(i -> {
                        try { i.nodetool("compact", "system_accord.journal"); }
                        catch (Throwable t) { logger.error("", t); }
                    });
                }

                if ((nextJournalFlushAt -= batchSize) <= 0)
                {
                    nextJournalFlushAt += settings.journalFlushInterval;
                    System.out.println("flushing journal...");
                    cluster.forEach(i -> {
                        try
                        {
                            if (!i.isShutdown())
                            {
                                i.runOnInstance(() -> {
                                    if (AccordService.started())
                                        ((AccordService) AccordService.instance()).journal().closeCurrentSegmentForTestingIfNonEmpty();
                                });
                            }
                        }
                        catch (Throwable t)
                        {
                            logger.error("", t);
                        }
                    });
                }

                if ((nextDataFlushAt -= batchSize) <= 0)
                {
                    nextDataFlushAt += settings.dataFlushInterval;
                    System.out.println("flushing data...");
                    cluster.forEach(i -> {
                        try
                        {
                            i.acceptOnInstance(name -> {
                                Schema.instance.getColumnFamilyStoreInstance(Schema.instance.getTableMetadata(KEYSPACE, name).id).forceFlush(UNIT_TESTS);
                            }, accordTableName);
                        }
                        catch (Throwable t)
                        {
                            logger.error("", t);
                        }
                    });
                }

                if ((nextCfkFlushAt -= batchSize) <= 0)
                {
                    nextCfkFlushAt += settings.cfkFlushInterval;
                    System.out.println("flushing data...");
                    cluster.forEach(i -> {
                        try
                        {
                            i.acceptOnInstance(name -> {
                                if (CommitLog.instance.isStarted())
                                    AccordKeyspace.AccordColumnFamilyStores.commandsForKey.forceFlush(UNIT_TESTS);
                            }, accordTableName);
                        }
                        catch (Throwable t)
                        {
                            logger.error("", t);
                        }
                    });
                }

                if ((nextRestartAt -= batchSize) <= 0)
                {
                    if (restarting == null || restarting.isDone())
                    {
                        if (restarting != null)
                            restarting.get();

                        nextRestartAt += settings.restartInterval;
                        int nodeIdx = 1 + random.nextInt(cluster.size());
                        out: for (int i = 0 ; i < coordinatorIndexes.length() ; ++i)
                        {
                            if (nodeIdx == coordinatorIndexes.get(i))
                            {
                                cont: while (true)
                                {
                                    int replaceIdx = 1 + random.nextInt(cluster.size());
                                    for (int j = 0 ; j < coordinatorIndexes.length() ; ++j)
                                    {
                                        if (coordinatorIndexes.get(j) == replaceIdx)
                                            continue cont;
                                    }
                                    coordinatorIndexes.set(i, replaceIdx);
                                    break out;
                                }
                            }
                        }
                        restarting = restartExecutor.submit(() -> {
                            System.out.printf("restarting node %d...\n", nodeIdx);
                            try
                            {
                                cluster.get(nodeIdx).shutdown().get();
                                cluster.get(nodeIdx).startup();
                                return null;
                            }
                            catch (InterruptedException | ExecutionException e)
                            {
                                throw new RuntimeException(e);
                            }
                        });
                    }
                }

                Long nowMillis = System.currentTimeMillis();
                EstimatedHistogram reads = readHistogram.getAndSet(new EstimatedHistogram(200));
                EstimatedHistogram writes = writeHistogram.getAndSet(new EstimatedHistogram(200));
                if (settings.traceSlowest > 0f)
                {
                    cluster.forEach(() -> {
                        AccordTracing tracing = ((AccordAgent)AccordService.instance().agent()).tracing();

                        tracing.forEach(Functions.alwaysTrue(), (txnId, state) -> {
                            state.forEach(event -> {
                                if (event.elapsedNanos() < MILLISECONDS.toNanos(100))
                                    return;

                                for (Message message : event.messages())
                                {
                                    long multiplier = message.atNanos < event.doneAtNanos() ? 1 : -1;
                                    System.out.printf("%s %s %s %s %s %s\n", txnId, event.kind, multiplier * (message.atNanos - event.atNanos)/1000000, message.nodeId, message.commandStoreId, message.message);
                                }
                            });
                        });
                        tracing.eraseAll();
                    });
                }
                if (settings.traceLast > 0)
                {
                    pauseOrStop.set(true);
                    Map<String, List<List<String>>> print = new HashMap<>();
                    for (int i = 1 ; i <= cluster.size() ; ++i)
                    {
                        cluster.get(i).acceptOnInstance(out -> {
                            AccordService service = (AccordService)AccordService.instance();
                            AccordTracing tracing = ((AccordAgent)AccordService.instance().agent()).tracing();
                            PriorityQueue<SortedByElapsed> candidates = new PriorityQueue<>(Comparator.comparingLong(c -> -c.elapsedMicros));
                            tracing.forEach(Functions.alwaysTrue(), (txnId, events) -> {
                                events.forEach(event -> {
                                    if (event.kind == Client)
                                    {
                                        long doneAtMicros = event.doneAtMicros();
                                        long elapsedMicros = doneAtMicros - event.txnId().hlc();
                                        if (elapsedMicros > 350000 && elapsedMicros < 390000)
                                            candidates.add(new SortedByElapsed(txnId, elapsedMicros));
                                    }
                                });
                            });

                            AtomicInteger storeId = new AtomicInteger();
                            while (!candidates.isEmpty())
                            {
                                SortedByElapsed sortedCandidate = candidates.poll();
                                if (sortedCandidate.elapsedMicros < 300000)
                                    return;

                                TxnId candidate = sortedCandidate.txnId;
                                storeId.lazySet(-1);
                                tracing.forEach(candidate, events -> {
                                    events.forEach(event -> {
                                        if (storeId.get() >= 0)
                                            return;
                                        for (Message message : event.messages())
                                        {
                                            if (message.nodeId < 0 && message.commandStoreId >= 0)
                                            {
                                                storeId.set(message.commandStoreId);
                                                break;
                                            }
                                        }
                                    });
                                });

                                if (storeId.get() >= 0)
                                {
                                    CommandStore commandStore = service.node().commandStores().forId(storeId.get());
                                    List<List<String>> result = AccordService.getBlocking(commandStore.submit(PreLoadContext.contextFor(candidate, "LoadTest"), safeStore -> {
                                        SafeCommand safeCommand = safeStore.unsafeGet(candidate);
                                        PartialDeps deps = safeCommand.current().partialDeps();
                                        if (deps == null)
                                            return null;
                                        List<List<String>> infos = new ArrayList<>();
                                        for (TxnId txnId : deps.txnIds())
                                        {
                                            List<String> info = new ArrayList<>();
                                            info.add(txnId.toString());
                                            infos.add(info);
                                        }
                                        List<String> info = new ArrayList<>();
                                        info.add(candidate.toString());
                                        infos.add(info);
                                        return infos;
                                    }));

                                    if (result != null)
                                    {
                                        for (List<String> info : result)
                                        {
                                            TxnId txnId = TxnId.parse(info.get(0));
                                            AccordService.getBlocking(commandStore.execute(PreLoadContext.contextFor(txnId, "LoadTest"), safeStore -> {
                                                SafeCommand safeCommand = safeStore.unsafeGet(txnId);
                                                if (safeCommand.current().executeAt != null)
                                                    info.add(safeCommand.current().executeAt.toString());
                                            }));
                                        }

                                        out.put(candidate.toString(), result);
                                        return;
                                    }
                                }
                            }
                        }, print);
                    }

                    for (int i = 1 ; i <= cluster.size() ; ++i)
                    {
                        cluster.get(i).acceptOnInstance(out -> {
                            AccordTracing tracing = ((AccordAgent)AccordService.instance().agent()).tracing();
                            for (Map.Entry<String, List<List<String>>> e : out.entrySet())
                            {
                                TxnId parentId = TxnId.parse(e.getKey());
                                for (List<String> infos : e.getValue())
                                {
                                    TxnId depId = TxnId.parse(infos.get(0));
                                    tracing.forEach(depId, events -> {
                                        events.forEach(event -> {
                                            infos.add(event.kind + ": [" + (event.idMicros - parentId.hlc()) + "..." + (event.doneAtMicros() - parentId.hlc()) + "][" + (event.idMicros - depId.hlc()) + "..." + (event.doneAtMicros() - depId.hlc()) + "]");
                                            if (event.kind == Execute)
                                            {
                                                for (Message message : event.messages())
                                                {
                                                    if (message.nodeId == parentId.node.id)
                                                    {
                                                        long atMicros = (event.idMicros + (message.atNanos - event.atNanos)/1000) - parentId.hlc();
                                                        infos.add(atMicros + ": " + message.message);
                                                    }
                                                }
                                            }
                                        });
                                    });
                                }
                            }
                        }, print);
                    }

                    for (Map.Entry<String, List<List<String>>> e : print.entrySet())
                    {
                        System.out.println("======" + e.getKey() + "======");
                        for (List<String> infos : e.getValue())
                            System.out.println(infos);
                    }
                    if (!print.isEmpty())
                        System.out.println();
                    pauseOrStop.set(false);
                    waitQueue.signalAll();
                }
                cluster.forEach(() -> {
                    refresh(AccordExecutorMetrics.INSTANCE.elapsedRunning);
                    refresh(AccordExecutorMetrics.INSTANCE.elapsed);
                    System.out.printf("%tT.%tL (%d %d %d %d %d %d)ms (%d %d %d %d %d %d)ms (%d %d %d %d %.0f, %d %d %d)us %d %d %d\n", nowMillis, nowMillis,
                                      getLatency(AccordCoordinatorMetrics.readMetrics.preacceptLatency, 0.5),
                                      getLatency(AccordCoordinatorMetrics.readMetrics.executeLatency, 0.5),
                                      getLatency(AccordCoordinatorMetrics.readMetrics.applyLatency, 0.5),
                                      getLatency(AccordCoordinatorMetrics.readMetrics.preacceptLatency, 0.999),
                                      getLatency(AccordCoordinatorMetrics.readMetrics.executeLatency, 0.999),
                                      getLatency(AccordCoordinatorMetrics.readMetrics.applyLatency, 0.999),
                                      getLatency(AccordCoordinatorMetrics.writeMetrics.preacceptLatency, 0.95),
                                      getLatency(AccordCoordinatorMetrics.writeMetrics.executeLatency, 0.5),
                                      getLatency(AccordCoordinatorMetrics.writeMetrics.applyLatency, 0.5),
                                      getLatency(AccordCoordinatorMetrics.writeMetrics.preacceptLatency, 0.999),
                                      getLatency(AccordCoordinatorMetrics.writeMetrics.executeLatency, 0.999),
                                      getLatency(AccordCoordinatorMetrics.writeMetrics.applyLatency, 0.999),
                                      getLatency(AccordExecutorMetrics.INSTANCE.elapsedRunning, 0.5),
                                      getLatency(AccordExecutorMetrics.INSTANCE.elapsedRunning, 0.9),
                                      getLatency(AccordExecutorMetrics.INSTANCE.elapsedRunning, 1.0),
                                      getCount(AccordExecutorMetrics.INSTANCE.elapsedRunning),
                                      getTotal(AccordExecutorMetrics.INSTANCE.elapsedRunning),
                                      getLatency(AccordExecutorMetrics.INSTANCE.elapsed, 0.5),
                                      getLatency(AccordExecutorMetrics.INSTANCE.elapsed, 0.9),
                                      getLatency(AccordExecutorMetrics.INSTANCE.elapsed, 0.999),
                                      AccordExecutorMetrics.INSTANCE.running.getValue(),
                                      AccordExecutorMetrics.INSTANCE.waitingToRun.getValue(),
                                      AccordExecutorMetrics.INSTANCE.preparingToRun.getValue()
                    );
                    clear(AccordExecutorMetrics.INSTANCE.elapsedRunning);
                    clear(AccordExecutorMetrics.INSTANCE.elapsed);
                });
                System.out.printf("%tT.%tL rate: %.2f/s (%d total)\n", nowMillis, nowMillis, (((float)batchSize * 1000) / NANOSECONDS.toMillis(System.nanoTime() - batchStart)), batchSize);
                System.out.printf("%tT.%tL reads : %d %d %d %d %d %d\n", nowMillis, nowMillis, reads.percentile(.25)/1000, reads.percentile(.5)/1000, reads.percentile(.95)/1000, reads.percentile(.99)/1000, reads.percentile(.999)/1000, reads.percentile(1)/1000);
                System.out.printf("%tT.%tL writes: %d %d %d %d %d %d\n", nowMillis, nowMillis, writes.percentile(.25)/1000, writes.percentile(.5)/1000, writes.percentile(.95)/1000, writes.percentile(.99)/1000, writes.percentile(.999)/1000, writes.percentile(1)/1000);

                class VerbCount
                {
                    final Verb verb;
                    final int count;

                    VerbCount(Verb verb, int count)
                    {
                        this.verb = verb;
                        this.count = count;
                    }
                }
                List<VerbCount> verbCounts = new ArrayList<>();
                for (Map.Entry<Verb, AtomicInteger> e : verbs.entrySet())
                {
                    int count = e.getValue().getAndSet(0);
                    if (count != 0) verbCounts.add(new VerbCount(e.getKey(), count));
                }
                verbCounts.sort(Comparator.comparing(v -> -v.count));

                StringBuilder verbSummary = new StringBuilder();
                for (VerbCount vs : verbCounts)
                {
                    {
                        if (verbSummary.length() > 0)
                            verbSummary.append(", ");
                        verbSummary.append(vs.verb);
                        verbSummary.append(": ");
                        verbSummary.append(vs.count);
                    }
                }
                System.out.printf("%tT.%tL verbs: %s\n", nowMillis, nowMillis, verbSummary);
            }
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            System.exit(1);
        }
    }

    private static void refresh(Histogram histogram)
    {
        if (histogram instanceof ShardedHistogram)
            ((ShardedHistogram) histogram).refresh();
        if (histogram instanceof ShardedDecayingHistogram)
            ((ShardedDecayingHistogram) histogram).refresh();
    }

    private static long getLatency(Histogram histogram, double percentile)
    {
        return (long)(histogram.getSnapshot().getValue(percentile) / 1000);
    }

    private static long getCount(Histogram histogram)
    {
        return histogram.getSnapshot().size();
    }

    private static double getTotal(Histogram histogram)
    {
        Snapshot snapshot = histogram.getSnapshot();
        return (snapshot.getMean() * 0.0001d * snapshot.size());
    }

    private static void clear(Histogram histogram)
    {
        if (histogram instanceof ShardedHistogram)
            ((ShardedHistogram) histogram).clear();
        if (histogram instanceof ShardedDecayingHistogram)
            ((ShardedDecayingHistogram) histogram).clear();
    }

    private static long getLatency(Timer timer, double percentile)
    {
        if (timer instanceof SnapshottingTimer)
            return (long) (((SnapshottingTimer) timer).getPercentileSnapshot().getValue(percentile) / 1000);
        return (long)(timer.getSnapshot().getValue(0.999) / 1000);
    }

    private static long getSize(Timer timer)
    {
        if (timer instanceof SnapshottingTimer)
            return ((SnapshottingTimer) timer).getPercentileSnapshot().size();
        return timer.getSnapshot().size();
    }

    @Override
    protected Logger logger()
    {
        return logger;
    }

    private static void computeWorstLatencies()
    {
        int[] qs = new int[LATENCIES.length];
        for (int i = 0 ; i < qs.length ; ++i)
        {
            int[] copy = LATENCIES[i].clone();
            Arrays.sort(copy);
            qs[i] = copy[copy.length/2];
        }
        int[] ws = new int[qs.length];
        for (int i = 0 ; i < qs.length ; ++i)
        {
            int iw = Integer.MIN_VALUE;
            for (int j = 0; j < qs.length ; ++j)
                iw = Math.max(iw, qs[i] + 3*qs[j] + LATENCIES[i][j]);
            ws[i] = iw;
        }
        System.out.println(Arrays.toString(ws));
        Arrays.fill(ws, 0);
        for (int i = 0 ; i < qs.length ; ++i)
        {
            for (int j = 0 ; j < qs.length ; ++j)
            {
                if (j == i) continue;
                if (qs[j] > 2*qs[i]) continue;
                int w = qs[i] + 4*qs[j] + LATENCIES[i][j];
                if (w > ws[i])
                    ws[i] = w;
            }
        }
        System.out.println(Arrays.toString(ws));
    }

    @Ignore
    @Test
    public void testLoad() throws Exception
    {
        testLoad(ycsbA(new SettingsBuilder(), 100_000)
                 .setRatePerSecond(1600).setMinRatePerSecond(200)
                 .setIncreaseRatePerSecondInterval(5000)
                 .build());
    }

    public static void main(String[] args) throws Throwable
    {
        computeWorstLatencies();

        DistributedTestBase.beforeClass();
        AccordLoadTest.setUp();
        AccordLoadTest test = new AccordLoadTest();
        try
        {
            test.setup();
            test.testLoad(withArtificialLatencies(ycsbA(new SettingsBuilder(), 100_000)
//                                                  .setRatePerSecond(400).setMinRatePerSecond(200)
//                                                  .setRatePerSecond(800).setMinRatePerSecond(200)
                                                  .setRatePerSecond(1600).setMinRatePerSecond(200)
                                                  .setIncreaseRatePerSecondInterval(5000)
//                                                  .setTraceLast(5000)
            ).build());
        }
        finally
        {
            test.tearDown();
        }
    }

    static class SortedByElapsed
    {
        final TxnId txnId;
        final long elapsedMicros;

        SortedByElapsed(TxnId txnId, long elapsedMicros)
        {
            this.txnId = txnId;
            this.elapsedMicros = elapsedMicros;
        }
    }

}

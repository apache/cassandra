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
import java.util.BitSet;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
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
import java.util.function.IntSupplier;

import com.google.common.util.concurrent.RateLimiter;

import org.agrona.collections.IntArrayList;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.shared.DistributedTestBase;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;

public class AccordLoadTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordLoadTest.class);

    @BeforeClass
    public static void setUp() throws IOException
    {
        CassandraRelevantProperties.SIMULATOR_STARTED.setString(Long.toString(MILLISECONDS.toSeconds(currentTimeMillis())));
        int nodeCount = 3;
        AccordTestBase.setupCluster(builder -> builder.withDCs(nodeCount).withConfig(config -> {
            config.with(Feature.NETWORK, Feature.GOSSIP)
                  .set("accord.shard_durability_target_splits", "8")
                  .set("accord.shard_durability_max_splits", "16")
                  .set("accord.shard_durability_cycle", "1m")
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
        final int clientRatePerSecond;
        final int keysPerOperation;
        final float readRatio;
        final IntSupplier keySelector;
        final boolean readBeforeWrite;

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
            this.clientRatePerSecond = builder.ratePerSecond;
            this.keysPerOperation = builder.keysPerOperation;
            this.readRatio = builder.readChance;
            this.keySelector = builder.keySelector;
            this.readBeforeWrite = builder.readBeforeWrite;
        }
    }

    // interval is measured in terms of *operations* unless otherwise specified
    public static class SettingsBuilder
    {
        int repairInterval = Integer.MAX_VALUE;
        int compactionInterval = Integer.MAX_VALUE;
        int journalFlushInterval = Integer.MAX_VALUE;
        int cfkFlushInterval = Integer.MAX_VALUE;
        int cfkCompactionPeriodSeconds = Integer.MAX_VALUE;
        int dataFlushInterval = Integer.MAX_VALUE;
        int restartInterval = Integer.MAX_VALUE;
        int restartDecay = 2;
        int batchSize = 1000;
        long batchPeriodNanos = SECONDS.toNanos(10);
        int clientConcurrency = 50;
        int clients = -1;
        int ratePerSecond = 1000;
        int keysPerOperation = 1;
        float readChance = 0.5f;
        IntSupplier keySelector;
        boolean readBeforeWrite;

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

        public SettingsBuilder setKeysPerOperation(int keysPerOperation)
        {
            this.keysPerOperation = keysPerOperation;
            return this;
        }

        public SettingsBuilder setReadChance(float readChance)
        {
            this.readChance = readChance;
            return this;
        }

        public SettingsBuilder setReadBeforeWrite(boolean readBeforeWrite)
        {
            this.readBeforeWrite = readBeforeWrite;
            return this;
        }

        public SettingsBuilder setKeySelector(IntSupplier keySelector)
        {
            this.keySelector = keySelector;
            return this;
        }

        public Settings build()
        {
            return new Settings(this);
        }
    }

    private static SettingsBuilder ycsbA(SettingsBuilder builder, int keyCount)
    {
        return builder.setKeySelector(ycsbZipfian(keyCount))
                      .setReadChance(0.5f);
    }

    private static SettingsBuilder ycsbB(SettingsBuilder builder, int keyCount)
    {
        return builder.setKeySelector(ycsbZipfian(keyCount))
                      .setReadChance(0.95f);
    }

    private static SettingsBuilder ycsbC(SettingsBuilder builder, int keyCount)
    {
        return builder.setKeySelector(ycsbZipfian(keyCount))
                      .setReadChance(1.0f);

    }

    private static IntSupplier ycsbZipfian(int keyCount)
    {
        ZipfDistribution distribution = new ZipfDistribution(new JDKRandomGenerator(), keyCount, 0.99);
        return distribution::sample;
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
            final ExecutorService clientExecutor = Executors.newFixedThreadPool(settings.clients);
            final BitSet initialised = new BitSet();

            java.util.concurrent.Future<?> restarting = null;
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            if (settings.cfkCompactionPeriodSeconds > 0)
            {
                cluster.forEach(i -> i.runOnInstance(() -> {
                    ((AccordService) AccordService.instance()).journal().compactor().updateCompactionPeriod(settings.cfkCompactionPeriodSeconds, SECONDS);
                }));
            }

            final AtomicBoolean stop = new AtomicBoolean();
            Random random = new Random();
            Semaphore completed = new Semaphore(0);
            AtomicIntegerArray coordinatorIndexes = new AtomicIntegerArray(clientCount);
            final List<java.util.concurrent.Future<?>> clients = new ArrayList<>();
            final EstimatedHistogram histogram = new EstimatedHistogram(200);
            if (settings.clients >= cluster.size())
                throw new IllegalArgumentException("Cannot have more clients than nodes");
            if (settings.restartInterval < Integer.MAX_VALUE && settings.clients + 1 >= cluster.size())
                throw new IllegalArgumentException("If restarting, cannot have as many clients as nodes, as must reroute client requests during restart");

            for (int client = 0 ; client < clientCount ; ++client)
            {
                final int clientIndex = client;
                coordinatorIndexes.set(client, client + 1);
                final RateLimiter rateLimiter = RateLimiter.create(settings.clientRatePerSecond);
                final Semaphore inFlight = new Semaphore(0);
                clients.add(clientExecutor.submit(() -> {
                    while (!stop.get())
                    {
                        int coordinatorIdx = coordinatorIndexes.get(clientIndex);
                        ICoordinator coordinator = cluster.coordinator(coordinatorIdx);
                        try
                        {
                            rateLimiter.acquire();
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
                                        histogram.add(NANOSECONDS.toMicros(System.nanoTime() - commandStart));
                                        synchronized (initialised)
                                        {
                                            keys.forEachInt(initialised::set);
                                        }
                                    }
                                    else
                                    {
                                        logger.error("{}", fail.getMessage());
                                    }
                                }, "UPDATE " + qualifiedAccordTableName + " SET v = 0 WHERE k IN ?", ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, keys);

                            }
                            else if (random.nextFloat() < settings.readRatio)
                            {
                                coordinator.executeWithResult((success, fail) -> {
                                    inFlight.release();
                                    completed.release();
                                    if (fail == null)
                                        histogram.add(NANOSECONDS.toMicros(System.nanoTime() - commandStart));
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
                                        histogram.add(NANOSECONDS.toMicros(System.nanoTime() - commandStart));
                                    else
                                        logger.error("{}", fail.getMessage());
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

            while (true)
            {
                long batchStart = System.nanoTime();
                int batchSize = 0;

                if (completed.tryAcquire(settings.batchSize, settings.batchPeriodNanos, NANOSECONDS))
                    batchSize = settings.batchSize;
                batchSize += completed.drainPermits();

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

                final Date date = new Date();
                System.out.printf("%tT rate: %.2f/s (%d total)\n", date, (((float)settings.batchSize * 1000) / NANOSECONDS.toMillis(System.nanoTime() - batchStart)), batchSize);
                System.out.printf("%tT percentiles: %d %d %d %d\n", date, histogram.percentile(.25)/1000, histogram.percentile(.5)/1000, histogram.percentile(.95)/1000, histogram.percentile(.99)/1000, histogram.percentile(1)/1000);

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
                System.out.printf("%tT verbs: %s\n", date, verbSummary);
            }
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    protected Logger logger()
    {
        return logger;
    }

    public static void main(String[] args) throws Throwable
    {
        DistributedTestBase.beforeClass();
        AccordLoadTest.setUp();
        AccordLoadTest test = new AccordLoadTest();
        test.setup();
        test.testLoad(ycsbA(new SettingsBuilder(), 1000).build());
    }
}

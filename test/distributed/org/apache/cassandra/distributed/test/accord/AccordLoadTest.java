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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.RateLimiter;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.shared.DistributedTestBase;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.utils.EstimatedHistogram;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class AccordLoadTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordLoadTest.class);

    @BeforeClass
    public static void setUp() throws IOException
    {
        CassandraRelevantProperties.SIMULATOR_STARTED.setString(Long.toString(MILLISECONDS.toSeconds(currentTimeMillis())));
//        AccordTestBase.setupCluster(builder -> builder, 3);
        AccordTestBase.setupCluster(builder -> builder.withConfig(config -> config
                                                                            .with(Feature.NETWORK, Feature.GOSSIP)
                                                                            .set("accord.shard_durability_target_splits", "64")
                                                                            .set("accord.shard_durability_cycle", "5m")
//                                                                            .set("accord.ephemeral_read_enabled", "true")
                                                                            .set("accord.gc_delay", "5s")), 3);
    }

    @Ignore
    @Test
    public void testLoad() throws Exception
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

            ICoordinator coordinator = cluster.coordinator(1);
            final int repairInterval = Integer.MAX_VALUE;
            //                 final int repairInterval = 3000;
            final int compactionInterval = Integer.MAX_VALUE;
//                     final int compactionInterval = 3000;
            final int flushInterval = Integer.MAX_VALUE;
//                     final int flushInterval = 1000;
            final int compactionPeriodSeconds = 1;
            final int restartInterval = 150_000_000;
            final int batchSizeLimit = 1000;
            final long batchTime = TimeUnit.SECONDS.toNanos(10);
            final int concurrency = 100;
            final int ratePerSecond = 1000;
            final int keyCount = 10_000;
            final float readChance = 0.33f;
            long nextRepairAt = repairInterval;
            long nextCompactionAt = compactionInterval;
            long nextFlushAt = flushInterval;
            long nextRestartAt = restartInterval;
            final ExecutorService restartExecutor = Executors.newSingleThreadExecutor();
            final BitSet initialised = new BitSet();

            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            cluster.forEach(i -> i.runOnInstance(() -> {
                if (compactionPeriodSeconds > 0)
                    ((AccordService) AccordService.instance()).journal().compactor().updateCompactionPeriod(1, SECONDS);
                //                     ((AccordSpec.JournalSpec)((AccordService) AccordService.instance()).journal().configuration()).segmentSize = 128 << 10;
            }));

            Random random = new Random();
            //                 CopyOnWriteArrayList<Throwable> exceptions = new CopyOnWriteArrayList<>();
            final Semaphore inFlight = new Semaphore(concurrency);
            final RateLimiter rateLimiter = RateLimiter.create(ratePerSecond);
            //                 long testStart = System.nanoTime();
            //                 while (NANOSECONDS.toMinutes(System.nanoTime() - testStart) < 10 && exceptions.size() < 10000)
            while (true)
            {
                final EstimatedHistogram histogram = new EstimatedHistogram(200);
                long batchStart = System.nanoTime();
                long batchEnd = batchStart + batchTime;
                int batchSize = 0;
                while (batchSize < batchSizeLimit)
                {
                    inFlight.acquire();
                    rateLimiter.acquire();
                    long commandStart = System.nanoTime();
                    int k = random.nextInt(keyCount);
                    if (random.nextFloat() < readChance)
                    {
                        coordinator.executeWithResult((success, fail) -> {
                            inFlight.release();
                            if (fail == null) histogram.add(NANOSECONDS.toMicros(System.nanoTime() - commandStart));
                            //                             else exceptions.add(fail);
                        }, "SELECT * FROM " + qualifiedAccordTableName + " WHERE k = ?;", ConsistencyLevel.SERIAL, k);
                    }
                    else if (initialised.get(k))
                    {
                        coordinator.executeWithResult((success, fail) -> {
                            inFlight.release();
                            if (fail == null) histogram.add(NANOSECONDS.toMicros(System.nanoTime() - commandStart));
                            //                             else exceptions.add(fail);
                        }, "UPDATE " + qualifiedAccordTableName + " SET v += 1 WHERE k = ? IF EXISTS;", ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, k);
                    }
                    else
                    {
                        initialised.set(k);
                        coordinator.executeWithResult((success, fail) -> {
                            inFlight.release();
                            if (fail == null) histogram.add(NANOSECONDS.toMicros(System.nanoTime() - commandStart));
                            //                             else exceptions.add(fail);
                        }, "UPDATE " + qualifiedAccordTableName + " SET v = 0 WHERE k = ? IF NOT EXISTS;", ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, k);
                    }
                    batchSize++;
                    if (System.nanoTime() >= batchEnd)
                        break;
                }

                if ((nextRepairAt -= batchSize) <= 0)
                {
                    nextRepairAt += repairInterval;
                    System.out.println("repairing...");
                    cluster.coordinator(1).instance().nodetool("repair", qualifiedAccordTableName);
                }

                if ((nextCompactionAt -= batchSize) <= 0)
                {
                    nextCompactionAt += compactionInterval;
                    System.out.println("compacting accord...");
                    cluster.forEach(i -> {
                        i.nodetool("compact", "system_accord.journal");
                    });
                }

                if ((nextFlushAt -= batchSize) <= 0)
                {
                    nextFlushAt += flushInterval;
                    System.out.println("flushing journal...");
                    cluster.forEach(i -> i.runOnInstance(() -> {
                        ((AccordService) AccordService.instance()).journal().closeCurrentSegmentForTestingIfNonEmpty();
                    }));
                }

                if ((nextRestartAt -= batchSize) <= 0)
                {
                    nextRestartAt += restartInterval;
                    int nodeIdx = random.nextInt(cluster.size());

                    restartExecutor.submit(() -> {
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

                final Date date = new Date();
                System.out.printf("%tT rate: %.2f/s (%d total)\n", date, (((float)batchSizeLimit * 1000) / NANOSECONDS.toMillis(System.nanoTime() - batchStart)), batchSize);
                System.out.printf("%tT percentiles: %d %d %d %d\n", date, histogram.percentile(.25)/1000, histogram.percentile(.5)/1000, histogram.percentile(.75)/1000, histogram.percentile(1)/1000);

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
        test.testLoad();
    }
}

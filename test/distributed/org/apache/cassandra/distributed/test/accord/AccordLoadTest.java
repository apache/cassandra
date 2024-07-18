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
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.RateLimiter;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.shared.DistributedTestBase;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.EstimatedHistogram;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class AccordLoadTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordLoadTest.class);

    @BeforeClass
    public static void setUp() throws IOException
    {
        CassandraRelevantProperties.SIMULATOR_STARTED.setString(Long.toString(MILLISECONDS.toSeconds(currentTimeMillis())));
        AccordTestBase.setupCluster(builder -> builder, 2);
    }

    @Ignore
    @Test
    public void testLoad() throws Exception
    {
        test("CREATE TABLE " + qualifiedTableName + " (k int, v int, PRIMARY KEY(k)) WITH transactional_mode = 'full'",
             cluster -> {

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
                 final int repairInterval = 3000;
                 final int batchSize = 1000;
                 final int concurrency = 100;
                 final int ratePerSecond = 1000;
                 final int keyCount = 10;
                 final float readChance = 0.33f;
                 long nextRepairAt = repairInterval;
                 for (int i = 1; i <= keyCount; i++)
                     coordinator.execute("INSERT INTO " + qualifiedTableName + " (k, v) VALUES (0, 0) USING TIMESTAMP 0;", ConsistencyLevel.ALL, i);

                 Random random = new Random();
//                 CopyOnWriteArrayList<Throwable> exceptions = new CopyOnWriteArrayList<>();
                 final Semaphore inFlight = new Semaphore(concurrency);
                 final RateLimiter rateLimiter = RateLimiter.create(ratePerSecond);
                 long testStart = System.nanoTime();
//                 while (NANOSECONDS.toMinutes(System.nanoTime() - testStart) < 10 && exceptions.size() < 10000)
                 while (true)
                 {
                     final EstimatedHistogram histogram = new EstimatedHistogram(200);
                     long batchStart = System.nanoTime();
                     for (int i = 0 ; i < batchSize ; ++i)
                     {
                         inFlight.acquire();
                         rateLimiter.acquire();
                         long commandStart = System.nanoTime();
                         if (random.nextFloat() < readChance)
                         {
                             coordinator.executeWithResult((success, fail) -> {
                                 inFlight.release();
                                 if (fail == null) histogram.add(NANOSECONDS.toMicros(System.nanoTime() - commandStart));
                                 //                             else exceptions.add(fail);
                             }, "SELECT * FROM " + qualifiedTableName + " WHERE k = ?;", ConsistencyLevel.SERIAL, random.nextInt(keyCount));
                         }
                         else
                         {
                             coordinator.executeWithResult((success, fail) -> {
                                 inFlight.release();
                                 if (fail == null) histogram.add(NANOSECONDS.toMicros(System.nanoTime() - commandStart));
    //                             else exceptions.add(fail);
                             }, "UPDATE " + qualifiedTableName + " SET v += 1 WHERE k = ? IF EXISTS;", ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, random.nextInt(keyCount));
                         }
                     }

                     if ((nextRepairAt -= batchSize) <= 0)
                     {
                         nextRepairAt += repairInterval;
                         System.out.println("repairing...");
                         cluster.coordinator(1).instance().nodetool("repair", qualifiedTableName);
                     }

                     final Date date = new Date();
                     System.out.printf("%tT rate: %.2f/s\n", date, (((float)batchSize * 1000) / NANOSECONDS.toMillis(System.nanoTime() - batchStart)));
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
        );
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

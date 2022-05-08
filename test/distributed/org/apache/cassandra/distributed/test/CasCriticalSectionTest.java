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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

/**
 * A simple sanity check that uses CAS as mutex: each lock tries to CAS a variable thread_id for a specific row,
 * and set its own thread id. Then it sleeps for a short time, and releases the lock.
 *
 * Write timeouts are handled by simply re-reading the variable and checking if locking has actually succeeded.
 */
public class CasCriticalSectionTest extends TestBaseImpl
{
    private static Random rng = new Random();
    private static final int threadCount = 5;
    private static final int rowCount = 1;

    @Test
    public void criticalSectionTest() throws IOException, InterruptedException
    {
        try (Cluster cluster = init(Cluster.create(5, c -> c.set("paxos_variant", "v2")
                                                            .set("write_request_timeout_in_ms", 2000L)
                                                            .set("cas_contention_timeout_in_ms", 2000L)
                                                            .set("request_timeout_in_ms", 2000L))))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (pk int, ck int, thread_id int, PRIMARY KEY(pk, ck))");

            List<Thread> threads = new ArrayList<>();

            AtomicBoolean failed = new AtomicBoolean(false);
            AtomicBoolean stop = new AtomicBoolean(false);
            BooleanSupplier exitCondition = () -> failed.get() || stop.get();

            for (int i = 0; i < rowCount; i++)
            {
                final int rowId = i;
                AtomicInteger counter = new AtomicInteger();
                cluster.coordinator(1)
                       .execute("insert into " + KEYSPACE + ".tbl (pk, ck, thread_id) VALUES (?, ?, ?) IF NOT EXISTS",
                                ConsistencyLevel.QUORUM,
                                1, rowId, 0);

                // threads should be numbered from 1 to allow 0 to be "unlocked"
                for (int j = 1; j <= threadCount; j++)
                {
                    int threadId = j;
                    AtomicInteger lockedTimes = new AtomicInteger();
                    AtomicInteger unlockedTimes = new AtomicInteger();

                    Runnable sanityCheck = () -> {
                        Assert.assertEquals(lockedTimes.get(), unlockedTimes.get());
                    };
                    threads.add(new Thread(() -> {
                        while (!exitCondition.getAsBoolean())
                        {
                            while (!tryLockOnce(cluster, threadId, rowId))
                            {
                                if (exitCondition.getAsBoolean())
                                {
                                    sanityCheck.run();
                                    return;
                                }
                            }
                            int ctr = counter.getAndIncrement();
                            if (ctr != 0)
                            {
                                failed.set(true);
                                Assert.fail(String.format("Thread %s encountered lock that is held by %d participants while trying to lock.",
                                                          Thread.currentThread().getName(),
                                                          ctr));
                            }

                            // hold lock for a bit
                            Uninterruptibles.sleepUninterruptibly(rng.nextInt(5), TimeUnit.MILLISECONDS);
                            ctr = counter.decrementAndGet();
                            if (ctr != 0)
                            {
                                failed.set(true);
                                Assert.fail(String.format("Thread %s encountered lock that is held by %d participants while trying to unlock.",
                                                          Thread.currentThread().getName(),
                                                          ctr));
                            }
                            while (!tryUnlockOnce(cluster, threadId, rowId))
                            {
                                if (exitCondition.getAsBoolean())
                                {
                                    sanityCheck.run();
                                    return;
                                }
                            }
                        }
                        sanityCheck.run();
                    }, String.format("CAS Thread %d-%d", rowId, threadId)));
                }
            }

            for (Thread thread : threads)
                thread.start();

            Thread.sleep(TimeUnit.SECONDS.toMillis(30));
            stop.set(true);

            for (Thread thread : threads)
                thread.join();

            Assert.assertFalse(failed.get());
        }
    }

    public static boolean isCasSuccess(Object[][] res)
    {
        if (res == null || res.length != 1)
            return false;

        return Arrays.equals(res[0], new Object[] {true});
    }

    public static boolean tryLockOnce(Cluster cluster, int threadId, int rowId)
    {
        Object[][] res = null;

        try
        {
            res = cluster.coordinator(rng.nextInt(cluster.size()) + 1)
                         .execute("update " + KEYSPACE + ".tbl SET thread_id = ? WHERE pk = ? AND ck = ? IF thread_id = 0",
                                  ConsistencyLevel.QUORUM,
                                  threadId, 1, rowId);
            return isCasSuccess(res);
        }
        catch (Throwable writeTimeout)
        {
            while (true)
            {
                try
                {
                    res = cluster.coordinator(rng.nextInt(cluster.size()) + 1)
                                 .execute("SELECT thread_id FROM " + KEYSPACE + ".tbl WHERE pk = ? AND ck = ?",
                                          ConsistencyLevel.SERIAL,
                                          1, rowId);
                    break;
                }
                catch (Throwable t)
                {
                    // retry
                }
            }

            return (int) res[0][0] == threadId;
        }
    }

    public static boolean tryUnlockOnce(Cluster cluster, int threadId, int rowId)
    {
        Object[][] res = null;

        try
        {
            res = cluster.coordinator(rng.nextInt(cluster.size()) + 1)
                         .execute("update " + KEYSPACE + ".tbl SET thread_id = ? WHERE pk = ? AND ck = ? IF thread_id = ?",
                                  ConsistencyLevel.QUORUM,
                                  0, 1, rowId, threadId);
            return isCasSuccess(res);
        }
        catch (Throwable writeTimeout)
        {
            while (true)
            {
                try
                {
                    res = cluster.coordinator(rng.nextInt(cluster.size()) + 1)
                                 .execute("SELECT thread_id FROM " + KEYSPACE + ".tbl WHERE pk = ? AND ck = ?",
                                          ConsistencyLevel.SERIAL,
                                          1, rowId);
                    break;
                }
                catch (Throwable t)
                {
                    // retry
                }
            }

            return (int) res[0][0] != threadId;
        }
    }

}
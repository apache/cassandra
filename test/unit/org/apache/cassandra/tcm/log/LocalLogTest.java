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

package org.apache.cassandra.tcm.log;

import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.tcm.Epoch.EMPTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LocalLogTest
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void appendToFillGapWithConsecutiveBufferedEntries()
    {
        LocalLog log = LocalLog.sync(cm(), LogStorage.None, false);
        Epoch start = log.metadata().epoch;
        assertEquals(EMPTY, start);

        Entry e1 = entry(1), e2 = entry(2), e3 = entry(3);

        log.append(e2);
        Epoch tail = log.waitForHighestConsecutive().epoch;

        assertEquals(tail, start);

        log.append(e3);
        tail = log.waitForHighestConsecutive().epoch;
        assertEquals(tail, start);

        log.append(e1);
        tail = log.waitForHighestConsecutive().epoch;
        assertEquals(e3.epoch, tail);
    }

    @Test
    public void appendFuzzTest() throws InterruptedException
    {
        for (int i = 0; i < 10000; i++)
        {
            singleAppendFuzzTest();
        }
    }

    public void singleAppendFuzzTest() throws InterruptedException
    {
        long seed = System.nanoTime();
        Random random = new Random(seed);
        int entryCount = random.nextInt(90) + 10;
        ImmutableList.Builder<Entry> builder = ImmutableList.builderWithExpectedSize(entryCount);
        for (int i = 0; i < entryCount; i++)
            builder.add(entry(i + 1));
        ImmutableList<Entry> entries = builder.build();
        Set<Entry> submitted = ConcurrentHashMap.newKeySet();

        int threads = 10;
        CountDownLatch begin = CountDownLatch.newCountDownLatch(1);
        CountDownLatch finish = CountDownLatch.newCountDownLatch(threads);
        CountDownLatch finishReaders = CountDownLatch.newCountDownLatch(threads);
        ExecutorPlus executor = executorFactory().configurePooled("APPENDER", threads * 2).build();
        LocalLog log = LocalLog.asyncForTests(LogStorage.None, cm(), false);
        List<Entry> committed = new CopyOnWriteArrayList<>(); // doesn't need to be concurrent, since log is single-threaded
        log.addListener((e, m) -> committed.add(e));

        for (int i = 0; i < threads; i++)
        {
            executor.submit(() -> {
                begin.awaitUninterruptibly();
                while (submitted.size() < entryCount)
                {
                    // grab a random slice of up to 10 entries from the list and try to append them to the log
                    // end can be size + 1 because sublist is end-exclusive
                    int end = random.nextInt(entryCount + 1);
                    int start = Math.max(0, end - (random.nextInt(10) + 1));
                    List<Entry> toAppend = entries.subList(start, end);
                    log.append(toAppend);
                    submitted.addAll(toAppend);
                }
                finish.decrement();
            });
        }

        for (int i = 0; i < threads; i++)
        {
            executor.submit(() -> {
                begin.awaitUninterruptibly();
                while (submitted.size() < entryCount)
                {
                    Epoch waitFor = entries.get(random.nextInt(entries.size())).epoch;
                    try
                    {
                        Assert.assertTrue(log.awaitAtLeast(waitFor).epoch.isEqualOrAfter(waitFor));
                    }
                    catch (InterruptedException | TimeoutException e)
                    {
                        // ignore
                    }
                }
                finishReaders.decrement();
            });
        }

        begin.decrement();
        finish.awaitUninterruptibly();

        log.waitForHighestConsecutive();

        assertEquals(entries.get(entries.size() - 1).epoch, log.metadata().epoch);

        if (!entries.equals(committed))
            fail("Committed list didn't match expected." +
                 "\n\tCommitted: " + toString(committed) +
                 "\n\tExpected : " + toString(entries) +
                 "\n\tPending: " + log.pendingBufferSize() +
                 "\n\tSeed: " + seed);
        assertEquals(0, log.pendingBufferSize());

        finishReaders.awaitUninterruptibly();

        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        log.close();
    }

    public static String toString(Collection<? extends Entry> entries)
    {
        return entries.stream()
                      .map((e) -> Integer.toString(((CustomTransformation.PokeInt) ((CustomTransformation) e.transform).child()).v))
                      .collect(Collectors.joining(","));
    }

    static Entry entry(int i)
    {
        return entry(i, i);
    }

    static Entry entry(int i, long epoch)
    {
        return new Entry(new Entry.Id(i),
                         Epoch.create(epoch),
                         new CustomTransformation(CustomTransformation.PokeInt.NAME, new CustomTransformation.PokeInt(i)));
    }

    static ClusterMetadata cm()
    {
        return new ClusterMetadata(Murmur3Partitioner.instance);
    }
}
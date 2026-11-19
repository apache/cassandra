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

package org.apache.cassandra.db.tries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.generateKeys;

public abstract class ThreadedTestBase<T, R extends BaseTrie<T, ?, ?>>
{
    // Note: This should not be run by default with verification to have the higher concurrency of faster writes and reads.

    private static final int COUNT = 30000;
    private static final int OTHERS = COUNT / 10;
    private static final int PROGRESS_UPDATE = COUNT / 15;
    private static final int READERS = 0;
    private static final int WALKERS = 1;
    private static final Random rand = new Random();

    abstract T value(ByteComparable b);
    abstract R makeTrie(OpOrder readOrder);
    abstract void add(R trie, ByteComparable b, T v, int iteration) throws TrieSpaceExhaustedException;

    @Test
    public void testThreaded() throws InterruptedException
    {
        OpOrder readOrder = new OpOrder();
        ByteComparable[] src = generateKeys(rand, COUNT + OTHERS);
        R trie = makeTrie(readOrder);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        List<Thread> threads = new ArrayList<>();
        AtomicBoolean writeCompleted = new AtomicBoolean(false);
        AtomicInteger writeProgress = new AtomicInteger(0);

        for (int i = 0; i < WALKERS; ++i)
            threads.add(new Thread(() -> {
                try
                {
                    while (!writeCompleted.get())
                    {
                        int min = writeProgress.get();
                        int count = 0;
                        try (OpOrder.Group group = readOrder.start())
                        {
                            for (Map.Entry<ByteComparable.Preencoded, T> en : trie.entrySet())
                            {
                                T v = value(en.getKey());
                                Assert.assertEquals(en.getKey().byteComparableAsString(VERSION), v, en.getValue());
                                ++count;
                            }
                        }
                        Assert.assertTrue("Got only " + count + " while progress is at " + min, count >= min);
                    }
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    errors.add(t);
                }
            }));

        for (int i = 0; i < READERS; ++i)
        {
            threads.add(new Thread(() -> {
                try
                {
                    Random r = ThreadLocalRandom.current();
                    while (!writeCompleted.get())
                    {
                        int min = writeProgress.get();

                        for (int i1 = 0; i1 < PROGRESS_UPDATE; ++i1)
                        {
                            int index = r.nextInt(COUNT + OTHERS);
                            ByteComparable b = src[index];
                            T v = value(b);
                            try (OpOrder.Group group = readOrder.start())
                            {
                                T result = trie.get(b);
                                if (result != null)
                                {
                                    Assert.assertTrue("Got not added " + index + " when COUNT is " + COUNT,
                                                      index < COUNT);
                                    Assert.assertEquals("Failed " + index, v, result);
                                }
                                else if (index < min)
                                    Assert.fail("Failed index " + index + " while progress is at " + min);
                            }
                        }
                    }
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    errors.add(t);
                }
            }));
        }

//        threads.add
//               (new Thread(() -> {
            try
            {
                for (int i = 0; i < COUNT; i++)
                {
                    ByteComparable b = src[i];

                    // Note: Because we don't ensure order when calling resolve, just use a hash of the key as payload
                    // (so that all sources have the same value).
                    T v = value(b);
                    add(trie, b, v, i);

                    if (i % PROGRESS_UPDATE == 0)
                        writeProgress.set(i);
                }
            }
            catch (Throwable t)
            {
                t.printStackTrace();
                errors.add(t);
            }
            finally
            {
                writeCompleted.set(true);
            }
//        }));

        for (Thread t : threads)
            t.start();

        for (Thread t : threads)
            t.join();

        if (!errors.isEmpty())
            Assert.fail("Got errors:\n" + errors);
    }
}

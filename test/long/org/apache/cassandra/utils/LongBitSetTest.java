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
package org.apache.cassandra.utils;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;

import org.apache.cassandra.utils.obs.OffHeapBitSet;
import org.apache.cassandra.utils.obs.OpenBitSet;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongBitSetTest
{
    private static final Logger logger = LoggerFactory.getLogger(LongBitSetTest.class);
    private static final Random random = new Random();

    public void populateRandom(OffHeapBitSet offbs, OpenBitSet obs, long index)
    {
        if (random.nextBoolean())
        {
            offbs.set(index);
            obs.set(index);
        }
    }

    public void compare(OffHeapBitSet offbs, OpenBitSet obs, long index)
    {
        if (offbs.get(index) != obs.get(index))
            throw new RuntimeException();
        Assert.assertEquals(offbs.get(index), obs.get(index));
    }

    @Test
    public void testBitSetOperations()
    {
        long size_to_test = Integer.MAX_VALUE / 40;
        long size_and_excess = size_to_test + 20;
        OffHeapBitSet offbs = new OffHeapBitSet(size_and_excess);
        OpenBitSet obs = new OpenBitSet(size_and_excess);
        for (long i = 0; i < size_to_test; i++)
            populateRandom(offbs, obs, i);

        for (long i = 0; i < size_to_test; i++)
            compare(offbs, obs, i);
    }

    @Test
    public void timeit()
    {
        long size_to_test = Integer.MAX_VALUE / 10; // about 214 million
        long size_and_excess = size_to_test + 20;

        OpenBitSet obs = new OpenBitSet(size_and_excess);
        OffHeapBitSet offbs = new OffHeapBitSet(size_and_excess);
        logger.info("||Open BS set's|Open BS get's|Open BS clear's|Offheap BS set's|Offheap BS get's|Offheap BS clear's|");
        // System.out.println("||Open BS set's|Open BS get's|Open BS clear's|Offheap BS set's|Offheap BS get's|Offheap BS clear's|");
        loopOnce(obs, offbs, size_to_test);
    }

    public void loopOnce(OpenBitSet obs, OffHeapBitSet offbs, long size_to_test)
    {
        StringBuffer buffer = new StringBuffer();
        // start off fresh.
        System.gc();
        long start = System.nanoTime();
        for (long i = 0; i < size_to_test; i++)
            obs.set(i);
        buffer.append("||").append(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        start = System.nanoTime();
        for (long i = 0; i < size_to_test; i++)
            obs.get(i);
        buffer.append("|").append(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        start = System.nanoTime();
        for (long i = 0; i < size_to_test; i++)
            obs.clear(i);
        buffer.append("|").append(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        System.gc();
        start = System.nanoTime();
        for (long i = 0; i < size_to_test; i++)
            offbs.set(i);
        buffer.append("|").append(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        start = System.nanoTime();
        for (long i = 0; i < size_to_test; i++)
            offbs.get(i);
        buffer.append("|").append(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        start = System.nanoTime();
        for (long i = 0; i < size_to_test; i++)
            offbs.clear(i);
        buffer.append("|").append(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).append("|");
        logger.info(buffer.toString());
        // System.out.println(buffer.toString());
    }

    /**
     * Just to make sure JIT doesn't come on our way
     */
    @Test
    // @Ignore
    public void loopIt()
    {
        long size_to_test = Integer.MAX_VALUE / 10; // about 214 million
        long size_and_excess = size_to_test + 20;

        OpenBitSet obs = new OpenBitSet(size_and_excess);
        OffHeapBitSet offbs = new OffHeapBitSet(size_and_excess);
        for (int i = 0; i < 10; i++)
            // 10 times to do approx 2B keys each.
            loopOnce(obs, offbs, size_to_test);
    }
}

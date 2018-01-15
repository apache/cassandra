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

package org.apache.cassandra.utils.obs;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OffHeapBitSetTest
{
    private static final Random random = new Random();

    static void compare(IBitSet bs, IBitSet newbs)
    {
        assertEquals(bs.capacity(), newbs.capacity());
        for (long i = 0; i < bs.capacity(); i++)
            Assert.assertEquals(bs.get(i), newbs.get(i));
    }

    private void testOffHeapSerialization(boolean oldBfFormat) throws IOException
    {
        try (OffHeapBitSet bs = new OffHeapBitSet(100000))
        {
            for (long i = 0; i < bs.capacity(); i++)
                if (random.nextBoolean())
                    bs.set(i);

            DataOutputBuffer out = new DataOutputBuffer();
            if (oldBfFormat)
                bs.serializeOldBfFormat(out);
            else
                bs.serialize(out);

            DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.getData()));
            try (OffHeapBitSet newbs = OffHeapBitSet.deserialize(in, oldBfFormat))
            {
                compare(bs, newbs);
            }
        }
    }

    @Test
    public void testSerialization() throws IOException
    {
        testOffHeapSerialization(true);
        testOffHeapSerialization(false);
    }

    @Test
    public void testBitSetGetClear()
    {
        int size = Integer.MAX_VALUE / 4000;
        try (OffHeapBitSet bs = new OffHeapBitSet(size))
        {
            List<Integer> randomBits = Lists.newArrayList();
            for (int i = 0; i < 10; i++)
                randomBits.add(random.nextInt(size));

            for (long randomBit : randomBits)
                bs.set(randomBit);

            for (long randomBit : randomBits)
                assertEquals(true, bs.get(randomBit));

            for (long randomBit : randomBits)
                bs.clear(randomBit);

            for (long randomBit : randomBits)
                assertEquals(false, bs.get(randomBit));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedLargeSize()
    {
        long size = 64L * Integer.MAX_VALUE + 1; // Max size 16G * 8 bits
        OffHeapBitSet bs = new OffHeapBitSet(size);
    }

    @Test
    public void testInvalidIndex()
    {
        OffHeapBitSet bs = new OffHeapBitSet(10);
        int invalidIdx[] = {-1, 64, 1000};

        for (int i : invalidIdx)
        {
            try
            {
                bs.set(i);
            }
            catch (AssertionError e)
            {
                assertTrue(e.getMessage().startsWith("Illegal bounds"));
                continue;
            }
            fail(String.format("expect exception for index %d", i));
        }

        for (int i : invalidIdx)
        {
            try
            {
                bs.get(i);
            }
            catch (AssertionError e)
            {
                assertTrue(e.getMessage().startsWith("Illegal bounds"));
                continue;
            }
            fail(String.format("expect exception for index %d", i));
        }
    }
}

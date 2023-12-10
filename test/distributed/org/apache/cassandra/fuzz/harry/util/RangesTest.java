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

package org.apache.cassandra.fuzz.harry.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.harry.util.DescriptorRanges;

public class RangesTest
{

    @Test
    public void simpleRangesTest()
    {
        List<DescriptorRanges.DescriptorRange> list = Arrays.asList(inclusiveRange(10, 20, 10),
                                                                    inclusiveRange(40, 50, 10),
                                                                    inclusiveRange(60, 70, 10),
                                                                    inclusiveRange(80, 90, 10));
        Collections.shuffle(list);
        DescriptorRanges ranges = new DescriptorRanges(list);

        Assert.assertTrue(ranges.isShadowed(10, 5));
        Assert.assertFalse(ranges.isShadowed(10, 20));
        Assert.assertFalse(ranges.isShadowed(15, 20));
        Assert.assertTrue(ranges.isShadowed(49, 5));
        Assert.assertFalse(ranges.isShadowed(55, 5));
        Assert.assertFalse(ranges.isShadowed(50, 20));
        Assert.assertTrue(ranges.isShadowed(90, 9));
    }

    @Test
    public void randomizedRangesTest()
    {
        for (int i = 0; i < 1000; i++)
            _randomizedRangesTest();
    }

    private void _randomizedRangesTest()
    {
        List<DescriptorRanges.DescriptorRange> rangesList = new ArrayList<>();
        Random rnd = new Random();
        for (int i = 0; i < 100; i++)
        {
            long a = rnd.nextInt(1000);
            long b = rnd.nextInt(1000);
            DescriptorRanges.DescriptorRange range = new DescriptorRanges.DescriptorRange(Math.min(a, b),
                                                                                          Math.max(a,b),
                                                                                          rnd.nextBoolean(),
                                                                                          rnd.nextBoolean(),
                                                                                          rnd.nextInt(1000));
            rangesList.add(range);
        }
        DescriptorRanges ranges = new DescriptorRanges(rangesList);

        for (int i = 0; i < 10000; i++)
        {
            long descriptor = rnd.nextLong();
            long ts = rnd.nextInt(1000);
            Assert.assertEquals(matchLinear(rangesList, descriptor, ts),
                                ranges.isShadowed(descriptor, ts));

        }
    }

    public boolean matchLinear(List<DescriptorRanges.DescriptorRange> ranges, long descriptor, long ts)
    {
        for (DescriptorRanges.DescriptorRange range : ranges)
        {
            if (range.contains(descriptor, ts))
                return true;
        }
        return false;
    }

    public DescriptorRanges.DescriptorRange inclusiveRange(long start, long end, long ts)
    {
        return new DescriptorRanges.DescriptorRange(start, end, true, true, 10);
    }
}

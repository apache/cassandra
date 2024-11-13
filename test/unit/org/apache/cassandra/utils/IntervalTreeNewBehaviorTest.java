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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

public class IntervalTreeNewBehaviorTest
{
    final Random rnd = new Random();

    @BeforeClass
    public static void setup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testIntervalTreeIdentical()
    {
        for (int i = 0; i < 100; i++)
        {
            List<Interval<Integer, Void>> intervals = generateRandomIntervals(10_000);
            DatabaseDescriptor.setUseNewBehaviorForIntervalTreeBuild(false);
            IntervalTree<Integer, Void, Interval<Integer, Void>> it1 = IntervalTree.build(intervals);
            DatabaseDescriptor.setUseNewBehaviorForIntervalTreeBuild(true);
            IntervalTree<Integer, Void, Interval<Integer, Void>> it2 = IntervalTree.build(intervals);
            Assert.assertEquals(it1, it2);
        }
    }

    @Test
    public void testIntervalTreeIdenticalSingleInterval()
    {
        for (int i = 0; i < 100; i++)
        {
            List<Interval<Integer, Void>> intervals = generateRandomIntervals(1);
            DatabaseDescriptor.setUseNewBehaviorForIntervalTreeBuild(false);
            IntervalTree<Integer, Void, Interval<Integer, Void>> it1 = IntervalTree.build(intervals);
            DatabaseDescriptor.setUseNewBehaviorForIntervalTreeBuild(true);
            IntervalTree<Integer, Void, Interval<Integer, Void>> it2 = IntervalTree.build(intervals);
            Assert.assertEquals(it1, it2);
        }
    }

    private List<Interval<Integer, Void>> generateRandomIntervals(int numIntervals)
    {
        List<Interval<Integer, Void>> ret = new ArrayList<>();
        for (int i = 0; i < numIntervals; i++)
        {
            int start = rnd.nextInt(Integer.MAX_VALUE);
            int end = start + rnd.nextInt(Integer.MAX_VALUE - start);
            ret.add(Interval.create(start, end));
        }
        return ret;
    }
}

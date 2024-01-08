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

package org.apache.cassandra.index.sai.utils;

import java.util.Random;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.junit.Test;

import static org.apache.cassandra.index.sai.utils.SoftLimitUtil.softLimit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SoftLimitUtilTest
{
    private final Random random = new Random();

    @Test
    public void perItemProbabilityOne()
    {
        assertEquals(0, softLimit(0, 0.999, 1.0));
        assertEquals(1, softLimit(1, 0.999, 1.0));
        assertEquals(100, softLimit(100, 0.999, 1.0));
        assertEquals(1000000, softLimit(1000000, 0.999, 1.0));
        assertEquals(Integer.MAX_VALUE, softLimit(Integer.MAX_VALUE, 0.999, 1.0));
    }

    @Test
    public void perItemProbabilityZero()
    {
        assertEquals(0, softLimit(0, 0.999, 0.0), 0);
        assertEquals(Integer.MAX_VALUE, softLimit(1, 0.999, 0.0));
        assertEquals(Integer.MAX_VALUE, softLimit(100, 0.999, 0.0));
        assertEquals(Integer.MAX_VALUE, softLimit(1000000, 0.999, 0.0));
        assertEquals(Integer.MAX_VALUE, softLimit(Integer.MAX_VALUE, 0.999, 0.0));
    }

    @Test
    public void confidenceLevelZero()
    {
        // By setting confidenceLevel to 0.0 we say we don't care about failures
        assertEquals(0, softLimit(0, 0.0, 0.5));
        assertEquals(1, softLimit(1, 0.0, 0.5));
        assertEquals(100, softLimit(100, 0.0, 0.5));
        assertEquals(1000000, softLimit(1000000, 0.0, 0.5));
        assertEquals(Integer.MAX_VALUE, softLimit(Integer.MAX_VALUE, 0.0, 0.5));
    }

    @Test
    public void confidenceLevelHalf()
    {
        assertEquals(0, softLimit(0, 0.5, 0.5));
        assertEquals(1, softLimit(1, 0.5, 0.5));
        // For large enough numbers, the number of items we need to query should be very close to targetLimit / perItemProbability.
        assertEquals(2000000, softLimit(1000000, 0.5, 0.5), 2);
        assertEquals(5000000, softLimit(1000000, 0.5, 0.2), 5);
        assertEquals(10000000, softLimit(1000000, 0.5, 0.1), 10);
        assertEquals(100000000, softLimit(1000000, 0.5, 0.01), 100);
        assertEquals(1000000000, softLimit(1000000, 0.5, 0.001), 1000);
        assertEquals(Integer.MAX_VALUE, softLimit(Integer.MAX_VALUE, 0.5, 0.5));
    }

    @Test
    public void confidenceLevelNearOne()
    {
        assertEquals(0, softLimit(0, 0.999, 0.5));
        assertEquals(10, softLimit(1, 0.999, 0.5)); // 1.0 - 0.5 ^ 10 is a tad more than 0.999

        // Intuition-driven tests.
        // We need to query a bit more than targetLimit / perItemProbability to get good confidence.
        // The higher the targetLimit, the relatively closer to targetLimit / perItemProbability the softLimit is.

        assertTrue(softLimit(10, 0.999, 0.5) >= 30);
        assertTrue(softLimit(10, 0.999, 0.5) <= 50);

        assertTrue(softLimit(10, 0.999, 0.1) >= 200);
        assertTrue(softLimit(10, 0.999, 0.1) <= 300);

        assertTrue(softLimit(100, 0.999, 0.5) >= 220);
        assertTrue(softLimit(100, 0.999, 0.5) <= 250);

        assertTrue(softLimit(100, 0.999, 0.1) >= 1300);
        assertTrue(softLimit(100, 0.999, 0.1) <= 1500);

        assertTrue(softLimit(100, 0.999, 0.001) >= 130000);
        assertTrue(softLimit(100, 0.999, 0.001) <= 150000);

        assertTrue(softLimit(1000, 0.999, 0.1) >= 10500);
        assertTrue(softLimit(1000, 0.999, 0.1) <= 11500);

        assertTrue(softLimit(1000, 0.999, 0.001) >= 1100000);
        assertTrue(softLimit(1000, 0.999, 0.001) <= 1150000);
    }

    @Test
    public void resultIsNonDecreasingWithConfidence()
    {
        for (int i = 0; i < 1000; i++)
        {
            int n = (int) Math.pow(10.0, 6 * random.nextDouble());
            double c1 = random.nextDouble();
            double c2 = random.nextDouble();
            double p = random.nextDouble();
            int l1 = softLimit(n, c1, p);
            int l2 = softLimit(n, c2, p);
            assertTrue("n: " + n + ", p: " + p + ", l1: " + l1 + ", l2: " + l2 + ", c1: " + c1 + ", c2: " + c2,
                       (l1 > l2) == (c1 > c2) || l1 == l2);
        }
    }

    @Test
    public void resultIsNonIncreasingWithPerItemProbability()
    {
        for (int i = 0; i < 1000; i++)
        {
            int n = (int) Math.pow(10.0, 6 * random.nextDouble());
            double c = random.nextDouble();         // [0.0, 1.0)
            double p1 = 1.0 - random.nextDouble();  // (0.0, 1.0]
            double p2 = 1.0 - random.nextDouble();  // (0.0, 1.0]
            int l1 = softLimit(n, c, p1);
            int l2 = softLimit(n, c, p2);
            assertTrue("n: " + n + ", c: " + c + ", l1: " + l1 + ", l2: " + l2 + ", p1: " + p1 + ", p2: " + p2,
                       (l1 > l2) == (p1 < p2) || l1 == l2);
        }
    }

    @Test
    public void randomized()
    {
        for (int i = 0; i < 1000; i++)
        {
            int n = (int) Math.pow(10.0, 6 * random.nextDouble());
            double c = random.nextDouble();         // [0.0, 1.0)
            double p = 1.0 - random.nextDouble();   // (0.0, 1.0]
            int softLimit = softLimit(n, c, p);
            assertTrue(softLimit >= n);

            // Estimate the probability we get at least n successes after softLimit tries with probability p.
            // Make sure we have enough confidence we get at least n, but not enough we get n + 1.
            BinomialDistribution successDistribution = new BinomialDistribution(softLimit, p);
            double confidenceLowerBound = probabilityOfAtLeastNSuccesses(successDistribution, n + 1);
            double confidenceUpperBound = probabilityOfAtLeastNSuccesses(successDistribution, n);
            assertTrue(c + " lower than required lower bound " + confidenceLowerBound,c >= confidenceLowerBound);
            assertTrue(c + " higher than required upper bound " + confidenceLowerBound, c < confidenceUpperBound);
        }
    }

    private double probabilityOfAtLeastNSuccesses(BinomialDistribution distribution, int n)
    {
        return n > 0
               ? 1.0 - distribution.cumulativeProbability(n - 1)
               : 1.0;
    }
}
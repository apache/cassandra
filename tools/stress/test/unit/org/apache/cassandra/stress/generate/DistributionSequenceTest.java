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

package org.apache.cassandra.stress.generate;

import org.junit.Test;

import org.apache.cassandra.stress.settings.OptionDistribution;

import static org.junit.Assert.*;

public class DistributionSequenceTest
{
    @Test
    public void simpleSequence() throws Exception
    {
        Distribution dist = OptionDistribution.get("seq(1..10)").get();
        assertTrue(dist instanceof DistributionSequence);

        assertEquals(1, dist.minValue());
        assertEquals(10, dist.maxValue());
        assertEquals(5, dist.average());

        assertEquals(1, dist.inverseCumProb(0d));
        assertEquals(10, dist.inverseCumProb(1d));

        long min = dist.next();
        assertEquals(1,min);

        long last = min;
        for (int i=0; i<9; i++)
        {
            long next = dist.next();
            assertEquals(next, last+1); //increase by one each step
            last = next;
        }

        assertEquals(1, dist.next()); // wrapping
    }


    @Test
    public void negValueSequence() throws Exception
    {
        Distribution dist = OptionDistribution.get("seq(-1000..-10)").get();
        assertTrue(dist instanceof DistributionSequence);

        assertEquals(-1000, dist.minValue());
        assertEquals( -10, dist.maxValue());
        assertEquals(-504, dist.average());

        assertEquals(-1000, dist.inverseCumProb(0d));
        assertEquals(-10, dist.inverseCumProb(1d));

        long min = dist.next();
        assertEquals(-1000, min);

        long last = min;
        long next = dist.next();
        while (last<next)
        {
            assertEquals(next, last+1); //increase by one each step
            last = next;
            next = dist.next();
        }

        assertEquals(-10, last); // wrapping
        assertEquals(-1000, next); // wrapping
    }

    @Test
    public void bigSequence() throws Exception
    {
        Distribution dist = OptionDistribution.get(String.format("seq(1..%d)", Long.MAX_VALUE)).get();
        assertTrue(dist instanceof DistributionSequence);

        assertEquals(1, dist.minValue());
        assertEquals(Long.MAX_VALUE, dist.maxValue());

        assertEquals(1, dist.inverseCumProb(0d));
        assertEquals(Long.MAX_VALUE, dist.inverseCumProb(1d));

    }

    @Test
    public void setSeed() throws Exception
    {
        Distribution dist = OptionDistribution.get("seq(1..10)").get();
        assertTrue(dist instanceof DistributionSequence);

        for (int seed=1; seed<500; seed+=seed)
        {
            dist.setSeed(seed);
            assertEquals(1, dist.minValue());
            assertEquals(10, dist.maxValue());
            assertEquals(5, dist.average());

            assertEquals(1, dist.inverseCumProb(0d));
            assertEquals(10, dist.inverseCumProb(1d));

            long last = dist.next();
            for (int i = 0; i < 9; i++)
            {
                long next = dist.next();
                if (next>1)
                {
                    assertEquals(next, last + 1); //increase by one each step
                }else{
                    assertEquals(last, 10); //wrap after the end
                }
                last = next;
            }
        }
    }
}

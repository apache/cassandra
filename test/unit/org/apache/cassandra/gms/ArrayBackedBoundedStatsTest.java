/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.gms;

import java.util.Arrays;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ArrayBackedBoundedStatsTest {

    @Test
    public void test()
    {
        int size = 4;

        ArrayBackedBoundedStats bsd = new ArrayBackedBoundedStats(size);
        //check the values for an empty result
        assertEquals(0, bsd.mean(), 0.001d);

        bsd.add(1L); //this one falls out, over limit
        bsd.add(2L);
        bsd.add(3L);
        bsd.add(4L);
        bsd.add(5L);

        //verify that everything is in there
        long [] expected = new long[] {2,3,4,5};
        assertArrivalIntervals(bsd, expected);

        //check results
        assertEquals(3.5, bsd.mean(), 0.001d);
    }

    private void assertArrivalIntervals(ArrayBackedBoundedStats bsd, long [] expected)
    {
        Arrays.sort(expected);
        Arrays.sort(bsd.getArrivalIntervals());
        assertTrue(Arrays.equals(bsd.getArrivalIntervals(), expected));

    }

    @Test
    public void testMultipleRounds() throws Exception
    {
        int size = 5;
        ArrayBackedBoundedStats bsd = new ArrayBackedBoundedStats(size);

        for(long i=0; i <= 1000;i++)
        {
            bsd.add(i);
        }

        long [] expected = new long[] {1000,999,998,997, 996};
        assertArrivalIntervals(bsd, expected);
    }
}

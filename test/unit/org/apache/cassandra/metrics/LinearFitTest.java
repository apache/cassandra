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

package org.apache.cassandra.metrics;

import org.junit.Test;

import org.apache.cassandra.metrics.PairedSlidingWindowReservoir.IntIntPair;

import static org.junit.Assert.assertEquals;

public class LinearFitTest
{
    @Test
    public void testInterceptSlopeForLinearValues()
    {
        // tests values that are perfectly linear
        var values = new IntIntPair[10];
        for (int i = 0; i < values.length; i++)
            values[i] = new IntIntPair(i, i * 2);

        var pair = LinearFit.interceptSlopeFor(values);
        assertEquals(0.0, pair.left, 0.01);
        assertEquals(2.0, pair.right, 0.01);

        values = new IntIntPair[10];
        for (int i = 0; i < values.length; i++)
            values[i] = new IntIntPair(i, 1 + i * 2);

        pair = LinearFit.interceptSlopeFor(values);
        assertEquals(1.0, pair.left, 0.01d);
        assertEquals(2.0, pair.right, 0.01d);

        values = new IntIntPair[10];
        for (int i = 0; i < values.length; i++)
            values[i] = new IntIntPair(i, -1 + i * 2);

        pair = LinearFit.interceptSlopeFor(values);
        assertEquals(-1.0, pair.left, 0.01d);
        assertEquals(2.0, pair.right, 0.01d);
    }

    @Test
    public void testInterceptSlopeWithNoise()
    {
        // values are +/- 20 from perfectly linear
        var values = new IntIntPair[] {
            new IntIntPair(1, 66),
            new IntIntPair(2, 108),
            new IntIntPair(3, 70),
            new IntIntPair(4, 112),
            new IntIntPair(5, 74),
            new IntIntPair(6, 116),
            new IntIntPair(7, 78),
            new IntIntPair(8, 120),
            new IntIntPair(9, 82)
        };
        var pair = LinearFit.interceptSlopeFor(values);
        // verified with sklearn
        assertEquals(2.0, pair.right, 0.01);
        assertEquals(81.78, pair.left, 0.01);
    }

    @Test
    public void testInterceptSlopeWithMoreNoise()
    {
        // values are pseudorandomly distributed
        var values = new IntIntPair[] {
            new IntIntPair(931, 1366),
            new IntIntPair(973, 822),
            new IntIntPair(200, 1308),
            new IntIntPair(332, 708),
            new IntIntPair(677, 1186),
            new IntIntPair(401, 7112),
            new IntIntPair(111, 166),
            new IntIntPair(503, 734),
            new IntIntPair(738, 78),
            new IntIntPair(829, 8120)
        };
        var pair = LinearFit.interceptSlopeFor(values);
        // verified with sklearn
        assertEquals(1.31, pair.right, 0.01);
        assertEquals(1412.35, pair.left, 0.01);
    }
}

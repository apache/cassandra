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

import static org.junit.Assert.assertEquals;

public class QuickSlidingWindowReservoirTest
{
    @Test
    public void testSize()
    {
        var sr = new QuickSlidingWindowReservoir(10);
        assertEquals(0, sr.size());

        for (int i = 0; i < 20; i++)
        {
            sr.update(i);
            assertEquals(Math.min(10, i + 1), sr.size());
        }
    }

    @Test
    public void testGetMean()
    {
        var sr = new QuickSlidingWindowReservoir(10);

        assertEquals(0.0, sr.getMean(), 0.01d);

        sr.update(1);
        sr.update(2);
        sr.update(3);
        assertEquals(2.0d, sr.getMean(), 0.01d);

        for (int i = 0; i < 10; i++)
            sr.update(10);

        assertEquals(10.0d, sr.getMean(), 0.01d);
    }

    @Test
    public void testGetSnapshot()
    {
        var sr = new QuickSlidingWindowReservoir(10);
        for (int i = 0; i < 10; i++)
            sr.update(i);

        var snapshot = sr.getSnapshot();
        assertEquals(10, snapshot.size());
        assertEquals(sr.getMean(), snapshot.getMean(), 0.01d);
    }
}
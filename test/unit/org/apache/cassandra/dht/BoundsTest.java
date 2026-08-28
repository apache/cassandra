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

package org.apache.cassandra.dht;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BoundsTest
{
    private Bounds<Token> bounds(long left, long right)
    {
        return new Bounds<Token>(new Murmur3Partitioner.LongToken(left), new Murmur3Partitioner.LongToken(right));
    }

    /**
     * [0,1],[0,5],[1,8],[4,10] = [0, 10]
     * [15,19][19,20] = [15,20]
     * [21, 22] = [21,22]
     */
    @Test
    public void testGetNonOverlappingBounds()
    {
        List<Bounds<Token>> bounds = new ArrayList<>();
        bounds.add(bounds(19, 20));
        bounds.add(bounds(0, 1));
        bounds.add(bounds(4, 10));
        bounds.add(bounds(15, 19));
        bounds.add(bounds(0, 5));
        bounds.add(bounds(21, 22));
        bounds.add(bounds(1, 8));

        Set<Bounds<Token>> nonOverlappingBounds = Bounds.getNonOverlappingBounds(bounds);
        assertEquals(3, nonOverlappingBounds.size());
        assertTrue(nonOverlappingBounds.contains(bounds(0, 10)));
        assertTrue(nonOverlappingBounds.contains(bounds(15,20)));
        assertTrue(nonOverlappingBounds.contains(bounds(21,22)));
    }

    /**
     * [1, 100] [2, 5] = [1, 100]
     */
    @Test
    public void testGetNonOverlappingBounds2()
    {
        List<Bounds<Token>> bounds = new ArrayList<>();
        bounds.add(bounds(1, 100));
        bounds.add(bounds(2, 5));

        Set<Bounds<Token>> nonOverlappingBounds = Bounds.getNonOverlappingBounds(bounds);
        assertEquals(1, nonOverlappingBounds.size());
        assertTrue(nonOverlappingBounds.contains(bounds(1, 100)));
    }

    /**
     * [1, 100] [99, 101] = [1, 101]
     */
    @Test
    public void testGetNonOverlappingBounds3()
    {
        List<Bounds<Token>> bounds = new ArrayList<>();
        bounds.add(bounds(1, 100));
        bounds.add(bounds(99, 101));

        Set<Bounds<Token>> nonOverlappingBounds = Bounds.getNonOverlappingBounds(bounds);
        assertEquals(1, nonOverlappingBounds.size());
        assertTrue(nonOverlappingBounds.contains(bounds(1, 101)));
    }

    /**
     * [0, 2] [1, 100] = [0, 100]
     */
    @Test
    public void testGetNonOverlappingBounds4()
    {
        List<Bounds<Token>> bounds = new ArrayList<>();
        bounds.add(bounds(0, 2));
        bounds.add(bounds(1, 100));

        Set<Bounds<Token>> nonOverlappingBounds = Bounds.getNonOverlappingBounds(bounds);
        assertEquals(1, nonOverlappingBounds.size());
        assertTrue(nonOverlappingBounds.contains(bounds(0, 100)));
    }

    /**
     * [0, 0] [0, 100] = [0, 100]
     */
    @Test
    public void testGetNonOverlappingBounds5()
    {
        List<Bounds<Token>> bounds = new ArrayList<>();
        bounds.add(bounds(0, 0));
        bounds.add(bounds(0, 100));

        Set<Bounds<Token>> nonOverlappingBounds = Bounds.getNonOverlappingBounds(bounds);
        assertEquals(1, nonOverlappingBounds.size());
        assertTrue(nonOverlappingBounds.contains(bounds(0, 100)));
    }
}

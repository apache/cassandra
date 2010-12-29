package org.apache.cassandra.utils;
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


import static org.junit.Assert.*;

import java.util.Iterator;

import org.junit.Test;

public class BoundedStatsDequeTest
{

    @Test
    public void test()
    {
        int size = 4;
        
        BoundedStatsDeque bsd = new BoundedStatsDeque(size);
        //check the values for an empty result
        assertEquals(0, bsd.size());
        assertEquals(0, bsd.sum(), 0.001d);
        assertEquals(Double.NaN, bsd.mean(), 0.001d);
        assertEquals(Double.NaN, bsd.variance(), 0.001d);
        assertEquals(Double.NaN, bsd.stdev(), 0.001d);
        assertEquals(0, bsd.sumOfDeviations(), 0.001d);
        
        bsd.add(1d); //this one falls out, over limit
        bsd.add(2d);
        bsd.add(3d);
        bsd.add(4d);
        bsd.add(5d);
        
        //verify that everything is in there
        Iterator<Double> iter = bsd.iterator();
        assertTrue(iter.hasNext());
        assertEquals(2d, iter.next(), 0);
        assertTrue(iter.hasNext());
        assertEquals(3d, iter.next(), 0);
        assertTrue(iter.hasNext());
        assertEquals(4d, iter.next(), 0);
        assertTrue(iter.hasNext());
        assertEquals(5d, iter.next(), 0);
        assertFalse(iter.hasNext());
        
        //check results
        assertEquals(size, bsd.size());
        assertEquals(14, bsd.sum(), 0.001d);
        assertEquals(3.5, bsd.mean(), 0.001d);
        assertEquals(1.25, bsd.variance(), 0.001d);
        assertEquals(1.1180d, bsd.stdev(), 0.001d);
        assertEquals(5, bsd.sumOfDeviations(), 0.001d);
        
        //check that it clears properly
        bsd.clear();
        assertFalse(bsd.iterator().hasNext());
    }

}

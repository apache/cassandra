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


import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.cassandra.utils.IntervalTree.Interval;
import org.junit.Test;

public class IntervalTest extends TestCase
{
    @Test
    public void testEncloses() throws Exception
    {
        Interval interval = new Interval(0,5,null);
        Interval interval1 = new Interval(0, 10, null);
        Interval interval2 = new Interval(5,10,null);
        Interval interval3 = new Interval(0, 11, null);


        assertTrue(interval.encloses(interval));
        assertTrue(interval1.encloses(interval));
        assertFalse(interval.encloses(interval2));
        assertTrue(interval1.encloses(interval2));
        assertFalse(interval1.encloses(interval3));
    }
    @Test
    public void testContains() throws Exception
    {
        Interval interval = new Interval(0, 5, null);
        assertTrue(interval.contains(0));
        assertTrue(interval.contains(5));
        assertFalse(interval.contains(-1));
        assertFalse(interval.contains(6));
    }
    @Test
    public void testIntersects() throws Exception
    {
        Interval interval = new Interval(0,5,null);
        Interval interval1 = new Interval(0, 10, null);
        Interval interval2 = new Interval(5,10,null);
        Interval interval3 = new Interval(0, 11, null);
        Interval interval5 = new Interval(6,12,null);

        assertTrue(interval.intersects(interval1));
        assertTrue(interval.intersects(interval2));
        assertTrue(interval.intersects(interval3));
        assertFalse(interval.intersects(interval5));
    }
}

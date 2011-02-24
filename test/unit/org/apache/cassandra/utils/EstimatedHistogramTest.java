/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import org.junit.Test;

import static org.junit.Assert.*;


public class EstimatedHistogramTest
{
    @Test
    public void testSimple()
    {
        // 0 and 1 map to the same, first bucket
        EstimatedHistogram histogram = new EstimatedHistogram();
        histogram.add(0);
        assertEquals(1, histogram.get(0));
        histogram.add(1);
        assertEquals(2, histogram.get(0));
    }

    @Test
    public void testOverflow()
    {
        EstimatedHistogram histogram = new EstimatedHistogram(1);
        histogram.add(100);
        assert histogram.isOverflowed();
        assertEquals(Long.MAX_VALUE, histogram.max());
    }

    @Test
    public void testMinMax()
    {
        EstimatedHistogram histogram = new EstimatedHistogram();
        histogram.add(16);
        assertEquals(15, histogram.min());
        assertEquals(17, histogram.max());
    }

    @Test
    public void testFindingCorrectBuckets()
    {
        EstimatedHistogram histogram = new EstimatedHistogram();
        histogram.add(23282687);
        assert !histogram.isOverflowed();
        assertEquals(1, histogram.getBuckets(false)[histogram.buckets.length() - 2]);

        histogram.add(9);
        assertEquals(1, histogram.getBuckets(false)[8]);

        histogram.add(20);
        histogram.add(21);
        histogram.add(22);
        assertEquals(2, histogram.getBuckets(false)[13]);
        assertEquals(5021848, histogram.mean());
    }
}

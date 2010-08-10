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
    public void testFindingCorrectBuckets()
    {
        EstimatedHistogram histogram = new EstimatedHistogram();

        histogram.add(0L);
        assertEquals(1, histogram.get(false)[0]);

        histogram.add(33282687);
        assertEquals(1, histogram.get(false)[histogram.buckets.length()-1]);

        histogram.add(1);
        assertEquals(1, histogram.get(false)[1]);

        histogram.add(9);
        assertEquals(1, histogram.get(false)[8]);

        histogram.add(20);
        histogram.add(21);
        histogram.add(22);
        assertEquals(3, histogram.get(false)[13]);
        assertEquals(1, histogram.min());
        assertEquals(25109160, histogram.max());
        assertEquals(20, histogram.median());
    }
}

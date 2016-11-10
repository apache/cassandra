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
import static junit.framework.Assert.assertFalse;

public class LatencyMetricsTest
{
    /**
     * Test bitsets in a "real-world" environment, i.e., bloom filters
     */
    @Test
    public void testGetRecentLatency()
    {
        final LatencyMetrics l = new LatencyMetrics("test", "test");
        Runnable r = new Runnable()
        {
            public void run()
            {
                for (int i = 0; i < 10000; i++)
                {
                    l.addNano(1000);
                }
            }
        };
        new Thread(r).start();

        for (int i = 0; i < 10000; i++)
        {
            Double recent = l.latency.getOneMinuteRate();
            assertFalse(recent.equals(Double.POSITIVE_INFINITY));
        }
    }
}
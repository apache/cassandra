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
package org.apache.cassandra.utils;

import static org.junit.Assert.*;

import org.junit.Test;

public class NanoTimeToCurrentTimeMillisTest
{
    @Test
    public void testTimestampOrdering() throws Exception
    {
        long nowNanos = System.nanoTime();
        long now = System.currentTimeMillis();
        long lastConverted = 0;
        for (long ii = 0; ii < 10000000; ii++)
        {
            now = Math.max(now, System.currentTimeMillis());
            if (ii % 10000 == 0)
            {
                synchronized (NanoTimeToCurrentTimeMillis.TIMESTAMP_UPDATE)
                {
                    NanoTimeToCurrentTimeMillis.TIMESTAMP_UPDATE.notify();
                }
                Thread.sleep(1);
            }
            nowNanos = Math.max(now, System.nanoTime());
            long convertedNow = NanoTimeToCurrentTimeMillis.convert(nowNanos);
            assertTrue("convertedNow = " + convertedNow + " lastConverted = " + lastConverted + " in iteration " + ii, convertedNow >= (lastConverted - 1));
            lastConverted = convertedNow;
            //Seems to be off by as much as two milliseconds sadly
            assertTrue("now = " + now + " convertedNow = " + convertedNow + " in iteration " + ii, (now - 2) <= convertedNow);

        }
    }
}

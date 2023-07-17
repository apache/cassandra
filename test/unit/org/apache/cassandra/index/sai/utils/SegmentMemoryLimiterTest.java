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
package org.apache.cassandra.index.sai.utils;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SegmentMemoryLimiterTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @After
    public void restoreDefaults()
    {
        SegmentMemoryLimiter.reset();
    }

    @Test
    public void shouldStartAtZeroUsage()
    {
        assertEquals(0, SegmentMemoryLimiter.currentBytesUsed());
        assertFalse(SegmentMemoryLimiter.usageExceedsLimit(0));
    }

    @Test
    public void shouldRegisterUsageBelowLimit()
    {
        SegmentMemoryLimiter.increment(4);
        assertEquals(4, SegmentMemoryLimiter.currentBytesUsed());
        assertFalse(SegmentMemoryLimiter.usageExceedsLimit(0));
    }

    @Test
    public void shouldRegisterUsageExceedingLimit()
    {
        SegmentMemoryLimiter.setLimitBytes(9);
        SegmentMemoryLimiter.increment(10);
        assertEquals(10, SegmentMemoryLimiter.currentBytesUsed());
        assertTrue(SegmentMemoryLimiter.usageExceedsLimit(10));
    }

    @Test
    public void shouldReturnBelowLimit()
    {
        SegmentMemoryLimiter.setLimitBytes(9);
        SegmentMemoryLimiter.increment(10);
        assertEquals(10, SegmentMemoryLimiter.currentBytesUsed());
        assertTrue(SegmentMemoryLimiter.usageExceedsLimit(10));

        SegmentMemoryLimiter.decrement(3);
        assertEquals(7, SegmentMemoryLimiter.currentBytesUsed());
        assertFalse(SegmentMemoryLimiter.usageExceedsLimit(10));
    }

    @Test
    public void shouldZeroTrackerAfterFlush()
    {
        SegmentMemoryLimiter.increment(5);
        SegmentMemoryLimiter.decrement(5);
        assertEquals(0, SegmentMemoryLimiter.currentBytesUsed());
        assertFalse(SegmentMemoryLimiter.usageExceedsLimit(0));
    }

    @Test
    public void activeBuilderLifeCycle()
    {
        assertEquals(0, SegmentMemoryLimiter.getActiveBuilderCount());
        SegmentMemoryLimiter.registerBuilder();
        assertEquals(1, SegmentMemoryLimiter.getActiveBuilderCount());
        SegmentMemoryLimiter.unregisterBuilder();
        assertEquals(0, SegmentMemoryLimiter.getActiveBuilderCount());
    }

    @Test
    public void usageLimitLifeCycle()
    {
        SegmentMemoryLimiter.setLimitBytes(40);
        assertFalse(SegmentMemoryLimiter.usageExceedsLimit(40));
        SegmentMemoryLimiter.registerBuilder();
        assertFalse(SegmentMemoryLimiter.usageExceedsLimit(40));
        SegmentMemoryLimiter.registerBuilder();
        assertFalse(SegmentMemoryLimiter.usageExceedsLimit(20));
    }
}

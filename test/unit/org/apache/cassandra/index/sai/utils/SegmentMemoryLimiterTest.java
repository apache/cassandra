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
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SegmentMemoryLimiterTest extends SAITester
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldStartAtZeroUsage()
    {
        assertEquals(0, memoryLimiter.currentBytesUsed());
        assertFalse(memoryLimiter.usageExceedsLimit(0));
    }

    @Test
    public void shouldRegisterUsageBelowLimit()
    {
        memoryLimiter.increment(4);
        assertEquals(4, memoryLimiter.currentBytesUsed());
        assertFalse(memoryLimiter.usageExceedsLimit(0));
    }

    @Test
    public void shouldRegisterUsageExceedingLimit()
    {
        memoryLimiter.setLimitBytes(9);
        memoryLimiter.increment(10);
        assertEquals(10, memoryLimiter.currentBytesUsed());
        assertTrue(memoryLimiter.usageExceedsLimit(10));
    }

    @Test
    public void shouldReturnBelowLimit()
    {
        memoryLimiter.setLimitBytes(9);
        memoryLimiter.increment(10);
        assertEquals(10, memoryLimiter.currentBytesUsed());
        assertTrue(memoryLimiter.usageExceedsLimit(10));

        memoryLimiter.decrement(3);
        assertEquals(7, memoryLimiter.currentBytesUsed());
        assertFalse(memoryLimiter.usageExceedsLimit(10));
    }

    @Test
    public void shouldZeroTrackerAfterFlush()
    {
        memoryLimiter.increment(5);
        memoryLimiter.decrement(5);
        assertEquals(0, memoryLimiter.currentBytesUsed());
        assertFalse(memoryLimiter.usageExceedsLimit(0));
    }

    @Test
    public void activeBuilderLifeCycle()
    {
        assertEquals(0, memoryLimiter.getActiveBuilderCount());
        memoryLimiter.registerBuilder();
        assertEquals(1, memoryLimiter.getActiveBuilderCount());
        memoryLimiter.unregisterBuilder();
        assertEquals(0, memoryLimiter.getActiveBuilderCount());
    }

    @Test
    public void usageLimitLifeCycle()
    {
        memoryLimiter.setLimitBytes(40);
        assertFalse(memoryLimiter.usageExceedsLimit(40));
        memoryLimiter.registerBuilder();
        assertFalse(memoryLimiter.usageExceedsLimit(40));
        memoryLimiter.registerBuilder();
        assertFalse(memoryLimiter.usageExceedsLimit(20));
    }
}

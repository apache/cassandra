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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NamedMemoryLimiterTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    @Test
    public void shouldStartAtZeroUsage()
    {
        NamedMemoryLimiter limiter = new NamedMemoryLimiter(9, "Test");
        assertEquals(0, limiter.currentBytesUsed());
        assertFalse(limiter.usageExceedsLimit());
    }

    @Test
    public void shouldRegisterUsageBelowLimit()
    {
        NamedMemoryLimiter limiter = new NamedMemoryLimiter(9, "Test");
        limiter.increment(4);
        assertEquals(4, limiter.currentBytesUsed());
        assertFalse(limiter.usageExceedsLimit());
    }

    @Test
    public void shouldRegisterUsageExceedingLimit()
    {
        NamedMemoryLimiter limiter = new NamedMemoryLimiter(9, "Test");
        limiter.increment(10);
        assertEquals(10, limiter.currentBytesUsed());
        assertTrue(limiter.usageExceedsLimit());
    }

    @Test
    public void shouldReturnBelowLimit()
    {
        NamedMemoryLimiter limiter = new NamedMemoryLimiter(9, "Test");
        
        limiter.increment(10);
        assertEquals(10, limiter.currentBytesUsed());
        assertTrue(limiter.usageExceedsLimit());

        limiter.decrement(3);
        assertEquals(7, limiter.currentBytesUsed());
        assertFalse(limiter.usageExceedsLimit());
    }

    @Test
    public void shouldZeroTrackerAfterFlush()
    {
        NamedMemoryLimiter limiter = new NamedMemoryLimiter(9, "Test");
        limiter.increment(5);
        limiter.decrement(5);
        assertEquals(0, limiter.currentBytesUsed());
        assertFalse(limiter.usageExceedsLimit());
    }
}

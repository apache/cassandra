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

package org.apache.cassandra.service.accord.execution;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test was authored by Claude (Anthropic).
 */
public class AccordCacheEntryRecoveryVisitTest
{
    @Test
    public void halvingsCountVisitsModulo64()
    {
        AccordCacheEntry<Object, Object, ?> entry = new AccordCacheEntry<>(new Object(), null);
        int status = entry.status().ordinal();
        assertEquals(0, entry.ageHalvings());
        for (int visit = 1 ; visit <= 64 * 3 + 5 ; ++visit)
        {
            entry.recordRecoveryVisit();
            assertEquals(visit % 64, entry.ageHalvings());
        }
        // none of this disturbs the entry's status (in particular, wrapping does not carry into the flags above)
        assertEquals(status, entry.status().ordinal());
        assertTrue(!entry.isNoEvict() && !entry.isInconsistent() && !entry.isUnsafeToRead());
    }

    @Test
    public void markNoEvictClearsRecoveryVisits()
    {
        AccordCacheEntry<Object, Object, ?> entry = new AccordCacheEntry<>(new Object(), null);
        for (int i = 0 ; i < 63 ; ++i)
            entry.recordRecoveryVisit();
        entry.markNoEvict(5, 7);
        assertTrue(entry.isNoEvict());
        assertEquals(5, entry.noEvictGeneration());
        assertEquals(7, entry.noEvictMaxAge());
    }
}

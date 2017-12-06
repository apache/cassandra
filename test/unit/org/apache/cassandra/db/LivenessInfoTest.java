/**
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

package org.apache.cassandra.db;

import static org.junit.Assert.*;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.mapred.machines_jsp;
import org.junit.Test;

public class LivenessInfoTest
{
    @Test
    public void testSupersedes()
    {
        LivenessInfo first;
        LivenessInfo second;
        int nowInSeconds = FBUtilities.nowInSeconds();

        // timestamp supersedes for normal liveness info
        first = LivenessInfo.create(100, 0, nowInSeconds);
        second = LivenessInfo.create(101, 0, nowInSeconds);
        assertSupersedes(second, first);

        // timestamp supersedes for ttl
        first = LivenessInfo.create(100, 0, nowInSeconds);
        second = LivenessInfo.expiring(99, 1, nowInSeconds);
        assertSupersedes(first, second);

        // timestamp supersedes for mv expired liveness
        first = LivenessInfo.create(100, 0, nowInSeconds);
        second = LivenessInfo.withExpirationTime(99, LivenessInfo.EXPIRED_LIVENESS_TTL, nowInSeconds);
        assertSupersedes(first, second);

        // timestamp ties, ttl supersedes non-ttl
        first = LivenessInfo.expiring(100, 1, nowInSeconds);
        second = LivenessInfo.create(100, 0, nowInSeconds);
        assertSupersedes(first, second);

        // timestamp ties, greater localDeletionTime supersedes
        first = LivenessInfo.expiring(100, 2, nowInSeconds);
        second = LivenessInfo.expiring(100, 1, nowInSeconds);
        assertSupersedes(first, second);

        first = LivenessInfo.expiring(100, 5, nowInSeconds - 4);
        second = LivenessInfo.expiring(100, 2, nowInSeconds);
        assertSupersedes(second, first);

        // timestamp ties, mv expired liveness supersedes normal ttl
        first = LivenessInfo.withExpirationTime(100, LivenessInfo.EXPIRED_LIVENESS_TTL, nowInSeconds);
        second = LivenessInfo.expiring(100, 1000, nowInSeconds);
        assertSupersedes(first, second);

        // timestamp ties, mv expired liveness supersedes non-ttl
        first = LivenessInfo.withExpirationTime(100, LivenessInfo.EXPIRED_LIVENESS_TTL, nowInSeconds);
        second = LivenessInfo.create(100, 0, nowInSeconds);
        assertSupersedes(first, second);

        // timestamp ties, both are mv expired liveness, local deletion time win
        first = LivenessInfo.withExpirationTime(100, LivenessInfo.EXPIRED_LIVENESS_TTL, nowInSeconds + 1);
        second = LivenessInfo.withExpirationTime(100, LivenessInfo.EXPIRED_LIVENESS_TTL, nowInSeconds);
        assertSupersedes(first, second);
    }

    @Test
    public void testIsLive()
    {
        int nowInSeconds = FBUtilities.nowInSeconds();

        assertIsLive(LivenessInfo.create(100, 0, nowInSeconds), nowInSeconds - 3, true);
        assertIsLive(LivenessInfo.create(100, 0, nowInSeconds), nowInSeconds, true);
        assertIsLive(LivenessInfo.create(100, 0, nowInSeconds), nowInSeconds + 3, true);

        assertIsLive(LivenessInfo.expiring(100, 2, nowInSeconds), nowInSeconds - 3, true);
        assertIsLive(LivenessInfo.expiring(100, 2, nowInSeconds), nowInSeconds, true);
        assertIsLive(LivenessInfo.expiring(100, 2, nowInSeconds), nowInSeconds + 3, false);

        assertIsLive(LivenessInfo.withExpirationTime(100, LivenessInfo.EXPIRED_LIVENESS_TTL, nowInSeconds), nowInSeconds - 3, false);
        assertIsLive(LivenessInfo.withExpirationTime(100, LivenessInfo.EXPIRED_LIVENESS_TTL, nowInSeconds), nowInSeconds, false);
        assertIsLive(LivenessInfo.withExpirationTime(100, LivenessInfo.EXPIRED_LIVENESS_TTL, nowInSeconds), nowInSeconds + 3, false);
    }

    /**
     * left supersedes right, right doesn't supersede left.
     */
    private static void assertSupersedes(LivenessInfo left, LivenessInfo right)
    {
        assertTrue(left.supersedes(right));
        assertFalse(right.supersedes(left));
    }

    private static void assertIsLive(LivenessInfo info, int nowInSec, boolean alive)
    {
        assertEquals(info.isLive(nowInSec), alive);
    }
}

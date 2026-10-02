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

import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.service.accord.execution.PositionClock.BOOST_FADE;
import static org.apache.cassandra.service.accord.execution.PositionClock.BOOST_LIMIT;
import static org.apache.cassandra.service.accord.execution.PositionClock.age;
import static org.apache.cassandra.service.accord.execution.PositionClock.boost;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test was authored by Claude (Anthropic).
 */
public class PositionClockTest
{
    static final long HLC = 1_790_000_000_000_000L;
    static final long NANOS = 987_654_321_000L;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void fifoPositionsIncreaseBeforeAnyHlc()
    {
        PositionClock clock = new PositionClock();
        assertEquals(1, clock.assignNew(0, NANOS));
        assertEquals(2, clock.assignNew(0, NANOS + 1_000_000_000L)); // no anchor yet: elapsed time is not counted
        assertEquals(3, clock.assignNew(0, NANOS));
    }

    @Test
    public void youngHlcKeepsExactOrder()
    {
        PositionClock clock = new PositionClock();
        assertEquals(HLC, clock.assignNew(HLC, NANOS));
        assertEquals(HLC - 10, clock.assignNew(HLC - 10, NANOS));
        assertEquals(HLC - BOOST_LIMIT / 2, clock.assignNew(HLC - BOOST_LIMIT / 2, NANOS));
        // FIFO work is placed at the clock, i.e. behind all of the above
        assertTrue(clock.assignNew(0, NANOS) > HLC);
    }

    @Test
    public void futureHlcIsKeptAndAdvancesClock()
    {
        PositionClock clock = new PositionClock();
        clock.assignNew(HLC, NANOS);
        long future = HLC + 5_000_000;
        assertEquals(future, clock.assignNew(future, NANOS));
        assertTrue(clock.nextPosition() > future);
        assertTrue(clock.assignNew(0, NANOS) > future);
    }

    @Test
    public void clockAdvancesWithElapsedTimeOnceAnchored()
    {
        PositionClock clock = new PositionClock();
        clock.assignNew(HLC, NANOS);
        long elapsedMicros = 5_000;
        long fifo = clock.assignNew(0, NANOS + elapsedMicros * 1000);
        assertTrue(fifo >= HLC + elapsedMicros);
        // a consequence of old work registered at this time is aged against the advanced clock
        long now = clock.nextPosition();
        long oldParent = now - 10 * (BOOST_LIMIT + Math.max(0, BOOST_FADE));
        assertEquals(age(oldParent, now), clock.assignInherited(oldParent, NANOS + elapsedMicros * 1000));
    }

    @Test
    public void clockNeverGoesBackwards()
    {
        PositionClock clock = new PositionClock();
        clock.assignNew(HLC, NANOS);
        long last = clock.assignNew(0, NANOS + 10_000_000L); // 10ms later
        // a newer (but behind our clock) HLC re-anchors the clock; it must not move it backwards
        clock.assignNew(HLC + 1, NANOS + 10_000_000L);
        long next = clock.assignNew(0, NANOS + 10_000_000L);
        assertTrue(next > last);
        // nor should a task created earlier than the anchor
        next = clock.assignNew(0, NANOS);
        assertTrue(next > last);
    }

    @Test
    public void youngConsequenceKeepsParentPosition()
    {
        PositionClock clock = new PositionClock();
        clock.assignNew(HLC, NANOS);
        long parent = HLC - BOOST_LIMIT / 2;
        assertEquals(parent, clock.assignInherited(parent, NANOS));
    }

    @Test
    public void boostShape()
    {
        for (long a = 0 ; a <= BOOST_LIMIT ; a += Math.max(1, BOOST_LIMIT / 1000))
            assertEquals(a, boost(a));

        long prev = boost(BOOST_LIMIT);
        long max = BOOST_LIMIT + (BOOST_FADE >= 0 ? BOOST_FADE : BOOST_LIMIT) * 2;
        for (long a = BOOST_LIMIT ; a <= max ; a += Math.max(1, BOOST_LIMIT / 1000))
        {
            long b = boost(a);
            assertTrue(b >= 0 && b <= Math.min(a, BOOST_LIMIT));
            assertTrue("boost must not increase beyond BOOST_LIMIT", b <= prev);
            prev = b;
        }

        if (BOOST_FADE >= 0)
        {
            assertEquals(0, boost(BOOST_LIMIT + BOOST_FADE));
            assertEquals(0, boost(Long.MAX_VALUE / 2));
            if (BOOST_FADE > 0)
                assertEquals(BOOST_LIMIT / 2, boost(BOOST_LIMIT + BOOST_FADE / 2), 1);
        }
        else
        {
            assertEquals(BOOST_LIMIT, boost(Long.MAX_VALUE / 2));
        }
    }

    @Test
    public void agedPositionIsBoundedAndNeverBelowOriginal()
    {
        Random random = new Random(0);
        for (int i = 0 ; i < 100_000 ; ++i)
        {
            long now = HLC + random.nextInt(1 << 30);
            long position = now - (random.nextBoolean() ? random.nextInt(1 << 22) : random.nextInt(1 << 30)) + random.nextInt(1000);
            long aged = age(position, now);
            assertTrue(aged >= position); // so consequences never precede their parent
            if (position < now)
            {
                assertTrue(aged >= now - BOOST_LIMIT); // so no task may overtake by more than BOOST_LIMIT
                assertTrue(aged <= now);
            }
            else
            {
                assertEquals(position, aged);
            }
        }
    }
}

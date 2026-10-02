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

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.AccordConfig;
import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Assigns each task its position, which is the key by which it is prioritised in every queue (the runnable queues
 * at both the executor and command store level, and the cache entry queues).
 *
 * <p>A task's position is fixed when it is registered, and derives from its HLC (if any) and the time it was
 * registered, so that an older HLC may take priority over newer work, but only by a bounded amount:
 * a task registered at {@code now} has a position in {@code [now - BOOST_LIMIT, now]} (or later, for an HLC that is
 * ahead of our clock). Since our clock advances with real time, a task may therefore only be overtaken by work
 * registered within {@code BOOST_LIMIT} after it, so that no task may be starved indefinitely, whatever the HLC (or
 * inherited position) of the work that follows it.
 *
 * <p>Concretely: {@code position = now - boost(now - hlc)}, where {@code boost(age) = age} for ages up to
 * {@code BOOST_LIMIT} (i.e. exact HLC order), thereafter declining linearly to zero over {@code BOOST_FADE} (so that
 * very stale work is queued FIFO with new work, rather than dominating it).
 *
 * <p>Consequences inherit their parent's position, and this is re-aged against the clock when they are registered.
 * While the parent's position is younger than {@code BOOST_LIMIT} this is exactly the parent's position (so a young
 * chain of consequences keeps its place); thereafter the chain cannot claim more than {@code BOOST_LIMIT} of priority,
 * so chains of consequences cannot hold an old position indefinitely. Note that since the age of a consequence is
 * measured from its inherited position (not the original HLC), and each re-aging resets this age to at most
 * {@code BOOST_LIMIT}, a long-lived chain retains up to {@code BOOST_LIMIT} of priority rather than fading via
 * {@code BOOST_FADE}; i.e. the fade applies to the HLC of new work only, and chains are bounded as by a simple clamp.
 * The exception is consequences that are pre-setup with their parent's state, which inherit exactly.
 *
 * <p>The clock is maintained in position (HLC microsecond) units, but without assuming any relationship between the
 * HLCs we receive and our own wall clock: it is anchored by the most recent HLC of newly submitted work, and extended
 * by the (nanoTime) interval elapsed since that work was created, measured via {@link Task#createdAt} of the work
 * being registered. Before any HLC is seen, it advances only by the assignment of FIFO positions.
 */
final class PositionClock
{
    static final long BOOST_LIMIT, BOOST_FADE; // micros; BOOST_FADE < 0 means the boost does not decline

    static
    {
        AccordConfig config = DatabaseDescriptor.getAccord();
        BOOST_LIMIT = config.queue_priority_boost_limit.to(TimeUnit.MICROSECONDS);
        BOOST_FADE = config.queue_priority_boost_fade == null ? -1 : config.queue_priority_boost_fade.to(TimeUnit.MICROSECONDS);
    }

    // the next FIFO position, and our clock: greater than every position we have assigned, except an HLC ahead of our clock
    long next = 1;
    // the most recent HLC of new work, and the nanoTime it was created (anchorPosition == 0 means no anchor yet)
    private long anchorPosition, anchorNanos;

    long nextPosition()
    {
        return next;
    }

    /**
     * @param position the task's HLC, or zero if it has none (and should be queued FIFO)
     * @param nanos the nanoTime the task was created
     */
    long assignNew(long position, long nanos)
    {
        long now = now(nanos);
        if (position == 0)
            return next++;

        if (position > anchorPosition && (anchorPosition == 0 || nanos - anchorNanos >= 0))
        {
            anchorPosition = position;
            anchorNanos = nanos;
        }

        if (position >= now)
        {
            next = position + 1;
            return position;
        }

        return age(position, now);
    }

    /**
     * @param position the position inherited from the task's parent
     * @param nanos the nanoTime the task was created
     */
    long assignInherited(long position, long nanos)
    {
        return age(position, now(nanos));
    }

    private long now(long nanos)
    {
        if (anchorPosition != 0)
        {
            long elapsedMicros = (nanos - anchorNanos) / 1000;
            if (elapsedMicros > 0 && anchorPosition + elapsedMicros > next)
                next = anchorPosition + elapsedMicros;
        }
        return next;
    }

    static long age(long position, long now)
    {
        long age = now - position;
        if (age <= 0)
            return position;
        return now - boost(age);
    }

    /**
     * The amount by which work of the given age (in micros) may take priority over new work.
     * Always in {@code [0, min(age, BOOST_LIMIT)]}.
     */
    static long boost(long age)
    {
        if (age <= BOOST_LIMIT)
            return age;
        if (BOOST_FADE < 0)
            return BOOST_LIMIT;
        long over = age - BOOST_LIMIT;
        if (over >= BOOST_FADE)
            return 0;
        return BOOST_LIMIT - (long) (BOOST_LIMIT * ((double) over / BOOST_FADE));
    }
}

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

package org.apache.cassandra.replication;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;

import accord.utils.Invariants;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.IntegerInterval;

/**
 * Tracks offsets of clean (i.e. memtable->sstable flushed) and dirty (i.e. not yet durably persisted in sstable)
 * allocations.
 *
 * Mutations in segments marked as clean do not need to be replayed.
 *
 * Tracks which parts of a commit log segment contain unflushed data for each table, and determines when all
 * mutations associated with a segment are fully memtable->sstable flushed
 *
 * Maintains per-table states:
 * - "dirty" high mark (bumped when new allocation is made in the segment)
 * - "clean" intervals (min/max bounds reported via memtable flushes)
 *
 * A segment is considered clean when all dirty intervals are covered by clean intervals for every table.
 */
public class SegmentStateTracker
{
    final long segmentId;

    private final Map<TableId, IntervalState> states = new HashMap<>(32);
    private final Lock lock = new ReentrantLock();

    public SegmentStateTracker(long segmentId)
    {
        this.segmentId = segmentId;
    }

    public long segmentId()
    {
        return segmentId;
    }

    @VisibleForTesting
    public boolean isClean()
    {
        return removeCleanFromDirty();
    }

    /**
     * Removes clean table intervals from tracked states. Returns true if all intervals are clean.
     */
    public boolean removeCleanFromDirty()
    {
        lock.lock();
        try
        {
            states.values().removeIf(state -> !state.isDirty());
            return states.isEmpty();
        }
        finally
        {
            lock.unlock();
        }
    }

    public Set<TableId> getDirtyTableIds()
    {
        lock.lock();
        try
        {
            Set<TableId> dirty = new HashSet<>();
            for (Map.Entry<TableId, IntervalState> entry : states.entrySet())
            {
                if (entry.getValue().isDirty())
                    dirty.add(entry.getKey());
            }
            return dirty;
        }
        finally
        {
            lock.unlock();
        }
    }

    public void markDirty(TableId tableId, CommitLogPosition ptr)
    {
        markDirty(tableId, ptr.segmentId, ptr.position);
    }

    public void markDirty(TableId tableId, long segmentId, int position)
    {
        Invariants.require(segmentId == this.segmentId);
        lock.lock();
        try
        {
            IntervalState state = states.computeIfAbsent(tableId, k -> new IntervalState(position, position));
            state.markDirty(position);
        }
        finally
        {
            lock.unlock();
        }
    }

    public void markClean(TableId tableId, CommitLogPosition lowerBound, CommitLogPosition upperBound)
    {
        Invariants.require(lowerBound.compareTo(upperBound) <= 0, "%s should be smaller than %s", lowerBound, upperBound);
        if (lowerBound.segmentId > segmentId || upperBound.segmentId < segmentId)
            return;

        int lower = lowerBound.segmentId == segmentId ? lowerBound.position : 0;
        int upper = upperBound.segmentId == segmentId ? upperBound.position : Integer.MAX_VALUE;

        lock.lock();
        try
        {
            IntervalState state = states.get(tableId);
            if (state != null)
                state.markClean(lower, upper);
        }
        finally
        {
            lock.unlock();
        }
    }

    private static class IntervalState
    {
        static final long[] EMPTY = new long[0];

        // dirty interval in this segment; if interval is not covered by the clean set, the log contains unflushed data
        private long dirty;
        // clean intervals; separate map from above to permit marking Cfs clean whilst the log is still in use
        private long[] clean = EMPTY;

        public IntervalState(int lower, int upper)
        {
            this(make(lower, upper));
        }

        private IntervalState(long dirty)
        {
            this.dirty = dirty;
        }

        public void markClean(int start, int end)
        {
            this.clean = IntegerInterval.Set.add(clean, start, end);
        }

        public boolean isDirty()
        {
            return !IntegerInterval.Set.covers(clean, lower(dirty), upper(dirty));
        }

        /**
         * Expands the interval to cover the given value by extending one of its sides if necessary.
         */
        public void markDirty(int value)
        {
            int upper = upper(dirty);
            int lower = lower(dirty);
            if (value > upper)
                upper = value;
            else if (value < lower)
                lower = value;
            this.dirty = make(lower, upper);
        }

        public String toString()
        {
            long dirty = this.dirty;
            long[] clean = this.clean;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < clean.length; i++)
            {
                long l = clean[i];
                if (i > 0)
                    sb.append(',');
                sb.append('[').append(lower(l)).append(',').append(upper(l)).append("]");
            }
            return "dirty:[" + lower(dirty) + ',' + upper(dirty) + "], clean:[" + sb + "]";
        }

        private static long make(int lower, int upper)
        {
            assert lower <= upper;
            return ((lower & 0xFFFFFFFFL) << 32) | upper & 0xFFFFFFFFL;
        }

        private static int lower(long interval)
        {
            return (int) (interval >>> 32);
        }

        private static int upper(long interval)
        {
            return (int) interval;
        }
    }

    @Override
    public String toString() {
        return "DefaultStateTracker{" +
                "segmentId=" + segmentId +
                ", states=" + states +
                '}';
    }
}

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import accord.utils.Invariants;
import com.google.common.annotations.VisibleForTesting;
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
        removeCleanFromDirty();
        lock.lock();
        try
        {
            return states.isEmpty();
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Should be called _only_ for a static segment to ensure there can be no way for interval state
     * to go back from clean to dirty.
     *
     * Removes all clean (i.e. memtable -> sstable flushed) from dirty interval. If metadata tracking for all intervals of all tables
     * are clean, returns true. False otherwise.
     */
    public boolean removeCleanFromDirty()
    {
        List<Map.Entry<TableId, IntervalState>> states;
        // Take a "snapshot" of states, while holding a lock
        lock.lock();
        try
        {
            states = new ArrayList<>(this.states.entrySet());
        }
        finally
        {
            lock.unlock();
        }

        int[] remove = new int[states.size()];
        int removeCount = 0;

        // Check if any of the remaining items can be cleaned up, without holding a lock
        for (int i = 0; i < states.size(); i++)
        {
            IntervalState state = states.get(i).getValue();
            if (!state.isDirty())
                remove[removeCount++] = i;
        }

        // Remove all fully covered items, while holding a lock
        if (removeCount > 0)
        {
            lock.lock();
            try
            {
                if (this.states.size() == removeCount)
                {
                    this.states.clear();
                    return true;
                }

                for (int i = 0; i < removeCount; i++)
                {
                    Map.Entry<TableId, IntervalState> e = states.get(remove[i]);
                    this.states.remove(e.getKey());
                }
            }
            finally
            {
                lock.unlock();
            }
        }

        return false;
    }

    public void markDirty(TableId tableId, CommitLogPosition ptr)
    {
        markDirty(tableId, ptr.segmentId, ptr.position);
    }

    public void markDirty(TableId tableId, long segmentId, int position)
    {
        Invariants.require(segmentId == this.segmentId);
        IntervalState state;
        lock.lock();
        try
        {
            state = states.computeIfAbsent(tableId, (k) -> {
                // Initialize with given position as both low and high bound to ensure we correctly set
                // lower bound when marking as clean
                return new IntervalState(position, position);
            });
        }
        finally
        {
            lock.unlock();
        }
        state.markDirty(position);
    }

    public void markClean(TableId tableId, CommitLogPosition lowerBound, CommitLogPosition upperBound)
    {
        Invariants.require(lowerBound.compareTo(upperBound) <= 0, "%s should be smaller than %s", lowerBound, upperBound);
        if (lowerBound.segmentId > segmentId || upperBound.segmentId < segmentId)
            return;

        IntervalState state;
        lock.lock();
        try
        {
            state = states.get(tableId);
        }
        finally
        {
            lock.unlock();
        }

        if (state != null)
        {
            // TODO (required): test this logic
            // Only mark clean ranges for _this_ segment
            int lower = lowerBound.segmentId == segmentId ? lowerBound.position : 0;
            int upper = upperBound.segmentId == segmentId ? upperBound.position : Integer.MAX_VALUE;
            state.markClean(lower, upper);
        }
    }

    private static class IntervalState
    {
        static final long[] EMPTY = new long[0];

        // dirty interval in this segment; if interval is not covered by the clean set, the log contains unflushed data
        volatile long dirty;
        // clean intervals; separate map from above to permit marking Cfs clean whilst the log is still in use
        volatile long[] clean = EMPTY;

        private static final AtomicLongFieldUpdater<IntervalState>              dirtyUpdater = AtomicLongFieldUpdater.newUpdater     (IntervalState.class,               "dirty");
        private static final AtomicReferenceFieldUpdater<IntervalState, long[]> cleanUpdater = AtomicReferenceFieldUpdater.newUpdater(IntervalState.class, long[].class, "clean");

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
            long[] prev;
            long[] next;
            do
            {
                prev = this.clean;
                next = IntegerInterval.Set.add(prev, start, end);
            }
            while (!cleanUpdater.compareAndSet(this, prev, next));
        }

        public boolean isDirty()
        {
            long[] clean = this.clean;
            long dirty = this.dirty;
            return !IntegerInterval.Set.covers(clean, lower(dirty), upper(dirty));
        }

        /**
         * Expands the interval to cover the given value by extending one of its sides if necessary.
         * Mutates this. Thread-safe.
         */
        public void markDirty(int value)
        {
            long prev;
            int lower;
            int upper;
            do
            {
                prev = dirty;
                upper = upper(prev);
                lower = lower(prev);
                if (value > upper) // common case
                    upper = value;
                else if (value < lower)
                    lower = value;
            }
            while (!dirtyUpdater.compareAndSet(this, prev, make(lower, upper)));
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

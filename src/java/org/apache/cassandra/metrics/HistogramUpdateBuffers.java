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

package org.apache.cassandra.metrics;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.CassandraThread;
import org.apache.cassandra.utils.MonotonicClock;

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;

import static org.apache.cassandra.config.CassandraRelevantProperties.DECAYING_ESTIMATED_HISTOGRAM_RELAXED_FLUSH_WINDOW_MS;

/**
 * Buffers {@link DecayingEstimatedHistogramReservoir} updates per thread, so the reservoirs' shared arrays are
 * touched once per flush instead of once per update.
 * <p/>
 * Each thread has one thread-local buffer, shared by every reservoir it updates. An update is stored as one packed int:
 * <pre>
 *   bits 31..12 : reservoir id (20 bits, see {@link #MAX_ID})
 *   bits 11..4  : bucket index (8 bits, MAX_BUCKET_COUNT = 237 fits it)
 *   bits  3..0  : seconds since the buffer's baseline (4 bits)
 * </pre>
 * A flush counts duplicates and hands each reservoir one contiguous range.
 * <p/>
 * The seconds field is used to update decay weight. We use delta encoding here. Four bits only span
 * {@value #MAX_DELTA} seconds, so a buffer that outlives that must drain and re-baseline.
 * <p/>
 * Reservoirs are only grouped, not ordered against each other,
 * so the id field can use the sign bit: it is only ever hashed and compared for equality.
 * <p/>
 * Reservoir ids come from a {@link FreeMetricIdSetTracker}. A reservoir that cannot get an id updates its arrays directly instead.
 * <p/>
 * Concurrency. The buffer is a single-producer ring. Appending is lock-free: store into the ring, then release-store
 * the write index. Draining threads hold the buffer's monitor only to copy out {@code [drainIndex, writeIndex)} and advance {@code drainIndex};
 * they aggregate and apply outside it.
 * The producer never writes past {@code drainIndex + CAPACITY}, so it cannot overwrite a slot a drainer has yet to read.
 * The baseline moves only while the ring is empty, so every entry in a drained batch decodes against the same one.
 */
public final class HistogramUpdateBuffers
{
    private HistogramUpdateBuffers() {}

    // buffered updates per thread; a power of two
    @VisibleForTesting
    public static final int CAPACITY = 1024;
    static final int INDEX_MASK = CAPACITY - 1;

    private static final int DELTA_BITS = 4;
    private static final int BUCKET_BITS = 8;
    private static final int ID_BITS = 20;

    private static final int BUCKET_SHIFT = DELTA_BITS;
    private static final int ID_SHIFT = DELTA_BITS + BUCKET_BITS;

    private static final int DELTA_MASK = (1 << DELTA_BITS) - 1;
    private static final int BUCKET_MASK = (1 << BUCKET_BITS) - 1;
    /** Largest seconds offset from the baseline an entry can hold. */
    @VisibleForTesting
    static final int MAX_DELTA = DELTA_MASK;
    @VisibleForTesting
    static final int MAX_ID = (1 << ID_BITS) - 1;

    static
    {
        assert Integer.bitCount(CAPACITY) == 1 : CAPACITY + " must be a power of two";
        assert DecayingEstimatedHistogramReservoir.MAX_BUCKET_COUNT + 1 <= BUCKET_MASK : "bucket index does not fit " + BUCKET_BITS + " bits";
        assert ID_BITS + BUCKET_BITS + DELTA_BITS <= Integer.SIZE : "an entry must fit an int";
    }

    private static final AtomicInteger reservoirIdGenerator = new AtomicInteger();
    @VisibleForTesting
    static final FreeMetricIdSetTracker freeIds = new FreeMetricIdSetTracker();

    private static final Object reservoirsGuard = new Object();
    // id -> reservoir, read when applying a batch. Weak, so a dropped reservoir can still be collected.
    private static volatile AtomicReferenceArray<WeakReference<DecayingEstimatedHistogramReservoir>> reservoirs =
        new AtomicReferenceArray<>(16);


    private static final List<Buffer> allBuffers = new CopyOnWriteArrayList<>();

    /** How stale a flush {@link #relaxedFlush()} settles for. Zero makes every metrics read flush. */
    @VisibleForTesting
    static long relaxedFlushWindowNanos =
        TimeUnit.MILLISECONDS.toNanos(DECAYING_ESTIMATED_HISTOGRAM_RELAXED_FLUSH_WINDOW_MS.getLong());

    private static volatile long lastFlushAtNanos;

    private static final FastThreadLocal<Buffer> holderLocal = new FastThreadLocal<>()
    {
        @Override
        protected Buffer initialValue()
        {
            return Buffer.create();
        }

        @Override
        protected void onRemoval(Buffer value)
        {
            value.release();
        }
    };

    // open addressed table for aggregating a batch; sized so the load factor stays at or below 0.5
    private static final int TABLE_BITS = Integer.numberOfTrailingZeros(CAPACITY) + 1;
    private static final int TABLE_SIZE = 1 << TABLE_BITS;
    private static final int TABLE_MASK = TABLE_SIZE - 1;

    /** Working space for one drain, one per draining thread so a scrape allocates nothing per buffer visited. */
    @VisibleForTesting
    public static final class Scratch
    {
        // copy of the drained ring region, taken under the buffer monitor
        public final int[] entries = new int[CAPACITY];
        // entry -> count, packed as (entry << 32) | count. 0 means empty.
        final long[] entryToCountTable = new long[TABLE_SIZE];
        // which table slots are in use, to compact and clear
        final int[] usedSlotsInCountMap = new int[CAPACITY];
        // the (entry, count) pairs, compacted out of the table and grouped by reservoir
        final long[] aggregatedEntries = new long[CAPACITY];
        // memoization of decay weights
        final long[] weights = new long[MAX_DELTA + 1];
        // id directory, filled while aggregating. Open addressed, holds id + 1 so 0 stays "empty".
        final int[] reservoirIds = new int[TABLE_SIZE];
        // dense index assigned to the id in that slot
        final int[] denseSlot = new int[TABLE_SIZE];
        // per dense index: how many distinct entries carry the id, then its start in aggregated
        final int[] entryCountPerReservoir = new int[CAPACITY];
        final int[] idOffset = new int[CAPACITY];
        // which idSlots are in use, so clearing costs one step per distinct id
        final int[] spareSlotPerReservoir = new int[CAPACITY];
        // dense index of each distinct entry, parallel to usedSlots
        final int[] denseSlotPerEntry = new int[CAPACITY];
    }

    private static final FastThreadLocal<Scratch> scratchLocal = new FastThreadLocal<>()
    {
        @Override
        protected Scratch initialValue()
        {
            return new Scratch();
        }
    };

    static void register(Buffer buffer)
    {
        allBuffers.add(buffer);

        if (!(Thread.currentThread() instanceof FastThreadLocalThread))
            ThreadLocalMetrics.destroyWhenUnreachable(Thread.currentThread(), buffer::release);
    }

    static void unregister(Buffer buffer)
    {
        allBuffers.remove(buffer);
    }

    private static Buffer currentBuffer()
    {
        Thread currentThread = Thread.currentThread();
        if (currentThread instanceof CassandraThread)
            return ((CassandraThread) currentThread).getHistogramUpdateBuffer();
        return holderLocal.get();
    }

    /** @return the id or -1 if ids ran out and the reservoir must update directly */
    static int registerReservoir(DecayingEstimatedHistogramReservoir reservoir)
    {
        int id = freeIds.getFreeMetricId();
        if (id < 0)
            id = reservoirIdGenerator.getAndIncrement();

        if (id > MAX_ID)
            return -1;

        WeakReference<DecayingEstimatedHistogramReservoir> ref = new WeakReference<>(reservoir);
        synchronized (reservoirsGuard)
        {
            AtomicReferenceArray<WeakReference<DecayingEstimatedHistogramReservoir>> current = reservoirs;
            if (id < current.length())
            {
                current.set(id, ref);
            }
            else
            {
                AtomicReferenceArray<WeakReference<DecayingEstimatedHistogramReservoir>> grown =
                    new AtomicReferenceArray<>(Math.max(id + 1, current.length() + (current.length() >> 1)));
                for (int i = 0; i < current.length(); i++)
                    grown.set(i, current.get(i));
                // fill the slot before publishing the array, so the array never becomes visible without it
                grown.set(id, ref);
                reservoirs = grown;
            }
        }
        return id;
    }

    static void recycleReservoir(int id)
    {
        // drain first, so no buffered entry still carries this id when another reservoir takes it
        flush();
        synchronized (reservoirsGuard)
        {
            AtomicReferenceArray<WeakReference<DecayingEstimatedHistogramReservoir>> current = reservoirs;
            if (id < current.length())
                current.set(id, null);
        }
        freeIds.markAsFree(id);
    }

    static void append(int reservoirId, int bucket, long nowSeconds)
    {
        currentBuffer().append(reservoirId, bucket, nowSeconds);
    }

    static void flush()
    {
        for (Buffer buffer : allBuffers)
            drainAndApply(buffer);
        lastFlushAtNanos = MonotonicClock.Global.approxTime.now();
    }

    /**
     * {@link #flush()}, unless one finished less than {@link #relaxedFlushWindowNanos} ago.
     * <p/>
     * A flush walks every thread's buffer, and a metrics scrape reads thousands of histograms one after another, so
     * without this each of those reads repeats the walk only to find the buffers the first one already drained.
     * Metrics tolerate being a few milliseconds behind, so the repeats are skipped.
     */
    static void relaxedFlush()
    {
        long sinceLastFlush = MonotonicClock.Global.approxTime.now() - lastFlushAtNanos;
        if (sinceLastFlush < relaxedFlushWindowNanos)
            return;

        flush();
    }

    static void drainAndApply(Buffer buffer)
    {
        // nothing appended since the last drain
        if (buffer.publishedWriteIndex() == buffer.drainIndex)
            return;

        Scratch scratch = scratchLocal.get();
        int[] entries = scratch.entries;
        int count;
        long ownerThreadId;
        long baselineSeconds;
        synchronized (buffer)
        {
            long drainIndex = buffer.drainIndex;
            long writeIndex = buffer.publishedWriteIndex();
            count = (int) (writeIndex - drainIndex);
            if (count <= 0)
                return;

            for (int i = 0; i < count; i++)
                entries[i] = buffer.ring[(int) ((drainIndex + i) & INDEX_MASK)];

            buffer.drainIndex = writeIndex;
            ownerThreadId = buffer.ownerThreadId;
            baselineSeconds = buffer.baselineSeconds;
        }
        apply(scratch, count, ownerThreadId, baselineSeconds);
    }

    /**
     * Reduces the batch to one (entry, count) pair per distinct entry and gives each reservoir its range in one call.
     */
    private static void apply(Scratch scratch, int count, long ownerThreadId, long baselineSeconds)
    {
        long[] aggregated = scratch.aggregatedEntries;
        long[] weights = scratch.weights;

        int distinct = aggregate(scratch, count);

        int from = 0;
        while (from < distinct)
        {
            int id = idOfAggregated(aggregated[from]);
            int to = from + 1;
            while (to < distinct && idOfAggregated(aggregated[to]) == id)
                to++;

            DecayingEstimatedHistogramReservoir reservoir = reservoirFor(id);
            if (reservoir != null)
                reservoir.applyBufferedUpdates(aggregated, from, to, ownerThreadId, baselineSeconds, weights);
            from = to;
        }
    }

    /**
     * Counts each distinct entry of {@code scratch.entries[0..count)} into {@code scratch.aggregatedEntries},
     * grouped so that every reservoir owns one contiguous range, and leaves used tables empty for the next flush.
     * Based on microbenchmarks such grouping is 20-40% faster than a sorting approach (even after identical entries aggregation).
     *
     * HIGH LEVEL IDEAS
     * - Count number of entries using an open addressing hashtable
     * - In parallel, count number of entries per reservoir, using another open addressing hashtable
     * - Using the per-reservoir counts we can calculate offsets for reservoir ranges in the result array
     * - Iterate over entries hastable entries and copy them into the corresponent reservoir ranges in the result.
     * - For each reservoir range we track an offset of the next free position
     *
     * EXAMPLE
     * An example: a batch whose distinct entries belong to reservoirs 41, 7 and 903,
     * with 3, 2 and 2 distinct entries each.
     * hash(41) is 305, hash(903) is 305 as well (a collision case), hash(7) is 1180.
     *
     * COUNT TABLE — keyed by hash(entry)
     *
     *   entryToCountTable[]    one long per slot, [ entry | count ], 0 = free, an open addressing hashtable with linear probing
     *   usedSlotsInCountMap[]  the occupied slots in first seen order, so compacting and clearing cost one
     *                          step per distinct entry instead of a sweep of the whole table
     *
     *
     * SPARSE SIDE — keyed by hash(reservoir id), TABLE_SIZE slots
     *
     *   slot    reservoirIds[]  (id + 1, 0 = free)      denseSlot[]  (dense position)
     *          ┌──────────────────────────────────────────────────────────────┐
     *   ...    │               0                 │      (stale, unread)       │
     *   304    │               0                 │      (stale, unread)       │
     *   305    │              42   ←── id 41     │             0              │
     *   306    │             904   ←── id 903    │             2              │ ← probed, 305 taken
     *   ...    │               0                 │      (stale, unread)       │
     *   1180   │               8   ←── id 7      │             1              │
     *          └──────────────────────────────────────────────────────────────┘
     *                  key: what is stored           value: which row below
     *
     *   id + 1 is stored instead of id, to use 0 as free marker
     *   denseSlot only means anything where reservoirIds is non-zero; elsewhere it is never read, so never cleared.
     *
     * DENSE SIDE — one row per distinct reservoir
     *
     *   dense   entryCountPerReservoir[]      idOffset[]          spareSlotPerReservoir[]
     *           distinct entries              start in            slot to clear on spare side
     *           with this id                  aggregatedEntries   at the end
     *          ┌──────────────────────────────────────────────────────────────┐
     *     0    │          3          ────►         0        │       305       │  id 41
     *     1    │          2          ────►         3        │      1180       │  id 7
     *     2    │          2          ────►         5        │       306       │  id 903
     *          └──────────────────────────────────────────────────────────────┘
     *                     3+2+2 = 7 = distinct entries               one clear step per id,
     *                     the prefix sum runs over the 3 ids,        not TABLE_SIZE
     *
     * LINKING SPARE AND DENSE SIDE
     *   denseSlotPerEntry[] holds the dense row of each distinct entry, so the scattering step needs no second probe.
     *
     * THE RESULT
     *     aggregatedEntries[ idOffset[ denseSlotPerEntry[i] ]++ ] = entryToCountTable[ usedSlotsInCountMap[i] ]
     *
     *   aggregatedEntries   [ 0  1  2 | 3  4 | 5  6 ]
     *                         id 41     id 7   id 903      one contiguous range per reservoir
     *
     * @return how many distinct entries were written
     */
    @VisibleForTesting
    public static int aggregate(Scratch scratch, int count)
    {
        int[] entries = scratch.entries;

        // open addressing hash table to count entries: hash(entry) -> [entry | count]
        long[] entryToCountTable = scratch.entryToCountTable;
        // used slot -> entry counter table position
        int[] usedSlotsInCountMap = scratch.usedSlotsInCountMap;

        // spare table: hash(reservoir id) -> [reservoir id]
        //                                    [dense table position]
        int[] reservoirIds = scratch.reservoirIds; // open addressing hash table
        int[] denseSlot = scratch.denseSlot;

        // dense table: dense table position -> [entries count]
        //                                      [spare table position]
        int[] entryCountPerReservoir = scratch.entryCountPerReservoir;
        int[] spareSlotPerReservoir = scratch.spareSlotPerReservoir;

        // used slot -> dense table position
        int[] denseSlotPerEntry = scratch.denseSlotPerEntry;

        int distinctEntries = 0;
        int denseTableFreeSlot = 0;

        for (int i = 0; i < count; i++)
        {
            int entry = entries[i];
            int entryTableSlot = hash(entry);
            while (true)
            {
                long entryTableOccupant = entryToCountTable[entryTableSlot];
                if (entryTableOccupant == 0)
                {
                    entryToCountTable[entryTableSlot] = ((long) entry << Integer.SIZE) | 1L;
                    int reservoirId = reservoirIdOf(entry);
                    int key = reservoirId + 1; // 0 means non-set
                    int reservoirIdSlot = hash(reservoirId);
                    while (true)
                    {
                        int spareTableOccupant = reservoirIds[reservoirIdSlot];
                        if (spareTableOccupant == 0)
                        {
                            reservoirIds[reservoirIdSlot] = key;
                            denseSlot[reservoirIdSlot] = denseTableFreeSlot;
                            entryCountPerReservoir[denseTableFreeSlot] = 1;
                            spareSlotPerReservoir[denseTableFreeSlot] = reservoirIdSlot;
                            denseSlotPerEntry[distinctEntries] = denseTableFreeSlot;
                            denseTableFreeSlot++;
                            break;
                        }
                        if (spareTableOccupant == key)
                        {
                            int denseTablePosition = denseSlot[reservoirIdSlot];
                            entryCountPerReservoir[denseTablePosition]++;
                            denseSlotPerEntry[distinctEntries] = denseTablePosition;
                            break;
                        }
                        reservoirIdSlot = (reservoirIdSlot + 1) & TABLE_MASK; // linear probing
                    }
                    usedSlotsInCountMap[distinctEntries++] = entryTableSlot;
                    break;
                }
                if (entryOf(entryTableOccupant) == entry)
                {
                    entryToCountTable[entryTableSlot] = entryTableOccupant + 1;
                    break;
                }
                entryTableSlot = (entryTableSlot + 1) & TABLE_MASK; // linear probing
            }
        }

        // turn the per id counts into start positions of reservoir's ranges
        int[] idOffset = scratch.idOffset;
        int acc = 0;
        for (int k = 0; k < denseTableFreeSlot; k++)
        {
            idOffset[k] = acc;
            acc += entryCountPerReservoir[k];
        }

        // compact out of the table, scattering each entry into its reservoir's range
        long[] aggregatedEntries = scratch.aggregatedEntries;
        for (int i = 0; i < distinctEntries; i++)
        {
            int slot = usedSlotsInCountMap[i];
            int reservoirRangeOffset = idOffset[denseSlotPerEntry[i]]++;
            aggregatedEntries[reservoirRangeOffset] = entryToCountTable[slot];
            entryToCountTable[slot] = 0; // only used slots are cleared, so empty ones cost nothing
        }

        // clear used spare table rows
        for (int k = 0; k < denseTableFreeSlot; k++)
            reservoirIds[spareSlotPerReservoir[k]] = 0;
        return distinctEntries;
    }

    // Knuth's multiplicative hashing
    @VisibleForTesting
    static int hash(int entry)
    {
        return (entry * 0x9E3779B1) >>> (Integer.SIZE - TABLE_BITS);
    }

    static int entryOf(long aggregated)
    {
        return (int) (aggregated >>> Integer.SIZE);
    }

    /** How many times that entry occurred in the batch. */
    static int countOf(long aggregated)
    {
        return (int) aggregated;
    }

    /** The reservoir id of an (entry, count) pair */
    static int idOfAggregated(long aggregated)
    {
        return (int) (aggregated >>> (Integer.SIZE + ID_SHIFT));
    }

    private static DecayingEstimatedHistogramReservoir reservoirFor(int id)
    {
        AtomicReferenceArray<WeakReference<DecayingEstimatedHistogramReservoir>> refs = reservoirs;
        if (id < 0 || id >= refs.length())
            return null;
        WeakReference<DecayingEstimatedHistogramReservoir> ref = refs.get(id);
        return ref == null ? null : ref.get();
    }

    @VisibleForTesting
    public static int encode(int id, int bucket, int secondsDelta)
    {
        return (id << ID_SHIFT) | (bucket << BUCKET_SHIFT) | secondsDelta;
    }

    static int reservoirIdOf(int entry)
    {
        return entry >>> ID_SHIFT;
    }

    static int bucketOf(int entry)
    {
        return (entry >>> BUCKET_SHIFT) & BUCKET_MASK;
    }

    /** Seconds between the entry and its buffer's baseline at the time it was appended. */
    static int timeDeltaOf(int entry)
    {
        return entry & DELTA_MASK;
    }

    @VisibleForTesting
    static boolean isRegistered(Buffer buffer)
    {
        return allBuffers.contains(buffer);
    }

    public static final class Buffer
    {
        // writeIndex is written with release and read with acquire: the append path avoids a volatile store's
        // fence, and a drainer still sees the ring entries that precede the index it reads.
        private static final VarHandle WRITE_INDEX;
        static
        {
            try
            {
                WRITE_INDEX = MethodHandles.lookup().findVarHandle(Buffer.class, "writeIndex", long.class);
            }
            catch (ReflectiveOperationException e)
            {
                throw new ExceptionInInitializerError(e);
            }
        }

        final long ownerThreadId = Thread.currentThread().getId();
        final int[] ring = new int[CAPACITY];

        // written by the owner, read by drainers
        @SuppressWarnings("unused") // written through the WRITE_INDEX VarHandle (setRelease)
        private long writeIndex;
        // advanced by drainers under "this" monitor; volatile so the owner sees room free up
        volatile long drainIndex;
        // owner's copy of drainIndex, possibly stale. Keeps the append path off the shared field.
        private long cachedDrainIndex;
        // what every entry's delta is relative to. Written by the owner and read by drainers under "this" monitor,
        // and only moved while the ring is empty, so one batch always has one baseline.
        long baselineSeconds;

        private boolean released;

        private Buffer() {}

        public static Buffer create()
        {
            Buffer buffer = new Buffer();
            register(buffer);
            return buffer;
        }

        public void release()
        {
            synchronized (this)
            {
                if (released)
                    return;
                released = true;
            }
            // unregister first, so a flush cannot pick this buffer up after we drain it
            unregister(this);
            drainAndApply(this);
        }

        /** to call only by the owning thread. */
        void append(int id, int bucket, long nowSeconds)
        {
            long delta = nowSeconds - baselineSeconds;
            long w = writeIndex;
            if (delta < 0 || delta > MAX_DELTA || w - cachedDrainIndex >= CAPACITY)
            {
                delta = makeRoom(nowSeconds, delta);
                w = writeIndex;
            }
            ring[(int) (w & INDEX_MASK)] = encode(id, bucket, (int) delta);
            WRITE_INDEX.setRelease(this, w + 1);
        }

        /**
         * Handles an append that cannot go ahead: the ring is full, or the update falls outside the baseline's
         * window. Both are fixed by draining.
         *
         * @return the delta to record
         */
        private long makeRoom(long nowSeconds, long delta)
        {
            if (delta < 0 || delta > MAX_DELTA)
            {
                drainAndApply(this);
                synchronized (this)
                {
                    // safe: the drain emptied the ring, so no entry still refers to the old baseline
                    baselineSeconds = nowSeconds;
                }
                cachedDrainIndex = drainIndex;
                return 0; // because we reset baselineSeconds
            }

            if (writeIndex - drainIndex >= CAPACITY)
                drainAndApply(this);
            cachedDrainIndex = drainIndex;
            return delta;
        }

        /** Reads the write index, pairing with the owner's release-store. */
        long publishedWriteIndex()
        {
            return (long) WRITE_INDEX.getAcquire(this);
        }
    }
}

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

package org.apache.cassandra.harry.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import org.agrona.collections.Long2ObjectHashMap;

import accord.utils.Invariants;

import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.gen.rng.SeedableEntropySource;
import org.apache.cassandra.utils.ArrayUtils;

/**
 * Invertible generator allows you to provide _any_ data type. Harry is based on the idea that descriptors
 * can be inflated into values, and values can be turned back into descriptors. Descriptors follow the sorting
 * order of the values they were generated from. This makes _writing_ these generators a bit more complex.
 * There is a library of lightweight generators available for simple cases.
 *
 * InvertibleGenerator decouples descriptor order from value order, and allows descriptor to be used simply as
 * a seed for generating values. Since it tracks all descriptors it generated values from in a sorted order,
 * it can always turn the given value back into a descriptor by inflating log(population) values and comparing them
 * to the searched value. In other words, it trades memory required for storing map of values to CPU required
 * to re-compute the value order.
 *
 * TODO (expected): custom invertible generator for bool, u8, u16, u32, etc, for efficiency.
 * TODO (expected): implement support for tuple/vector/udt, and other multi-cell types.
 * TODO (expected): use smaller-entropy values for descriptors, as we only need to generate _distinct_ values. In principe, we can just use counters.
 */
public class InvertibleGenerator<T> implements HistoryBuilder.IndexedBijection<T>
{
    public static long MAX_ENTROPY = 1L << 63;

    private static final boolean PARANOIA = false;
    private static final int INFLATE_CACHE_SIZE = 10_000;
    // Number of top levels of the deflate binary-search tree to pin permanently (up to ~2^depth entries), so
    // the hottest comparisons are never evicted by the FIFO backstop. Tunable.
    private static final int BINARY_SEARCH_CACHE_DEPTH = 10;

    private static final ConcurrentSkipListMap<Integer, long[]> REUSE_DESCRIPTOR_ARRAYS = new ConcurrentSkipListMap<>();

    private long[] descriptors;
    private final long descriptorCount;

    private final Comparator<T> comparator;
    private final Cache<T> inflateCache;

    public static <T> HistoryBuilder.IndexedBijection<T> fromType(EntropySource rng, int population, ColumnSpec<T> spec)
    {
        if (spec.gen instanceof HistoryBuilder.IndexedBijection)
            return (HistoryBuilder.IndexedBijection<T>) spec.gen;
        return new InvertibleGenerator<>(rng, spec.type.typeEntropy(), population, spec.gen, spec.type.comparator());
    }

    @Override
    public void discard()
    {
        if (descriptors.length > 100)
            REUSE_DESCRIPTOR_ARRAYS.putIfAbsent(descriptors.length, descriptors);
        descriptors = null;
    }

    public InvertibleGenerator(EntropySource rng,
                               /* unsigned */ long typeEntropy,
                               int population,
                               Generator<T> gen,
                               Comparator<T> comparator)
    {
        Invariants.require(population > 0,
                              "Population should be strictly positive %d", population);
        Invariants.require(Long.compareUnsigned(typeEntropy, 0) > 0,
                              "Type entropy should be strictly positive, but was %d: %s", typeEntropy, gen);

        // We can / will generate at most that many values
        if (Long.compareUnsigned(typeEntropy, Integer.MAX_VALUE) > 0)
            typeEntropy = Integer.MAX_VALUE;

        population = (int) Math.min(typeEntropy, population);

        this.comparator = comparator;
        LongFunction<T> compute = descriptor -> SeedableEntropySource.computeWithSeed(descriptor, gen::generate);

        Map.Entry<Integer, long[]> e = REUSE_DESCRIPTOR_ARRAYS.ceilingEntry(population);
        if (population > 100 && e != null && e.getValue().length < population * 2 && REUSE_DESCRIPTOR_ARRAYS.remove(e.getKey(), e.getValue()))
            this.descriptors = e.getValue();
        else
            this.descriptors = new long[population];

        // Generate a population of values into a throwaway boxed list, sort it by value, and copy the distinct
        // values (now adjacent) into the primitive descriptor array. The list and the value cache exist only
        // for the duration of construction; the long-lived state is descriptors[]/descriptorCount.
        List<Long> candidates = new ArrayList<>(population);
        for (int i = 0 ; i < population ; ++i)
        {
            long candidate = rng.next();

            // Should never allocate these, however improbable that is
            if (MagicConstants.isMagicDescriptor(candidate))
                continue;

            candidates.add(candidate);
        }

        Cache<T> tmpCache = new UnboundedCache<>(population, compute);
        candidates.sort((d1, d2) -> comparator.compare(tmpCache.get(d1), tmpCache.get(d2)));
        int count = 0;
        for (int i = 0; i < candidates.size(); i++)
        {
            long candidate = candidates.get(i);
            if (count > 0 && comparator.compare(tmpCache.get(descriptors[count - 1]), tmpCache.get(candidate)) == 0)
                continue;
            descriptors[count++] = candidate;
        }
        descriptorCount = count;

        // Inflate cache: pin the hottest binary-search entries (the top few levels, visited by every deflate)
        // so they are never evicted, and fall back to a bounded FIFO for everything else.
        this.inflateCache = new HierarchicalCache<>(new BinarySearchPathCache<>(descriptors, count, BINARY_SEARCH_CACHE_DEPTH, compute),
                                                    new FifoCache<>(INFLATE_CACHE_SIZE, compute));

        // Check there are no duplicates, and items are properly sorted.
        if (PARANOIA)
        {
            T prev = inflate(descriptors[0]);
            for (int i = 1; i < descriptorCount; i++)
            {
                T current = inflate(descriptors[i]);
                Invariants.require( comparator.compare(current, prev) > 0,
                                       "%s should be strictly after %s", prev, current);
                prev = current;
            }
        }
    }

    @Override
    public long idxFor(long descriptor)
    {
        // descriptors[] is value-sorted, so the index of a descriptor is where its value sorts.
        return binarySearch(inflate(descriptor));
    }

    @Override
    public long descriptorAt(long idx)
    {
        return descriptors[(int) idx];
    }

    @Override
    public T inflate(long descriptor)
    {
        Invariants.require(!MagicConstants.isMagicDescriptor(descriptor),
                           "Should not be able to inflate %d, as it's magic value", descriptor);
        return inflateCache.get(descriptor);
    }

    @Override
    public long deflate(T value)
    {
        final int idx = binarySearch(value);
        if (PARANOIA)
        {
            if (idx < 0)
            {
                for (int i = 0; i < descriptorCount; i++)
                {
                    Object expected = inflate(descriptors[i]);
                    if (value.getClass().isArray())
                    {
                        Object[] valueArr = (Object[]) value;
                        Object[] expectedArr = (Object[]) expected;
                        Invariants.require(comparator.compare((T) expected, value) != 0,
                                           "%s was found: %s", Arrays.toString(expectedArr), Arrays.toString(valueArr));
                    }
                    else
                    {
                        Invariants.require(comparator.compare((T) expected, value) != 0,
                                           "%s was found: %s", expected, value);
                    }
                }
            }
            else
            {
                long res = descriptors[idx];
                Object expected = inflate(res);
                if (value.getClass().isArray())
                {
                    Object[] valueArr = (Object[]) value;
                    Object[] expectedArr = (Object[]) expected;

                    Invariants.require(comparator.compare((T) expected, value) == 0,
                                       "%s != %s", Arrays.toString(expectedArr), Arrays.toString(valueArr));
                }
                else
                {
                    Invariants.require(comparator.compare((T) expected, value) == 0,
                                       "%s != %s", expected, value);
                }

                return res;
            }
        }

        if (idx < 0)
        {
            int start = Math.max(0, idx - 2);
            List<Object> nearby = new ArrayList<>();
            for (int i = start; i < start + 2; i++)
                nearby.add(inflate(descriptors[i]));
            throw new IllegalStateException(String.format("Could not find: %s\nNearby objects: %s",
                                                          ArrayUtils.toString(value), nearby.stream().map(ArrayUtils::toString).collect(Collectors.toList())));
        }

        return descriptors[idx];
    }


    @Override
    public int byteSize()
    {
        return Long.BYTES;
    }

    private int binarySearch(T key)
    {
        int low = 0, mid = (int) descriptorCount, high = mid - 1, result = -1;
        while (low <= high)
        {
            mid = (low + high) >>> 1;
            result = comparator.compare(key, inflate(descriptors[mid]));
            if (result > 0)
                low = mid + 1;
            else if (result == 0)
                return mid;
            else
                high = mid - 1;
        }
        return -mid - (result < 0 ? 1 : 2);
    }

    @Override
    public int compare(long d1, long d2)
    {
        if (d1 == d2)
            return 0;
        T v1 = inflate(d1);
        T v2 = inflate(d2);
        return comparator.compare(v1, v2);
    }

    /**
     * Returns a number of allocated descriptors
     */
    @Override
    public long population()
    {
        return descriptorCount;
    }

    public Comparator<Long> descriptorsComparator()
    {
        Map<Long, Integer> descriptorToIdx = new HashMap<>();
        for (int i = 0; i < descriptorCount; i++)
            descriptorToIdx.put(descriptors[i], i);
        return Comparator.comparingInt(descriptorToIdx::get);
    }

    /**
     * A value cache keyed by descriptor.
     */
    public interface Cache<T>
    {
        /** Returns the cached value for {@code descriptor}, or {@code null} if it is not cached; never computes. */
        T lookup(long descriptor);

        /** Returns the value for {@code descriptor}, computing it (and possibly caching it) on a miss. */
        T get(long descriptor);
    }

    /**
     * Fixed-size FIFO cache: a ring buffer tracks insertion order for eviction while a map provides O(1)
     * lookup by descriptor. Misses are filled with the supplied compute function.
     */
    public static final class FifoCache<T> implements Cache<T>
    {
        private final int capacity;
        private final LongFunction<T> compute;
        private final long[] ring;
        private final Long2ObjectHashMap<T> map;
        private int pos = 0;
        private int count = 0;

        public FifoCache(int capacity, LongFunction<T> compute)
        {
            this.capacity = capacity;
            this.compute = compute;
            this.ring = new long[capacity];
            this.map = new Long2ObjectHashMap<>(capacity, 0.75f);
        }

        @Override
        public T lookup(long descriptor)
        {
            return map.get(descriptor);
        }

        @Override
        public T get(long descriptor)
        {
            T cached = map.get(descriptor);
            if (cached != null)
                return cached;

            T value = compute.apply(descriptor);
            if (count == capacity)
                map.remove(ring[pos]);   // evict the oldest entry to make room
            else
                count++;

            ring[pos] = descriptor;
            map.put(descriptor, value);
            pos = (pos + 1) % capacity;
            return value;
        }
    }

    /**
     * Unbounded cache that retains every entry it computes; intended for short-lived, build-time use where the
     * whole population is touched and recomputation must be avoided. Not suitable as a long-lived cache.
     */
    public static final class UnboundedCache<T> implements Cache<T>
    {
        private final LongFunction<T> compute;
        private final Long2ObjectHashMap<T> map;

        public UnboundedCache(int initialCapacity, LongFunction<T> compute)
        {
            this.compute = compute;
            this.map = new Long2ObjectHashMap<>(Math.max(1, initialCapacity), 0.75f);
        }

        @Override
        public T lookup(long descriptor)
        {
            return map.get(descriptor);
        }

        @Override
        public T get(long descriptor)
        {
            T cached = map.get(descriptor);
            if (cached != null)
                return cached;

            T value = compute.apply(descriptor);
            map.put(descriptor, value);
            return value;
        }
    }

    /**
     * Caches exactly the entries that a binary search over a sorted descriptor array would touch in its first {@code depth} steps.
     */
    public static final class BinarySearchPathCache<T> implements Cache<T>
    {
        private final LongFunction<T> compute;
        private final Long2ObjectHashMap<T> map;

        public BinarySearchPathCache(long[] sortedDescriptors, int size, int depth, LongFunction<T> compute)
        {
            this.compute = compute;
            this.map = new Long2ObjectHashMap<>(Math.max(1, 1 << Math.min(depth, 16)), 0.75f);
            cachePath(sortedDescriptors, 0, size - 1, depth);
        }

        private void cachePath(long[] sortedDescriptors, int lo, int hi, int depth)
        {
            if (depth <= 0 || lo > hi)
                return;
            int mid = (lo + hi) >>> 1;
            long descriptor = sortedDescriptors[mid];
            map.put(descriptor, compute.apply(descriptor));
            cachePath(sortedDescriptors, lo, mid - 1, depth - 1);
            cachePath(sortedDescriptors, mid + 1, hi, depth - 1);
        }

        @Override
        public T lookup(long descriptor)
        {
            return map.get(descriptor);
        }

        @Override
        public T get(long descriptor)
        {
            T cached = map.get(descriptor);
            return cached != null ? cached : compute.apply(descriptor);
        }
    }

    /**
     * Composes two caches into a hierarchy: lookups try {@code primary} first and fall back to {@code secondry}
     * on a miss.
     */
    public static final class HierarchicalCache<T> implements Cache<T>
    {
        private final Cache<T> primary;
        private final Cache<T> secondary;

        public HierarchicalCache(Cache<T> primary, Cache<T> secondary)
        {
            this.primary = primary;
            this.secondary = secondary;
        }

        @Override
        public T lookup(long descriptor)
        {
            T value = primary.lookup(descriptor);
            return value != null ? value : secondary.lookup(descriptor);
        }

        @Override
        public T get(long descriptor)
        {
            T value = primary.lookup(descriptor);
            return value != null ? value : secondary.get(descriptor);
        }
    }
}

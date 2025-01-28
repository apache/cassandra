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

package accord.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import com.google.common.collect.Iterables;

import accord.utils.random.Picker;

// TODO (expected): merge with C* RandomSource
public interface RandomSource
{
    static RandomSource wrap(Random random)
    {
        return new WrappedRandomSource(random);
    }

    void nextBytes(byte[] bytes);

    boolean nextBoolean();
    default BooleanSupplier uniformBools() { return this::nextBoolean; }
    default BooleanSupplier biasedUniformBools(float chance) { return () -> decide(chance); }
    default Supplier<BooleanSupplier> biasedUniformBoolsSupplier(float minChance)
    {
        return () -> {
            float chance = minChance + (1 - minChance)*nextFloat();
            return () -> decide(chance);
        };
    }

    /**
     * Returns true with a probability of {@code chance}. This is logically the same as
     * <pre>{@code nextFloat() < chance}</pre>
     *
     * @param chance cumulative probability in range [0..1]
     */
    default boolean decide(float chance)
    {
        return nextFloat() < chance;
    }

    /**
     * Returns true with a probability of {@code chance}. This is logically the same as
     * <pre>{@code nextDouble() < chance}</pre>
     *
     * @param chance cumulative probability in range [0..1]
     */
    default boolean decide(double chance)
    {
        return nextDouble() < chance;
    }

    int nextInt();
    default int nextInt(int maxExclusive) { return nextInt(0, maxExclusive); }
    default int nextInt(int minInclusive, int maxExclusive)
    {
        // this is diff behavior than ThreadLocalRandom, which returns nextInt
        if (minInclusive >= maxExclusive)
            throw new IllegalArgumentException(String.format("Min (%s) should be less than max (%d).", minInclusive, maxExclusive));

        int result = nextInt();
        int delta = maxExclusive - minInclusive;
        int mask = delta - 1;
        if ((delta & mask) == 0) // power of two
            result = (result & mask) + minInclusive;
        else if (delta > 0)
        {
            // reject over-represented candidates
            for (int u = result >>> 1;                  // ensure nonnegative
                 u + mask - (result = u % delta) < 0;   // rejection check
                 u = nextInt() >>> 1)                   // retry
                ;
            result += minInclusive;
        }
        else
        {
            // range not representable as int
            while (result < minInclusive || result >= maxExclusive)
                result = nextInt();
        }
        return result;
    }
    default int nextBiasedInt(int minInclusive, int median, int maxExclusive)
    {
        checkBiasedUniform(minInclusive, median, maxExclusive);

        int range = Math.max(maxExclusive - median, median - minInclusive) * 2;
        int next = nextInt(range) - range/2;
        next += median;
        return next >= median ? next <  maxExclusive ? next : nextInt(median, maxExclusive)
                              : next >= minInclusive ? next : minInclusive == median ? median : nextInt(minInclusive, median);
    }

    default IntSupplier uniformInts(int minInclusive, int maxExclusive) { return () -> nextInt(minInclusive, maxExclusive); }
    default IntSupplier biasedUniformInts(int minInclusive, int median, int maxExclusive)
    {
        checkBiasedUniform(minInclusive, median, maxExclusive);
        return () -> nextBiasedInt(minInclusive, median, maxExclusive);
    }
    default Supplier<IntSupplier> biasedUniformIntsSupplier(int absoluteMinInclusive, int absoluteMaxExclusive, int minMedian, int maxMedian, int minRange, int maxRange)
    {
        return biasedUniformIntsSupplier(absoluteMinInclusive, absoluteMaxExclusive, minMedian, (minMedian+maxMedian)/2, maxMedian, minRange, (minRange+maxRange)/2, maxRange);
    }
    default Supplier<IntSupplier> biasedUniformIntsSupplier(int absoluteMinInclusive, int absoluteMaxExclusive, int minMedian, int medianMedian, int maxMedian, int minRange, int medianRange, int maxRange)
    {
        checkBiasedUniform(minMedian, medianMedian, maxMedian);
        checkBiasedUniform(minRange, medianRange, maxRange);
        if (minMedian < absoluteMinInclusive)
            throw new IllegalArgumentException(String.format("absoluteMin (%s) should be less than or equal to minMedian (%s)", absoluteMinInclusive, minMedian));
        if (maxMedian > absoluteMaxExclusive)
            throw new IllegalArgumentException(String.format("absoluteMax (%s) should be greater than or equal to maxMedian (%s)", absoluteMaxExclusive, maxMedian));
        if (minRange < 1)
            throw new IllegalArgumentException(String.format("minRange (%s) should be greater than or equal to 1", minRange));
        return () -> {
            int median = nextBiasedInt(minMedian, medianMedian, maxMedian);
            int minInclusive = Math.max(absoluteMinInclusive, median - nextBiasedInt(minRange, medianRange, maxRange)/2);
            int maxExclusive = Math.min(absoluteMaxExclusive, median + (nextBiasedInt(minRange, medianRange, maxRange)+1)/2);
            return biasedUniformInts(minInclusive, median, maxExclusive);
        };
    }

    long nextLong();
    default long nextLong(long maxExclusive) { return nextLong(0, maxExclusive); }
    default long nextLong(long minInclusive, long maxExclusive)
    {
        // this is diff behavior than ThreadLocalRandom, which returns nextLong
        if (minInclusive >= maxExclusive)
            throw new IllegalArgumentException(String.format("Min (%s) should be less than max (%d).", minInclusive, maxExclusive));

        long result = nextLong();
        long delta = maxExclusive - minInclusive;
        long mask = delta - 1;
        if ((delta & mask) == 0L) // power of two
            result = (result & mask) + minInclusive;
        else if (delta > 0L)
        {
            // reject over-represented candidates
            for (long u = result >>> 1;                 // ensure nonnegative
                 u + mask - (result = u % delta) < 0L;  // rejection check
                 u = nextLong() >>> 1)                  // retry
                ;
            result += minInclusive;
        }
        else
        {
            // range not representable as long
            while (result < minInclusive || result >= maxExclusive)
                result = nextLong();
        }
        return result;
    }
    default long nextBiasedLong(long minInclusive, long median, long maxExclusive)
    {
        checkBiasedUniform(minInclusive, median, maxExclusive);

        long range = Math.max(maxExclusive - median, median - minInclusive) * 2;
        long next = nextLong(range) - range/2;
        next += median;
        return next >= median ? next <  maxExclusive ? next : nextLong(median, maxExclusive)
                              : next >= minInclusive ? next : minInclusive == median ? median : nextLong(minInclusive, median);
    }

    default LongSupplier uniformLongs(long minInclusive, long maxExclusive) { return () -> nextLong(minInclusive, maxExclusive); }
    default LongSupplier biasedUniformLongs(long minInclusive, long median, long maxExclusive)
    {
        checkBiasedUniform(minInclusive, median, maxExclusive);
        return () -> nextBiasedLong(minInclusive, median, maxExclusive);
    }
    default Supplier<LongSupplier> biasedUniformLongsSupplier(long absoluteMinInclusive, long absoluteMaxExclusive, long minMedian, long maxMedian, long minRange, long maxRange)
    {
        return biasedUniformLongsSupplier(absoluteMinInclusive, absoluteMaxExclusive, minMedian, (minMedian+maxMedian)/2, maxRange, minRange, (minRange+maxRange)/2, maxRange);
    }
    default Supplier<LongSupplier> biasedUniformLongsSupplier(long absoluteMinInclusive, long absoluteMaxExclusive, long minMedian, long medianMedian, long maxMedian, long minRange, long medianRange, long maxRange)
    {
        checkBiasedUniform(minMedian, medianMedian, maxMedian);
        checkBiasedUniform(minRange, medianRange, maxRange);
        if (minMedian < absoluteMinInclusive)
            throw new IllegalArgumentException(String.format("absoluteMin (%s) should be less than or equal to minMedian (%s)", absoluteMinInclusive, minMedian));
        if (maxMedian > absoluteMaxExclusive)
            throw new IllegalArgumentException(String.format("absoluteMax (%s) should be greater than or equal to maxMedian (%s)", absoluteMaxExclusive, maxMedian));
        if (minRange < 1)
            throw new IllegalArgumentException(String.format("minRange (%s) should be greater than or equal to 1", minRange));
        return () -> {
            long median = nextBiasedLong(minMedian, medianMedian, maxMedian);
            long minInclusive = Math.max(absoluteMinInclusive, median - nextBiasedLong(minRange, medianRange, maxRange)/2);
            long maxExclusive = Math.min(absoluteMaxExclusive, median + (1+nextBiasedLong(minRange, medianRange, maxRange))/2);
            return biasedUniformLongs(minInclusive, median, maxExclusive);
        };
    }

    static void checkBiasedUniform(long minInclusive, long median, long maxExclusive)
    {
        if (minInclusive > median)
            throw new IllegalArgumentException(String.format("Min (%s) should be equal to or less than median (%d).", minInclusive, median));
        if (median >= maxExclusive)
            throw new IllegalArgumentException(String.format("Median (%s) should be less than max (%d).", median, maxExclusive));
    }

    float nextFloat();

    double nextDouble();
    default double nextDouble(double maxExclusive) { return nextDouble(0, maxExclusive); }
    default double nextDouble(double minInclusive, double maxExclusive)
    {
        if (minInclusive >= maxExclusive)
            throw new IllegalArgumentException(String.format("Min (%s) should be less than max (%d).", minInclusive, maxExclusive));

        double result = nextDouble();
        result = result * (maxExclusive - minInclusive) + minInclusive;
        if (result >= maxExclusive) // correct for rounding
            result = Double.longBitsToDouble(Double.doubleToLongBits(maxExclusive) - 1);
        return result;
    }

    double nextGaussian();

    default int pickInt(int first, int second, int... rest)
    {
        int offset = nextInt(0, rest.length + 2);
        switch (offset)
        {
            case 0:  return first;
            case 1:  return second;
            default: return rest[offset - 2];
        }
    }

    default int pickInt(int[] array)
    {
        return pickInt(array, 0, array.length);
    }

    default int pickInt(int[] array, int offset, int length)
    {
        Invariants.checkIndexInBounds(array.length, offset, length);
        if (length == 1)
            return array[offset];
        return array[nextInt(offset, offset + length)];
    }

    default long pickLong(long first, long second, long... rest)
    {
        int offset = nextInt(0, rest.length + 2);
        switch (offset)
        {
            case 0:  return first;
            case 1:  return second;
            default: return rest[offset - 2];
        }
    }

    default long pickLong(long[] array)
    {
        return pickLong(array, 0, array.length);
    }

    default long pickLong(long[] array, int offset, int length)
    {
        Invariants.checkIndexInBounds(array.length, offset, length);
        if (length == 1)
            return array[offset];
        return array[nextInt(offset, offset + length)];
    }

    default <T> T pickOrderedSet(SortedSet<T> set)
    {
        int offset = nextInt(0, set.size());
        return Iterables.get(set, offset);
    }

    default <T> T pickOrderedSet(LinkedHashSet<T> set)
    {
        int offset = nextInt(0, set.size());
        return Iterables.get(set, offset);
    }

    default <T extends Enum<T>> T pickOrderedSet(EnumSet<T> set)
    {
        int offset = nextInt(0, set.size());
        return Iterables.get(set, offset);
    }

    default <T extends Comparable<? super T>> T pickUnorderedSet(Set<T> set)
    {
        if (set instanceof SortedSet)
            return pickOrderedSet((SortedSet<T>) set);
        List<T> values = new ArrayList<>(set);
        // Non-ordered sets may have different iteration order on different environments, which would make a seed produce different histories!
        // To avoid such a problem, make sure to apply a deterministic function (sort).
        values.sort(Comparator.naturalOrder());
        return pick(values);
    }

    default <T> T pick(T first, T second, T... rest)
    {
        int offset = nextInt(0, rest.length + 2);
        switch (offset)
        {
            case 0:  return first;
            case 1:  return second;
            default: return rest[offset - 2];
        }
    }

    default <T> T pick(T[] array)
    {
        return array[nextInt(array.length)];
    }

    default <T> T pick(List<T> values)
    {
        return pick(values, 0, values.size());
    }

    default <T> T pick(List<T> values, int offset, int length)
    {
        Invariants.checkIndexInBounds(values.size(), offset, length);
        if (length == 1)
            return values.get(offset);
        return values.get(nextInt(offset, offset + length));
    }

    default <T> Supplier<T> randomWeightedPicker(T[] objects) { return Picker.WeightedObjectPicker.randomWeighted(this, objects); }
    default <T> Supplier<T> randomWeightedPicker(T[] objects, float[] bias) { return Picker.WeightedObjectPicker.randomWeighted(this, objects, bias); }
    default <T> Supplier<T> weightedPicker(T[] objects, float[] proportionalWeights) { return Picker.WeightedObjectPicker.weighted(this, objects, proportionalWeights); }

    void setSeed(long seed);
    RandomSource fork();

    default long reset()
    {
        long seed = nextLong();
        setSeed(seed);
        return seed;
    }

    default Random asJdkRandom()
    {
        return new Random(nextLong())
        {
            @Override
            public void setSeed(long seed)
            {
                RandomSource.this.setSeed(seed);
            }

            @Override
            public void nextBytes(byte[] bytes)
            {
                RandomSource.this.nextBytes(bytes);
            }

            @Override
            public int nextInt()
            {
                return RandomSource.this.nextInt();
            }

            @Override
            public int nextInt(int bound)
            {
                return RandomSource.this.nextInt(bound);
            }

            @Override
            public long nextLong()
            {
                return RandomSource.this.nextLong();
            }

            @Override
            public boolean nextBoolean()
            {
                return RandomSource.this.nextBoolean();
            }

            @Override
            public float nextFloat()
            {
                return RandomSource.this.nextFloat();
            }

            @Override
            public double nextDouble()
            {
                return RandomSource.this.nextDouble();
            }

            @Override
            public double nextGaussian()
            {
                return RandomSource.this.nextGaussian();
            }
        };
    }
}

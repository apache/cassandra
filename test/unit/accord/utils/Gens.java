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

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.google.common.collect.Iterables;

public class Gens {
    private Gens() {
    }

    public static <T> Gen<T> flatten(Gen<Gen<T>> gen)
    {
        return rs -> gen.next(rs).next(rs);
    }

    public static <T> Gen<T> constant(T constant)
    {
        return ignore -> constant;
    }

    public static <T> Gen<T> constant(Supplier<T> constant)
    {
        return ignore -> constant.get();
    }

    public static <T> Gen<T> oneOf(Gen<? extends T>... gens)
    {
        switch (gens.length)
        {
            case 0: throw new IllegalArgumentException("Unable to select oneOf an empty list");
            case 1: return (Gen<T>) gens[0];
        }
        return oneOf(Arrays.asList(gens));
    }

    public static <T> Gen<T> oneOf(List<Gen<? extends T>> gens)
    {
        switch (gens.size())
        {
            case 0: throw new IllegalArgumentException("Unable to select oneOf an empty list");
            case 1: return (Gen<T>) gens.get(0);
        }
        return rs -> rs.pick(gens).next(rs);
    }

    public static <T> Gen<T> oneOf(Map<Gen<T>, Integer> values)
    {
        Gen<Gen<T>> gen = pick(values);
        return rs -> gen.next(rs).next(rs);
    }

    public static Gen.IntGen pickInt(int... ts)
    {
        return rs -> ts[rs.nextInt(0, ts.length)];
    }

    public static <T> Gen<T> pick(T... ts)
    {
        return pick(Arrays.asList(ts));
    }

    public static <T> Gen<T> pick(List<T> ts)
    {
        Gen.IntGen offset = ints().between(0, ts.size() - 1);
        return rs -> ts.get(offset.nextInt(rs));
    }

    public static <T extends Comparable<T>> Gen<T> pick(Set<T> set)
    {
        List<T> list = new ArrayList<>(set);
        // Non-ordered sets may have different iteration order on different environments, which would make a seed produce different histories!
        // To avoid such a problem, make sure to apply a deterministic function (sort).
        if (!(set instanceof NavigableSet))
            list.sort(Comparator.naturalOrder());
        return pick(list);
    }

    public static <T> Gen<T> pick(Map<T, Integer> values)
    {
        if (values == null || values.isEmpty())
            throw new IllegalArgumentException("values is empty");
        // if 2 values have the same weight we need some way to tie-break, but that isn't always possible...
        // this method relies on the map having some order and will reject any map that doesn't define a deterministic order
        if (!(values instanceof EnumMap || values instanceof LinkedHashMap))
            throw new IllegalArgumentException("pick(Map) requires a map with deterministic iteration; given " + values.getClass());
        if (values.size() == 1)
            return constant(Objects.requireNonNull(Iterables.getFirst(values.keySet(), null)));
        double totalWeight = values.values().stream().mapToDouble(Integer::intValue).sum();
        List<Weight<T>> list = new ArrayList<>(values.size());
        Iterator<Map.Entry<T, Integer>> it = values.entrySet().iterator();
        for (int i = 0; it.hasNext(); i++)
        {
            Map.Entry<T, Integer> e = it.next();
            list.add(new Weight<>(e.getKey(), e.getValue(), i));
        }
        Collections.sort(list);
        return rs -> {
            double value = rs.nextDouble() * totalWeight;
            for (Weight<T> w : list)
            {
                value -= w.weight;
                if (value <= 0)
                    return w.value;
            }
            return list.get(list.size() - 1).value;
        };
    }

    public static Gen.IntGen pickZipf(int[] array)
    {
        if (array == null || array.length == 0)
            throw new IllegalArgumentException("Empty array given");
        if (array.length == 1)
            return ignore -> array[0];
        BigDecimal[] weights = new BigDecimal[array.length];
        BigDecimal base = BigDecimal.valueOf(Math.pow(2, array.length));
        weights[0] = base;
        for (int i = 1; i < array.length; i++)
            weights[i] = base.divide(BigDecimal.valueOf(i + 1), RoundingMode.UP);
        BigDecimal totalWeights = Stream.of(weights).reduce(BigDecimal.ZERO, BigDecimal::add);

        return rs -> {
            BigDecimal value = BigDecimal.valueOf(rs.nextDouble()).multiply(totalWeights);
            for (int i = 0; i < weights.length; i++)
            {
                value = value.subtract(weights[i]);
                if (value.compareTo(BigDecimal.ZERO) <= 0)
                    return array[i];
            }
            return array[array.length - 1];
        };
    }

    public static Gen.LongGen pickZipf(long[] array)
    {
        if (array == null || array.length == 0)
            throw new IllegalArgumentException("Empty array given");
        if (array.length == 1)
            return ignore -> array[0];
        BigDecimal[] weights = new BigDecimal[array.length];
        BigDecimal base = BigDecimal.valueOf(Math.pow(2, array.length));
        weights[0] = base;
        for (int i = 1; i < array.length; i++)
            weights[i] = base.divide(BigDecimal.valueOf(i + 1), RoundingMode.UP);
        BigDecimal totalWeights = Stream.of(weights).reduce(BigDecimal.ZERO, BigDecimal::add);

        return rs -> {
            BigDecimal value = BigDecimal.valueOf(rs.nextDouble()).multiply(totalWeights);
            for (int i = 0; i < weights.length; i++)
            {
                value = value.subtract(weights[i]);
                if (value.compareTo(BigDecimal.ZERO) <= 0)
                    return array[i];
            }
            return array[array.length - 1];
        };
    }

    public static <T> Gen<T> pickZipf(T... array)
    {
        return pickZipf(Arrays.asList(array));
    }

    public static <T> Gen<T> pickZipf(List<T> array)
    {
        if (array == null || array.isEmpty())
            throw new IllegalArgumentException("Empty array given");
        if (array.size() == 1)
            return ignore -> array.get(0);
        BigDecimal[] weights = new BigDecimal[array.size()];
        BigDecimal base = BigDecimal.valueOf(Math.pow(2, array.size()));
        weights[0] = base;
        for (int i = 1; i < array.size(); i++)
            weights[i] = base.divide(BigDecimal.valueOf(i + 1), RoundingMode.UP);
        BigDecimal totalWeights = Stream.of(weights).reduce(BigDecimal.ZERO, BigDecimal::add);

        return rs -> {
            BigDecimal value = BigDecimal.valueOf(rs.nextDouble()).multiply(totalWeights);
            for (int i = 0; i < weights.length; i++)
            {
                value = value.subtract(weights[i]);
                if (value.compareTo(BigDecimal.ZERO) <= 0)
                    return array.get(i);
            }
            return array.get(array.size() - 1);
        };
    }

    public static Gen<Gen.IntGen> mixedDistribution(int minInclusive, int maxExclusive)
    {
        int domainSize = (maxExclusive - minInclusive + 1);
        if (domainSize < 0)
            throw new IllegalArgumentException("Range is too large; min=" + minInclusive + ", max=" + maxExclusive);
        int[] array, indexes;
        if (domainSize > 200) // randomly selected
        {
            int numBuckets = 10;
            int delta = domainSize / numBuckets;
            array = new int[numBuckets];
            for (int i = 0; i < numBuckets; i++)
                array[i] = minInclusive + i * delta;
            indexes = IntStream.range(0, array.length).toArray();
        }
        else
        {
            array = IntStream.range(minInclusive, maxExclusive).toArray();
            indexes = null;
        }
        return rs -> {
            switch (rs.nextInt(0, 2))
            {
                case 0: // uniform
                    return r -> r.nextInt(minInclusive, maxExclusive);
                case 1: // zipf
                    if (indexes == null)
                        return Gens.pickZipf(rs.nextBoolean() ? reverseAndCopy(array) : array);
                    return Gens.pickZipf(rs.nextBoolean() ? reverseAndCopy(indexes) : indexes).mapAsInt((r, index) -> {
                        int start = array[index];
                        int end = index == array.length - 1 ? maxExclusive : array[index + 1];
                        return r.nextInt(start, end);
                    });
                default:
                    throw new AssertionError();
            }
        };
    }

    private static int[] reverseAndCopy(int[] array)
    {
        array = Arrays.copyOf(array, array.length);
        for (int i = 0, mid = array.length / 2, j = array.length - 1; i < mid; i++, j--)
        {
            int tmp = array[i];
            array[i] = array[j];
            array[j] = tmp;
        }
        return array;
    }

    public static Gen<Gen.LongGen> mixedDistribution(long minInclusive, long maxExclusive)
    {
        long domainSize = (maxExclusive - minInclusive + 1);
        if (domainSize < 0)
            throw new IllegalArgumentException("Range is too large; min=" + minInclusive + ", max=" + maxExclusive);
        long[] array;
        int[] indexes;
        if (domainSize > 200) // randomly selected
        {
            int numBuckets = 10;
            long delta = domainSize / numBuckets;
            array = new long[numBuckets];
            for (int i = 0; i < numBuckets; i++)
                array[i] = minInclusive + i * delta;
            indexes = IntStream.range(0, array.length).toArray();
        }
        else
        {
            array = LongStream.range(minInclusive, maxExclusive).toArray();
            indexes = null;
        }
        return rs -> {
            switch (rs.nextInt(0, 2))
            {
                case 0: // uniform
                    return r -> r.nextLong(minInclusive, maxExclusive);
                case 1: // zipf
                    if (indexes == null)
                        return Gens.pickZipf(rs.nextBoolean() ? reverseAndCopy(array) : array);
                    return Gens.pickZipf(rs.nextBoolean() ? reverseAndCopy(indexes) : indexes).mapAsLong((r, index) -> {
                        long start = array[index];
                        long end = index == array.length - 1 ? maxExclusive : array[index + 1];
                        return r.nextLong(start, end);
                    });
                default:
                    throw new AssertionError();
            }
        };
    }

    private static long[] reverseAndCopy(long[] array)
    {
        array = Arrays.copyOf(array, array.length);
        for (int i = 0, mid = array.length / 2, j = array.length - 1; i < mid; i++, j--)
        {
            long tmp = array[i];
            array[i] = array[j];
            array[j] = tmp;
        }
        return array;
    }

    public static <T> Gen<Gen<T>> mixedDistribution(T... list)
    {
        return mixedDistribution(Arrays.asList(list));
    }

    public static <T> Gen<Gen<T>> mixedDistribution(List<T> list)
    {
        return rs -> {
            switch (rs.nextInt(0, 2))
            {
                case 0: // uniform
                    return r -> list.get(rs.nextInt(0, list.size()));
                case 1: // zipf
                    List<T> array = list;
                    if (rs.nextBoolean())
                    {
                        array = new ArrayList<>(list);
                        Collections.reverse(array);
                    }
                    return pickZipf(array);
                default:
                    throw new AssertionError();
            }
        };
    }

    public static Gen<char[]> charArray(Gen.IntGen sizes, char[] domain)
    {
        return charArray(sizes, domain, (a, b) -> true);
    }

    public interface IntCharBiPredicate
    {
        boolean test(int a, char b);
    }

    public static Gen<char[]> charArray(Gen.IntGen sizes, char[] domain, IntCharBiPredicate fn)
    {
        Gen.IntGen indexGen = ints().between(0, domain.length - 1);
        return rs -> {
            int size = sizes.nextInt(rs);
            char[] is = new char[size];
            for (int i = 0; i != size; i++)
            {
                char c;
                do
                {
                    c = domain[indexGen.nextInt(rs)];
                }
                while (!fn.test(i, c));
                is[i] = c;
            }
            return is;
        };
    }

    public static Gen<RandomSource> random() {
        return r -> r;
    }

    public static BooleanDSL bools()
    {
        return new BooleanDSL();
    }

    public static IntDSL ints()
    {
        return new IntDSL();
    }

    public static LongDSL longs() {
        return new LongDSL();
    }

    public static <T> ListDSL<T> lists(Gen<T> fn) {
        return new ListDSL<>(fn);
    }

    public static <T> ArrayDSL<T> arrays(Class<T> type, Gen<T> fn) {
        return new ArrayDSL<>(type, fn);
    }

    public static IntArrayDSL arrays(Gen.IntGen fn) {
        return new IntArrayDSL(fn);
    }

    public static LongArrayDSL arrays(Gen.LongGen fn) {
        return new LongArrayDSL(fn);
    }

    public static EnumDSL enums()
    {
        return new EnumDSL();
    }

    public static StringDSL strings()
    {
        return new StringDSL();
    }

    public static BooleanSupplier supplier(Gen<Boolean> gen, RandomSource rs)
    {
        return () -> gen.next(rs);
    }

    public static class BooleanDSL
    {
        public Gen<Boolean> all()
        {
            return RandomSource::nextBoolean;
        }

        public Gen<Boolean> biasedRepeatingRuns(double ratio, int maxRuns)
        {
            Invariants.checkArgument(ratio > 0 && ratio <= 1, "Expected %d to be larger than 0 and <= 1", ratio);
            double lower = ratio * .8;
            double upper = ratio * 1.2;
            return new Gen<Boolean>() {
                // run represents how many consecutaive true values should be returned; -1 implies no active "run" exists
                private int run = -1;
                private long falseCount = 0, trueCount = 0;
                @Override
                public Boolean next(RandomSource rs)
                {
                    if (run != -1)
                    {
                        run--;
                        trueCount++;
                        return true;
                    }
                    double currentRatio = trueCount / (double) (falseCount + trueCount);
                    if (currentRatio < lower)
                    {
                        // not enough true
                        trueCount++;
                        return true;
                    }
                    if (currentRatio > upper)
                    {
                        // not enough false
                        falseCount++;
                        return false;
                    }
                    if (rs.decide(ratio))
                    {
                        run = rs.nextInt(maxRuns);
                        run--;
                        trueCount++;
                        return true;
                    }
                    falseCount++;
                    return false;
                }
            };
        }

        public Gen<Gen<Boolean>> mixedDistribution()
        {
            return rs -> {
                int selection = rs.nextInt(0, 4);
                switch (selection)
                {
                    case 0: // uniform 50/50
                        return r -> r.nextBoolean();
                    case 1: // variable frequency
                        var freq = rs.nextFloat();
                        return r -> r.decide(freq);
                    case 2: // fixed result
                        boolean result = rs.nextBoolean();
                        return ignore -> result;
                    case 3: // biased repeating runs
                        return biasedRepeatingRuns(rs.nextDouble(), rs.nextInt(1, 100));
                    default:
                        throw new IllegalStateException("Unexpected int for bool selection: " + selection);
                }
            };
        }
    }

    public static class IntDSL
    {
        public Gen.IntGen of(int value)
        {
            return r -> value;
        }

        public Gen.IntGen all()
        {
            return RandomSource::nextInt;
        }

        public Gen.IntGen between(int min, int max)
        {
            Invariants.checkArgument(max >= min, "max (%d) < min (%d)", max, min);
            if (min == max)
                return of(min);
            // since bounds is exclusive, if max == max_value unable to do +1 to include... so will return a gen
            // that does not include
            if (max == Integer.MAX_VALUE)
                return r -> r.nextInt(min, max);
            return r -> r.nextInt(min, max + 1);
        }

        public Gen<Gen.IntGen> mixedDistribution(int minInclusive, int maxExclusive)
        {
            return Gens.mixedDistribution(minInclusive, maxExclusive);
        }
    }

    public static class LongDSL {
        public Gen.LongGen of(long value)
        {
            return r -> value;
        }

        public Gen.LongGen all() {
            return RandomSource::nextLong;
        }

        public Gen.LongGen between(long min, long max) {
            Invariants.checkArgument(max >= min);
            if (min == max)
                return of(min);
            // since bounds is exclusive, if max == max_value unable to do +1 to include... so will return a gen
            // that does not include
            if (max == Long.MAX_VALUE)
                return r -> r.nextLong(min, max);
            return r -> r.nextLong(min, max + 1);
        }
    }

    public static class EnumDSL
    {
        public <T extends Enum<T>> Gen<T> all(Class<T> klass)
        {
            return pick(klass.getEnumConstants());
        }

        public <T extends Enum<T>> Gen<Gen<T>> allMixedDistribution(Class<T> klass)
        {
            return mixedDistribution(klass.getEnumConstants());
        }

        public <T extends Enum<T>> Gen<T> allWithWeights(Class<T> klass, int... weights)
        {
            T[] constants = klass.getEnumConstants();
            if (constants.length != weights.length)
                throw new IllegalArgumentException(String.format("Total number of weights (%s) does not match the enum (%s)", Arrays.toString(weights), Arrays.toString(constants)));
            Map<T, Integer> values = new EnumMap<>(klass);
            for (int i = 0; i < constants.length; i++)
                values.put(constants[i], weights[i]);
            return pick(values);
        }
    }

    public static class StringDSL
    {
        public Gen<String> of(Gen.IntGen sizes, char[] domain)
        {
            // note, map is overloaded so String::new is ambugious to javac, so need a lambda here
            return charArray(sizes, domain).map(c -> new String(c));
        }

        public SizeBuilder<String> of(char[] domain)
        {
            return new SizeBuilder<>(sizes -> of(sizes, domain));
        }

        public Gen<String> of(Gen.IntGen sizes, char[] domain, IntCharBiPredicate fn)
        {
            // note, map is overloaded so String::new is ambugious to javac, so need a lambda here
            return charArray(sizes, domain, fn).map(c -> new String(c));
        }

        public SizeBuilder<String> of(char[] domain, IntCharBiPredicate fn)
        {
            return new SizeBuilder<>(sizes -> of(sizes, domain, fn));
        }

        public Gen<String> all(Gen.IntGen sizes)
        {
            return betweenCodePoints(sizes, Character.MIN_CODE_POINT, Character.MAX_CODE_POINT);
        }

        public SizeBuilder<String> all()
        {
            return new SizeBuilder<>(this::all);
        }

        public Gen<String> ascii(Gen.IntGen sizes)
        {
            return betweenCodePoints(sizes, 0, 127);
        }

        public SizeBuilder<String> ascii()
        {
            return new SizeBuilder<>(this::ascii);
        }

        public Gen<String> betweenCodePoints(Gen.IntGen sizes, int min, int max)
        {
            Gen.IntGen codePointGen = ints().between(min, max).filter(Character::isDefined);
            return rs -> {
                int[] array = new int[sizes.nextInt(rs)];
                for (int i = 0; i < array.length; i++)
                    array[i] = codePointGen.nextInt(rs);
                return new String(array, 0, array.length);
            };
        }

        public SizeBuilder<String> betweenCodePoints(int min, int max)
        {
            return new SizeBuilder<>(sizes -> betweenCodePoints(sizes, min, max));
        }
    }

    public static class SizeBuilder<T>
    {
        private final Function<Gen.IntGen, Gen<T>> fn;

        public SizeBuilder(Function<Gen.IntGen, Gen<T>> fn)
        {
            this.fn = fn;
        }

        public Gen<T> ofLength(int fixed)
        {
            return ofLengthBetween(fixed, fixed);
        }

        public Gen<T> ofLengthBetween(int min, int max)
        {
            return fn.apply(ints().between(min, max));
        }
    }

    public static class ListDSL<T> implements BaseSequenceDSL<ListDSL<T>, List<T>> {
        private final Gen<T> fn;

        public ListDSL(Gen<T> fn) {
            this.fn = Objects.requireNonNull(fn);
        }

        @Override
        public ListDSL<T> unique()
        {
            return new ListDSL<>(new GenReset<>(fn, false));
        }

        public ListDSL<T> uniqueBestEffort()
        {
            return new ListDSL<>(new GenReset<>(fn, true));
        }

        @Override
        public Gen<List<T>> ofSizeBetween(int minSize, int maxSize) {
            Gen.IntGen sizeGen = ints().between(minSize, maxSize);
            return r ->
            {
                Reset.tryReset(fn);
                int size = sizeGen.nextInt(r);
                List<T> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++)
                {
                    try
                    {
                        list.add(fn.next(r));
                    }
                    catch (IgnoreGenResult e)
                    {
                        // ignore
                    }
                }
                return list;
            };
        }
    }

    public static class ArrayDSL<T> implements BaseSequenceDSL<ArrayDSL<T>, T[]> {
        private final Class<T> type;
        private final Gen<T> fn;

        public ArrayDSL(Class<T> type, Gen<T> fn) {
            this.type = Objects.requireNonNull(type);
            this.fn = Objects.requireNonNull(fn);
        }

        @Override
        public ArrayDSL<T> unique()
        {
            return new ArrayDSL<>(type, new GenReset<>(fn, false));
        }

        public ArrayDSL<T> uniqueBestEffort()
        {
            return new ArrayDSL<>(type, new GenReset<>(fn, true));
        }

        @Override
        public Gen<T[]> ofSizeBetween(int minSize, int maxSize) {
            Gen.IntGen sizeGen = ints().between(minSize, maxSize);
            return r ->
            {
                Reset.tryReset(fn);
                int size = sizeGen.nextInt(r);
                T[] list = (T[]) Array.newInstance(type, size);
                for (int i = 0; i < size; i++)
                    list[i] = fn.next(r);
                return list;
            };
        }
    }

    public static class IntArrayDSL implements BaseSequenceDSL<IntArrayDSL, int[]> {
        private final Gen.IntGen fn;

        public IntArrayDSL(Gen.IntGen fn) {
            this.fn = Objects.requireNonNull(fn);
        }

        @Override
        public IntArrayDSL unique()
        {
            return new IntArrayDSL(new IntGenReset(fn));
        }

        @Override
        public Gen<int[]> ofSizeBetween(int minSize, int maxSize) {
            Gen.IntGen sizeGen = ints().between(minSize, maxSize);
            return r ->
            {
                int size = sizeGen.nextInt(r);
                int[] list = new int[size];
                for (int i = 0; i < size; i++)
                    list[i] = fn.nextInt(r);
                return list;
            };
        }
    }

    public static class LongArrayDSL implements BaseSequenceDSL<LongArrayDSL, long[]> {
        private final Gen.LongGen fn;

        public LongArrayDSL(Gen.LongGen fn) {
            this.fn = Objects.requireNonNull(fn);
        }

        @Override
        public LongArrayDSL unique()
        {
            return new LongArrayDSL(new LongGenReset(fn));
        }

        @Override
        public Gen<long[]> ofSizeBetween(int minSize, int maxSize) {
            Gen.IntGen sizeGen = ints().between(minSize, maxSize);
            return r ->
            {
                int size = sizeGen.nextInt(r);
                long[] list = new long[size];
                for (int i = 0; i < size; i++)
                    list[i] = fn.nextLong(r);
                return list;
            };
        }
    }

    public interface BaseSequenceDSL<A extends BaseSequenceDSL<A, B>, B>
    {
        A unique();

        Gen<B> ofSizeBetween(int min, int max);

        default Gen<B> ofSize(int size) {
            return ofSizeBetween(size, size);
        }
    }

    protected interface Reset {
        static void tryReset(Object o)
        {
            if (o instanceof Reset)
                ((Reset) o).reset();
        }

        void reset();
    }

    private static final class IgnoreGenResult extends RuntimeException
    {
        private static final IgnoreGenResult INSTANCE = new IgnoreGenResult();
        private IgnoreGenResult()
        {
            super(null, null, false, false);
        }
    }

    private static class GenReset<T> implements Gen<T>, Reset
    {
        private final Set<T> seen = new HashSet<>();
        private final Gen<T> fn;
        private final boolean bestEffort;

        private GenReset(Gen<T> fn, boolean bestEffort)
        {
            this.fn = fn;
            this.bestEffort = bestEffort;
        }

        @Override
        public T next(RandomSource random)
        {
            if (!bestEffort)
            {
                T value;
                while (!seen.add((value = fn.next(random)))) {}
                return value;
            }
            else
            {
                T value = null;
                int i;
                for (i = 0; i < 42 && !seen.add((value = fn.next(random))); i++) {}
                if (i == 42) throw IgnoreGenResult.INSTANCE;
                return value;
            }
        }

        @Override
        public void reset()
        {
            seen.clear();
        }
    }

    private static class IntGenReset implements Gen.IntGen, Reset
    {
        private final GenReset<Integer> base;

        private IntGenReset(Gen.IntGen fn)
        {
            this.base = new GenReset<>(fn, false);
        }
        @Override
        public int nextInt(RandomSource random) {
            return base.next(random);
        }

        @Override
        public void reset() {
            base.reset();
        }
    }

    private static class LongGenReset implements Gen.LongGen, Reset
    {
        private final GenReset<Long> base;

        private LongGenReset(Gen.LongGen fn)
        {
            this.base = new GenReset<>(fn, false);
        }
        @Override
        public long nextLong(RandomSource random) {
            return base.next(random);
        }

        @Override
        public void reset() {
            base.reset();
        }
    }

    private static class Weight<T> implements Comparable<Weight<T>>
    {
        private final T value;
        private final double weight;
        private final int index;

        private Weight(T value, double weight, int index) {
            this.value = value;
            this.weight = weight;
            this.index = index;
        }

        @Override
        public int compareTo(Weight<T> o) {
            int rc = Double.compare(weight, o.weight);
            if (rc == 0)
                rc = Integer.compare(index, o.index);
            return rc;
        }
    }
}

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

package org.apache.cassandra.simulator;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION)
public interface RandomSource
{
    public static class Choices<T>
    {
        final float[] cumulativeProbabilities;
        public final T[] options;

        private Choices(float[] cumulativeProbabilities, T[] options)
        {
            this.cumulativeProbabilities = cumulativeProbabilities;
            this.options = options;
        }

        public T choose(RandomSource random)
        {
            if (options.length == 0)
                return null;

            float choose = random.uniformFloat();
            int i = Arrays.binarySearch(cumulativeProbabilities, choose);

            if (i < 0) i = -1 - i;
            return options[i];
        }

        public Choices<T> without(T option)
        {
            for (int i = 0 ; i < options.length ; ++i)
            {
                if (option.equals(options[i]))
                {
                    float[] prob = new float[cumulativeProbabilities.length - 1];
                    T[] opts = (T[]) Array.newInstance(options.getClass().getComponentType(), options.length - 1);
                    System.arraycopy(cumulativeProbabilities, 0, prob, 0, i);
                    System.arraycopy(cumulativeProbabilities, i + 1, prob, i, this.options.length - (i + 1));
                    System.arraycopy(options, 0, opts, 0, i);
                    System.arraycopy(options, i + 1, opts, i, options.length - (i + 1));
                    for (int j = prob.length - 1 ; j > 1 ; --j)
                        prob[j] -= prob[j - 1];
                    return build(prob, opts);
                }
            }
            return this;
        }

        private static float[] randomCumulativeProbabilities(RandomSource random, int count)
        {
            float[] nonCumulativeProbabilities = new float[count];
            for (int i = 0 ; i < count ; ++i)
                nonCumulativeProbabilities[i] = random.uniformFloat();
            return cumulativeProbabilities(nonCumulativeProbabilities);
        }

        private static float[] cumulativeProbabilities(float[] nonCumulativeProbabilities)
        {
            int count = nonCumulativeProbabilities.length;
            if (count == 0)
                return new float[0];

            float[] result = new float[nonCumulativeProbabilities.length];
            float sum = 0;
            for (int i = 0 ; i < count ; ++i)
                result[i] = sum += nonCumulativeProbabilities[i];
            result[result.length - 1] = 1.0f;
            for (int i = 0 ; i < count - 1 ; ++i)
                result[i] = result[i] /= sum;
            return result;
        }

        public static <T> Choices<T> random(RandomSource random, T[] options)
        {
            return new Choices<>(randomCumulativeProbabilities(random, options.length), options);
        }

        public static <T> Choices<T> random(RandomSource random, T[] options, Map<T, float[]> bounds)
        {
            float[] nonCumulativeProbabilities = new float[options.length];
            for (int i = 0 ; i < options.length ; ++i)
            {
                float[] minmax = bounds.get(options[i]);
                float uniform = random.uniformFloat();
                nonCumulativeProbabilities[i] = minmax == null ? uniform : minmax[0] + (uniform * (minmax[1] - minmax[0]));
            }
            return new Choices<>(cumulativeProbabilities(nonCumulativeProbabilities), options);
        }

        public static <T> Choices<T> build(float[] nonCumulativeProbabilities, T[] options)
        {
            if (nonCumulativeProbabilities.length != options.length)
                throw new IllegalArgumentException();
            return new Choices<>(cumulativeProbabilities(nonCumulativeProbabilities), options);
        }

        public static <T> Choices<T> uniform(T ... options)
        {
            float[] nonCumulativeProbabilities = new float[options.length];
            Arrays.fill(nonCumulativeProbabilities, 1f / options.length);
            return new Choices<>(cumulativeProbabilities(nonCumulativeProbabilities), options);
        }
    }

    public static abstract class Abstract implements RandomSource
    {
        public abstract float uniformFloat();
        public abstract int uniform(int min, int max);
        public abstract long uniform(long min, long max);

        public LongSupplier uniqueUniformSupplier(long min, long max)
        {
            return uniqueUniformStream(min, max).iterator()::nextLong;
        }

        public LongStream uniqueUniformStream(long min, long max)
        {
            return uniformStream(min, max).distinct();
        }

        public LongStream uniformStream(long min, long max)
        {
            return LongStream.generate(() -> uniform(min, max));
        }

        public LongSupplier uniformSupplier(long min, long max)
        {
            return () -> uniform(min, max);
        }

        public IntSupplier uniqueUniformSupplier(int min, int max)
        {
            return uniqueUniformStream(min, max).iterator()::nextInt;
        }

        public IntStream uniqueUniformStream(int min, int max)
        {
            return uniformStream(min, max).distinct();
        }

        public IntStream uniformStream(int min, int max)
        {
            return IntStream.generate(() -> uniform(min, max));
        }

        public boolean decide(float chance)
        {
            return uniformFloat() < chance;
        }

        public int log2uniform(int min, int max)
        {
            return (int) log2uniform((long) min, max);
        }

        public long log2uniform(long min, long max)
        {
            return qlog2uniform(min, max, 64);
        }

        public long qlog2uniform(long min, long max, int quantizations)
        {
            return min + log2uniform(max - min, quantizations);
        }

        private long log2uniform(long max, int quantizations)
        {
            int maxBits = 64 - Long.numberOfLeadingZeros(max - 1);
            if (maxBits == 0)
                return 0;

            long min;
            if (maxBits <= quantizations)
            {
                int bits = uniform(0, maxBits);
                min = 1L << (bits - 1);
                max = Math.min(max, min * 2);
            }
            else
            {
                int bitsPerRange = (maxBits / quantizations);
                int i = uniform(0, quantizations);
                min = 1L << (i * bitsPerRange);
                max = Math.min(max, 1L << ((i + 1) * bitsPerRange));
            }

            return uniform(min, max);
        }

        public float qlog2uniformFloat(int quantizations)
        {
            return qlog2uniform(0, 1 << 24, quantizations) / (float)(1 << 24);
        }
    }

    public static class Default extends Abstract
    {
        private final Random random = new Random(0);

        public float uniformFloat() { return random.nextFloat(); }

        @Override
        public double uniformDouble()
        {
            return random.nextDouble();
        }

        public int uniform(int min, int max)
        {
            int delta = max - min;
            if (delta > 1) return min + random.nextInt(max - min);
            if (delta == 1) return min;
            if (min >= max)
                throw new IllegalArgumentException(String.format("Min (%s) should be less than max (%d).", min, max));
            return (int)uniform(min, (long)max);
        }

        public long uniform(long min, long max)
        {
            if (min >= max) throw new IllegalArgumentException();

            long delta = max - min;
            if (delta == 1) return min;
            if (delta == Long.MIN_VALUE && max == Long.MAX_VALUE) return random.nextLong();
            if (delta < 0) return random.longs(min, max).iterator().nextLong();
            if (delta <= Integer.MAX_VALUE) return min + uniform(0, (int) delta);

            long result = min + 1 == max ? min : min + ((random.nextLong() & 0x7fffffff) % (max - min));
            assert result >= min && result < max;
            return result;
        }

        public void reset(long seed)
        {
            random.setSeed(seed);
        }

        public long reset()
        {
            long seed = random.nextLong();
            reset(seed);
            return seed;
        }
    }

    IntStream uniqueUniformStream(int min, int max);

    LongSupplier uniqueUniformSupplier(long min, long max);
    LongStream uniqueUniformStream(long min, long max);
    LongStream uniformStream(long min, long max);

    // [min...max)
    int uniform(int min, int max);
    // [min...max)
    long uniform(long min, long max);

    /**
     * Select a number in the range [min, max), with a power of two in the range [0, max-min)
     * selected uniformly and a uniform value less than this power of two added to it
     */
    int log2uniform(int min, int max);
    long log2uniform(long min, long max);

    /**
     * Select a number in the range [min, max), with the range being split into
     * {@code quantizations} adjacent powers of two, a range being select from these
     * with uniform probability, and the value within that range being selected uniformly
     */
    long qlog2uniform(long min, long max, int quantizations);

    float uniformFloat();

    /**
     * Select a number in the range [0, 1), with the range being split into
     * {@code quantizations} adjacent powers of two; a range being select from these
     * with uniform probability, and the value within that range being selected uniformly
     *
     * This is used to distribute behavioural toggles more extremely between different runs of the simulator.
     */
    float qlog2uniformFloat(int quantizations);
    double uniformDouble();

    // options should be cumulative probability in range [0..1]
    boolean decide(float chance);

    void reset(long seed);
    long reset();
}


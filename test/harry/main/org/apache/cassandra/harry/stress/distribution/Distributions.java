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

package org.apache.cassandra.harry.stress.distribution;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.distribution.WeibullDistribution;
import org.apache.commons.math3.random.RandomGenerator;

import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.rng.SeedableEntropySource;

public class Distributions
{
    public static Distribution fixed(long v)
    {
        return new Fixed(v);
    }

    public static Distribution exp(EntropySource rng, long min, long max, double mean)
    {
        return new Exp(rng, min, max, mean);
    }

    public static Distribution extreme(EntropySource rng, long min, long max, double shape, double scale)
    {
        return new Extreme(rng, min, max, shape, scale);
    }

    public static Distribution offset(Distribution distribution, long offset)
    {
        return new OffsetDistribution(distribution, offset);
    }

    public static Distribution quantizedExtreme(EntropySource rng, long min, long max, double shape, double scale, int quantas)
    {
        return new Quantized(new Extreme(rng, min, max, shape, scale), quantas);
    }

    public static Distribution gaussian(EntropySource rng, long min, long max, double mean, double stdev)
    {
        return new Gaussian(rng, min, max, mean, stdev);
    }

    public static Distribution binomial(EntropySource rng, int n, double p)
    {
        return new Binomial(n, p);
    }

    public static Distribution zipf(EntropySource rng, int total, double skew)
    {
        return new Zipf(total, skew);
    }

    public static Distribution biased(EntropySource rng, long min, long max, double bias)
    {
        return new Biased(min, max, bias);
    }

    public static Distribution uniform(EntropySource rng, long min, long max)
    {
        return new Uniform(rng, min, max);
    }

    public static Distribution uniformRandom(long min, long max)
    {
        return new UniformRandom(min, max);
    }

    public static Distribution sequence(long start, long end)
    {
        return new Sequence(start, end);
    }

    public static Distribution invert(Distribution distribution)
    {
        if (distribution instanceof Inverted)
            return ((Inverted) distribution).wrapped;
        return new Inverted(distribution);
    }

    public static class Fixed implements Distribution
    {
        private final long value;

        public Fixed(long value)
        {
            this.value = value;
        }

        @Override
        public long next(long seed)
        {
            return value;
        }

        @Override
        public double nextDouble(long seed)
        {
            return (double) value;
        }

        @Override
        public long min()
        {
            return value;
        }

        @Override
        public long max()
        {
            return value;
        }
    }

    public static class Exp extends ApacheAdaptor
    {
        private final long min;
        private final long max;

        public Exp(EntropySource rng, long min, long max, double mean)
        {
            super(new ExponentialDistribution(new EntropySourceAdapter(rng), mean, ExponentialDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY), min, max);
            this.min = min;
            this.max = max;
        }

        @Override
        public long min()
        {
            return min;
        }

        @Override
        public long max()
        {
            return max;
        }
    }

    public static class Extreme extends ApacheAdaptor
    {
        private final long min;
        private final long max;

        public Extreme(EntropySource rng, long min, long max, double shape, double scale)
        {
            super(new WeibullDistribution(new EntropySourceAdapter(rng), shape, scale, WeibullDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY), min, max);
            this.min = min;
            this.max = max;
        }

        @Override
        public long min()
        {
            return min;
        }

        @Override
        public long max()
        {
            return max;
        }
    }

    public static class OffsetDistribution implements Distribution
    {
        private final Distribution base;
        private final long offset;

        public OffsetDistribution(Distribution base, long offset)
        {
            this.base = base;
            this.offset = offset;
        }

        @Override
        public long next(long seed)
        {
            return base.next(seed) + offset;
        }

        @Override
        public double nextDouble(long seed)
        {
            return base.nextDouble(seed) + offset;
        }

        @Override
        public long min()
        {
            return base.min() + offset;
        }

        @Override
        public long max()
        {
            return base.max() + offset;
        }
    }

    public static class Quantized implements Distribution
    {
        final Distribution delegate;
        final long[] bounds;

        public Quantized(ApacheAdaptor delegate, int quantas)
        {
            this.delegate = delegate;
            this.bounds = new long[quantas + 1];
            bounds[0] = delegate.min;
            bounds[quantas] = delegate.max + 1;
            for (int i = 1; i < quantas; i++)
                bounds[i] = delegate.inverseCumProb(i / (double) quantas);
        }

        @Override
        public long next(long seed)
        {
            return SeedableEntropySource.computeWithSeed(seed, (rng) -> {
                int quanta = quanta(delegate.next(seed));
                return bounds[quanta] + (long) (rng.nextDouble() * ((bounds[quanta + 1] - bounds[quanta])));
            });
        }

        @Override
        public double nextDouble(long seed)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long min()
        {
            return delegate.min();
        }

        @Override
        public long max()
        {
            return delegate.max();
        }

        int quanta(long val)
        {
            int i = Arrays.binarySearch(bounds, val);
            if (i < 0)
                return -2 - i;
            return i - 1;
        }
    }

    public static class Gaussian extends ApacheAdaptor
    {
        public Gaussian(EntropySource rng, long min, long max, double mean, double stdev)
        {
            super(new NormalDistribution(new EntropySourceAdapter(rng), mean, stdev, NormalDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY), min, max);
        }
    }

    // Models the number of successes in n independent trials
    public static class Binomial implements Distribution
    {
        private final int n;
        private final double p;
        private final long min;
        private final long max;

        public Binomial(int n, double p)
        {
            // number of trials
            this.n = n;
            // probability of success
            this.p = p;
            this.min = 0;
            // Max would be when all trials succeed
            this.max = n;
        }

        @Override
        public long next(long seed)
        {
            return SeedableEntropySource.computeWithSeed(seed, rng -> {
                long successes = 0;
                for (int i = 0; i < n; i++)
                {
                    if (rng.nextDouble() < p)
                    {
                        successes++;
                    }
                }
                return successes;
            });
        }

        @Override
        public double nextDouble(long seed)
        {
            return (double) next(seed) / n;
        }

        @Override
        public long min()
        {
            return min;
        }

        @Override
        public long max()
        {
            return max;
        }
    }

    public static class Zipf implements Distribution
    {
        private final int n;
        private final double[] probabilities;
        private final long min;
        private final long max;

        public Zipf(int total, double skew)
        {
            if (total <= 0) throw new IllegalArgumentException("n must be positive");
            if (skew <= 0) throw new IllegalArgumentException("s must be positive");

            this.n = total;
            this.min = 1;
            this.max = total;

            this.probabilities = new double[total];
            double sum = 0;
            for (int i = 0; i < total; i++)
            {
                probabilities[i] = 1.0 / Math.pow(i + 1, skew);
                sum += probabilities[i];
            }

            // normalize
            for (int i = 0; i < total; i++)
            {
                probabilities[i] /= sum;
                if (i > 0)
                    probabilities[i] += probabilities[i - 1];
            }
        }

        @Override
        public long next(long seed)
        {
            return SeedableEntropySource.computeWithSeed(seed, rng -> {
                double rand = rng.nextDouble();
                long index = Arrays.binarySearch(probabilities, rand);
                if (index < 0)
                    index = -(index + 1);
                return index + 1;
            });
        }


        @Override
        public double nextDouble(long seed)
        {
            return (double) next(seed) / n;
        }

        @Override
        public long min()
        {
            return min;
        }

        @Override
        public long max()
        {
            return max;
        }
    }

    public static class Biased implements Distribution
    {
        private final long min;
        private final long max;
        private final double bias;

        public Biased(long min, long max, double bias)
        {
            this.min = min;
            this.max = max;
            this.bias = bias;
        }

        @Override
        public long next(long seed)
        {
            return SeedableEntropySource.computeWithSeed(seed, rng -> {
                // TODO (desired): try and compare with a variant without exponentiation
                // f(x)=ax^p+b
                double v = rng.nextDouble();
                v = Math.pow(v, bias);
                return min + (long) (v * (max - min));
            });
        }

        @Override
        public double nextDouble(long seed)
        {
            return SeedableEntropySource.computeWithSeed(seed, rng -> {
                double raw = rng.nextDouble();
                return Math.pow(raw, bias);
            });
        }

        @Override
        public long min()
        {
            return min;
        }

        @Override
        public long max()
        {
            return max;
        }
    }

    public static class Uniform extends ApacheAdaptor
    {
        public Uniform(EntropySource rng, long min, long max)
        {
            super(new UniformRealDistribution(new EntropySourceAdapter(rng), min, max + 1), min, max);
        }
    }

    /**
     * A lightweight uniform random distribution that derives values purely from the seed,
     * without requiring a stateful EntropySource or Apache Commons Math.
     */
    public static class UniformRandom implements Distribution
    {
        private final long min;
        private final long max;
        private final long range;

        public UniformRandom(long min, long max)
        {
            if (min > max)
                throw new IllegalArgumentException("min (" + min + ") must be <= max (" + max + ")");
            this.min = min;
            this.max = max;
            this.range = max - min + 1;
        }

        @Override
        public long next(long seed)
        {
            return SeedableEntropySource.computeWithSeed(seed, rng -> min + Math.abs(rng.next() % range));
        }

        @Override
        public double nextDouble(long seed)
        {
            return SeedableEntropySource.computeWithSeed(seed, rng -> min + rng.nextDouble() * (max - min));
        }

        @Override
        public long min()
        {
            return min;
        }

        @Override
        public long max()
        {
            return max;
        }
    }

    public static class CDF implements Distribution
    {
        private final float[] chances;
        private final long[] bounds;

        public CDF(float[] chances, long[] bounds)
        {
            this.chances = chances;
            this.bounds = bounds;
        }

        @Override
        public long next(long seed)
        {
            return SeedableEntropySource.computeWithSeed(seed, rng -> {
                float f = rng.nextFloat();
                int i = Arrays.binarySearch(chances, f);
                if (i < 0) i = -1 - i;
                if (i == 0)
                    return bounds[0];

                return rng.nextLong(bounds[i - 1], bounds[i] + 1);
            });
        }

        @Override
        public double nextDouble(long seed)
        {
            return next(seed);
        }

        @Override
        public long min()
        {
            return bounds[0];
        }

        @Override
        public long max()
        {
            return bounds[bounds.length - 1];
        }
    }

    public static abstract class ApacheAdaptor implements Distribution
    {
        final AbstractRealDistribution delegate;
        final long min, max, delta;

        public ApacheAdaptor(AbstractRealDistribution delegate, long min, long max)
        {
            this.delegate = delegate;
            this.min = min;
            this.max = max;
            this.delta = max - min;
        }

        public void setSeed(long seed)
        {
            delegate.reseedRandomGenerator(seed);
        }

        @Override
        public synchronized long next(long seed)
        {
            delegate.reseedRandomGenerator(seed);
            return offset(min, delta, delegate.sample());
        }

        @Override
        public synchronized double nextDouble(long seed)
        {
            delegate.reseedRandomGenerator(seed);
            return offsetDouble(min, delta, delegate.sample());
        }

        public long inverseCumProb(double cumProb)
        {
            return offset(min, delta, delegate.inverseCumulativeProbability(cumProb));
        }

        private long offset(long min, long delta, double val)
        {
            long r = (long) val;
            if (r < 0)
                r = 0;
            if (r > delta)
                r = delta;
            return min + r;
        }

        private double offsetDouble(long min, long delta, double r)
        {
            if (r < 0)
                r = 0;
            if (r > delta)
                r = delta;
            return min + r;
        }

        @Override
        public long min()
        {
            return min;
        }

        @Override
        public long max()
        {
            return max;
        }
    }

    public static class Sequence implements Distribution
    {
        private final long start;
        private final long totalCount;
        private final AtomicLong next = new AtomicLong();

        public Sequence(long start, long end)
        {
            if (start > end)
                throw new IllegalStateException();
            this.start = start;
            this.totalCount = 1 + end - start;
        }

        private long nextWithWrap()
        {
            long next = this.next.getAndIncrement();
            return start + (next % totalCount);
        }

        @Override
        public long next(long seed)
        {
            return nextWithWrap();
        }

        @Override
        public double nextDouble(long seed)
        {
            return nextWithWrap();
        }

        @Override
        public long min()
        {
            return start;
        }

        @Override
        public long max()
        {
            return start + totalCount;
        }
    }

    public static class Inverted implements Distribution
    {
        final Distribution wrapped;
        final long min;
        final long max;

        public Inverted(Distribution wrapped)
        {
            this.wrapped = wrapped;
            this.min = wrapped.min();
            this.max = wrapped.max();
        }

        @Override
        public long next(long seed)
        {
            return max - (wrapped.next(seed) - min);
        }

        @Override
        public double nextDouble(long seed)
        {
            return max - (wrapped.nextDouble(seed) - min);
        }

        @Override
        public long min()
        {
            return min;
        }

        @Override
        public long max()
        {
            return max;
        }
    }

    public static class EntropySourceAdapter implements RandomGenerator
    {
        private final EntropySource entropySource;

        public EntropySourceAdapter(EntropySource entropySource)
        {
            this.entropySource = entropySource;
        }

        @Override
        public void setSeed(int seed)
        {
            entropySource.seed(seed);
        }

        @Override
        public void setSeed(int[] seed)
        {
            // Convert the int array to a long seed
            long longSeed = 0;
            for (int s : seed)
            {
                longSeed = longSeed * 31 + s;
            }
            entropySource.seed(longSeed);
        }

        @Override
        public void setSeed(long seed)
        {
            entropySource.seed(seed);
        }

        @Override
        public void nextBytes(byte[] bytes)
        {
            for (int i = 0; i < bytes.length; i++)
            {
                bytes[i] = (byte) entropySource.nextInt(256);
            }
        }

        @Override
        public int nextInt()
        {
            return entropySource.nextInt();
        }

        @Override
        public int nextInt(int n)
        {
            return entropySource.nextInt(n);
        }

        @Override
        public long nextLong()
        {
            return entropySource.next();
        }

        @Override
        public boolean nextBoolean()
        {
            return entropySource.nextBoolean();
        }

        @Override
        public float nextFloat()
        {
            return entropySource.nextFloat();
        }

        @Override
        public double nextDouble()
        {
            return entropySource.nextDouble();
        }

        @Override
        public double nextGaussian()
        {
            double u = nextDouble();
            double v = nextDouble();
            return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
        }
    }
}
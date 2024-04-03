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

package org.quicktheories.impl;

import java.util.Objects;
import java.util.Random;

import org.quicktheories.core.DetatchedRandomnessSource;
import org.quicktheories.core.RandomnessSource;

/**
 * The {@link Constraint} class does not expose {@link Constraint#min()} or {@link Constraint#max()} outside the package, so
 * the only way to build a {@link RandomnessSource} is to define it in the package "org.quicktheories.impl"
 */
public class JavaRandom implements RandomnessSource, DetatchedRandomnessSource
{
    private final Random random;

    public JavaRandom(long seed)
    {
        this.random = new Random(seed);
    }

    public JavaRandom(Random random)
    {
        this.random = Objects.requireNonNull(random);
    }

    public void setSeed(long seed)
    {
        this.random.setSeed(seed);
    }

    public static JavaRandom wrap(RandomnessSource rnd)
    {
        if (rnd instanceof JavaRandom)
            return (JavaRandom) rnd;
        return new JavaRandom(rnd.next(Constraint.none().withNoShrinkPoint()));
    }

    @Override
    public long next(Constraint constraint)
    {
        long max = constraint.max();
        return nextLong(constraint.min(), max == Long.MAX_VALUE ? max : max + 1);
    }

    private long nextLong(long minInclusive, long maxExclusive)
    {
        // pulled from accord.utils.RandomSource#nextLong(long, long)...
        // long term it will be great to unify the two and drop QuickTheories classes all together
        // this is diff behavior than ThreadLocalRandom, which returns nextLong
        if (minInclusive >= maxExclusive)
            throw new IllegalArgumentException(String.format("Min (%s) should be less than max (%d).", minInclusive, maxExclusive));

        long result = random.nextLong();
        long delta = maxExclusive - minInclusive;
        long mask = delta - 1;
        if ((delta & mask) == 0L) // power of two
            result = (result & mask) + minInclusive;
        else if (delta > 0L)
        {
            // reject over-represented candidates
            for (long u = result >>> 1;                 // ensure nonnegative
                 u + mask - (result = u % delta) < 0L;  // rejection check
                 u = random.nextLong() >>> 1)                  // retry
                ;
            result += minInclusive;
        }
        else
        {
            // range not representable as long
            while (result < minInclusive || result >= maxExclusive)
                result = random.nextLong();
        }
        return result;
    }

    @Override
    public DetatchedRandomnessSource detach()
    {
        return this;
    }

    @Override
    public void registerFailedAssumption()
    {

    }

    @Override
    public void commit()
    {
        // no-op
    }
}

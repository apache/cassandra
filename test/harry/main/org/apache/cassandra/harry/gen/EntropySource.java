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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;

/**
 * Random generator interface that offers:
 * * Settable seed
 * * Ability to generate multiple "next" random seeds
 * * Ability to generate multiple "dependent" seeds, from which we can retrace the base seed with subtraction
 */
public interface EntropySource
{
    long next();
    void seed(long seed);

    // We derive from entropy source here to avoid letting the step change state for other states
    // For example, if you start drawing more entropy bits from one of the steps, but won't change
    // other steps, their states won't change either.
    EntropySource derive();

    int nextInt();
    int nextInt(int max);
    int nextInt(int min, int max);
    float nextFloat();

    /**
     * Code is adopted from a similar method in JDK 17, and has to be removed as soon as we migrate to JDK 17.
     */
    default long nextLong(long min, long max) {
        long ret = next();
        if (min < max) {
            final long n = max - min;
            final long m = n - 1;
            if ((n & m) == 0L) {
                ret = (ret & m) + min;
            } else if (n > 0L) {
                for (long u = ret >>> 1;
                     u + m - (ret = u % n) < 0L;
                     u = next() >>> 1)
                    ;
                ret += min;
            }
            else {
                while (ret < min || ret >= max)
                    ret = next();
            }
        }
        return ret;
    }

    default float nextFloat(float min, float max) {
        float r = nextFloat();
        if (min < max) {
            r = r * (max - min) + min;
            if (r >= max)
                r = Float.intBitsToFloat(Float.floatToIntBits(max) - 1);
        }
        return r;
    }

    boolean nextBoolean();

    @VisibleForTesting
    static EntropySource forTests()
    {
        return forTests(System.currentTimeMillis());
    }

    @VisibleForTesting
    static EntropySource forTests(long seed)
    {
        System.out.println("Seed: " + seed);
        return new JdkRandomEntropySource(seed);
    }
}


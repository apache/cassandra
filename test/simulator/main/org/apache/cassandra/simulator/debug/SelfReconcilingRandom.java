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

package org.apache.cassandra.simulator.debug;

import java.util.function.Supplier;

import org.agrona.collections.Long2LongHashMap;
import org.apache.cassandra.simulator.RandomSource;

import static org.apache.cassandra.simulator.SimulatorUtils.failWithOOM;

public class SelfReconcilingRandom implements Supplier<RandomSource>
{
    static class Map extends Long2LongHashMap
    {
        public Map(long missingValue)
        {
            super(missingValue);
        }

        public boolean put(int i, long v)
        {
            int size = this.size();
            super.put(i, v);
            return size != this.size();
        }
    }

    final Map map = new Map(Long.MIN_VALUE);
    boolean isNextPrimary = true;

    static abstract class AbstractVerifying extends RandomSource.Abstract
    {
        final RandomSource wrapped;
        int cur = 0;

        protected AbstractVerifying(RandomSource wrapped)
        {
            this.wrapped = wrapped;
        }

        abstract void next(long verify);

        public int uniform(int min, int max)
        {
            int v = wrapped.uniform(min, max);
            next(v);
            return v;
        }

        public long uniform(long min, long max)
        {
            long v = wrapped.uniform(min, max);
            next(v);
            return v;
        }

        public float uniformFloat()
        {
            float v = wrapped.uniformFloat();
            next(Float.floatToIntBits(v));
            return v;
        }

        public double uniformDouble()
        {
            double v = wrapped.uniformDouble();
            next(Double.doubleToLongBits(v));
            return v;
        }
    }

    public RandomSource get()
    {
        if (isNextPrimary)
        {
            isNextPrimary = false;
            return new AbstractVerifying(new RandomSource.Default())
            {
                void next(long verify)
                {
                    map.put(++cur, verify);
                }

                public void reset(long seed)
                {
                    map.clear();
                    cur = 0;
                    wrapped.reset(seed);
                }

                public long reset()
                {
                    map.clear();
                    cur = 0;
                    return wrapped.reset();
                }
            };
        }

        return new AbstractVerifying(new RandomSource.Default())
        {
            void next(long v)
            {
                long value = map.get(++cur);
                if (value == Long.MIN_VALUE)
                    throw failWithOOM();
                map.remove(cur);
                if (value != v)
                    throw failWithOOM();
            }

            public void reset(long seed)
            {
                cur = 0;
                wrapped.reset(seed);
            }

            public long reset()
            {
                cur = 0;
                return wrapped.reset();
            }
        };
    }
}
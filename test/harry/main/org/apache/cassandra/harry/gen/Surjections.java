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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.harry.gen.rng.PcgRSUFast;
import org.apache.cassandra.harry.gen.rng.RngUtils;

public class Surjections
{
    public static <T> Surjection<T> constant(Supplier<T> constant)
    {
        return (current) -> constant.get();
    }

    public static <T> Surjection<T> constant(T constant)
    {
        return (current) -> constant;
    }

    public static <T> Surjection<T> pick(List<T> ts)
    {
        return new Surjection<T>()
        {
            public T inflate(long current)
            {
                return ts.get(RngUtils.asInt(current, 0, ts.size() - 1));
            }

            @Override
            public String toString()
            {
                return String.format("Surjection#pick{from=%s}", ts);
            }
        };
    }

    public static long[] weights(int... weights)
    {
        long[] res = new long[weights.length];
        for (int i = 0; i < weights.length; i++)
        {
            long w = weights[i];
            res[i] = w << 32 | i;
        }
        return res;
    }

    public static <T> Surjection<T> weighted(int[] weights, T... items)
    {
        return weighted(weights(weights), items);
    }

    public static <T> Surjection<T> weighted(long[] weights, T... items)
    {
        assert weights.length == items.length;

        Arrays.sort(weights);
        TreeMap<Integer, T> weightMap = new TreeMap<Integer, T>();
        int prev = 0;
        for (int i = 0; i < weights.length; i++)
        {
            long orig = weights[i];
            int weight = (int) (orig >> 32);
            int idx = (int) orig;
            weightMap.put(prev, items[idx]);
            prev += weight;
        }

        return (i) -> {
            int weight = RngUtils.asInt(i, 0, 100);
            return weightMap.floorEntry(weight).getValue();
        };
    }

    public static <T> Surjection<T> weighted(Map<T, Integer> weights)
    {
        TreeMap<Integer, T> weightMap = new TreeMap<Integer, T>();
        int sum = 0;
        for (Map.Entry<T, Integer> entry : weights.entrySet())
        {
            sum += entry.getValue();
            weightMap.put(sum, entry.getKey());
        }

        int max = sum;
        return (i) -> {
            int weight = RngUtils.asInt(i, 0, max);
            return weightMap.ceilingEntry(weight).getValue();
        };
    }

    public static <T> Surjection<T> pick(T... ts)
    {
        return pick(Arrays.asList(ts));
    }

    public static <T extends Enum<T>> Surjection<T> enumValues(Class<T> e)
    {
        return enumValues(e, e.getEnumConstants());
    }

    public static <T extends Enum<T>> Surjection<T> enumValues(Class<T> klass, T... e)
    {
        return pick(Arrays.asList(e));
    }

    public interface Surjection<T>
    {
        T inflate(long descriptor);

        default <T1> Surjection<T1> map(Function<T, T1> map)
        {
            return (current) -> map.apply(inflate(current));
        }

        default LongFunction<T> toFn()
        {
            return new LongFunction<T>()
            {
                public T apply(long value)
                {
                    return inflate(value);
                }
            };
        }

        default Generator<T> toGenerator()
        {
            return new Generator<T>()
            {
                public T generate(EntropySource rng)
                {
                    return inflate(rng.next());
                }
            };
        }

        @VisibleForTesting
        default Supplier<T> toSupplier()
        {
            EntropySource rng = new PcgRSUFast(0, 0);
            return () -> inflate(rng.next());
        }
    }
}

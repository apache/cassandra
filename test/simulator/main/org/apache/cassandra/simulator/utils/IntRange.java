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

package org.apache.cassandra.simulator.utils;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.apache.cassandra.simulator.RandomSource;

public class IntRange
{
    public final int min;
    public final int max;

    public IntRange(int min, int max)
    {
        this.min = min;
        this.max = max;
    }

    public IntRange(long min, long max)
    {
        Preconditions.checkArgument(min < Integer.MAX_VALUE);
        Preconditions.checkArgument(max <= Integer.MAX_VALUE);
        this.min = (int)min;
        this.max = (int)max;
    }

    public IntRange(long min, long max, TimeUnit from, TimeUnit to)
    {
        this(to.convert(min,from), to.convert(max, from));
    }

    public int select(RandomSource random)
    {
        if (min == max) return min;
        return random.uniform(min, 1 + max);
    }

    public int select(RandomSource random, int minlb, int maxub)
    {
        int min = Math.max(this.min, minlb);
        int max = Math.min(this.max, maxub);
        if (min >= max) return min;
        return random.uniform(min, 1 + max);
    }

    public static Optional<IntRange> parseRange(Optional<String> chance)
    {
        return chance.map(s -> new IntRange(Integer.parseInt(s.replaceFirst("\\.\\.+[0-9]+", "")),
                                         Integer.parseInt(s.replaceFirst("[0-9]+\\.\\.+", ""))));
    }
}

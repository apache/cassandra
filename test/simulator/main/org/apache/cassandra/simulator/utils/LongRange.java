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

import org.apache.cassandra.simulator.RandomSource;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LongRange
{
    public final long min;
    public final long max;

    public LongRange(long min, long max)
    {
        this.min = min;
        this.max = max;
    }

    public LongRange(long min, long max, TimeUnit from, TimeUnit to)
    {
        this(to.convert(min,from), to.convert(max, from));
    }

    public long select(RandomSource random)
    {
        if (min == max) return min;
        return random.uniform(min, 1 + max);
    }

    public long select(RandomSource random, long minlb, long maxub)
    {
        long min = Math.max(this.min, minlb);
        long max = Math.min(this.max, maxub);
        if (min >= max) return min;
        return random.uniform(min, 1 + max);
    }

    public static Optional<LongRange> parseRange(Optional<String> chance)
    {
        return chance.map(s -> new LongRange(Integer.parseInt(s.replaceFirst("\\.\\.+[0-9]+", "")),
                                             Integer.parseInt(s.replaceFirst("[0-9]+\\.\\.+", ""))));
    }

    public static Optional<LongRange> parseNanosRange(Optional<String> chance)
    {
        if (!chance.isPresent())
            return Optional.empty();

        String parse = chance.get();
        TimeUnit units = parseUnits(parse);
        parse = stripUnits(parse, units);
        return Optional.of(new LongRange(Long.parseLong(parse.replaceFirst("\\.\\.+[0-9]+", "")),
                                         Long.parseLong(parse.replaceFirst("[0-9]+\\.\\.+", "")),
                                         units, NANOSECONDS));
    }

    public static TimeUnit parseUnits(String parse)
    {
        TimeUnit units;
        if (parse.endsWith("ms")) units = TimeUnit.MILLISECONDS;
        else if (parse.endsWith("us")) units = TimeUnit.MICROSECONDS;
        else if (parse.endsWith("ns")) units = NANOSECONDS;
        else if (parse.endsWith("s")) units = SECONDS;
        else throw new IllegalArgumentException("Unable to parse period range: " + parse);
        return units;
    }

    public static String stripUnits(String parse, TimeUnit units)
    {
        return parse.substring(0, parse.length() - (units == SECONDS ? 1 : 2));
    }

}

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
package org.apache.cassandra.config;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;

import org.apache.cassandra.exceptions.ConfigurationException;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Represents a positive time duration. Wrapper class for Cassandra duration configuration parameters, providing to the
 * users the opportunity to be able to provide config with a unit of their choice in cassandra.yaml as per the available
 * options. (CASSANDRA-15234)
 */
public class DurationSpec
{
    /**
     * Immutable map that matches supported time units according to a provided smallest supported time unit
     */
    private static final ImmutableMap<TimeUnit, ImmutableSet<TimeUnit>> MAP_UNITS_PER_MIN_UNIT =
    ImmutableMap.of(MILLISECONDS, ImmutableSet.of(MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS),
                    SECONDS, ImmutableSet.of(SECONDS, MINUTES, HOURS, DAYS),
                    MINUTES, ImmutableSet.of(MINUTES, HOURS, DAYS));
    /**
     * The Regexp used to parse the duration provided as String.
     */
    private static final Pattern TIME_UNITS_PATTERN = Pattern.compile(("^(\\d+)(d|h|s|ms|us|µs|ns|m)$"));

    private static final Pattern VALUES_PATTERN = Pattern.compile(("\\d+"));

    private final long quantity;

    private final TimeUnit unit;

    public DurationSpec(String value)
    {
        if (value == null || value.equals("null") || value.toLowerCase(Locale.ROOT).equals("nan"))
        {
            quantity = 0;
            unit = MILLISECONDS;
            return;
        }

        //parse the string field value
        Matcher matcher = TIME_UNITS_PATTERN.matcher(value);

        if(matcher.find())
        {
            quantity = Long.parseLong(matcher.group(1));
            unit = fromSymbol(matcher.group(2));
        }
        else
        {
            throw new ConfigurationException("Invalid duration: " + value + " Accepted units: d, h, m, s, ms, us, µs," +
                                             " ns where case matters and " + "only non-negative values");
        }
    }

    DurationSpec(long quantity, TimeUnit unit)
    {
        if (quantity < 0)
            throw new ConfigurationException("Invalid duration: value must be positive");

        this.quantity = quantity;
        this.unit = unit;
    }

    private DurationSpec(double quantity, TimeUnit unit)
    {
        this(Math.round(quantity), unit);
    }

    public DurationSpec(String value, TimeUnit minUnit)
    {
        if (value == null || value.equals("null") || value.toLowerCase(Locale.ROOT).equals("nan"))
        {
            quantity = 0;
            unit = minUnit;
            return;
        }

        if (!MAP_UNITS_PER_MIN_UNIT.containsKey(minUnit))
            throw new ConfigurationException("Invalid smallest unit set for " + value);

        Matcher matcher = TIME_UNITS_PATTERN.matcher(value);

        if(matcher.find())
        {
            quantity = Long.parseLong(matcher.group(1));
            unit = fromSymbol(matcher.group(2));

            if (!MAP_UNITS_PER_MIN_UNIT.get(minUnit).contains(unit))
                throw new ConfigurationException("Invalid duration: " + value + " Accepted units:" + MAP_UNITS_PER_MIN_UNIT.get(minUnit));
        }
        else
        {
            throw new ConfigurationException("Invalid duration: " + value + " Accepted units:" + MAP_UNITS_PER_MIN_UNIT.get(minUnit) +
                                             " where case matters and only non-negative values.");
        }
    }

    /**
     * Creates a {@code DurationSpec} of the specified amount of milliseconds.
     *
     * @param milliseconds the amount of milliseconds
     * @return a duration
     */
    public static DurationSpec inMilliseconds(long milliseconds)
    {
        return new DurationSpec(milliseconds, MILLISECONDS);
    }

    public static DurationSpec inDoubleMilliseconds(double milliseconds)
    {
        return new DurationSpec(milliseconds, MILLISECONDS);
    }

    /**
     * Creates a {@code DurationSpec} of the specified amount of seconds.
     *
     * @param seconds the amount of seconds
     * @return a duration
     */
    public static DurationSpec inSeconds(long seconds)
    {
        return new DurationSpec(seconds, SECONDS);
    }

    /**
     * Creates a {@code DurationSpec} of the specified amount of minutes.
     *
     * @param minutes the amount of minutes
     * @return a duration
     */
    public static DurationSpec inMinutes(long minutes)
    {
        return new DurationSpec(minutes, MINUTES);
    }

    /**
     * Creates a {@code DurationSpec} of the specified amount of hours.
     *
     * @param hours the amount of hours
     * @return a duration
     */
    public static DurationSpec inHours(long hours)
    {
        return new DurationSpec(hours, HOURS);
    }

    /**
     * Creates a {@code DurationSpec} of the specified amount of seconds. Custom method for special cases.
     *
     * @param value which can be in the old form only presenting the quantity or the post CASSANDRA-15234 form - a
     * value consisting of quantity and unit. This method is necessary for three parameters which didn't change their
     * names but only their value format. (key_cache_save_period, row_cache_save_period, counter_cache_save_period)
     * @return a duration
     */
    public static DurationSpec inSecondsString(String value)
    {
        //parse the string field value
        Matcher matcher = VALUES_PATTERN.matcher(value);

        long seconds;
        //if the provided string value is just a number, then we create a Duration Spec value in seconds
        if (matcher.matches())
        {
            seconds = Long.parseLong(value);
            return new DurationSpec(seconds, SECONDS);
        }

        //otherwise we just use the standard constructors
        return new DurationSpec(value);
    }

    /**
     * @param symbol the time unit symbol
     * @return the time unit associated to the specified symbol
     */
    static TimeUnit fromSymbol(String symbol)
    {
        switch (symbol.toLowerCase())
        {
            case "d": return DAYS;
            case "h": return HOURS;
            case "m": return MINUTES;
            case "s": return SECONDS;
            case "ms": return MILLISECONDS;
            case "us":
            case "µs": return TimeUnit.MICROSECONDS;
            case "ns": return TimeUnit.NANOSECONDS;
        }
        throw new ConfigurationException(String.format("Unsupported time unit: %s. Supported units are: %s",
                                                       symbol, Arrays.stream(TimeUnit.values())
                                                                     .map(DurationSpec::getSymbol)
                                                                     .collect(Collectors.joining(", "))));
    }

    /**
     * @param targetUnit the time unit
     * @return this duration in the specified time unit
     */
    public long to(TimeUnit targetUnit)
    {
        return targetUnit.convert(quantity, unit);
    }

    /**
     * @return this duration in number of hours
     */
    public long toHours()
    {
        return unit.toHours(quantity);
    }

    /**
     * Returns this duration in number of minutes as an {@code int}
     *
     * @return this duration in number of minutes or {@code Integer.MAX_VALUE} if the number of minutes is too large.
     */
    public int toHoursAsInt()
    {
        return Ints.saturatedCast(toHours());
    }

    /**
     * @return this duration in number of minutes
     */
    public long toMinutes()
    {
        return unit.toMinutes(quantity);
    }

    /**
     * Returns this duration in number of minutes as an {@code int}
     *
     * @return this duration in number of minutes or {@code Integer.MAX_VALUE} if the number of minutes is too large.
     */
    public int toMinutesAsInt()
    {
        return Ints.saturatedCast(toMinutes());
    }

    /**
     * @return this duration in number of seconds
     */
    public long toSeconds()
    {
        return unit.toSeconds(quantity);
    }

    /**
     * Returns this duration in number of seconds as an {@code int}
     *
     * @return this duration in number of seconds or {@code Integer.MAX_VALUE} if the number of seconds is too large.
     */
    public int toSecondsAsInt()
    {
        return Ints.saturatedCast(toSeconds());
    }

    /**
     * @return this duration in number of nanoseconds
     */
    public long toNanoseconds()
    {
        return unit.toNanos(quantity);
    }

    /**
     * Returns this duration in number of nanoseconds as an {@code int}
     *
     * @return this duration in number of nanoseconds or {@code Integer.MAX_VALUE} if the number of nanoseconds is too large.
     */
    public int toNanosecondsAsInt()
    {
        return Ints.saturatedCast(toNanoseconds());
    }

    /**
     * @return this duration in number of milliseconds
     */
    public long toMilliseconds()
    {
        return unit.toMillis(quantity);
    }

    /**
     * @return the duration value in milliseconds
     */
    public static long toMilliseconds(DurationSpec quantity)
    {
        return quantity.toMilliseconds();
    }

    /**
     * Returns this duration in number of milliseconds as an {@code int}
     *
     * @return this duration in number of milliseconds or {@code Integer.MAX_VALUE} if the number of milliseconds is too large.
     */
    public int toMillisecondsAsInt()
    {
        return Ints.saturatedCast(toMilliseconds());
    }

    @Override
    public int hashCode()
    {
        // Milliseconds seems to be a reasonable tradeoff
        return Objects.hash(unit.toMillis(quantity));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;

        if (!(obj instanceof DurationSpec))
            return false;

        DurationSpec other = (DurationSpec) obj;
        if (unit == other.unit)
            return quantity == other.quantity;

        // Due to overflows we can only guarantee that the 2 durations are equal if we get the same results
        // doing the conversion in both directions.
        return unit.convert(other.quantity, other.unit) == quantity && other.unit.convert(quantity, unit) == other.quantity;
    }

    @Override
    public String toString()
    {
        return quantity + getSymbol(unit);
    }

    /**
     * Returns the symbol associated to the specified unit
     *
     * @param unit the time unit
     * @return the time unit symbol
     */
    static String getSymbol(TimeUnit unit)
    {
        switch (unit)
        {
            case DAYS: return "d";
            case HOURS: return "h";
            case MINUTES: return "m";
            case SECONDS: return "s";
            case MILLISECONDS: return "ms";
            case MICROSECONDS: return "us";
            case NANOSECONDS: return "ns";
        }
        throw new AssertionError();
    }
}

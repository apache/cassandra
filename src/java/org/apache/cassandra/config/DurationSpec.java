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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.primitives.Ints;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Represents a positive time duration. Wrapper class for Cassandra duration configuration parameters, providing to the
 * users the opportunity to be able to provide config with a unit of their choice in cassandra.yaml as per the available
 * options. (CASSANDRA-15234)
 */
public abstract class DurationSpec
{
    /**
     * The Regexp used to parse the duration provided as String.
     */
    private static final Pattern UNITS_PATTERN = Pattern.compile(("^(\\d+)(d|h|s|ms|us|µs|ns|m)$"));

    private final long quantity;

    private final TimeUnit unit;

    private DurationSpec(long quantity, TimeUnit unit, TimeUnit minUnit, long max)
    {
        this.quantity = quantity;
        this.unit = unit;

        validateMinUnit(unit, minUnit, quantity + " " + unit);
        validateQuantity(quantity, unit, minUnit, max);
    }

    private DurationSpec(double quantity, TimeUnit unit, TimeUnit minUnit, long max)
    {
        this(Math.round(quantity), unit, minUnit, max);
    }

    private DurationSpec(String value, TimeUnit minUnit)
    {
        Matcher matcher = UNITS_PATTERN.matcher(value);

        if (matcher.find())
        {
            quantity = Long.parseLong(matcher.group(1));
            unit = fromSymbol(matcher.group(2));

            // this constructor is used only by extended classes for min unit; upper bound and min unit are guarded there accordingly
        }
        else
        {
            throw new IllegalArgumentException("Invalid duration: " + value + " Accepted units:" + acceptedUnits(minUnit) +
                                               " where case matters and only non-negative values.");
        }
    }

    private DurationSpec(String value, TimeUnit minUnit, long max)
    {
        this(value, minUnit);

        validateMinUnit(unit, minUnit, value);
        validateQuantity(value, quantity(), unit(), minUnit, max);
    }

    private static void validateMinUnit(TimeUnit unit, TimeUnit minUnit, String value)
    {
        if (unit.compareTo(minUnit) < 0)
            throw new IllegalArgumentException(String.format("Invalid duration: %s Accepted units:%s", value, acceptedUnits(minUnit)));
    }

    private static String acceptedUnits(TimeUnit minUnit)
    {
        TimeUnit[] units = TimeUnit.values();
        return Arrays.toString(Arrays.copyOfRange(units, minUnit.ordinal(), units.length));
    }

    private static void validateQuantity(String value, long quantity, TimeUnit sourceUnit, TimeUnit minUnit, long max)
    {
        // no need to validate for negatives as they are not allowed at first place from the regex

        if (minUnit.convert(quantity, sourceUnit) >= max)
            throw new IllegalArgumentException("Invalid duration: " + value + ". It shouldn't be more than " +
                                             (max - 1) + " in " + minUnit.name().toLowerCase());
    }

    private static void validateQuantity(long quantity, TimeUnit sourceUnit, TimeUnit minUnit, long max)
    {
        if (quantity < 0)
            throw new IllegalArgumentException("Invalid duration: value must be non-negative");

        if (minUnit.convert(quantity, sourceUnit) >= max)
            throw new IllegalArgumentException(String.format("Invalid duration: %d %s. It shouldn't be more than %d in %s",
                                                           quantity, sourceUnit.name().toLowerCase(),
                                                           max - 1, minUnit.name().toLowerCase()));
    }

    // get vs no-get prefix is not consistent in the code base, but for classes involved with config parsing, it is
    // imporant to be explicit about get/set as this changes how parsing is done; this class is a data-type, so is
    // not nested, having get/set can confuse parsing thinking this is a nested type
    public long quantity()
    {
        return quantity;
    }

    public TimeUnit unit()
    {
        return unit;
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
            case "µs": return MICROSECONDS;
            case "ns": return TimeUnit.NANOSECONDS;
        }
        throw new IllegalArgumentException(String.format("Unsupported time unit: %s. Supported units are: %s",
                                                       symbol, Arrays.stream(TimeUnit.values())
                                                                     .map(DurationSpec::symbol)
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
        return quantity + symbol(unit);
    }

    /**
     * Returns the symbol associated to the specified unit
     *
     * @param unit the time unit
     * @return the time unit symbol
     */
    // get vs no-get prefix is not consistent in the code base, but for classes involved with config parsing, it is
    // imporant to be explicit about get/set as this changes how parsing is done; this class is a data-type, so is
    // not nested, having get/set can confuse parsing thinking this is a nested type
    static String symbol(TimeUnit unit)
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

    /**
     * Represents a duration used for Cassandra configuration. The bound is [0, Long.MAX_VALUE) in nanoseconds.
     * If the user sets a different unit - we still validate that converted to nanoseconds the quantity will not exceed
     * that upper bound. (CASSANDRA-17571)
     */
    public final static class LongNanosecondsBound extends DurationSpec
    {
        /**
         * Creates a {@code DurationSpec.LongNanosecondsBound} of the specified amount.
         * The bound is [0, Long.MAX_VALUE) in nanoseconds.
         *
         * @param value the duration
         */
        public LongNanosecondsBound(String value)
        {
            super(value, NANOSECONDS, Long.MAX_VALUE);
        }

        /**
         * Creates a {@code DurationSpec.LongNanosecondsBound} of the specified amount in the specified unit.
         * The bound is [0, Long.MAX_VALUE) in nanoseconds.
         *
         * @param quantity where quantity shouldn't be bigger than Long.MAX_VALUE - 1 in nanoseconds
         * @param unit in which the provided quantity is
         */
        public LongNanosecondsBound(long quantity, TimeUnit unit)
        {
            super(quantity, unit, NANOSECONDS, Long.MAX_VALUE);
        }

        /**
         * Creates a {@code DurationSpec.LongNanosecondsBound} of the specified amount in nanoseconds.
         * The bound is [0, Long.MAX_VALUE) in nanoseconds.
         *
         * @param nanoseconds where nanoseconds shouldn't be bigger than Long.MAX_VALUE-1
         */
        public LongNanosecondsBound(long nanoseconds)
        {
            this(nanoseconds, NANOSECONDS);
        }

        /**
         * @return this duration in number of nanoseconds
         */
        public long toNanoseconds()
        {
            return unit().toNanos(quantity());
        }
    }

    /**
     * Represents a duration used for Cassandra configuration. The bound is [0, Long.MAX_VALUE) in microseconds.
     * If the user sets a different unit - we still validate that converted to microseconds the quantity will not exceed
     * that upper bound. (CASSANDRA-17571)
     */
    public final static class LongMicrosecondsBound extends DurationSpec
    {
        /**
         * Creates a {@code DurationSpec.LongMicrosecondsBound} of the specified amount.
         * The bound is [0, Long.MAX_VALUE) in microseconds.
         *
         * @param value the duration
         */
        public LongMicrosecondsBound(String value)
        {
            super(value, MICROSECONDS, Long.MAX_VALUE);
        }

        /**
         * Creates a {@code DurationSpec.LongMicrosecondsBound} of the specified amount in the specified unit.
         * The bound is [0, Long.MAX_VALUE) in milliseconds.
         *
         * @param quantity where quantity shouldn't be bigger than Long.MAX_VALUE - 1 in microseconds
         * @param unit in which the provided quantity is
         */
        public LongMicrosecondsBound(long quantity, TimeUnit unit)
        {
            super(quantity, unit, MICROSECONDS, Long.MAX_VALUE);
        }

        /**
         * Creates a {@code DurationSpec.LongMicrosecondsBound} of the specified amount in microseconds.
         * The bound is [0, Long.MAX_VALUE) in microseconds.
         *
         * @param microseconds where milliseconds shouldn't be bigger than Long.MAX_VALUE-1
         */
        public LongMicrosecondsBound(long microseconds)
        {
            this(microseconds, MICROSECONDS);
        }

        /**
         * @return this duration in number of milliseconds
         */
        public long toMicroseconds()
        {
            return unit().toMicros(quantity());
        }

        /**
         * @return this duration in number of seconds
         */
        public long toSeconds()
        {
            return unit().toSeconds(quantity());
        }
    }

    /**
     * Represents a duration used for Cassandra configuration. The bound is [0, Long.MAX_VALUE) in milliseconds.
     * If the user sets a different unit - we still validate that converted to milliseconds the quantity will not exceed
     * that upper bound. (CASSANDRA-17571)
     */
    public final static class LongMillisecondsBound extends DurationSpec
    {
        /**
         * Creates a {@code DurationSpec.LongMillisecondsBound} of the specified amount.
         * The bound is [0, Long.MAX_VALUE) in milliseconds.
         *
         * @param value the duration
         */
        public LongMillisecondsBound(String value)
        {
            super(value, MILLISECONDS, Long.MAX_VALUE);
        }

        /**
         * Creates a {@code DurationSpec.LongMillisecondsBound} of the specified amount in the specified unit.
         * The bound is [0, Long.MAX_VALUE) in milliseconds.
         *
         * @param quantity where quantity shouldn't be bigger than Long.MAX_VALUE - 1 in milliseconds
         * @param unit in which the provided quantity is
         */
        public LongMillisecondsBound(long quantity, TimeUnit unit)
        {
            super(quantity, unit, MILLISECONDS, Long.MAX_VALUE);
        }

        /**
         * Creates a {@code DurationSpec.LongMillisecondsBound} of the specified amount in milliseconds.
         * The bound is [0, Long.MAX_VALUE) in milliseconds.
         *
         * @param milliseconds where milliseconds shouldn't be bigger than Long.MAX_VALUE-1
         */
        public LongMillisecondsBound(long milliseconds)
        {
            this(milliseconds, MILLISECONDS);
        }

        /**
         * @return this duration in number of milliseconds
         */
        public long toMilliseconds()
        {
            return unit().toMillis(quantity());
        }
    }

    /**
     * Represents a duration used for Cassandra configuration. The bound is [0, Long.MAX_VALUE) in seconds.
     * If the user sets a different unit - we still validate that converted to seconds the quantity will not exceed
     * that upper bound. (CASSANDRA-17571)
     */
    public final static class LongSecondsBound extends DurationSpec
    {
        /**
         * Creates a {@code DurationSpec.LongSecondsBound} of the specified amount.
         * The bound is [0, Long.MAX_VALUE) in seconds.
         *
         * @param value the duration
         */
        public LongSecondsBound(String value)
        {
            super(value, SECONDS, Long.MAX_VALUE);
        }

        /**
         * Creates a {@code DurationSpec.LongSecondsBound} of the specified amount in the specified unit.
         * The bound is [0, Long.MAX_VALUE) in seconds.
         *
         * @param quantity where quantity shouldn't be bigger than Long.MAX_VALUE - 1 in seconds
         * @param unit in which the provided quantity is
         */
        public LongSecondsBound(long quantity, TimeUnit unit)
        {
            super(quantity, unit, SECONDS, Long.MAX_VALUE);
        }

        /**
         * Creates a {@code DurationSpec.LongSecondsBound} of the specified amount in seconds.
         * The bound is [0, Long.MAX_VALUE) in seconds.
         *
         * @param seconds where seconds shouldn't be bigger than Long.MAX_VALUE-1
         */
        public LongSecondsBound(long seconds)
        {
            this(seconds, SECONDS);
        }

        /**
         * @return this duration in number of milliseconds
         */
        public long toMilliseconds()
        {
            return unit().toMillis(quantity());
        }

        /**
         * @return this duration in number of seconds
         */
        public long toSeconds()
        {
            return unit().toSeconds(quantity());
        }
    }

    /**
     * Represents a duration used for Cassandra configuration. The bound is [0, Integer.MAX_VALUE) in minutes.
     * If the user sets a different unit - we still validate that converted to minutes the quantity will not exceed
     * that upper bound. (CASSANDRA-17571)
     */
    public final static class IntMinutesBound extends DurationSpec
    {
        /**
         * Creates a {@code DurationSpec.IntMinutesBound} of the specified amount. The bound is [0, Integer.MAX_VALUE) in minutes.
         * The bound is [0, Integer.MAX_VALUE) in minutes.
         *
         * @param value the duration
         */
        public IntMinutesBound(String value)
        {
            super(value, MINUTES, Integer.MAX_VALUE);
        }

        /**
         * Creates a {@code DurationSpec.IntMinutesBound} of the specified amount in the specified unit.
         * The bound is [0, Integer.MAX_VALUE) in minutes.
         *
         * @param quantity where quantity shouldn't be bigger than Integer.MAX_VALUE - 1 in minutes
         * @param unit in which the provided quantity is
         */
        public IntMinutesBound(long quantity, TimeUnit unit)
        {
            super(quantity, unit, MINUTES, Integer.MAX_VALUE);
        }

        /**
         * Creates a {@code DurationSpec.IntMinutesBound} of the specified amount in minutes.
         * The bound is [0, Integer.MAX_VALUE) in minutes.
         *
         * @param minutes where minutes shouldn't be bigger than Integer.MAX_VALUE-1
         */
        public IntMinutesBound(long minutes)
        {
            this(minutes, MINUTES);
        }

        /**
         * Returns this duration in number of milliseconds as an {@code int}
         *
         * @return this duration in number of milliseconds or {@code Integer.MAX_VALUE} if the number of milliseconds is too large.
         */
        public int toMilliseconds()
        {
            return Ints.saturatedCast(unit().toMillis(quantity()));
        }

        /**
         * Returns this duration in number of seconds as an {@code int}
         *
         * @return this duration in number of seconds or {@code Integer.MAX_VALUE} if the number of seconds is too large.
         */
        public int toSeconds()
        {
            return Ints.saturatedCast(unit().toSeconds(quantity()));
        }

        /**
         * Returns this duration in number of minutes as an {@code int}
         *
         * @return this duration in number of minutes or {@code Integer.MAX_VALUE} if the number of minutes is too large.
         */
        public int toMinutes()
        {
            return Ints.saturatedCast(unit().toMinutes(quantity()));
        }
    }

    /**
     * Represents a duration used for Cassandra configuration. The bound is [0, Integer.MAX_VALUE) in seconds.
     * If the user sets a different unit - we still validate that converted to seconds the quantity will not exceed
     * that upper bound. (CASSANDRA-17571)
     */
    public final static class IntSecondsBound extends DurationSpec
    {
        private static final Pattern VALUES_PATTERN = Pattern.compile(("\\d+"));

        /**
         * Creates a {@code DurationSpec.IntSecondsBound} of the specified amount. The bound is [0, Integer.MAX_VALUE) in seconds.
         * The bound is [0, Integer.MAX_VALUE) in seconds.
         *
         * @param value the duration
         */
        public IntSecondsBound(String value)
        {
            super(value, SECONDS, Integer.MAX_VALUE);
        }

        /**
         * Creates a {@code DurationSpec.IntSecondsBound} of the specified amount in the specified unit.
         * The bound is [0, Integer.MAX_VALUE) in seconds.
         *
         * @param quantity where quantity shouldn't be bigger than Integer.MAX_VALUE - 1 in seconds
         * @param unit in which the provided quantity is
         */
        public IntSecondsBound(long quantity, TimeUnit unit)
        {
            super(quantity, unit, SECONDS, Integer.MAX_VALUE);
        }

        /**
         * Creates a {@code DurationSpec.IntSecondsBound} of the specified amount in seconds.
         * The bound is [0, Integer.MAX_VALUE) in seconds.
         *
         * @param seconds where seconds shouldn't be bigger than Integer.MAX_VALUE-1
         */
        public IntSecondsBound(long seconds)
        {
            this(seconds, SECONDS);
        }

        /**
         * Creates a {@code DurationSpec.IntSecondsBound} of the specified amount in seconds, expressed either as the
         * number of seconds without unit, or as a regular quantity with unit.
         * Used in the Converters for a few parameters which changed only type, but not names
         * The bound is [0, Integer.MAX_VALUE) in seconds.
         *
         * @param value where value shouldn't be bigger than Integer.MAX_VALUE-1 in seconds
         */
        public static IntSecondsBound inSecondsString(String value)
        {
            //parse the string field value
            Matcher matcher = VALUES_PATTERN.matcher(value);

            long seconds;
            //if the provided string value is just a number, then we create a IntSecondsBound value in seconds
            if (matcher.matches())
            {
                seconds = Integer.parseInt(value);
                return new IntSecondsBound(seconds, TimeUnit.SECONDS);
            }

            //otherwise we just use the standard constructors
            return new IntSecondsBound(value);
        }

        /**
         * Returns this duration in the number of nanoseconds as an {@code int}
         *
         * @return this duration in number of nanoseconds or {@code Integer.MAX_VALUE} if the number of nanoseconds is too large.
         */
        public int toNanoseconds()
        {
            return Ints.saturatedCast(unit().toNanos(quantity()));
        }

        /**
         * Returns this duration in number of milliseconds as an {@code int}
         *
         * @return this duration in number of milliseconds or {@code Integer.MAX_VALUE} if the number of milliseconds is too large.
         */
        public int toMilliseconds()
        {
            return Ints.saturatedCast(unit().toMillis(quantity()));
        }

        /**
         * Returns this duration in number of seconds as an {@code int}
         *
         * @return this duration in number of seconds or {@code Integer.MAX_VALUE} if the number of seconds is too large.
         */
        public int toSeconds()
        {
            return Ints.saturatedCast(unit().toSeconds(quantity()));
        }
    }

    /**
     * Represents a duration used for Cassandra configuration. The bound is [0, Integer.MAX_VALUE) in milliseconds.
     * If the user sets a different unit - we still validate that converted to milliseconds the quantity will not exceed
     * that upper bound. (CASSANDRA-17571)
     */
    public final static class IntMillisecondsBound extends DurationSpec
    {
        /**
         * Creates a {@code DurationSpec.IntMillisecondsBound} of the specified amount. The bound is [0, Integer.MAX_VALUE) in milliseconds.
         * The bound is [0, Integer.MAX_VALUE) in milliseconds.
         *
         * @param value the duration
         */
        public IntMillisecondsBound(String value)
        {
            super(value, MILLISECONDS, Integer.MAX_VALUE);
        }

        /**
         * Creates a {@code DurationSpec.IntMillisecondsBound} of the specified amount in the specified unit.
         * The bound is [0, Integer.MAX_VALUE) in milliseconds.
         *
         * @param quantity where quantity shouldn't be bigger than Integer.MAX_VALUE - 1 in milliseconds
         * @param unit in which the provided quantity is
         */
        public IntMillisecondsBound(long quantity, TimeUnit unit)
        {
            super(quantity, unit, MILLISECONDS, Integer.MAX_VALUE);
        }

        /**
         * Creates a {@code DurationSpec.IntMillisecondsBound} of the specified amount in milliseconds.
         * The bound is [0, Integer.MAX_VALUE) in milliseconds.
         *
         * @param milliseconds where milliseconds shouldn't be bigger than Integer.MAX_VALUE-1
         */
        public IntMillisecondsBound(long milliseconds)
        {
            this(milliseconds, MILLISECONDS);
        }

        /**
         * Below constructor is used only for backward compatibility for the old commitlog_sync_group_window_in_ms before 4.1
         * Creates a {@code DurationSpec.IntMillisecondsBound} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Intetger.MAX_VALUE - 1 in milliseconds
         * @param unit in which the provided quantity is
         */
        public IntMillisecondsBound(double quantity, TimeUnit unit)
        {
            super(quantity, unit, MILLISECONDS, Integer.MAX_VALUE);
        }

        /**
         * Returns this duration in number of milliseconds as an {@code int}
         *
         * @return this duration in number of milliseconds or {@code Integer.MAX_VALUE} if the number of milliseconds is too large.
         */
        public int toMilliseconds()
        {
            return Ints.saturatedCast(unit().toMillis(quantity()));
        }
    }
}

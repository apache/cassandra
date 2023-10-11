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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.math.DoubleMath;
import com.google.common.primitives.Ints;

import static org.apache.cassandra.config.DataRateSpec.DataRateUnit.BYTES_PER_SECOND;

/**
 * Represents a data rate type used for cassandra configuration. It supports the opportunity for the users to be able to
 * add units to the confiuration parameter value. (CASSANDRA-15234)
 */
public abstract class DataRateSpec
{
    /**
     * The Regexp used to parse the rate provided as String in cassandra.yaml.
     */
    private static final Pattern UNITS_PATTERN = Pattern.compile("^(\\d+)(MiB/s|KiB/s|B/s)$");

    private final long quantity;

    private final DataRateUnit unit;

    private DataRateSpec(String value)
    {
        //parse the string field value
        Matcher matcher = UNITS_PATTERN.matcher(value);

        if (!matcher.find())
            throw new IllegalArgumentException("Invalid data rate: " + value + " Accepted units: MiB/s, KiB/s, B/s where " +
                                                "case matters and " + "only non-negative values are valid");

        quantity = Long.parseLong(matcher.group(1));
        unit = DataRateUnit.fromSymbol(matcher.group(2));
    }

    private DataRateSpec(String value, DataRateUnit minUnit, long max)
    {
        this(value);

        validateQuantity(value, quantity(), unit(), minUnit, max);
    }

    private DataRateSpec(long quantity, DataRateUnit unit, DataRateUnit minUnit, long max)
    {
        this.quantity = quantity;
        this.unit = unit;

        validateQuantity(quantity, unit, minUnit, max);
    }

    private static void validateQuantity(String value, double quantity, DataRateUnit unit, DataRateUnit minUnit, long max)
    {
        // negatives are not allowed by the regex pattern
        if (minUnit.convert(quantity, unit) >= max)
            throw new IllegalArgumentException("Invalid data rate: " + value + ". It shouldn't be more than " +
                                             (max - 1) + " in " + minUnit.name().toLowerCase());
    }

    private static void validateQuantity(double quantity, DataRateUnit unit, DataRateUnit minUnit, long max)
    {
        if (quantity < 0)
            throw new IllegalArgumentException("Invalid data rate: value must be non-negative");

        if (minUnit.convert(quantity, unit) >= max)
            throw new IllegalArgumentException(String.format("Invalid data rate: %s %s. It shouldn't be more than %d in %s",
                                                       quantity, unit.name().toLowerCase(),
                                                       max - 1, minUnit.name().toLowerCase()));
    }

    // get vs no-get prefix is not consistent in the code base, but for classes involved with config parsing, it is
    // imporant to be explicit about get/set as this changes how parsing is done; this class is a data-type, so is
    // not nested, having get/set can confuse parsing thinking this is a nested type
    /**
     * @return the data rate unit assigned.
     */
    public DataRateUnit unit()
    {
        return unit;
    }

    /**
     * @return the data rate quantity.
     */
    private double quantity()
    {
        return quantity;
    }

    /**
     * @return the data rate in bytes per second
     */
    public double toBytesPerSecond()
    {
        return unit.toBytesPerSecond(quantity);
    }

    /**
     * Returns the data rate in bytes per second as an {@code int}
     *
     * @return the data rate in bytes per second or {@code Integer.MAX_VALUE} if the rate is too large.
     */
    public int toBytesPerSecondAsInt()
    {
        return Ints.saturatedCast(Math.round(toBytesPerSecond()));
    }

    /**
     * @return the data rate in kibibytes per second
     */
    public double toKibibytesPerSecond()
    {
        return unit.toKibibytesPerSecond(quantity);
    }

    /**
     * Returns the data rate in kibibytes per second as an {@code int}
     *
     * @return the data rate in kibibytes per second or {@code Integer.MAX_VALUE} if the number of kibibytes is too large.
     */
    public int toKibibytesPerSecondAsInt()
    {
        return Ints.saturatedCast(Math.round(toKibibytesPerSecond()));
    }

    /**
     * @return the data rate in mebibytes per second
     */
    public double toMebibytesPerSecond()
    {
        return unit.toMebibytesPerSecond(quantity);
    }

    /**
     * Returns the data rate in mebibytes per second as an {@code int}
     *
     * @return the data rate in mebibytes per second or {@code Integer.MAX_VALUE} if the number of mebibytes is too large.
     */
    public int toMebibytesPerSecondAsInt()
    {
        return Ints.saturatedCast(Math.round(toMebibytesPerSecond()));
    }

    /**
     * This method is required in order to support backward compatibility with the old unit used for a few Data Rate
     * parameters before CASSANDRA-15234
     *
     * @return the data rate in megabits per second.
     */
    public double toMegabitsPerSecond()
    {
        return unit.toMegabitsPerSecond(quantity);
    }

    /**
     * Returns the data rate in megabits per second as an {@code int}. This method is required in order to support
     * backward compatibility with the old unit used for a few Data Rate parameters before CASSANDRA-15234
     *
     * @return the data rate in mebibytes per second or {@code Integer.MAX_VALUE} if the number of mebibytes is too large.
     */
    public int toMegabitsPerSecondAsInt()
    {
        return Ints.saturatedCast(Math.round(toMegabitsPerSecond()));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(unit.toKibibytesPerSecond(quantity));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;

        if (!(obj instanceof DataRateSpec))
            return false;

        DataRateSpec other = (DataRateSpec) obj;
        if (unit == other.unit)
            return quantity == other.quantity;

        // Due to overflows we can only guarantee that the 2 data rates are equal if we get the same results
        // doing the conversion in both directions.
        return unit.convert(other.quantity, other.unit) == quantity && other.unit.convert(quantity, unit) == other.quantity;
    }

    @Override
    public String toString()
    {
        return (DoubleMath.isMathematicalInteger(quantity) ? (long) quantity : quantity) + unit.symbol;
    }

    /**
     * Represents a data rate used for Cassandra configuration. The bound is [0, Long.MAX_VALUE) in bytes per second.
     * If the user sets a different unit, we still validate that converted to bytes per second the quantity will not exceed
     * that upper bound. (CASSANDRA-17571)
     */
    public final static class LongBytesPerSecondBound extends DataRateSpec
    {
        /**
         * Creates a {@code DataRateSpec.LongBytesPerSecondBound} of the specified amount.
         *
         * @param value the data rate
         */
        public LongBytesPerSecondBound(String value)
        {
            super(value, BYTES_PER_SECOND, Long.MAX_VALUE);
        }

        /**
         * Creates a {@code DataRateSpec.LongBytesPerSecondBound} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Long.MAX_VALUE - 1 in bytes per second
         * @param unit     in which the provided quantity is
         */
        public LongBytesPerSecondBound(long quantity, DataRateUnit unit)
        {
            super(quantity, unit, BYTES_PER_SECOND, Long.MAX_VALUE);
        }

        /**
         * Creates a {@code DataRateSpec.LongBytesPerSecondBound} of the specified amount in bytes per second.
         *
         * @param bytesPerSecond where bytesPerSecond shouldn't be bigger than Long.MAX_VALUE
         */
        public LongBytesPerSecondBound(long bytesPerSecond)
        {
            this(bytesPerSecond, BYTES_PER_SECOND);
        }

        // this one should be used only for backward compatibility for stream_throughput_outbound and inter_dc_stream_throughput_outbound
        // which were in megabits per second in 4.0. Do not start using it for any new properties
        /** @deprecated See CASSANDRA-17225 */
        @Deprecated(since = "4.1")
        public static LongBytesPerSecondBound megabitsPerSecondInBytesPerSecond(long megabitsPerSecond)
        {
            final long BYTES_PER_MEGABIT = 125_000;
            long bytesPerSecond = megabitsPerSecond * BYTES_PER_MEGABIT;

            if (megabitsPerSecond >= Integer.MAX_VALUE)
                throw new IllegalArgumentException("Invalid data rate: " + megabitsPerSecond + " megabits per second; " +
                                                   "stream_throughput_outbound and inter_dc_stream_throughput_outbound" +
                                                   " should be between 0 and " + (Integer.MAX_VALUE - 1) + " in megabits per second");

            return new LongBytesPerSecondBound(bytesPerSecond, BYTES_PER_SECOND);
        }
    }

    public enum DataRateUnit
    {
        BYTES_PER_SECOND("B/s")
        {
            public double toBytesPerSecond(double d)
            {
                return d;
            }

            public double toKibibytesPerSecond(double d)
            {
                return d / 1024.0;
            }

            public double toMebibytesPerSecond(double d)
            {
                return d / (1024.0 * 1024.0);
            }

            public double toMegabitsPerSecond(double d)
            {
                return (d / 125000.0);
            }

            public double convert(double source, DataRateUnit sourceUnit)
            {
                return sourceUnit.toBytesPerSecond(source);
            }
        },
        KIBIBYTES_PER_SECOND("KiB/s")
        {
            public double toBytesPerSecond(double d)
            {
                return x(d, 1024.0, (MAX / 1024.0));
            }

            public double toKibibytesPerSecond(double d)
            {
                return d;
            }

            public double toMebibytesPerSecond(double d)
            {
                return d / 1024.0;
            }

            public double toMegabitsPerSecond(double d)
            {
                return d / 122.0;
            }

            public double convert(double source, DataRateUnit sourceUnit)
            {
                return sourceUnit.toKibibytesPerSecond(source);
            }
        },
        MEBIBYTES_PER_SECOND("MiB/s")
        {
            public double toBytesPerSecond(double d)
            {
                return x(d, (1024.0 * 1024.0), (MAX / (1024.0 * 1024.0)));
            }

            public double toKibibytesPerSecond(double d)
            {
                return x(d, 1024.0, (MAX / 1024.0));
            }

            public double toMebibytesPerSecond(double d)
            {
                return d;
            }

            public double toMegabitsPerSecond(double d)
            {
                if (d > MAX / (MEGABITS_PER_MEBIBYTE))
                    return MAX;
                return d * MEGABITS_PER_MEBIBYTE;
            }

            public double convert(double source, DataRateUnit sourceUnit)
            {
                return sourceUnit.toMebibytesPerSecond(source);
            }
        };

        static final double MAX = Long.MAX_VALUE;
        static final double MEGABITS_PER_MEBIBYTE = 8.388608;

        /**
         * Scale d by m, checking for overflow. This has a short name to make above code more readable.
         */
        static double x(double d, double m, double over)
        {
            assert (over > 0.0) && (over < (MAX - 1)) && (over == (MAX / m));

            if (d > over)
                return MAX;
            return d * m;
        }

        /**
         * @param symbol the unit symbol
         * @return the rate unit corresponding to the given symbol
         */
        public static DataRateUnit fromSymbol(String symbol)
        {
            for (DataRateUnit value : values())
            {
                if (value.symbol.equalsIgnoreCase(symbol))
                    return value;
            }
            throw new IllegalArgumentException(String.format("Unsupported data rate unit: %s. Supported units are: %s",
                                                             symbol, Arrays.stream(values())
                                                                           .map(u -> u.symbol)
                                                                           .collect(Collectors.joining(", "))));
        }

        /**
         * The unit symbol
         */
        private final String symbol;

        DataRateUnit(String symbol)
        {
            this.symbol = symbol;
        }

        public double toBytesPerSecond(double d)
        {
            throw new AbstractMethodError();
        }

        public double toKibibytesPerSecond(double d)
        {
            throw new AbstractMethodError();
        }

        public double toMebibytesPerSecond(double d)
        {
            throw new AbstractMethodError();
        }

        public double toMegabitsPerSecond(double d)
        {
            throw new AbstractMethodError();
        }

        public double convert(double source, DataRateUnit sourceUnit)
        {
            throw new AbstractMethodError();
        }
    }
}

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

import com.google.common.primitives.Ints;

/**
 * Represents a data rate type used for cassandra configuration. It supports the opportunity for the users to be able to
 * add units to the confiuration parameter value. (CASSANDRA-15234)
 */
public final class DataRateSpec
{
    /**
     * The Regexp used to parse the rate provided as String in cassandra.yaml.
     */
    private static final Pattern BIT_RATE_UNITS_PATTERN = Pattern.compile("^(\\d+)(MiB/s|KiB/s|B/s)$");

    private final double quantity;

    private final DataRateUnit unit;

    public DataRateSpec(String value)
    {
        //parse the string field value
        Matcher matcher = BIT_RATE_UNITS_PATTERN.matcher(value);

        if (!matcher.find())
            throw new IllegalArgumentException("Invalid bit rate: " + value + " Accepted units: MiB/s, KiB/s, B/s where " +
                                             "case matters and " + "only non-negative values are valid");

        quantity = Long.parseLong(matcher.group(1));
        unit = DataRateUnit.fromSymbol(matcher.group(2));
    }

    DataRateSpec(double quantity, DataRateUnit unit)
    {
        if (quantity < 0)
            throw new IllegalArgumentException("Invalid bit rate: value must be non-negative");

        if (quantity > Long.MAX_VALUE)
            throw new NumberFormatException("Invalid bit rate: value must be between 0 and Long.MAX_VALUE = 9223372036854775807");

        this.quantity = quantity;
        this.unit = unit;
    }

    /**
     * Creates a {@code DataRateSpec} of the specified amount of bits per second.
     *
     * @param bytesPerSecond the amount of bytes per second
     * @return a {@code DataRateSpec}
     */
    public static DataRateSpec inBytesPerSecond(long bytesPerSecond)
    {
        return new DataRateSpec(bytesPerSecond, DataRateUnit.BYTES_PER_SECOND);
    }

    /**
     * Creates a {@code DataRateSpec} of the specified amount of kibibytes per second.
     *
     * @param kibibytesPerSecond the amount of kibibytes per second
     * @return a {@code DataRateSpec}
     */
    public static DataRateSpec inKibibytesPerSecond(long kibibytesPerSecond)
    {
        return new DataRateSpec(kibibytesPerSecond, DataRateUnit.KIBIBYTES_PER_SECOND);
    }

    /**
     * Creates a {@code DataRateSpec} of the specified amount of mebibytes per second.
     *
     * @param mebibytesPerSecond the amount of mebibytes per second
     * @return a {@code DataRateSpec}
     */
    public static DataRateSpec inMebibytesPerSecond(long mebibytesPerSecond)
    {
        return new DataRateSpec(mebibytesPerSecond, DataRateUnit.MEBIBYTES_PER_SECOND);
    }

    /**
     * Creates a {@code DataRateSpec} of the specified amount of mebibytes per second.
     *
     * @param megabitsPerSecond the amount of megabits per second
     * @return a {@code DataRateSpec}
     */
    public static DataRateSpec megabitsPerSecondInMebibytesPerSecond(long megabitsPerSecond)
    {
        final double MEBIBYTES_PER_MEGABIT = 0.119209289550781;
        double mebibytesPerSecond = (double)megabitsPerSecond * MEBIBYTES_PER_MEGABIT;

        return new DataRateSpec(mebibytesPerSecond, DataRateUnit.MEBIBYTES_PER_SECOND);
    }

    /**
     * @return the data rate unit assigned.
     */
    public DataRateUnit getUnit()
    {
        return unit;
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
        return Math.round(quantity) + unit.symbol;
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

            public double toMegabitsPerSecond(double d) { return (d / 125000.0); }

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
                return Math.round(d * MEGABITS_PER_MEBIBYTE);
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

        public double toMegabitsPerSecond(double d) { throw new AbstractMethodError(); }

        public double convert(double source, DataRateUnit sourceUnit)
        {
            throw new AbstractMethodError();
        }
    }
}
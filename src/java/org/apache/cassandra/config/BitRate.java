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
 * Represents a bit rate.
 */
public final class BitRate
{
    /**
     * The Regexp used to parse the rate provided as String.
     */
    private static final Pattern BIT_RATE_UNITS_PATTERN = Pattern.compile("^(\\d+)(MiB/s|mib/s|MiB/S|KiB/s|kib/s|KiB/S|B/s|b/s|B/S)$");

    private final long quantity;

    private final BitRateUnit unit;

    public BitRate(String value)
    {
        //parse the string field value
        Matcher matcher = BIT_RATE_UNITS_PATTERN.matcher(value);

        if (!matcher.find())
        {
            throw new IllegalArgumentException("Invalid bit rate: " + value + " Accepted units: MiB/s, KiB/s, B/s.");
        }

        quantity = Long.parseLong(matcher.group(1));
        unit = BitRateUnit.fromSymbol(matcher.group(2));
    }

    private BitRate(long quantity, BitRateUnit unit)
    {
        this.quantity = quantity;
        this.unit = unit;
    }

    /**
     * Creates a {@code BitRate} of the specified amount of bits per second.
     *
     * @param bitsPerSeconds the amount of bits per second
     * @return a {@code BitRate}
     */
    public static BitRate inBitsPerSeconds(long bitsPerSeconds)
    {
        return new BitRate(bitsPerSeconds, BitRateUnit.BITS_PER_SECOND);
    }

    /**
     * Creates a {@code BitRate} of the specified amount of kilobits per second.
     *
     * @param kilobitsPerSeconds the amount of kilobits per second
     * @return a {@code BitRate}
     */
    public static BitRate inKilobitsPerSecond(long kilobitsPerSeconds)
    {
        return new BitRate(kilobitsPerSeconds, BitRateUnit.KILOBITS_PER_SECOND);
    }

    /**
     * Creates a {@code BitRate} of the specified amount of megabits per second.
     *
     * @param megabitsPerSeconds the amount of megabits per second
     * @return a {@code BitRate}
     */
    public static BitRate inMegabitsPerSecond(long megabitsPerSeconds)
    {
        return new BitRate(megabitsPerSeconds, BitRateUnit.MEGABITS_PER_SECOND);
    }

    /**
     * Returns the bit rate unit.
     *
     * @return the bit rate unit.
     */
    public BitRateUnit getUnit()
    {
        return unit;
    }

    /**
     * Returns the bit rate in bits per seconds
     *
     * @return the bit rate in bits per seconds
     */
    public long toBitsPerSecond()
    {
        return unit.toBitsPerSecond(quantity);
    }

    /**
     * Returns the bit rate in bits per seconds as an {@code int}
     *
     * @return the bit rate in bits per secondss or {@code Integer.MAX_VALUE} if the rate is too large.
     */
    public int toBitsPerSecondAsInt()
    {
        return Ints.saturatedCast(toBitsPerSecond());
    }

    /**
     * Returns the bit rate in kilobits per seconds
     *
     * @return the bit rate in kilobits per seconds
     */
    public long toKilobitsPerSecond()
    {
        return unit.toKilobitsPerSecond(quantity);
    }

    /**
     * Returns the bit rate in kilobits per seconds as an {@code int}
     *
     * @return the bit rate in kilobits per seconds or {@code Integer.MAX_VALUE} if the number of kilobits is too large.
     */
    public int toKilobitsPerSecondAsInt()
    {
        return Ints.saturatedCast(toKilobitsPerSecond());
    }

    /**
     * Returns the bit rate in megabits per seconds
     *
     * @return the bit rate in megabits per seconds
     */
    public long toMegabitsPerSecond()
    {
        return unit.toMegabitsPerSeconds(quantity);
    }

    /**
     * Returns the bit rate in megabits per seconds as an {@code int}
     *
     * @return the bit rate in megabits per seconds or {@code Integer.MAX_VALUE} if the number of megabits is too large.
     */
    public int toMegabitsPerSecondAsInt()
    {
        return Ints.saturatedCast(toMegabitsPerSecond());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(unit.toKilobitsPerSecond(quantity));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;

        if (!(obj instanceof BitRate))
            return false;

        BitRate other = (BitRate) obj;
        if (unit == other.unit)
            return quantity == other.quantity;

        // Due to overflows we can only guaranty that the 2 bit rates are equals if we get the same results
        // doing the convertion in both directions.
        return unit.convert(other.quantity, other.unit) == quantity && other.unit.convert(quantity, unit) == other.quantity;
    }

    @Override
    public String toString()
    {
        return quantity + unit.symbol;
    }

    public enum BitRateUnit
    {
        BITS_PER_SECOND("b/s")
        {
            public long toBitsPerSecond(long d)
            {
                return d;
            }

            public long toKilobitsPerSecond(long d)
            {
                return d / 1000;
            }

            public long toMegabitsPerSeconds(long d)
            {
                return d / (1000 * 1000);
            }

            public long convert(long source, BitRateUnit sourceUnit)
            {
                return sourceUnit.toBitsPerSecond(source);
            }
        },
        KILOBITS_PER_SECOND("kib/s")
        {
            public long toBitsPerSecond(long d)
            {
                return x(d, 1000, MAX / 1000);
            }

            public long toKilobitsPerSecond(long d)
            {
                return d;
            }

            public long toMegabitsPerSeconds(long d)
            {
                return d / 1000;
            }

            public long convert(long source, BitRateUnit sourceUnit)
            {
                return sourceUnit.toKilobitsPerSecond(source);
            }
        },
        MEGABITS_PER_SECOND("mib/s")
        {
            public long toBitsPerSecond(long d)
            {
                return x(d, 1000 * 1000, MAX / (1000 * 1000));
            }

            public long toKilobitsPerSecond(long d)
            {
                return x(d, 1000, MAX / (1000));
            }

            public long toMegabitsPerSeconds(long d)
            {
                return d;
            }

            public long convert(long source, BitRateUnit sourceUnit)
            {
                return sourceUnit.toMegabitsPerSeconds(source);
            }
        };

        static final long MAX = Long.MAX_VALUE;

        /**
         * Scale d by m, checking for overflow. This has a short name to make above code more readable.
         */
        static long x(long d, long m, long over)
        {
            if (d > over)
                return Long.MAX_VALUE;
            return d * m;
        }

        /**
         * Returns the rate unit corresponding to the given symbol. 
         *
         * @param symbol the unit symbol
         * @return the rate unit corresponding to the given symbol
         */
        public static BitRateUnit fromSymbol(String symbol)
        {
            for (BitRateUnit value : values())
            {
                if (value.symbol.equalsIgnoreCase(symbol))
                    return value;
            }
            throw new IllegalArgumentException(String.format("Unsupported bit rate unit: %s. Supported units are: %s",
                                                             symbol, Arrays.stream(values())
                                                                           .map(u -> u.symbol)
                                                                           .collect(Collectors.joining(", "))));
        }

        /**
         * The unit symbol
         */
        private final String symbol;

        BitRateUnit(String symbol)
        {
            this.symbol = symbol;
        }

        public long toBitsPerSecond(long d)
        {
            throw new AbstractMethodError();
        }

        public long toKilobitsPerSecond(long d)
        {
            throw new AbstractMethodError();
        }

        public long toMegabitsPerSeconds(long d)
        {
            throw new AbstractMethodError();
        }

        public long convert(long source, BitRateUnit sourceUnit)
        {
            throw new AbstractMethodError();
        }
    }
}

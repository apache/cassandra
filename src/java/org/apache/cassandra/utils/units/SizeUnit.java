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
package org.apache.cassandra.utils.units;

import com.google.common.annotations.VisibleForTesting;

/**
 * A {@code SizeUnit} represents byte sizes at a given unit of granularity and provide utility methods to convert across
 * units. A {@code SizeUnit} does not maintain size information (see {@link SizeValue}), but only represent the unit
 * itself. A kilobyte is defined a 1024 bytes, a megabyte as 1024 kilobytes, etc...
 */
public enum SizeUnit
{
    BYTES("B")
    {
        public long convert(long s, SizeUnit u)
        {
            return u.toBytes(s);
        }

        public long toBytes(long s)
        {
            return s;
        }

        public long toKiloBytes(long s)
        {
            return s / (C1 / C0);
        }

        public long toMegaBytes(long s)
        {
            return s / (C2 / C0);
        }

        public long toGigaBytes(long s)
        {
            return s / (C3 / C0);
        }

        public long toTeraBytes(long s)
        {
            return s / (C4 / C0);
        }
    },
    KILOBYTES("kB")
    {
        public long convert(long s, SizeUnit u)
        {
            return u.toKiloBytes(s);
        }

        public long toBytes(long s)
        {
            return x(s, C1 / C0, MAX / (C1 / C0));
        }

        public long toKiloBytes(long s)
        {
            return s;
        }

        public long toMegaBytes(long s)
        {
            return s / (C2 / C1);
        }

        public long toGigaBytes(long s)
        {
            return s / (C3 / C1);
        }

        public long toTeraBytes(long s)
        {
            return s / (C4 / C1);
        }
    },
    MEGABYTES("MB")
    {
        public long convert(long s, SizeUnit u)
        {
            return u.toMegaBytes(s);
        }

        public long toBytes(long s)
        {
            return x(s, C2 / C0, MAX / (C2 / C0));
        }

        public long toKiloBytes(long s)
        {
            return x(s, C2 / C1, MAX / (C2 / C1));
        }

        public long toMegaBytes(long s)
        {
            return s;
        }

        public long toGigaBytes(long s)
        {
            return s / (C3 / C2);
        }

        public long toTeraBytes(long s)
        {
            return s / (C4 / C2);
        }
    },
    GIGABYTES("GB")
    {
        public long convert(long s, SizeUnit u)
        {
            return u.toGigaBytes(s);
        }

        public long toBytes(long s)
        {
            return x(s, C3 / C0, MAX / (C3 / C0));
        }

        public long toKiloBytes(long s)
        {
            return x(s, C3 / C1, MAX / (C3 / C1));
        }

        public long toMegaBytes(long s)
        {
            return x(s, C3 / C2, MAX / (C3 / C2));
        }

        public long toGigaBytes(long s)
        {
            return s;
        }

        public long toTeraBytes(long s)
        {
            return s / (C4 / C3);
        }
    },
    TERABYTES("TB")
    {
        public long convert(long s, SizeUnit u)
        {
            return u.toTeraBytes(s);
        }

        public long toBytes(long s)
        {
            return x(s, C4 / C0, MAX / (C4 / C0));
        }

        public long toKiloBytes(long s)
        {
            return x(s, C4 / C1, MAX / (C4 / C1));
        }

        public long toMegaBytes(long s)
        {
            return x(s, C4 / C2, MAX / (C4 / C2));
        }

        public long toGigaBytes(long s)
        {
            return x(s, C4 / C3, MAX / (C4 / C3));
        }

        public long toTeraBytes(long s)
        {
            return s;
        }
    };

    /**
     * The string symbol for that unit
     **/
    public final String symbol;

    SizeUnit(String symbol)
    {
        this.symbol = symbol;
    }

    // Handy constants for conversion methods (all are visible for testing)
    static final long C0 = 1L;
    static final long C1 = C0 * 1024L;
    static final long C2 = C1 * 1024L;
    static final long C3 = C2 * 1024L;
    static final long C4 = C3 * 1024L;

    private static final long MAX = Long.MAX_VALUE;

    /**
     * Scale d by m, checking for overflow.
     * This has a short name to make above code more readable.
     */
    @VisibleForTesting
    static long x(long d, long m, long over)
    {
        if (d > over) return Long.MAX_VALUE;
        if (d < -over) return Long.MIN_VALUE;
        return d * m;
    }

    /**
     * Convert the given size in the given unit to this unit. Conversions from finer to coarser granularities truncate,
     * so lose precision. For example converting {@code 1023} bytes to kilobytes results in {@code 0}. Conversions from
     * coarser to finer granularities with arguments that would numerically overflow saturate to <tt>Long.MIN_VALUE</tt>
     * if negative or <tt>Long.MAX_VALUE</tt> if positive.
     * <p>
     * For example, to convert 10 megabytes to bytes, use: {@code SizeUnit.BYTES.convert(10L, SizeUnit.MEGABYTES)}.
     *
     * @param sourceSize the size in the given {@code sourceUnit}.
     * @param sourceUnit the unit of the {@code sourceSize} argument
     * @return the converted size in this unit, or {@code Long.MIN_VALUE} if conversion would negatively overflow, or
     * {@code Long.MAX_VALUE} if it would positively overflow.
     */
    public abstract long convert(long sourceSize, SizeUnit sourceUnit);

    /**
     * Equivalent to {@code BYTES.convert(size, this)}.
     *
     * @param size the size
     * @return the converted size, or {@code Long.MIN_VALUE} if conversion would negatively overflow, or
     * {@code Long.MAX_VALUE} if it would positively overflow.
     * @see #convert
     */
    public abstract long toBytes(long size);

    /**
     * Equivalent to {@code KILOBYTES.convert(size, this)}.
     *
     * @param size the size
     * @return the converted size, or {@code Long.MIN_VALUE} if conversion would negatively overflow, or
     * {@code Long.MAX_VALUE} if it would positively overflow.
     * @see #convert
     */
    public abstract long toKiloBytes(long size);

    /**
     * Equivalent to {@code MEGABYTES.convert(size, this)}.
     *
     * @param size the size
     * @return the converted size, or {@code Long.MIN_VALUE} if conversion would negatively overflow, or
     * {@code Long.MAX_VALUE} if it would positively overflow.
     * @see #convert
     */
    public abstract long toMegaBytes(long size);

    /**
     * Equivalent to {@code GIGABYTES.convert(size, this)}.
     *
     * @param size the size
     * @return the converted size, or {@code Long.MIN_VALUE} if conversion would negatively overflow, or
     * {@code Long.MAX_VALUE} if it would positively overflow.
     * @see #convert
     */
    public abstract long toGigaBytes(long size);

    /**
     * Equivalent to {@code TERABYTES.convert(size, this)}.
     *
     * @param size the size
     * @return the converted size, or {@code Long.MIN_VALUE} if conversion would negatively overflow, or
     * {@code Long.MAX_VALUE} if it would positively overflow.
     * @see #convert
     */
    public abstract long toTeraBytes(long size);

    /**
     * Creates a {@link SizeValue} using the provided {@code value} and this unit.
     *
     * @param value the value.
     * @return a new {@link SizeValue} for {@code value} at this unit.
     */
    public SizeValue value(long value)
    {
        return SizeValue.of(value, this);
    }

    /**
     * Returns a Human Readable representation of the provided value in this unit.
     * <p>
     * Note that this method may discard precision for the sake of returning a more human readable value. In other
     * words, if {@code value} is large, it will be converted to a bigger, more readable unit, even this imply
     * truncating the value.
     *
     * @param value the value in this unit.
     * @return a potentially truncated but human readable representation of {@code value}.
     */
    public String toHumanReadableString(long value)
    {
        return Units.toString(value, this);
    }

    /**
     * Returns a string representation particularly suitable for logging a value. of this unit.
     * <p>
     * The returned representation combines the value displayed in bytes (for the sake of script parsing the log, so
     * they don't have to bother with unit conversion), followed by the representation from {@link #toHumanReadableString} for
     * humans.
     *
     * @param value the value in this unit.
     * @return a string representation suitable for logging the value.
     */
    public String toLogString(long value)
    {
        return Units.toLogString(value, this);
    }

    /**
     * Returns a string representation of a value in this unit.
     *
     * @param value the value in this unit.
     * @return a string representation of {@code value} in this unit.
     */
    public String toString(long value)
    {
        return Units.formatValue(value) + symbol;
    }

    /**
     * Given a value in this unit, returns the smallest (most fine grained) unit in which that value can be represented
     * without overflowing.
     *
     * @param value the value in this unit.
     * @return the smallest unit, potentially this unit, at which the value can be represented without overflowing. If
     * {@code value == Long.MAX_VALUE}, then this unit is returned.
     */
    SizeUnit smallestRepresentableUnit(long value)
    {
        int i = ordinal();
        while (i > 0 && value < Long.MAX_VALUE)
        {
            value = x(value, C1, MAX / C1);
            i--;
        }
        return SizeUnit.values()[i];
    }
}

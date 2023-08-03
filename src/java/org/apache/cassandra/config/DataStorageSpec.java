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

import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.BYTES;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.KIBIBYTES;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.MEBIBYTES;

/**
 * Represents an amount of data storage. Wrapper class for Cassandra configuration parameters, providing to the
 * users the opportunity to be able to provide config with a unit of their choice in cassandra.yaml as per the available
 * options. (CASSANDRA-15234)
 */
public abstract class DataStorageSpec
{
    /**
     * The Regexp used to parse the storage provided as String.
     */
    private static final Pattern UNITS_PATTERN = Pattern.compile("^(\\d+)(GiB|MiB|KiB|B)$");

    private final long quantity;

    private final DataStorageUnit unit;

    private DataStorageSpec(long quantity, DataStorageUnit unit, DataStorageUnit minUnit, long max, String value)
    {
        this.quantity = quantity;
        this.unit = unit;

        validateMinUnit(unit, minUnit, value);
        validateQuantity(quantity, unit, minUnit, max);
    }

    private DataStorageSpec(String value, DataStorageUnit minUnit)
    {
        //parse the string field value
        Matcher matcher = UNITS_PATTERN.matcher(value);

        if (matcher.find())
        {
            quantity = Long.parseLong(matcher.group(1));
            unit = DataStorageUnit.fromSymbol(matcher.group(2));

            // this constructor is used only by extended classes for min unit; upper bound and min unit are guarded there accordingly
        }
        else
        {
            throw new IllegalArgumentException("Invalid data storage: " + value + " Accepted units:" + acceptedUnits(minUnit) +
                                               " where case matters and only non-negative values are accepted");
        }
    }

    private DataStorageSpec(String value, DataStorageUnit minUnit, long max)
    {
        this(value, minUnit);

        validateMinUnit(unit, minUnit, value);
        validateQuantity(value, quantity(), unit(), minUnit, max);
    }

    private static void validateMinUnit(DataStorageUnit sourceUnit, DataStorageUnit minUnit, String value)
    {
        if (sourceUnit.compareTo(minUnit) < 0)
            throw new IllegalArgumentException(String.format("Invalid data storage: %s Accepted units:%s", value, acceptedUnits(minUnit)));
    }

    private static String acceptedUnits(DataStorageUnit minUnit)
    {
        DataStorageUnit[] units = DataStorageUnit.values();
        return Arrays.toString(Arrays.copyOfRange(units, minUnit.ordinal(), units.length));
    }

    private static void validateQuantity(String value, long quantity, DataStorageUnit sourceUnit, DataStorageUnit minUnit, long max)
    {
        // no need to validate for negatives as they are not allowed at first place from the regex

        if (minUnit.convert(quantity, sourceUnit) >= max)
            throw new IllegalArgumentException("Invalid data storage: " + value + ". It shouldn't be more than " +
                                               (max - 1) + " in " + minUnit.name().toLowerCase());
    }

    private static void validateQuantity(long quantity, DataStorageUnit sourceUnit, DataStorageUnit minUnit, long max)
    {
        if (quantity < 0)
            throw new IllegalArgumentException("Invalid data storage: value must be non-negative");

        if (minUnit.convert(quantity, sourceUnit) >= max)
            throw new IllegalArgumentException(String.format("Invalid data storage: %d %s. It shouldn't be more than %d in %s",
                                                             quantity, sourceUnit.name().toLowerCase(),
                                                             max - 1, minUnit.name().toLowerCase()));
    }

    // get vs no-get prefix is not consistent in the code base, but for classes involved with config parsing, it is
    // imporant to be explicit about get/set as this changes how parsing is done; this class is a data-type, so is
    // not nested, having get/set can confuse parsing thinking this is a nested type
    /**
     * @return the data storage quantity.
     */
    public long quantity()
    {
        return quantity;
    }

    /**
     * @return the data storage unit.
     */
    public DataStorageUnit unit()
    {
        return unit;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(unit.toKibibytes(quantity));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;

        if (!(obj instanceof DataStorageSpec))
            return false;

        DataStorageSpec other = (DataStorageSpec) obj;
        if (unit == other.unit)
            return quantity == other.quantity;

        // Due to overflows we can only guarantee that the 2 storages are equal if we get the same results
        // doing the convertion in both directions.
        return unit.convert(other.quantity, other.unit) == quantity && other.unit.convert(quantity, unit) == other.quantity;
    }

    @Override
    public String toString()
    {
        return quantity + unit.symbol;
    }

    /**
     * Represents a data storage quantity used for Cassandra configuration. The bound is [0, Long.MAX_VALUE) in bytes.
     * If the user sets a different unit - we still validate that converted to bytes the quantity will not exceed
     * that upper bound. (CASSANDRA-17571)
     */
    public final static class LongBytesBound extends DataStorageSpec
    {
        /**
         * Creates a {@code DataStorageSpec.LongBytesBound} of the specified amount.
         *
         * @param value the data storage
         */
        public LongBytesBound(String value)
        {
            super(value, BYTES, Long.MAX_VALUE);
        }

        /**
         * Creates a {@code DataStorageSpec.LongBytesBound} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Long.MAX_VALUE - 1 in bytes
         * @param unit in which the provided quantity is
         */
        public LongBytesBound(long quantity, DataStorageUnit unit)
        {
            super(quantity, unit, BYTES, Long.MAX_VALUE, quantity + unit.symbol);
        }

        /**
         * Creates a {@code DataStorageSpec.LongBytesBound} of the specified amount in bytes.
         *
         * @param bytes where bytes shouldn't be bigger than Long.MAX_VALUE-1
         */
        public LongBytesBound(long bytes)
        {
            this(bytes, BYTES);
        }

        /**
         * @return the amount of data storage in bytes
         */
        public long toBytes()
        {
            return unit().toBytes(quantity());
        }

        /**
         * @return the amount of data storage in mebibytes
         */
        public int toMebibytesInt()
        {
            return Ints.saturatedCast(unit().toMebibytes(quantity()));
        }
    }

    /**
     * Represents a data storage quantity used for Cassandra configuration. The bound is [0, Integer.MAX_VALUE) in bytes.
     * If the user sets a different unit - we still validate that converted to bytes the quantity will not exceed
     * that upper bound. (CASSANDRA-17571)
     */
    public final static class IntBytesBound extends DataStorageSpec
    {
        /**
         * Creates a {@code DataStorageSpec.IntBytesBound} of the specified amount.
         *
         * @param value the data storage
         */
        public IntBytesBound(String value)
        {
            super(value, BYTES, Integer.MAX_VALUE);
        }

        /**
         * Creates a {@code DataStorageSpec.IntBytesBound} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Integer.MAX_VALUE - 1 in bytes
         * @param unit in which the provided quantity is
         */
        public IntBytesBound(long quantity, DataStorageUnit unit)
        {
            super(quantity, unit, BYTES, Integer.MAX_VALUE, quantity + unit.symbol);
        }

        /**
         * Creates a {@code DataStorageSpec.IntBytesBound} of the specified amount in bytes.
         *
         * @param bytes where bytes shouldn't be bigger than Integer.MAX_VALUE-1
         */
        public IntBytesBound(long bytes)
        {
            this(bytes, BYTES);
        }

        /**
         * Returns the amount of data storage in bytes as an {@code int}
         *
         * @return the amount of data storage in bytes or {@code Integer.MAX_VALUE} if the number of bytes is too large.
         */
        public int toBytes()
        {
            return Ints.saturatedCast(unit().toBytes(quantity()));
        }
    }

    /**
     * Represents a data storage quantity used for Cassandra configuration. The bound is [0, Integer.MAX_VALUE) in kibibytes.
     * If the user sets a different unit - we still validate that converted to kibibytes the quantity will not exceed
     * that upper bound. (CASSANDRA-17571)
     */
    public final static class IntKibibytesBound extends DataStorageSpec
    {
        /**
         * Creates a {@code DataStorageSpec.IntKibibytesBound} of the specified amount.
         *
         * @param value the data storage
         */
        public IntKibibytesBound(String value)
        {
            super(value, KIBIBYTES, Integer.MAX_VALUE);
        }

        /**
         * Creates a {@code DataStorageSpec.IntKibibytesBound} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Integer.MAX_VALUE - 1 in kibibytes
         * @param unit in which the provided quantity is
         */
        public IntKibibytesBound(long quantity, DataStorageUnit unit)
        {
            super(quantity, unit, KIBIBYTES, Integer.MAX_VALUE, quantity + unit.symbol);
        }

        /**
         * Creates a {@code DataStorageSpec.IntKibibytesBound} of the specified amount in kibibytes.
         *
         * @param kibibytes where kibibytes shouldn't be bigger than Integer.MAX_VALUE-1
         */
        public IntKibibytesBound(long kibibytes)
        {
            this(kibibytes, KIBIBYTES);
        }

        /**
         * Returns the amount of data storage in bytes as an {@code int}
         *
         * @return the amount of data storage in bytes or {@code Integer.MAX_VALUE} if the number of bytes is too large.
         */
        public int toBytes()
        {
            return Ints.saturatedCast(unit().toBytes(quantity()));
        }

        /**
         * Returns the amount of data storage in kibibytes as an {@code int}
         *
         * @return the amount of data storage in kibibytes or {@code Integer.MAX_VALUE} if the number of kibibytes is too large.
         */
        public int toKibibytes()
        {
            return Ints.saturatedCast(unit().toKibibytes(quantity()));
        }

        /**
         * @return the amount of data storage in bytes.
         */
        public long toBytesInLong()
        {
           return unit().toBytes(quantity());
        }
    }

    /**
     * Represents a data storage quantity used for Cassandra configuration. The bound is [0, Long.MAX_VALUE) in mebibytes.
     * If the user sets a different unit - we still validate that converted to mebibytes the quantity will not exceed
     * that upper bound. (CASSANDRA-17571)
     */
    public final static class LongMebibytesBound extends DataStorageSpec
    {
        /**
         * Creates a {@code DataStorageSpec.LongMebibytesBound} of the specified amount.
         *
         * @param value the data storage
         */
        public LongMebibytesBound(String value)
        {
            super(value, MEBIBYTES, Long.MAX_VALUE);
        }

        /**
         * Creates a {@code DataStorageSpec.LongMebibytesBound} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Long.MAX_VALUE - 1 in mebibytes
         * @param unit in which the provided quantity is
         */
        public LongMebibytesBound(long quantity, DataStorageUnit unit)
        {
            super(quantity, unit, MEBIBYTES, Long.MAX_VALUE, quantity + unit.symbol);
        }

        /**
         * Creates a {@code DataStorageSpec.LongMebibytesBound} of the specified amount in mebibytes.
         *
         * @param mebibytes where mebibytes shouldn't be bigger than Long.MAX_VALUE-1
         */
        public LongMebibytesBound(long mebibytes)
        {
            this(mebibytes, MEBIBYTES);
        }

        /**
         * @return the amount of data storage in bytes
         */
        public long toBytes()
        {
            return unit().toBytes(quantity());
        }

        /**
         * @return the amount of data storage in kibibytes
         */
        public long toKibibytes()
        {
            return unit().toKibibytes(quantity());
        }

        /**
         * @return the amount of data storage in mebibytes
         */
        public long toMebibytes()
        {
            return unit().toMebibytes(quantity());
        }
    }

    /**
     * Represents a data storage quantity used for Cassandra configuration. The bound is [0, Integer.MAX_VALUE) in mebibytes.
     * If the user sets a different unit - we still validate that converted to mebibytes the quantity will not exceed
     * that upper bound. (CASSANDRA-17571)
     */
    public final static class IntMebibytesBound extends DataStorageSpec
    {
        /**
         * Creates a {@code DataStorageSpec.IntMebibytesBound} of the specified amount.
         *
         * @param value the data storage
         */
        public IntMebibytesBound(String value)
        {
            super(value, MEBIBYTES, Integer.MAX_VALUE);
        }

        /**
         * Creates a {@code DataStorageSpec.IntMebibytesBound} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Integer.MAX_VALUE - 1 in mebibytes
         * @param unit in which the provided quantity is
         */
        public IntMebibytesBound(long quantity, DataStorageUnit unit)
        {
            super(quantity, unit, MEBIBYTES, Integer.MAX_VALUE, quantity + unit.symbol);
        }

        /**
         * Creates a {@code DataStorageSpec.IntMebibytesBound} of the specified amount in mebibytes.
         *
         * @param mebibytes where mebibytes shouldn't be bigger than Integer.MAX_VALUE-1
         */
        public IntMebibytesBound(long mebibytes)
        {
            this(mebibytes, MEBIBYTES);
        }

        /**
         * Returns the amount of data storage in bytes as an {@code int}
         *
         * @return the amount of data storage in bytes or {@code Integer.MAX_VALUE} if the number of bytes is too large.
         */
        public int toBytes()
        {
            return Ints.saturatedCast(unit().toBytes(quantity()));
        }

        /**
         * Returns the amount of data storage in kibibytes as an {@code int}
         *
         * @return the amount of data storage in kibibytes or {@code Integer.MAX_VALUE} if the number of kibibytes is too large.
         */
        public int toKibibytes()
        {
            return Ints.saturatedCast(unit().toKibibytes(quantity()));
        }

        /**
         * Returns the amount of data storage in mebibytes as an {@code int}
         *
         * @return the amount of data storage in mebibytes or {@code Integer.MAX_VALUE} if the number of mebibytes is too large.
         */
        public int toMebibytes()
        {
            return Ints.saturatedCast(unit().toMebibytes(quantity()));
        }

        /**
         * Returns the amount of data storage in bytes as {@code long}
         *
         * @return the amount of data storage in bytes.
         */
        public long toBytesInLong()
        {
            return unit().toBytes(quantity());
        }
    }

    public enum DataStorageUnit
    {
        BYTES("B")
        {
            public long toBytes(long d)
            {
                return d;
            }

            public long toKibibytes(long d)
            {
                return (d / 1024L);
            }

            public long toMebibytes(long d)
            {
                return (d / (1024L * 1024));
            }

            public long toGibibytes(long d)
            {
                return (d / (1024L * 1024 * 1024));
            }

            public long convert(long source, DataStorageUnit sourceUnit)
            {
                return sourceUnit.toBytes(source);
            }
        },
        KIBIBYTES("KiB")
        {
            public long toBytes(long d)
            {
                return x(d, 1024L, (MAX / 1024L));
            }

            public long toKibibytes(long d)
            {
                return d;
            }

            public long toMebibytes(long d)
            {
                return (d / 1024L);
            }

            public long toGibibytes(long d)
            {
                return (d / (1024L * 1024));
            }

            public long convert(long source, DataStorageUnit sourceUnit)
            {
                return sourceUnit.toKibibytes(source);
            }
        },
        MEBIBYTES("MiB")
        {
            public long toBytes(long d)
            {
                return x(d, (1024L * 1024), MAX / (1024L * 1024));
            }

            public long toKibibytes(long d)
            {
                return x(d, 1024L, (MAX / 1024L));
            }

            public long toMebibytes(long d)
            {
                return d;
            }

            public long toGibibytes(long d)
            {
                return (d / 1024L);
            }

            public long convert(long source, DataStorageUnit sourceUnit)
            {
                return sourceUnit.toMebibytes(source);
            }
        },
        GIBIBYTES("GiB")
        {
            public long toBytes(long d)
            {
                return x(d, (1024L * 1024 * 1024), MAX / (1024L * 1024 * 1024));
            }

            public long toKibibytes(long d)
            {
                return x(d, (1024L * 1024), MAX / (1024L * 1024));
            }

            public long toMebibytes(long d)
            {
                return x(d, 1024L, (MAX / 1024L));
            }

            public long toGibibytes(long d)
            {
                return d;
            }

            public long convert(long source, DataStorageUnit sourceUnit)
            {
                return sourceUnit.toGibibytes(source);
            }
        };

        /**
         * Scale d by m, checking for overflow. This has a short name to make above code more readable.
         */
        static long x(long d, long m, long over)
        {
            assert (over > 0) && (over < (MAX-1L)) && (over == (MAX / m));

            if (d > over)
                return Long.MAX_VALUE;
            return Math.multiplyExact(d, m);
        }

        /**
         * @param symbol the unit symbol
         * @return the memory unit corresponding to the given symbol
         */
        public static DataStorageUnit fromSymbol(String symbol)
        {
            for (DataStorageUnit value : values())
            {
                if (value.symbol.equalsIgnoreCase(symbol))
                    return value;
            }
            throw new IllegalArgumentException(String.format("Unsupported data storage unit: %s. Supported units are: %s",
                                                           symbol, Arrays.stream(values())
                                                                         .map(u -> u.symbol)
                                                                         .collect(Collectors.joining(", "))));
        }

        static final long MAX = Long.MAX_VALUE;

        /**
         * The unit symbol
         */
        private final String symbol;

        DataStorageUnit(String symbol)
        {
            this.symbol = symbol;
        }

        public long toBytes(long d)
        {
            throw new AbstractMethodError();
        }

        public long toKibibytes(long d)
        {
            throw new AbstractMethodError();
        }

        public long toMebibytes(long d)
        {
            throw new AbstractMethodError();
        }

        public long toGibibytes(long d)
        {
            throw new AbstractMethodError();
        }

        public long convert(long source, DataStorageUnit sourceUnit)
        {
            throw new AbstractMethodError();
        }
    }
}

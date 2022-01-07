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

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Represents an amount of data storage. Wrapper class for Cassandra configuration parameters, providing to the
 * users the opportunity to be able to provide config with a unit of their choice in cassandra.yaml as per the available
 * options. (CASSANDRA-15234)
 */
public final class DataStorageSpec
{
    /**
     * The Regexp used to parse the storage provided as String.
     */
    private static final Pattern STORAGE_UNITS_PATTERN = Pattern.compile("^(\\d+)(GiB|MiB|KiB|B)$");

    private final long quantity;

    private final DataStorageUnit unit;

    public DataStorageSpec(String value)
    {
        if (value == null || value.equals("null"))
        {
            quantity = 0;
            unit = DataStorageUnit.MEBIBYTES; // the unit doesn't really matter as 0 is 0 in all units
            return;
        }

        //parse the string field value
        Matcher matcher = STORAGE_UNITS_PATTERN.matcher(value);

        if (!matcher.find())
        {
            throw new ConfigurationException("Invalid data storage: " + value + " Accepted units: B, KiB, MiB, GiB" +
                                               " where case matters and only non-negative values are accepted");
        }

        quantity = Long.parseLong(matcher.group(1));
        unit = DataStorageUnit.fromSymbol(matcher.group(2));
    }

    DataStorageSpec(long quantity, DataStorageUnit unit)
    {
        if (quantity < 0)
            throw new ConfigurationException("DataStorage value must be positive");

        this.quantity = quantity;
        this.unit = unit;
    }

    /**
     * Creates a {@code DataStorageSpec} of the specified amount of bytes.
     *
     * @param bytes the amount of bytes
     * @return a {@code DataStorageSpec}
     */
    public static DataStorageSpec inBytes(long bytes)
    {
        return new DataStorageSpec(bytes, DataStorageUnit.BYTES);
    }

    /**
     * Creates a {@code DataStorageSpec} of the specified amount of kibibytes.
     *
     * @param kibibytes the amount of kibibytes
     * @return a {@code DataStorageSpec}
     */
    public static DataStorageSpec inKibibytes(long kibibytes)
    {
        return new DataStorageSpec(kibibytes, DataStorageUnit.KIBIBYTES);
    }

    /**
     * Creates a {@code DataStorageSpec} of the specified amount of mebibytes.
     *
     * @param mebibytes the amount of mebibytes
     * @return a {@code DataStorageSpec}
     */
    public static DataStorageSpec inMebibytes(long mebibytes)
    {
        return new DataStorageSpec(mebibytes, DataStorageUnit.MEBIBYTES);
    }

    /**
     * @return the data storage unit.
     */
    public DataStorageUnit getUnit()
    {
        return unit;
    }

    /**
     * @return the amount of data storage in bytes
     */
    public long toBytes()
    {
        return unit.toBytes(quantity);
    }

    /**
     * Returns the amount of data storage in bytes as an {@code int}
     *
     * @return the amount of data storage in bytes or {@code Integer.MAX_VALUE} if the number of bytes is too large.
     */
    public int toBytesAsInt()
    {
        return Ints.saturatedCast(toBytes());
    }

    /**
     * @return the amount of data storage in kibibytes
     */
    public long toKibibytes()
    {
        return unit.toKibibytes(quantity);
    }

    /**
     * Returns the amount of data storage in kibibytes as an {@code int}
     *
     * @return the amount of data storage in kibibytes or {@code Integer.MAX_VALUE} if the number of kibibytes is too large.
     */
    public int toKibibytesAsInt()
    {
        return Ints.saturatedCast(toKibibytes());
    }

    /**
     * @return the amount of data storage in mebibytes
     */
    public long toMebibytes()
    {
        return unit.toMebibytes(quantity);
    }

    /**
     * Returns the amount of data storage in mebibytes as an {@code int}
     *
     * @return the amount of data storage in mebibytes or {@code Integer.MAX_VALUE} if the number of mebibytes is too large.
     */
    public int toMebibytesAsInt()
    {
        return Ints.saturatedCast(toMebibytes());
    }

    /**
     * @return the amount of data storage in gibibytes
     */
    public long toGibibytes()
    {
        return unit.toGibibytes(quantity);
    }

    /**
     * Returns the amount of data storage in gibibytes as an {@code int}
     *
     * @return the amount of data storage in gibibytes or {@code Integer.MAX_VALUE} if the number of gibibytes is too large.
     */
    public int toGibibytesAsInt()
    {
        return Ints.saturatedCast(toGibibytes());
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
                return d / 1024;
            }

            public long toMebibytes(long d)
            {
                return d / (1024 * 1024);
            }

            public long toGibibytes(long d)
            {
                return d / (1024 * 1024 * 1024);
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
                return x(d, 1024, MAX / 1024);
            }

            public long toKibibytes(long d)
            {
                return d;
            }

            public long toMebibytes(long d)
            {
                return d / 1024;
            }

            public long toGibibytes(long d)
            {
                return d / (1024 * 1024);
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
                return x(d, 1024 * 1024, MAX / (1024 * 1024));
            }

            public long toKibibytes(long d)
            {
                return x(d, 1024, MAX / 1024);
            }

            public long toMebibytes(long d)
            {
                return d;
            }

            public long toGibibytes(long d)
            {
                return d / 1024;
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
                return x(d, 1024 * 1024 * 1024, MAX / (1024 * 1024 * 1024));
            }

            public long toKibibytes(long d)
            {
                return x(d, 1024 * 1024, MAX / 1024 * 1024);
            }

            public long toMebibytes(long d)
            {
                return x(d, 1024, MAX / 1024);
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
            if (d > over)
                return Long.MAX_VALUE;
            return d * m;
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
            throw new ConfigurationException(String.format("Unsupported data storage unit: %s. Supported units are: %s",
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

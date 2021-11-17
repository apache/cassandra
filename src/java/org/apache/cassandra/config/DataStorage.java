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
 * Represents an amount of data storage. Wrapper class for Cassandra configuration parameters, providing to the
 * users the opportunity to be able to provide config with a unit of their choice in cassandra.yaml as per the available
 * options. (CASSANDRA-15234)
 */
public final class DataStorage
{
    /**
     * The Regexp used to parse the storage provided as String.
     */
    private static final Pattern STORAGE_UNITS_PATTERN = Pattern.compile("^(\\d+)(kb|KB|mb|MB|gb|GB|b|B)$");

    private final long quantity;

    private final DataStorageUnit unit;

    public DataStorage(String value)
    {
        if(value == null || value.equals("null"))
        {
            quantity = 0;
            unit = DataStorageUnit.MEGABYTES; // the unit doesn't really matter as 0 is 0 in all units
            return;
        }
        //parse the string field value
        Matcher matcher = STORAGE_UNITS_PATTERN.matcher(value);

        if (!matcher.find())
        {
            throw new IllegalArgumentException("Invalid data storage: " + value);
        }

        quantity = Long.parseLong(matcher.group(1));
        unit = DataStorageUnit.fromSymbol(matcher.group(2));
    }

    private DataStorage(long quantity, DataStorageUnit unit)
    {
        this.quantity = quantity;
        this.unit = unit;
    }

    /**
     * Creates a {@code DataStorage} of the specified amount of bytes.
     *
     * @param bytes the amount of bytes
     * @return a {@code DataStorage}
     */
    public static DataStorage inBytes(long bytes)
    {
        return new DataStorage(bytes, DataStorageUnit.BYTES);
    }

    /**
     * Creates a {@code DataStorage} of the specified amount of kilobytes.
     *
     * @param kilobytes the amount of kilobytes
     * @return a {@code DataStorage}
     */
    public static DataStorage inKilobytes(long kilobytes)
    {
        return new DataStorage(kilobytes, DataStorageUnit.KILOBYTES);
    }

    /**
     * Creates a {@code DataStorage} of the specified amount of megabytes.
     *
     * @param megabytes the amount of megabytes
     * @return a {@code DataStorage}
     */
    public static DataStorage inMegabytes(long megabytes)
    {
        return new DataStorage(megabytes, DataStorageUnit.MEGABYTES);
    }

    /**
     * Returns the data storage unit.
     *
     * @return the data storage unit.
     */
    public DataStorageUnit getUnit()
    {
        return unit;
    }

    /**
     * Returns the amount of data storage in bytes
     *
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
     * Returns the amount of data storage in kilobytes
     *
     * @return the amount of data storage in kilobytes
     */
    public long toKilobytes()
    {
        return unit.toKilobytes(quantity);
    }

    /**
     * Returns the amount of data storage in kilobytes as an {@code int}
     *
     * @return the amount of data storage in kilobytes or {@code Integer.MAX_VALUE} if the number of kilobytes is too large.
     */
    public int toKilobytesAsInt()
    {
        return Ints.saturatedCast(toKilobytes());
    }

    /**
     * Returns the amount of data storage in megabytes
     *
     * @return the amount of data storage in megabytes
     */
    public long toMegabytes()
    {
        return unit.toMegabytes(quantity);
    }

    /**
     * Returns the amount of data storage in megabytes as an {@code int}
     *
     * @return the amount of data storage in megabytes or {@code Integer.MAX_VALUE} if the number of megabytes is too large.
     */
    public int toMegabytesAsInt()
    {
        return Ints.saturatedCast(toMegabytes());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(unit.toKilobytes(quantity));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;

        if (!(obj instanceof DataStorage))
            return false;

        DataStorage other = (DataStorage) obj;
        if (unit == other.unit)
            return quantity == other.quantity;

        // Due to overflows we can only guarantee that the 2 storages are equals if we get the same results
        // doing the convertion in both directions.
        return unit.convert(other.quantity, other.unit) == quantity && other.unit.convert(quantity, unit) == other.quantity;
    }

    @Override
    public String toString()
    {
        return quantity + unit.symbol;
    }

    public String quantityToString()
    {
        return String.valueOf(quantity);
    }

    public enum DataStorageUnit
    {
        BYTES("B")
        {
            public long toBytes(long d)
            {
                return d;
            }

            public long toKilobytes(long d)
            {
                return d / 1024;
            }

            public long toMegabytes(long d)
            {
                return d / (1024 * 1024);
            }

            public long toGigabytes(long d)
            {
                return d / (1024 * 1024 * 1024);
            }

            public long convert(long source, DataStorageUnit sourceUnit)
            {
                return sourceUnit.toBytes(source);
            }
        },
        KILOBYTES("KB")
        {
            public long toBytes(long d)
            {
                return x(d, 1024, MAX / 1024);
            }

            public long toKilobytes(long d)
            {
                return d;
            }

            public long toMegabytes(long d)
            {
                return d / 1024;
            }

            public long toGigabytes(long d)
            {
                return d / (1024 * 1024);
            }

            public long convert(long source, DataStorageUnit sourceUnit)
            {
                return sourceUnit.toKilobytes(source);
            }
        },
        MEGABYTES("MB")
        {
            public long toBytes(long d)
            {
                return x(d, 1024 * 1024, MAX / (1024 * 1024));
            }

            public long toKilobytes(long d)
            {
                return x(d, 1024, MAX / 1024);
            }

            public long toMegabytes(long d)
            {
                return d;
            }

            public long toGigabytes(long d)
            {
                return d / 1024;
            }

            public long convert(long source, DataStorageUnit sourceUnit)
            {
                return sourceUnit.toMegabytes(source);
            }
        },
        GIGABYTES("GB")
        {
            public long toBytes(long d)
            {
                return x(d, 1024 * 1024 * 1024, MAX / (1024 * 1024 * 1024));
            }

            public long toKilobytes(long d)
            {
                return x(d, 1024 * 1024, MAX / 1024 * 1024);
            }

            public long toMegabytes(long d)
            {
                return x(d, 1024, MAX / 1024);
            }

            public long toGigabytes(long d)
            {
                return d;
            }

            public long convert(long source, DataStorageUnit sourceUnit)
            {
                return sourceUnit.toGigabytes(source);
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
         * Returns the memory unit corresponding to the given symbol. 
         *
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
        private String symbol;

        DataStorageUnit(String symbol)
        {
            this.symbol = symbol;
        }

        public long toBytes(long d)
        {
            throw new AbstractMethodError();
        }

        public long toKilobytes(long d)
        {
            throw new AbstractMethodError();
        }

        public long toMegabytes(long d)
        {
            throw new AbstractMethodError();
        }

        public long toGigabytes(long d)
        {
            throw new AbstractMethodError();
        }

        public long convert(long source, DataStorageUnit sourceUnit)
        {
            throw new AbstractMethodError();
        }
    }
}

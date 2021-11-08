/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.config;

public interface Converter<Original, Current>
{
    Class<Original> getInputType();

    Current apply(Original value);

    public static final class IdentityConverter implements Converter<Object, Object>
    {
        public Class<Object> getInputType()
        {
            return null; // null means 'unchanged'  mostly used for renames
        }

        public Object apply(Object value)
        {
            return value;
        }
    }

    public static final class MillisDurationConverter implements Converter<Long, CassandraDuration>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public CassandraDuration apply(Long value)
        {
            if (value == null)
                return null;
            return CassandraDuration.inMilliseconds(value);
        }
    }

    public static final class MillisDurationInDoubleConverter implements Converter<Double, CassandraDuration>
    {

        public Class<Double> getInputType()
        {
            return Double.class;
        }

        public CassandraDuration apply(Double value)
        {
            if (value == null)
                return null;
            return CassandraDuration.inMilliseconds((long)value.doubleValue());
        }
    }

    public static final class MillisDurationConverterCustom implements Converter<Long, CassandraDuration>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public CassandraDuration apply(Long value)
        {
            if (value == null)
                return null;

            if (value.equals((long) -1))
                value = 0L;

            return CassandraDuration.inMilliseconds(value);
        }
    }

    public static final class SecondsDurationConverter implements Converter<Long, CassandraDuration>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public CassandraDuration apply(Long value)
        {
            if (value == null)
                return null;
            return CassandraDuration.inSeconds(value);
        }
    }

    public static final class MinutesDurationConverter implements Converter<Long, CassandraDuration>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public CassandraDuration apply(Long value)
        {
            if (value == null)
                return null;
            return CassandraDuration.inMinutes(value);
        }
    }

    public static final class MegabytesDataStorageConverter implements Converter<Long, DataStorage>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public DataStorage apply(Long value)
        {
            if (value == null)
                return null;
            return DataStorage.inMegabytes(value);
        }
    }

    public static final class KilobytesDataStorageConverter implements Converter<Long, DataStorage>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public DataStorage apply(Long value)
        {
            if (value == null)
                return null;
            return DataStorage.inKilobytes(value);
        }
    }

    public static final class BytesDataStorageConverter implements Converter<Long, DataStorage>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public DataStorage apply(Long value)
        {
            if (value == null)
                return null;
            return DataStorage.inBytes(value);
        }
    }

    public static final class MegabitsPerSecondBitRateConverter implements Converter<Long, BitRate>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public BitRate apply(Long value)
        {
            if (value == null)
                return null;
            return BitRate.inMegabitsPerSecond(value);
        }
    }
}

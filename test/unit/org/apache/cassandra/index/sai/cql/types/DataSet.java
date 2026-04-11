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
package org.apache.cassandra.index.sai.cql.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.serializers.TimeSerializer;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.index.sai.cql.types.IndexingTypeSupport.NUMBER_OF_VALUES;

public abstract class DataSet<T> extends SAITester
{
    public T[] values;

    public void init()
    {
        // used to create UDT
    }

    public abstract QuerySet querySet();

    public Collection<String> decorateIndexColumn(String column)
    {
        return Collections.singletonList(column);
    }

    public static abstract class NumericDataSet<T extends Number> extends DataSet<T>
    {
        NumericDataSet()
        {
            values = emptyValues();
            List<T> list = Arrays.asList(values);
            for (int index = 0; index < values.length; index += 2)
            {
                T value1, value2;
                do
                {
                    value1 = nextValue();
                    value1 = getRandom().nextBoolean() ? negate(value1) : abs(value1);
                    value2 = increment(value1);
                }
                while (list.contains(value1) || list.contains(value2));
                values[index] = value1;
                values[index + 1] = value2;
            }
            Arrays.sort(values);
        }

        abstract T[] emptyValues();

        abstract T nextValue();

        abstract T negate(T value);

        abstract T abs(T value);

        abstract T increment(T value);

        public QuerySet querySet()
        {
            return new QuerySet.NumericQuerySet();
        }
    }

    public static class IntDataSet extends NumericDataSet<Integer>
    {
        @Override
        Integer[] emptyValues()
        {
            return new Integer[NUMBER_OF_VALUES];
        }

        @Override
        Integer nextValue()
        {
            return getRandom().nextInt();
        }

        @Override
        Integer negate(Integer value)
        {
            return value > 0 ? -value : value;
        }

        @Override
        Integer abs(Integer value)
        {
            return value < 0 ? Math.abs(value) : value;
        }

        @Override
        Integer increment(Integer value)
        {
            return ++value;
        }

        public String toString()
        {
            return "int";
        }
    }

    public static class BigintDataSet extends NumericDataSet<Long>
    {
        @Override
        Long[] emptyValues()
        {
            return new Long[NUMBER_OF_VALUES];
        }

        @Override
        Long nextValue()
        {
            return getRandom().nextLong();
        }

        @Override
        Long negate(Long value)
        {
            return value > 0 ? -value : value;
        }

        @Override
        Long abs(Long value)
        {
            return value < 0 ? Math.abs(value) : value;
        }

        @Override
        Long increment(Long value)
        {
            return ++value;
        }

        public String toString()
        {
            return "bigint";
        }
    }

    public static class SmallintDataSet extends NumericDataSet<Short>
    {
        @Override
        Short[] emptyValues()
        {
            return new Short[NUMBER_OF_VALUES];
        }

        @Override
        Short nextValue()
        {
            return getRandom().nextShort();
        }

        @Override
        Short negate(Short value)
        {
            return value > 0 ? (short)-value : value;
        }

        @Override
        Short abs(Short value)
        {
            return value < 0 ? (short)Math.abs(value) : value;
        }

        @Override
        Short increment(Short value)
        {
            return ++value;
        }

        public String toString()
        {
            return "smallint";
        }
    }

    public static class TinyintDataSet extends NumericDataSet<Byte>
    {
        @Override
        Byte[] emptyValues()
        {
            return new Byte[NUMBER_OF_VALUES];
        }

        @Override
        Byte nextValue()
        {
            return getRandom().nextByte();
        }

        @Override
        Byte negate(Byte value)
        {
            return value > 0 ? (byte)-value : value;
        }

        @Override
        Byte abs(Byte value)
        {
            return value < 0 ? (byte)Math.abs(value) : value;
        }

        @Override
        Byte increment(Byte value)
        {
            return ++value;
        }

        public String toString()
        {
            return "tinyint";
        }
    }

    public static class VarintDataSet extends NumericDataSet<BigInteger>
    {
        @Override
        BigInteger[] emptyValues()
        {
            return new BigInteger[NUMBER_OF_VALUES];
        }

        @Override
        BigInteger nextValue()
        {
            return getRandom().nextBigInteger(16, 512);
        }

        @Override
        BigInteger negate(BigInteger value)
        {
            return value.signum() > 0 ? value.negate() : value;
        }

        @Override
        BigInteger abs(BigInteger value)
        {
            return value.signum() < 0 ? value.abs() : value;
        }

        @Override
        BigInteger increment(BigInteger value)
        {
            return value.add(BigInteger.ONE);
        }

        public String toString()
        {
            return "varint";
        }
    }

    public static class DecimalDataSet extends NumericDataSet<BigDecimal>
    {
        @Override
        BigDecimal[] emptyValues()
        {
            return new BigDecimal[NUMBER_OF_VALUES];
        }

        @Override
        BigDecimal nextValue()
        {
            return getRandom().nextBigDecimal(-1_000_000, 1_000_000, -64, 64);
        }

        @Override
        BigDecimal negate(BigDecimal value)
        {
            return value.signum() > 0 ? value.negate() : value;
        }

        @Override
        BigDecimal abs(BigDecimal value)
        {
            return value.signum() < 0 ? value.abs() : value;
        }

        @Override
        BigDecimal increment(BigDecimal value)
        {
            return value.add(BigDecimal.ONE);
        }

        public String toString()
        {
            return "decimal";
        }
    }


    public static class FloatDataSet extends NumericDataSet<Float>
    {
        @Override
        Float[] emptyValues()
        {
            return new Float[NUMBER_OF_VALUES];
        }

        @Override
        Float nextValue()
        {
            return getRandom().nextFloat();
        }

        @Override
        Float negate(Float value)
        {
            return value > 0 ? -value : value;
        }

        @Override
        Float abs(Float value)
        {
            return value < 0 ? Math.abs(value) : value;
        }

        @Override
        Float increment(Float value)
        {
            return ++value;
        }

        public String toString()
        {
            return "float";
        }
    }

    public static class DoubleDataSet extends NumericDataSet<Double>
    {
        @Override
        Double[] emptyValues()
        {
            return new Double[NUMBER_OF_VALUES];
        }

        @Override
        Double nextValue()
        {
            return getRandom().nextDouble();
        }

        @Override
        Double negate(Double value)
        {
            return value > 0 ? -value : value;
        }

        @Override
        Double abs(Double value)
        {
            return value < 0 ? Math.abs(value) : value;
        }

        @Override
        Double increment(Double value)
        {
            return ++value;
        }

        public String toString()
        {
            return "double";
        }
    }

    public static class AsciiDataSet extends DataSet<String>
    {
        public AsciiDataSet()
        {
            values = new String[NUMBER_OF_VALUES];
            List<String> list = Arrays.asList(values);
            for (int index = 0; index < values.length; index++)
            {
                String value;
                do
                {
                    value = getRandom().nextAsciiString(8, 256);
                }
                while (list.contains(value));
                values[index] = value;
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.LiteralQuerySet();
        }

        public String toString()
        {
            return "ascii";
        }
    }

    public static class BooleanDataSet extends DataSet<Boolean>
    {
        public BooleanDataSet()
        {
            values = new Boolean[NUMBER_OF_VALUES];
            for (int index = 0; index < values.length; index++)
            {
                values[index] = getRandom().nextBoolean();
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.BooleanQuerySet();
        }

        public String toString()
        {
            return "boolean";
        }
    }

    public static class TextDataSet extends DataSet<String>
    {
        public TextDataSet()
        {
            values = new String[NUMBER_OF_VALUES];
            List<String> list = Arrays.asList(values);
            for (int index = 0; index < values.length; index++)
            {
                String value;
                do
                {
                    value = getRandom().nextTextString(8, 256);
                }
                while (list.contains(value));
                values[index] = value;
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.LiteralQuerySet();
        }

        public String toString()
        {
            return "text";
        }
    }

    public static class DateDataSet extends DataSet<Integer>
    {
        public DateDataSet()
        {
            values = new Integer[NUMBER_OF_VALUES];
            List<Integer> list = Arrays.asList(values);
            long min = TimeUnit.DAYS.toMillis(Integer.MIN_VALUE);
            long max = TimeUnit.DAYS.toMillis(Integer.MAX_VALUE);
            long range = max - min;

            for (int index = 0; index < values.length; index++)
            {
                int value;
                do
                {
                    value = SimpleDateSerializer.timeInMillisToDay(min + Math.round(getRandom().nextDouble() * range));
                }
                while (list.contains(value));
                values[index] = value;
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.LiteralQuerySet();
        }

        public String toString()
        {
            return "date";
        }
    }

    public static class TimeDataSet extends DataSet<Long>
    {
        public TimeDataSet()
        {
            values = new Long[NUMBER_OF_VALUES];
            List<Long> list = Arrays.asList(values);
            for (int index = 0; index < values.length; index++)
            {
                Long value;
                do
                {
                    int hours = getRandom().nextIntBetween(0, 23);
                    int minutes = getRandom().nextIntBetween(0, 59);
                    int seconds = getRandom().nextIntBetween(0, 59);
                    long nanos = getRandom().nextIntBetween(0, 1000000000);
                    value = TimeSerializer.timeStringToLong(String.format("%s:%s:%s.%s", hours, minutes, seconds, nanos));
                }
                while (list.contains(value));
                values[index] = value;
            }
            Arrays.sort(values);
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.NumericQuerySet();
        }

        public String toString()
        {
            return "time";
        }
    }

    public static class TimestampDataSet extends DataSet<Date>
    {
        public TimestampDataSet()
        {
            values = new Date[NUMBER_OF_VALUES];
            List<Date> list = Arrays.asList(values);
            long min = Instant.EPOCH.getEpochSecond();
            long max = Instant.EPOCH.plus(100 * 365, ChronoUnit.DAYS).getEpochSecond();
            long range = max - min;

            for (int index = 0; index < values.length; index++)
            {
                Date value;
                do
                {
                    value = Date.from(Instant.ofEpochSecond(min + Math.round(getRandom().nextDouble() * range)));
                }
                while (list.contains(value));
                values[index] = value;
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.LiteralQuerySet();
        }

        public String toString()
        {
            return "timestamp";
        }
    }

    public static class UuidDataSet extends DataSet<UUID>
    {
        public UuidDataSet()
        {
            values = new UUID[NUMBER_OF_VALUES];
            List<UUID> list = Arrays.asList(values);

            for (int index = 0; index < values.length; index++)
            {
                UUID value;
                do
                {
                    value = UUID.randomUUID();
                }
                while (list.contains(value));
                values[index] = value;
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.LiteralQuerySet();
        }

        public String toString()
        {
            return "uuid";
        }
    }

    public static class TimeuuidDataSet extends DataSet<TimeUUID>
    {
        public TimeuuidDataSet()
        {
            values = new TimeUUID[NUMBER_OF_VALUES];
            List<TimeUUID> list = Arrays.asList(values);

            for (int index = 0; index < values.length; index++)
            {
                TimeUUID value;
                do
                {
                    value = TimeUUID.Generator.nextTimeUUID();
                }
                while (list.contains(value));
                values[index] = value;
            }
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.LiteralQuerySet();
        }

        public String toString()
        {
            return "timeuuid";
        }
    }

    public static class InetDataSet extends DataSet<InetAddress>
    {
        public InetDataSet()
        {
            values = new InetAddress[NUMBER_OF_VALUES];
            List<InetAddress> list = Arrays.asList(values);

            for (int index = 0; index < values.length; index++)
            {
                InetAddress value;
                do
                {
                    byte[] bytes;
                    if (getRandom().nextBoolean())
                        bytes = new byte[4];
                    else
                        bytes = new byte[16];
                    getRandom().nextBytes(bytes);
                    try
                    {
                        value = InetAddress.getByAddress(bytes);
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
                while (list.contains(value));
                values[index] = value;
            }
            IndexTermType indexTermType = createIndexTermType(InetAddressType.instance);
            Arrays.sort(values, (o1, o2) -> indexTermType.compare(indexTermType.asIndexBytes(ByteBuffer.wrap(o1.getAddress())),
                                                                  indexTermType.asIndexBytes(ByteBuffer.wrap(o2.getAddress()))));
        }

        @Override
        public QuerySet querySet()
        {
            return new QuerySet.NumericQuerySet();
        }

        public String toString()
        {
            return "inet";
        }
    }
}

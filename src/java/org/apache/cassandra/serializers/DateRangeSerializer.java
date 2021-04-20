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

package org.apache.cassandra.serializers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.marshal.datetime.DateRange;
import org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBound.Precision;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Responsible for {@link DateRange} serialization/deserialization with respect to the following format:
 * -------------------------
 * <type>[<time0><precision0>[<time1><precision1>]]
 *
 * Where:
 *
 * <type> is a [byte] encoding of
 * - 0x00 - single value as in "2001-01-01"
 * - 0x01 - closed range as in "[2001-01-01 TO 2001-01-31]"
 * - 0x02 - open range high as in "[2001-01-01 TO *]"
 * - 0x03 - open range low as in "[* TO 2001-01-01]"
 * - 0x04 - both ranges open as in "[* TO *]"
 * - 0x05 - single value open as in "*"
 *
 * <time0> is an optional [long] millisecond offset from epoch. Absent for <type> in [4,5], present otherwise.
 * Represents a single date value for <type> = 0, the range start for <type> in [1,2], or range end for <type> = 3.
 *
 * <precision0> is an optional [byte]s and represents the precision of field <time0>. Absent for <type> in [4,5], present otherwise.
 * Possible values are:
 * - 0x00 - year
 * - 0x01 - month
 * - 0x02 - day
 * - 0x03 - hour
 * - 0x04 - minute
 * - 0x05 - second
 * - 0x06 - millisecond
 *
 * <time1> is an optional [long] millisecond offset from epoch. Represents the range end for <type> = 1. Not present
 * otherwise.
 *
 * <precision1> is an optional [byte] and represents the precision of field <time1>. Only present if <type> = 1. Values
 * are the same as for <precision0>.
 */
public final class DateRangeSerializer extends TypeSerializer<DateRange>
{
    public static final DateRangeSerializer instance = new DateRangeSerializer();

    // e.g. [2001-01-01]
    private final static byte DATE_RANGE_TYPE_SINGLE_DATE = 0x00;
    // e.g. [2001-01-01 TO 2001-01-31]
    private final static byte DATE_RANGE_TYPE_CLOSED_RANGE = 0x01;
    // e.g. [2001-01-01 TO *]
    private final static byte DATE_RANGE_TYPE_OPEN_RANGE_HIGH = 0x02;
    // e.g. [* TO 2001-01-01]
    private final static byte DATE_RANGE_TYPE_OPEN_RANGE_LOW = 0x03;
    // [* TO *]
    private final static byte DATE_RANGE_TYPE_BOTH_OPEN_RANGE = 0x04;
    // *
    private final static byte DATE_RANGE_TYPE_SINGLE_DATE_OPEN = 0x05;

    /**
     * Size of the single serialized DateRange boundary. As specified in @{@link DateRangeSerializer}.
     *
     * Tightly coupled with {@link #deserializeDateRangeLowerBound(int, Object, ValueAccessor)} and
     * {@link #deserializeDateRangeUpperBound(int, Object, ValueAccessor)}.
     */
    private final static int SERIALIZED_DATE_RANGE_BOUND_SIZE = TypeSizes.LONG_SIZE + TypeSizes.BYTE_SIZE;

    private static final List<Integer> VALID_SERIALIZED_LENGTHS = ImmutableList.of(
            // types: 0x04, 0x05
            Byte.BYTES,
            // types: 0x00, 0x02, 0x03
            Byte.BYTES + Long.BYTES + Byte.BYTES,
            // types: 0x01
            Byte.BYTES + Long.BYTES + Byte.BYTES + Long.BYTES + Byte.BYTES
    );


    @Override
    public ByteBuffer serialize(DateRange dateRange)
    {
        if (dateRange == null)
        {
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }

        byte rangeType = encodeType(dateRange);

        int bufferSize = 1;
        if (!dateRange.getLowerBound().isUnbounded())
        {
            bufferSize += 9;
        }
        if (dateRange.isUpperBoundDefined() && !dateRange.getUpperBound().isUnbounded())
        {
            bufferSize += 9;
        }

        try (DataOutputBuffer output = new DataOutputBuffer(bufferSize))
        {
            output.writeByte(rangeType);
            DateRange.DateRangeBound lowerBound = dateRange.getLowerBound();
            if (!lowerBound.isUnbounded())
            {
                output.writeLong(lowerBound.getTimestamp().toEpochMilli());
                output.writeByte(lowerBound.getPrecision().toEncoded());
            }

            if (dateRange.isUpperBoundDefined())
            {
                DateRange.DateRangeBound upperBound = dateRange.getUpperBound();
                if (!upperBound.isUnbounded())
                {
                    output.writeLong(upperBound.getTimestamp().toEpochMilli());
                    output.writeByte(upperBound.getPrecision().toEncoded());
                }
            }
            return output.buffer();
        }
        catch (IOException e)
        {
            throw new AssertionError("Unexpected error", e);
        }
    }

    @Override
    public <V> DateRange deserialize(V value, ValueAccessor<V> accessor)
    {
        if (accessor.isEmpty(value))
        {
            return null;
        }

        try
        {
            byte type = accessor.toByte(value);
            int offset = TypeSizes.BYTE_SIZE;
            switch (type)
            {
                case DATE_RANGE_TYPE_SINGLE_DATE:
                    return new DateRange(deserializeDateRangeLowerBound(offset, value, accessor));
                case DATE_RANGE_TYPE_CLOSED_RANGE:
                    DateRange.DateRangeBound lowerBound = deserializeDateRangeLowerBound(offset, value, accessor);
                    offset += SERIALIZED_DATE_RANGE_BOUND_SIZE;
                    DateRange.DateRangeBound upperBound = deserializeDateRangeUpperBound(offset, value, accessor);
                    return new DateRange(lowerBound, upperBound);
                case DATE_RANGE_TYPE_OPEN_RANGE_HIGH:
                    return new DateRange(deserializeDateRangeLowerBound(offset, value, accessor), DateRange.DateRangeBound.UNBOUNDED);
                case DATE_RANGE_TYPE_OPEN_RANGE_LOW:
                    return new DateRange(DateRange.DateRangeBound.UNBOUNDED, deserializeDateRangeUpperBound(offset, value, accessor));
                case DATE_RANGE_TYPE_BOTH_OPEN_RANGE:
                    return new DateRange(DateRange.DateRangeBound.UNBOUNDED, DateRange.DateRangeBound.UNBOUNDED);
                case DATE_RANGE_TYPE_SINGLE_DATE_OPEN:
                    return new DateRange(DateRange.DateRangeBound.UNBOUNDED);
                default:
                    throw new IllegalArgumentException("Unknown date range type: " + type);
            }
        }
        catch (IOException e)
        {
            throw new AssertionError("Unexpected error", e);
        }
    }

    @Override
    public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
    {
        if (accessor.isEmpty(value))
        {
            return;
        }
        else if (!VALID_SERIALIZED_LENGTHS.contains(accessor.size(value)))
        {
            throw new MarshalException(String.format("Date range should be have %s bytes, got %d instead.", VALID_SERIALIZED_LENGTHS, accessor.size(value)));
        }
        DateRange dateRange = deserialize(value, accessor);
        validateDateRange(dateRange);
    }

    @Override
    public String toString(DateRange dateRange)
    {
        return dateRange == null ? "" : dateRange.formatToSolrString();
    }

    @Override
    public Class<DateRange> getType()
    {
        return DateRange.class;
    }

    private byte encodeType(DateRange dateRange)
    {
        if (dateRange.isUpperBoundDefined())
        {
            if (dateRange.getLowerBound().isUnbounded())
            {
                return dateRange.getUpperBound().isUnbounded() ? DATE_RANGE_TYPE_BOTH_OPEN_RANGE : DATE_RANGE_TYPE_OPEN_RANGE_LOW;
            }
            else
            {
                return dateRange.getUpperBound().isUnbounded() ? DATE_RANGE_TYPE_OPEN_RANGE_HIGH : DATE_RANGE_TYPE_CLOSED_RANGE;
            }
        }
        else
        {
            return dateRange.getLowerBound().isUnbounded() ? DATE_RANGE_TYPE_SINGLE_DATE_OPEN : DATE_RANGE_TYPE_SINGLE_DATE;
        }
    }

    private <V> DateRange.DateRangeBound deserializeDateRangeLowerBound(int offset, V value, ValueAccessor<V> accessor) throws IOException
    {
        long epochMillis = accessor.getLong(value, offset);
        offset += TypeSizes.LONG_SIZE;
        Precision precision = Precision.fromEncoded(accessor.getByte(value, offset));
        return DateRange.DateRangeBound.lowerBound(Instant.ofEpochMilli(epochMillis), precision);
    }

    private <V> DateRange.DateRangeBound deserializeDateRangeUpperBound(int offset, V value, ValueAccessor<V> accessor) throws IOException
    {
        long epochMillis = accessor.getLong(value, offset);
        offset += TypeSizes.LONG_SIZE;
        Precision precision = Precision.fromEncoded(accessor.getByte(value, offset));
        return DateRange.DateRangeBound.upperBound(Instant.ofEpochMilli(epochMillis), precision);
    }

    private void validateDateRange(DateRange dateRange)
    {
        if (dateRange != null && !dateRange.getLowerBound().isUnbounded() && dateRange.isUpperBoundDefined() && !dateRange.getUpperBound().isUnbounded())
        {
            if (dateRange.getLowerBound().getTimestamp().isAfter(dateRange.getUpperBound().getTimestamp()))
            {
                throw new MarshalException(String.format("Lower bound of a date range should be before upper bound, got: %s",
                        dateRange.formatToSolrString()));
            }
        }
    }
}

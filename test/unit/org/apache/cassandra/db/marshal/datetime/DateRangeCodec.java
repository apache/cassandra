/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.marshal.datetime;

import java.nio.ByteBuffer;
import java.text.ParseException;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import org.apache.cassandra.db.marshal.DateRangeType;
import org.apache.cassandra.serializers.DateRangeSerializer;

/**
 * {@link TypeCodec} that maps binary representation of {@link DateRangeType} to {@link DateRange}.
 */
public class DateRangeCodec extends TypeCodec<DateRange>
{
    public static final DateRangeCodec instance = new DateRangeCodec();

    private DateRangeCodec()
    {
        super(DataType.custom(DateRangeType.class.getName()), DateRange.class);
    }

    @Override
    public ByteBuffer serialize(DateRange dateRange, ProtocolVersion protocolVersion) throws InvalidTypeException
    {
        if (dateRange == null)
        {
            return null;
        }
        return DateRangeSerializer.instance.serialize(dateRange);
    }

    @Override
    public DateRange deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException
    {
        if (bytes == null || bytes.remaining() == 0)
        {
            return null;
        }
        return DateRangeSerializer.instance.deserialize(bytes);
    }

    @Override
    public DateRange parse(String value) throws InvalidTypeException
    {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
        {
            return null;
        }
        try
        {
            return DateRangeUtil.parseDateRange(value);
        }
        catch (ParseException e)
        {
            throw new IllegalArgumentException(String.format("Invalid date range literal: '%s'", value), e);
        }
    }

    @Override
    public String format(DateRange dateRange) throws InvalidTypeException
    {
        return dateRange == null ? "NULL" : dateRange.formatToSolrString();
    }
}

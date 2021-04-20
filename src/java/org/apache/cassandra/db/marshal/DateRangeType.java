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

package org.apache.cassandra.db.marshal;


import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.marshal.datetime.DateRange;
import org.apache.cassandra.db.marshal.datetime.DateRangeUtil;
import org.apache.cassandra.serializers.DateRangeSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;


/**
 * Date range C* type with lower and upper bounds represented as timestamps with a millisecond precision.
 */
public class DateRangeType extends AbstractType<DateRange>
{
    public static final DateRangeType instance = new DateRangeType();

    private DateRangeType()
    {
        super(ComparisonType.BYTE_ORDER);
    }

    @Override
    public ByteBuffer fromString(String source) throws MarshalException
    {
        if (source.isEmpty())
        {
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }
        try
        {
            DateRange dateRange = DateRangeUtil.parseDateRange(source);
            return decompose(dateRange);
        }
        catch (Exception e)
        {
            throw new MarshalException(String.format("Could not parse date range: %s %s", source, e.getMessage()), e);
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        DateRange dateRange = this.getSerializer().deserialize(buffer);
        return '"' + dateRange.formatToSolrString() + '"';
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String)
        {
            return new Constants.Value(fromString((String) parsed));
        }
        throw new MarshalException(String.format(
                "Expected a string representation of a date range value, but got a %s: %s",
                parsed.getClass().getSimpleName(), parsed));
    }

    @Override
    public boolean isEmptyValueMeaningless()
    {
        return true;
    }

    @Override
    public TypeSerializer<DateRange> getSerializer()
    {
        return DateRangeSerializer.instance;
    }
}

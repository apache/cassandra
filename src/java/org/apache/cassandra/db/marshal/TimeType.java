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
import java.time.LocalTime;
import java.time.ZoneOffset;

import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.TimeSerializer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteComparable.Version;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * Nanosecond resolution time values
 */
public class TimeType extends TemporalType<Long>
{
    public static final TimeType instance = new TimeType();

    private static final ArgumentDeserializer ARGUMENT_DESERIALIZER = new DefaultArgumentDeserializer(instance);

    private static final ByteBuffer DEFAULT_MASKED_VALUE = instance.decompose(0L);

    private TimeType() {super(ComparisonType.BYTE_ORDER);} // singleton

    public ByteBuffer fromString(String source) throws MarshalException
    {
        return decompose(TimeSerializer.timeStringToLong(source));
    }

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, Version version)
    {
        // While BYTE_ORDER would still work for this type, making use of the fixed length is more efficient.
        // This type does not allow non-present values, but we do just to avoid future complexity.
        return ByteSource.optionalFixedLength(accessor, data);
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        return ByteSourceInverse.getOptionalFixedLength(accessor, comparableBytes, 8);
    }

    @Override
    public boolean isValueCompatibleWithInternal(AbstractType<?> otherType)
    {
        return this == otherType || otherType == LongType.instance;
    }

    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            return new Constants.Value(fromString((String) parsed));
        }
        catch (ClassCastException exc)
        {
            throw new MarshalException(String.format(
                    "Expected a string representation of a time value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return '"' + TimeSerializer.instance.toString(TimeSerializer.instance.deserialize(buffer)) + '"';
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.TIME;
    }

    @Override
    public TypeSerializer<Long> getSerializer()
    {
        return TimeSerializer.instance;
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return ARGUMENT_DESERIALIZER;
    }

    @Override
    public ByteBuffer now()
    {
        return decompose(LocalTime.now(ZoneOffset.UTC).toNanoOfDay());
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return DEFAULT_MASKED_VALUE;
    }
}

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

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.BooleanSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class BooleanType extends AbstractType<Boolean>
{
    public static final BooleanType instance = new BooleanType();
    private static final ArgumentDeserializer ARGUMENT_DESERIALIZER = new DefaultArgumentDeserializer(instance);
    private static final ByteBuffer MASKED_VALUE = instance.decompose(false);

    BooleanType() {super(ComparisonType.CUSTOM);} // singleton

    @Override
    public boolean allowsEmpty()
    {
        return true;
    }

    @Override
    public boolean isEmptyValueMeaningless()
    {
        return true;
    }

    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        if (accessorL.isEmpty(left) || accessorR.isEmpty(right))
            return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));

        // False is 0, True is anything else, makes False sort before True.
        int v1 = accessorL.getByte(left, 0) == 0 ? 0 : 1;
        int v2 = accessorR.getByte(right, 0) == 0 ? 0 : 1;
        return v1 - v2;
    }

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, ByteComparable.Version version)
    {
        if (accessor.isEmpty(data))
            return null;
        byte b = accessor.toByte(data);
        if (b != 0)
            b = 1;
        return ByteSource.oneByte(b);
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        if (comparableBytes == null)
            return accessor.empty();
        int b = comparableBytes.next();
        return accessor.valueOf(b == 1);
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {

        if (source.isEmpty()|| source.equalsIgnoreCase(Boolean.FALSE.toString()))
            return decompose(false);

        if (source.equalsIgnoreCase(Boolean.TRUE.toString()))
            return decompose(true);

        throw new MarshalException(String.format("Unable to make boolean from '%s'", source));
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String)
            return new Constants.Value(fromString((String) parsed));
        else if (!(parsed instanceof Boolean))
            throw new MarshalException(String.format(
                    "Expected a boolean value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        return new Constants.Value(getSerializer().serialize((Boolean) parsed));
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return getSerializer().deserialize(buffer).toString();
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.BOOLEAN;
    }

    public TypeSerializer<Boolean> getSerializer()
    {
        return BooleanSerializer.instance;
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return ARGUMENT_DESERIALIZER;
    }

    @Override
    public int valueLengthIfFixed()
    {
        return 1;
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return MASKED_VALUE;
    }
}

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
import java.util.UUID;

import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class LexicalUUIDType extends AbstractType<UUID>
{
    public static class Serializer extends UUIDSerializer {}
    private static final Serializer SERIALIZER = new Serializer();

    public static final LexicalUUIDType instance = new LexicalUUIDType();

    private static final ArgumentDeserializer ARGUMENT_DESERIALIZER = new DefaultArgumentDeserializer(instance);

    private static final ByteBuffer MASKED_VALUE = instance.decompose(UUID.fromString("00000000-0000-0000-0000-000000000000"));

    LexicalUUIDType()
    {
        super(ComparisonType.CUSTOM);
    } // singleton

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
        return accessorL.toUUID(left).compareTo(accessorR.toUUID(right));
    }

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, ByteComparable.Version version)
    {
        if (data == null || accessor.isEmpty(data))
            return null;

        // fixed-length (hence prefix-free) representation, but
        // we have to sign-flip the highest bytes of the two longs
        return new ByteSource()
        {
            int bufpos = 0;

            public int next()
            {
                if (bufpos >= accessor.size(data))
                    return END_OF_STREAM;
                int v = accessor.getByte(data, bufpos) & 0xFF;
                if (bufpos == 0 || bufpos == 8)
                    v ^= 0x80;
                ++bufpos;
                return v;
            }
        };
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        // Optional-style encoding of empty values as null sources
        if (comparableBytes == null)
            return accessor.empty();

        long hiBits = ByteSourceInverse.getSignedLong(comparableBytes);
        long loBits = ByteSourceInverse.getSignedLong(comparableBytes);

        // Lexical UUIDs are stored as just two signed longs. The decoding of these longs flips their sign bit back, so
        // they can directly be used for constructing the original UUID.
        return UUIDType.makeUuidBytes(accessor, hiBits, loBits);
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        try
        {
            return decompose(UUID.fromString(source));
        }
        catch (IllegalArgumentException e)
        {
            throw new MarshalException(String.format("unable to make UUID from '%s'", source), e);
        }
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            return new Constants.Value(fromString((String) parsed));
        }
        catch (ClassCastException exc)
        {
            throw new MarshalException(String.format(
                    "Expected a string representation of a uuid, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
        }
    }

    @Override
    public TypeSerializer<UUID> getSerializer()
    {
        return SERIALIZER;
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return ARGUMENT_DESERIALIZER;
    }

    @Override
    public int valueLengthIfFixed()
    {
        return 16;
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return MASKED_VALUE;
    }
}

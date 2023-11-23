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
import java.util.regex.Pattern;

import com.google.common.primitives.UnsignedLongs;

import org.apache.cassandra.cql3.CQL3Type;
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
import org.apache.cassandra.utils.UUIDGen;

/**
 * Compares UUIDs using the following criteria:<br>
 * - if count of supplied bytes is less than 16, compare counts<br>
 * - compare UUID version fields<br>
 * - nil UUID is always lesser<br>
 * - compare timestamps if both are time-based<br>
 * - compare lexically, unsigned msb-to-lsb comparison<br>
 *
 * @see "com.fasterxml.uuid.UUIDComparator"
 */
public class UUIDType extends AbstractType<UUID>
{
    public static final UUIDType instance = new UUIDType();

    private static final ArgumentDeserializer ARGUMENT_DESERIALIZER = new DefaultArgumentDeserializer(instance);

    private static final ByteBuffer MASKED_VALUE = instance.decompose(UUID.fromString("00000000-0000-0000-0000-000000000000"));

    UUIDType()
    {
        super(ComparisonType.CUSTOM);
    }

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

        // Compare for length
        boolean p1 = accessorL.size(left) == 16, p2 = accessorR.size(right) == 16;
        if (!(p1 & p2))
        {
            // should we assert exactly 16 bytes (or 0)? seems prudent
            assert p1 || accessorL.isEmpty(left);
            assert p2 || accessorR.isEmpty(right);
            return p1 ? 1 : p2 ? -1 : 0;
        }

        // Compare versions
        long msb1 = accessorL.getLong(left, 0);
        long msb2 = accessorR.getLong(right, 0);

        int version1 = (int) ((msb1 >>> 12) & 0xf);
        int version2 = (int) ((msb2 >>> 12) & 0xf);
        if (version1 != version2)
            return version1 - version2;

        // bytes: version is top 4 bits of byte 6
        // then: [6.5-8), [4-6), [0-4)
        if (version1 == 1)
        {
            long reorder1 = TimeUUIDType.reorderTimestampBytes(msb1);
            long reorder2 = TimeUUIDType.reorderTimestampBytes(msb2);
            // we know this is >= 0, since the top 3 bits will be 0
            int c = Long.compare(reorder1, reorder2);
            if (c != 0)
                return c;
        }
        else
        {
            int c = UnsignedLongs.compare(msb1, msb2);
            if (c != 0)
                return c;
        }

        // Amusingly (or not so much), although UUIDType freely takes time UUIDs (UUIDs with version 1), it compares
        // them differently than TimeUUIDType. This is evident in the least significant bytes comparison (the code
        // below for UUIDType), where UUIDType treats them as unsigned bytes, while TimeUUIDType compares the bytes
        // signed. See CASSANDRA-8730 for details around this discrepancy.
        return UnsignedLongs.compare(accessorL.getLong(left, 8), accessorR.getLong(right, 8));
    }

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, ByteComparable.Version v)
    {
        if (accessor.isEmpty(data))
            return null;

        long msb = accessor.getLong(data, 0);
        long version = ((msb >>> 12) & 0xf);
        ByteBuffer swizzled = ByteBuffer.allocate(16);

        if (version == 1)
            swizzled.putLong(0, TimeUUIDType.reorderTimestampBytes(msb));
        else
            swizzled.putLong(0, (version << 60) | ((msb >>> 4) & 0x0FFFFFFFFFFFF000L) | (msb & 0xFFFL));

        swizzled.putLong(8, accessor.getLong(data, 8));

        // fixed-length thus prefix-free
        return ByteSource.fixedLength(swizzled);
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        // Optional-style encoding of empty values as null sources
        if (comparableBytes == null)
            return accessor.empty();

        // The UUID bits are stored as an unsigned fixed-length 128-bit integer.
        long hiBits = ByteSourceInverse.getUnsignedFixedLengthAsLong(comparableBytes, 8);
        long loBits = ByteSourceInverse.getUnsignedFixedLengthAsLong(comparableBytes, 8);

        long uuidVersion = hiBits >>> 60 & 0xF;
        if (uuidVersion == 1)
        {
            // If the version bits are set to 1, this is a time-based UUID, and its high bits are significantly more
            // shuffled than in other UUIDs. Revert the shuffle.
            hiBits = TimeUUIDType.reorderBackTimestampBytes(hiBits);
        }
        else
        {
            // For non-time UUIDs, the only thing that's needed is to put the version bits back where they were originally.
            hiBits = hiBits << 4 & 0xFFFFFFFFFFFF0000L
                     | uuidVersion << 12
                     | hiBits & 0x0000000000000FFFL;
        }

        return makeUuidBytes(accessor, hiBits, loBits);
    }

    static <V> V makeUuidBytes(ValueAccessor<V> accessor, long high, long low)
    {
        V buffer = accessor.allocate(16);
        accessor.putLong(buffer, 0, high);
        accessor.putLong(buffer, 8, low);
        return buffer;
    }

    @Override
    public boolean isValueCompatibleWithInternal(AbstractType<?> otherType)
    {
        return otherType instanceof UUIDType || otherType instanceof TimeUUIDType;
    }

    @Override
    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        ByteBuffer parsed = parse(source);
        if (parsed != null)
            return parsed;

        throw new MarshalException(String.format("Unable to make UUID from '%s'", source));
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.UUID;
    }

    @Override
    public TypeSerializer<UUID> getSerializer()
    {
        return UUIDSerializer.instance;
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return ARGUMENT_DESERIALIZER;
    }

    static final Pattern regexPattern = Pattern.compile("[A-Fa-f0-9]{8}\\-[A-Fa-f0-9]{4}\\-[A-Fa-f0-9]{4}\\-[A-Fa-f0-9]{4}\\-[A-Fa-f0-9]{12}");

    static ByteBuffer parse(String source)
    {
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        if (regexPattern.matcher(source).matches())
        {
            try
            {
                return UUIDGen.toByteBuffer(UUID.fromString(source));
            }
            catch (IllegalArgumentException e)
            {
                throw new MarshalException(String.format("Unable to make UUID from '%s'", source), e);
            }
        }

        return null;
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

    static int version(ByteBuffer uuid)
    {
        return (uuid.get(6) & 0xf0) >> 4;
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

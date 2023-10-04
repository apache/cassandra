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

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUIDAsBytes;

// Fully compatible with UUID, and indeed is interpreted as UUID for UDF
public abstract class AbstractTimeUUIDType<T> extends TemporalType<T>
{
    AbstractTimeUUIDType()
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

    @Override
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

        long msb1 = accessorL.getLong(left, 0);
        long msb2 = accessorR.getLong(right, 0);
        verifyVersion(msb1);
        verifyVersion(msb2);

        msb1 = reorderTimestampBytes(msb1);
        msb2 = reorderTimestampBytes(msb2);

        int c = Long.compare(msb1, msb2);
        if (c != 0)
            return c;

        // this has to be a signed per-byte comparison for compatibility
        // so we transform the bytes so that a simple long comparison is equivalent
        long lsb1 = signedBytesToNativeLong(accessorL.getLong(left, 8));
        long lsb2 = signedBytesToNativeLong(accessorR.getLong(right, 8));
        return Long.compare(lsb1, lsb2);
    }

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, ByteComparable.Version version)
    {
        if (accessor.isEmpty(data))
            return null;

        long hiBits = accessor.getLong(data, 0);
        verifyVersion(hiBits);
        ByteBuffer swizzled = ByteBuffer.allocate(16);
        swizzled.putLong(0, TimeUUIDType.reorderTimestampBytes(hiBits));
        swizzled.putLong(8, accessor.getLong(data, 8) ^ 0x8080808080808080L);

        return ByteSource.fixedLength(swizzled);
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        // Optional-style encoding of empty values as null sources
        if (comparableBytes == null)
            return accessor.empty();

        // The non-lexical UUID bits are stored as an unsigned fixed-length 128-bit integer.
        long hiBits = ByteSourceInverse.getUnsignedFixedLengthAsLong(comparableBytes, 8);
        long loBits = ByteSourceInverse.getUnsignedFixedLengthAsLong(comparableBytes, 8);

        hiBits = reorderBackTimestampBytes(hiBits);
        verifyVersion(hiBits);
        // In addition, TimeUUIDType also touches the low bits of the UUID (see CASSANDRA-8730 and DB-1758).
        loBits ^= 0x8080808080808080L;

        return UUIDType.makeUuidBytes(accessor, hiBits, loBits);
    }

    // takes as input 8 signed bytes in native machine order
    // returns the first byte unchanged, and the following 7 bytes converted to an unsigned representation
    // which is the same as a 2's complement long in native format
    public static long signedBytesToNativeLong(long signedBytes)
    {
        return signedBytes ^ 0x0080808080808080L;
    }

    private void verifyVersion(long hiBits)
    {
        long version = (hiBits >>> 12) & 0xF;
        if (version != 1)
            throw new MarshalException(String.format("Invalid UUID version %d for timeuuid",
                                                     version));
    }

    protected static long reorderTimestampBytes(long input)
    {
        return (input <<  48)
               | ((input <<  16) & 0xFFFF00000000L)
               |  (input >>> 32);
    }

    protected static long reorderBackTimestampBytes(long input)
    {
        // In a time-based UUID the high bits are significantly more shuffled than in other UUIDs - if [X] represents a
        // 16-bit tuple, [1][2][3][4] should become [3][4][2][1].
        // See the UUID Javadoc (and more specifically the high bits layout of a Leach-Salz UUID) to understand the
        // reasoning behind this bit twiddling in the first place (in the context of comparisons).
        return (input << 32)
               | ((input >>> 16) & 0xFFFF0000L)
               | (input >>> 48);
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        ByteBuffer parsed = UUIDType.parse(source);
        if (parsed == null)
            throw new MarshalException(String.format("Unknown timeuuid representation: %s", source));
        if (parsed.remaining() == 16 && UUIDType.version(parsed) != 1)
            throw new MarshalException("TimeUUID supports only version 1 UUIDs");
        return parsed;
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
            throw new MarshalException(
                    String.format("Expected a string representation of a timeuuid, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
        }
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.TIMEUUID;
    }

    @Override
    public ByteBuffer decomposeUntyped(Object value)
    {
        if (value instanceof UUID)
            return UUIDSerializer.instance.serialize((UUID) value);
        if (value instanceof TimeUUID)
            return TimeUUID.Serializer.instance.serialize((TimeUUID) value);
        return super.decomposeUntyped(value);
    }

    @Override
    public int valueLengthIfFixed()
    {
        return 16;
    }

    @Override
    public long toTimeInMillis(ByteBuffer value)
    {
        return TimeUUID.deserialize(value).unix(MILLISECONDS);
    }

    @Override
    public ByteBuffer addDuration(Number temporal, Duration duration)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer substractDuration(Number temporal, Duration duration)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer now()
    {
        return ByteBuffer.wrap(nextTimeUUIDAsBytes());
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof AbstractTimeUUIDType<?>;
    }
}

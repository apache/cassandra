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
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TimeUUIDSerializer;

public class TimeUUIDType extends AbstractType<UUID>
{
    public static final TimeUUIDType instance = new TimeUUIDType();

    TimeUUIDType()
    {
    } // singleton

    public int compare(ByteBuffer b1, ByteBuffer b2)
    {
        // Compare for length
        int s1 = b1.position(), s2 = b2.position();
        int l1 = b1.limit(), l2 = b2.limit();

        // should we assert exactly 16 bytes (or 0)? seems prudent
        boolean p1 = l1 - s1 == 16, p2 = l2 - s2 == 16;
        if (!(p1 & p2))
        {
            assert p1 | (l1 == s1);
            assert p2 | (l2 == s2);
            return p1 ? 1 : p2 ? -1 : 0;
        }

        long msb1 = b1.getLong(s1);
        long msb2 = b2.getLong(s2);
        msb1 = reorderTimestampBytes(msb1);
        msb2 = reorderTimestampBytes(msb2);

        assert (msb1 & topbyte(0xf0L)) == topbyte(0x10L);
        assert (msb2 & topbyte(0xf0L)) == topbyte(0x10L);

        int c = Long.compare(msb1, msb2);
        if (c != 0)
            return c;

        // this has to be a signed per-byte comparison for compatibility
        // so we transform the bytes so that a simple long comparison is equivalent
        long lsb1 = signedBytesToNativeLong(b1.getLong(s1 + 8));
        long lsb2 = signedBytesToNativeLong(b2.getLong(s2 + 8));
        return Long.compare(lsb1, lsb2);
    }

    // takes as input 8 signed bytes in native machine order
    // returns the first byte unchanged, and the following 7 bytes converted to an unsigned representation
    // which is the same as a 2's complement long in native format
    private static long signedBytesToNativeLong(long signedBytes)
    {
        return signedBytes ^ 0x0080808080808080L;
    }

    private static long topbyte(long topbyte)
    {
        return topbyte << 56;
    }

    protected static long reorderTimestampBytes(long input)
    {
        return    (input <<  48)
                  | ((input <<  16) & 0xFFFF00000000L)
                  |  (input >>> 32);
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
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.TIMEUUID;
    }

    public TypeSerializer<UUID> getSerializer()
    {
        return TimeUUIDSerializer.instance;
    }
}

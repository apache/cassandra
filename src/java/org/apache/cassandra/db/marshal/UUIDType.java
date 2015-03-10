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
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
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

    UUIDType()
    {
    }

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

        // Compare versions
        long msb1 = b1.getLong(s1);
        long msb2 = b2.getLong(s2);

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

        return UnsignedLongs.compare(b1.getLong(s1 + 8), b2.getLong(s2 + 8));
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

        throw new MarshalException(String.format("unable to coerce '%s' to UUID", source));
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.UUID;
    }

    public TypeSerializer<UUID> getSerializer()
    {
        return UUIDSerializer.instance;
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
                return ByteBuffer.wrap(UUIDGen.decompose(UUID.fromString(source)));
            }
            catch (IllegalArgumentException e)
            {
                throw new MarshalException(String.format("unable to make UUID from '%s'", source), e);
            }
        }
        return null;
    }

    static int version(ByteBuffer uuid)
    {
        return (uuid.get(6) & 0xf0) >> 4;
    }
}

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
import java.text.ParseException;
import java.util.UUID;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.serializers.TimestampSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.lang3.time.DateUtils;

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

        if ((b1 == null) || (b1.remaining() < 16))
        {
            return ((b2 == null) || (b2.remaining() < 16)) ? 0 : -1;
        }
        if ((b2 == null) || (b2.remaining() < 16))
        {
            return 1;
        }

        int s1 = b1.position();
        int s2 = b2.position();

        // Compare versions

        int v1 = (b1.get(s1 + 6) >> 4) & 0x0f;
        int v2 = (b2.get(s2 + 6) >> 4) & 0x0f;

        if (v1 != v2)
        {
            return v1 - v2;
        }

        // Compare timestamps for version 1

        if (v1 == 1)
        {
            // if both time-based, compare as timestamps
            int c = compareTimestampBytes(b1, b2);
            if (c != 0)
            {
                return c;
            }
        }

        // Compare the two byte arrays starting from the first
        // byte in the sequence until an inequality is
        // found. This should provide equivalent results
        // to the comparison performed by the RFC 4122
        // Appendix A - Sample Implementation.
        // Note: java.util.UUID.compareTo is not a lexical
        // comparison
        for (int i = 0; i < 16; i++)
        {
            int c = ((b1.get(s1 + i)) & 0xFF) - ((b2.get(s2 + i)) & 0xFF);
            if (c != 0)
            {
                return c;
            }
        }

        return 0;
    }

    private static int compareTimestampBytes(ByteBuffer o1, ByteBuffer o2)
    {
        int o1Pos = o1.position();
        int o2Pos = o2.position();

        int d = (o1.get(o1Pos + 6) & 0xF) - (o2.get(o2Pos + 6) & 0xF);
        if (d != 0)
        {
            return d;
        }

        d = (o1.get(o1Pos + 7) & 0xFF) - (o2.get(o2Pos + 7) & 0xFF);
        if (d != 0)
        {
            return d;
        }

        d = (o1.get(o1Pos + 4) & 0xFF) - (o2.get(o2Pos + 4) & 0xFF);
        if (d != 0)
        {
            return d;
        }

        d = (o1.get(o1Pos + 5) & 0xFF) - (o2.get(o2Pos + 5) & 0xFF);
        if (d != 0)
        {
            return d;
        }

        d = (o1.get(o1Pos) & 0xFF) - (o2.get(o2Pos) & 0xFF);
        if (d != 0)
        {
            return d;
        }

        d = (o1.get(o1Pos + 1) & 0xFF) - (o2.get(o2Pos + 1) & 0xFF);
        if (d != 0)
        {
            return d;
        }

        d = (o1.get(o1Pos + 2) & 0xFF) - (o2.get(o2Pos + 2) & 0xFF);
        if (d != 0)
        {
            return d;
        }

        return (o1.get(o1Pos + 3) & 0xFF) - (o2.get(o2Pos + 3) & 0xFF);
    }

    @Override
    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        // ffffffff-ffff-ffff-ffff-ffffffffff
        if (TimeUUIDType.regexPattern.matcher(source).matches())
        {
            UUID uuid;
            try
            {
                uuid = UUID.fromString(source);
                return ByteBuffer.wrap(UUIDGen.decompose(uuid));
            }
            catch (IllegalArgumentException e)
            {
                throw new MarshalException(String.format("unable to make UUID from '%s'", source), e);
            }
        }

        try
        {
            return ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(TimestampSerializer.dateStringToTimestamp(source)));
        }
        catch (MarshalException e)
        {
            throw new MarshalException(String.format("unable to make version 1 UUID from '%s'", source), e);
        }
    }

    @Override
    public boolean isValueCompatibleWithInternal(AbstractType<?> otherType)
    {
        return this == otherType || otherType == TimeUUIDType.instance;
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.UUID;
    }

    public TypeSerializer<UUID> getSerializer()
    {
        return UUIDSerializer.instance;
    }

}

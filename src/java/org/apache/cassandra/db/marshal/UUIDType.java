package org.apache.cassandra.db.marshal;

/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.UUID;

import org.apache.cassandra.cql.jdbc.JdbcUUID;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.lang.time.DateUtils;

import static org.apache.cassandra.cql.jdbc.JdbcDate.iso8601Patterns;

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

    public UUID compose(ByteBuffer bytes)
    {

        return JdbcUUID.instance.compose(bytes);
    }

    public void validate(ByteBuffer bytes)
    {
        if ((bytes.remaining() != 0) && (bytes.remaining() != 16))
        {
            throw new MarshalException("UUIDs must be exactly 16 bytes");
        }
    }

    public String getString(ByteBuffer bytes)
    {
        try
        {
            return JdbcUUID.instance.getString(bytes);
        }
        catch (org.apache.cassandra.cql.jdbc.MarshalException e)
        {
            throw new MarshalException(e.getMessage());
        }
    }

    public ByteBuffer decompose(UUID value)
    {
        return ByteBuffer.wrap(UUIDGen.decompose(value));
    }

    @Override
    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        ByteBuffer idBytes = null;

        // ffffffff-ffff-ffff-ffff-ffffffffff
        if (TimeUUIDType.regexPattern.matcher(source).matches())
        {
            UUID uuid;
            try
            {
                uuid = UUID.fromString(source);
                idBytes = ByteBuffer.wrap(UUIDGen.decompose(uuid));
            }
            catch (IllegalArgumentException e)
            {
                throw new MarshalException(String.format("unable to make UUID from '%s'", source), e);
            }
        }
        else if (source.toLowerCase().equals("now"))
        {
            idBytes = ByteBuffer.wrap(UUIDGen.decompose(UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress())));
        }
        // Milliseconds since epoch?
        else if (source.matches("^\\d+$"))
        {
            try
            {
                idBytes = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(Long.parseLong(source)));
            }
            catch (NumberFormatException e)
            {
                throw new MarshalException(String.format("unable to make version 1 UUID from '%s'", source), e);
            }
        }
        // Last chance, attempt to parse as date-time string
        else
        {
            try
            {
                long timestamp = DateUtils.parseDate(source, iso8601Patterns).getTime();
                idBytes = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(timestamp));
            }
            catch (ParseException e1)
            {
                throw new MarshalException(String.format("unable to coerce '%s' to version 1 UUID", source), e1);
            }
        }

        return idBytes;
    }
}

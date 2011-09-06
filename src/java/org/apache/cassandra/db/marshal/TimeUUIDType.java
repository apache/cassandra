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
import java.util.regex.Pattern;

import org.apache.cassandra.cql.jdbc.JdbcTimeUUID;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.lang.time.DateUtils;

import static org.apache.cassandra.cql.jdbc.JdbcDate.iso8601Patterns;

public class TimeUUIDType extends AbstractType<UUID>
{
    public static final TimeUUIDType instance = new TimeUUIDType();

    static final Pattern regexPattern = Pattern.compile("[A-Fa-f0-9]{8}\\-[A-Fa-f0-9]{4}\\-[A-Fa-f0-9]{4}\\-[A-Fa-f0-9]{4}\\-[A-Fa-f0-9]{12}");

    TimeUUIDType() {} // singleton

    public UUID compose(ByteBuffer bytes)
    {
        return JdbcTimeUUID.instance.compose(bytes);
    }

    public ByteBuffer decompose(UUID value)
    {
        return ByteBuffer.wrap(UUIDGen.decompose(value));
    }

    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        if (o1.remaining() == 0)
        {
            return o2.remaining() == 0 ? 0 : -1;
        }
        if (o2.remaining() == 0)
        {
            return 1;
        }
        int res = compareTimestampBytes(o1, o2);
        if (res != 0)
            return res;
        return o1.compareTo(o2);
    }

    private static int compareTimestampBytes(ByteBuffer o1, ByteBuffer o2)
    {
        int o1Pos = o1.position();
        int o2Pos = o2.position();

        int d = (o1.get(o1Pos+6) & 0xF) - (o2.get(o2Pos+6) & 0xF);
        if (d != 0) return d;

        d = (o1.get(o1Pos+7) & 0xFF) - (o2.get(o2Pos+7) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos+4) & 0xFF) - (o2.get(o2Pos+4) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos+5) & 0xFF) - (o2.get(o2Pos+5) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos) & 0xFF) - (o2.get(o2Pos) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos+1) & 0xFF) - (o2.get(o2Pos+1) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos+2) & 0xFF) - (o2.get(o2Pos+2) & 0xFF);
        if (d != 0) return d;

        return (o1.get(o1Pos+3) & 0xFF) - (o2.get(o2Pos+3) & 0xFF);
    }

    public String getString(ByteBuffer bytes)
    {
        try
        {
            return JdbcTimeUUID.instance.getString(bytes);
        }
        catch (org.apache.cassandra.cql.jdbc.MarshalException e)
        {
            throw new MarshalException(e.getMessage());
        }
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        
        ByteBuffer idBytes = null;
        
        // ffffffff-ffff-ffff-ffff-ffffffffff
        if (regexPattern.matcher(source).matches())
        {
            UUID uuid = null;
            try
            {
                uuid = UUID.fromString(source);
                idBytes = decompose(uuid);
            }
            catch (IllegalArgumentException e)
            {
                throw new MarshalException(String.format("unable to make UUID from '%s'", source), e);
            }
            
            if (uuid.version() != 1)
                throw new MarshalException("TimeUUID supports only version 1 UUIDs");
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

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 16 && bytes.remaining() != 0)
            throw new MarshalException(String.format("TimeUUID should be 16 or 0 bytes (%d)", bytes.remaining()));
        ByteBuffer slice = bytes.slice();
        // version is bits 4-7 of byte 6.
        if (bytes.remaining() > 0)
        {
            slice.position(6);
            if ((slice.get() & 0xf0) != 0x10)
                throw new MarshalException("Invalid version for TimeUUID type.");
        }
    }
}

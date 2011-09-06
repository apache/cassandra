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
import java.util.UUID;

import org.apache.cassandra.cql.jdbc.JdbcLexicalUUID;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

public class LexicalUUIDType extends AbstractType<UUID>
{
    public static final LexicalUUIDType instance = new LexicalUUIDType();

    LexicalUUIDType() {} // singleton

    public UUID compose(ByteBuffer bytes)
    {
        return JdbcLexicalUUID.instance.compose(bytes);
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

        return UUIDGen.getUUID(o1).compareTo(UUIDGen.getUUID(o2));
    }

    public String getString(ByteBuffer bytes)
    {
        try
        {
            return JdbcLexicalUUID.instance.getString(bytes);
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

        try
        {
            return decompose(UUID.fromString(source));
        }
        catch (IllegalArgumentException e)
        {
            throw new MarshalException(String.format("unable to make UUID from '%s'", source), e);
        }
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 16 && bytes.remaining() != 0)
            throw new MarshalException(String.format("LexicalUUID should be 16 or 0 bytes (%d)", bytes.remaining()));
        // not sure what the version should be for this.
    }
}

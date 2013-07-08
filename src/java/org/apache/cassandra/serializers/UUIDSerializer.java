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
package org.apache.cassandra.serializers;

import org.apache.cassandra.utils.UUIDGen;

import java.nio.ByteBuffer;
import java.util.UUID;

public class UUIDSerializer implements TypeSerializer<UUID>
{
    public static final UUIDSerializer instance = new UUIDSerializer();

    public UUID serialize(ByteBuffer bytes)
    {
        return UUIDGen.getUUID(bytes);
    }

    public ByteBuffer deserialize(UUID value)
    {
        return ByteBuffer.wrap(UUIDGen.decompose(value));
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 16 && bytes.remaining() != 0)
            throw new MarshalException(String.format("UUID should be 16 or 0 bytes (%d)", bytes.remaining()));
        // not sure what the version should be for this.
    }

    public String getString(ByteBuffer bytes)
    {
        if (bytes.remaining() == 0)
        {
            return "";
        }
        if (bytes.remaining() != 16)
        {
            throw new MarshalException("UUIDs must be exactly 16 bytes");
        }
        UUID uuid = UUIDGen.getUUID(bytes);
        return uuid.toString();
    }

    public String toString(UUID value)
    {
        return value.toString();
    }

    public Class<UUID> getType()
    {
        return UUID.class;
    }
}

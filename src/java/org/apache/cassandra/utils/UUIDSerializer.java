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
package org.apache.cassandra.utils;

import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class UUIDSerializer implements IVersionedSerializer<UUID>
{
    public static UUIDSerializer serializer = new UUIDSerializer();

    public void serialize(UUID uuid, DataOutputPlus out, int version) throws IOException
    {
        out.writeLong(uuid.getMostSignificantBits());
        out.writeLong(uuid.getLeastSignificantBits());
    }

    public UUID deserialize(DataInput in, int version) throws IOException
    {
        return new UUID(in.readLong(), in.readLong());
    }

    public long serializedSize(UUID uuid, int version)
    {
        return TypeSizes.NATIVE.sizeof(uuid.getMostSignificantBits()) + TypeSizes.NATIVE.sizeof(uuid.getLeastSignificantBits());
    }
}
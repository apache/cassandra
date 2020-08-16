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

import java.util.UUID;

import org.apache.cassandra.db.marshal.ValueAccessor;

public class UUIDSerializer extends TypeSerializer<UUID>
{
    public static final UUIDSerializer instance = new UUIDSerializer();

    public <V> UUID deserialize(V value, ValueAccessor<V> handle)
    {
        return handle.isEmpty(value) ? null : handle.toUUID(value);
    }

    public <V> V serialize(UUID value, ValueAccessor<V> handle)
    {
        return value == null ? handle.empty() : handle.valueOf(value);
    }

    public <T> void validate(T value, ValueAccessor<T> handle) throws MarshalException
    {
        if (handle.size(value) != 16 && handle.size(value) != 0)
            throw new MarshalException(String.format("UUID should be 16 or 0 bytes (%d)", handle.size(value)));
        // not sure what the version should be for this.
    }

    public String toString(UUID value)
    {
        return value == null ? "" : value.toString();
    }

    public Class<UUID> getType()
    {
        return UUID.class;
    }
}

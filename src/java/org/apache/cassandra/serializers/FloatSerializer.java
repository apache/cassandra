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

import org.apache.cassandra.db.marshal.ValueAccessor;

public class FloatSerializer extends TypeSerializer<Float>
{
    public static final FloatSerializer instance = new FloatSerializer();

    public <V> Float deserialize(V value, ValueAccessor<V> handle)
    {
        if (handle.isEmpty(value))
            return null;

        return handle.toFloat(value);
    }

    public <V> V serialize(Float value, ValueAccessor<V> handle)
    {
        return (value == null) ? handle.empty() : handle.valueOf(value);
    }

    public <T> void validate(T value, ValueAccessor<T> handle) throws MarshalException
    {
        int size = handle.size(value);
        if (size != 4 && size != 0)
            throw new MarshalException(String.format("Expected 4 or 0 byte value for a float (%d)", size));
    }

    public String toString(Float value)
    {
        return value == null ? "" : String.valueOf(value);
    }

    public Class<Float> getType()
    {
        return Float.class;
    }
}

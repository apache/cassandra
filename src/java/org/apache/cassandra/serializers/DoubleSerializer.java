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

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DoubleSerializer extends TypeSerializer<Double>
{
    public static final DoubleSerializer instance = new DoubleSerializer();

    public <V> Double deserialize(V value, ValueAccessor<V> accessor)
    {
        if (accessor.isEmpty(value))
            return null;
        return accessor.toDouble(value);
    }

    public ByteBuffer serialize(Double value)
    {
        return (value == null) ? ByteBufferUtil.EMPTY_BYTE_BUFFER : ByteBufferUtil.bytes(value);
    }

    public <T> void validate(T value, ValueAccessor<T> accessor) throws MarshalException
    {
        if (accessor.size(value) != 8 && !accessor.isEmpty(value))
            throw new MarshalException(String.format("Expected 8 or 0 byte value for a double (%d)", accessor.size(value)));
    }

    public String toString(Double value)
    {
        return value == null ? "" : value.toString();
    }

    public Class<Double> getType()
    {
        return Double.class;
    }
}

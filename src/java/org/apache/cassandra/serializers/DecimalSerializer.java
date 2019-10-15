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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class DecimalSerializer extends TypeSerializer<BigDecimal>
{
    public static final DecimalSerializer instance = new DecimalSerializer();

    public <V> BigDecimal deserialize(V value, ValueAccessor<V> handle)
    {
        if (value == null || handle.isEmpty(value))
            return null;

        // do not consume the contents of the ByteBuffer
        int scale = handle.getInt(value, 0);
        BigInteger bi = new BigInteger(handle.toArray(value, 4, handle.size(value) - 4));
        return new BigDecimal(bi, scale);
    }

    public <V> V serialize(BigDecimal value, ValueAccessor<V> handle)
    {
        if (value == null)
            return handle.empty();

        BigInteger bi = value.unscaledValue();
        int scale = value.scale();
        byte[] bibytes = bi.toByteArray();

        ByteBuffer bytes = ByteBuffer.allocate(4 + bibytes.length);
        bytes.putInt(scale);
        bytes.put(bibytes);
        bytes.rewind();
        return handle.valueOf(bytes);  // FIXME: value write ops
    }

    public <T> void validate(T value, ValueAccessor<T> handle) throws MarshalException
    {
        // We at least store the scale.
        if (handle.size(value) != 0 && handle.size(value) < 4)
            throw new MarshalException(String.format("Expected 0 or at least 4 bytes (%d)", handle.size(value)));
    }

    public String toString(BigDecimal value)
    {
        return value == null ? "" : value.toPlainString();
    }

    public Class<BigDecimal> getType()
    {
        return BigDecimal.class;
    }
}

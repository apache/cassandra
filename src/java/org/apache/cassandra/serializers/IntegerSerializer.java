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
import org.apache.cassandra.utils.ByteBufferUtil;

import java.math.BigInteger;
import java.nio.ByteBuffer;

public class IntegerSerializer extends TypeSerializer<BigInteger>
{
    public static final IntegerSerializer instance = new IntegerSerializer();

    public <V> BigInteger deserialize(V value, ValueAccessor<V> accessor)
    {
        return !accessor.isEmpty(value) ? new BigInteger(accessor.toArray(value)) : null;
    }

    public ByteBuffer serialize(BigInteger value)
    {
        return value == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : ByteBuffer.wrap(value.toByteArray());
    }

    public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
    {
        // no invalid integers.
    }

    public String toString(BigInteger value)
    {
        return value == null ? "" : value.toString(10);
    }

    public Class<BigInteger> getType()
    {
        return BigInteger.class;
    }
}

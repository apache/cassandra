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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;

public class TokenUtf8Type extends PseudoUtf8Type
{
    public static final TokenUtf8Type instance = new TokenUtf8Type();
    static final TypeSerializer<String> tokenSerializer = new UTF8Serializer()
    {
        @Override
        public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
        {
            super.validate(value, accessor);
            String str = deserialize(value, accessor);
            if (null == getPartitioner().getTokenFactory().fromString(str))
                throw new MarshalException("Invalid Token: " + str);
        }
    };

    private static final ArgumentDeserializer ARGUMENT_DESERIALIZER = new DefaultArgumentDeserializer(instance);
    private static final ByteBuffer MASKED_VALUE = ByteBufferUtil.EMPTY_BYTE_BUFFER;

    TokenUtf8Type() {} // singleton

    String describe() { return "Token"; }

    @Override
    public TypeSerializer<String> getSerializer()
    {
        return tokenSerializer;
    }

    @Override
    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        String leftStr = UTF8Serializer.instance.deserialize(left, accessorL);
        String rightStr = UTF8Serializer.instance.deserialize(right, accessorR);
        Token leftToken = getPartitioner().getTokenFactory().fromString(leftStr);
        Token rightToken = getPartitioner().getTokenFactory().fromString(rightStr);
        return leftToken.compareTo(rightToken);
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return ARGUMENT_DESERIALIZER;
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return MASKED_VALUE;
    }
}

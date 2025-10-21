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

import accord.primitives.TxnId;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TxnIdUtf8Type extends PseudoUtf8Type
{
    public static final TxnIdUtf8Type instance = new TxnIdUtf8Type();
    static final TypeSerializer<String> txnIdSerializer = new UTF8Serializer()
    {
        @Override
        public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
        {
            super.validate(value, accessor);
            String str = deserialize(value, accessor);
            if (null == TxnId.tryParse(str))
                throw new MarshalException("Invalid TxnId: " + str);
        }
    };

    private static final ArgumentDeserializer ARGUMENT_DESERIALIZER = new DefaultArgumentDeserializer(instance);
    private static final ByteBuffer MASKED_VALUE = ByteBufferUtil.EMPTY_BYTE_BUFFER;

    TxnIdUtf8Type() {} // singleton

    String describe() { return "TxnId"; }

    @Override
    public TypeSerializer<String> getSerializer()
    {
        return txnIdSerializer;
    }

    @Override
    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        String leftStr = UTF8Serializer.instance.deserialize(left, accessorL);
        String rightStr = UTF8Serializer.instance.deserialize(right, accessorR);
        TxnId leftId = TxnId.parse(leftStr);
        TxnId rightId = TxnId.parse(rightStr);
        return leftId.compareTo(rightId);
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

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
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DenseFloat32Type extends AbstractType<float[]>
{
    public static final DenseFloat32Type instance = new DenseFloat32Type();

    private DenseFloat32Type() {
        super(ComparisonType.BYTE_ORDER);
    }

    @Override
    public TypeSerializer<float[]> getSerializer()
    {
        return Serializer.instance;
    }

    @Override
    public ByteBuffer fromString(String source) throws MarshalException
    {
        // TODO
        // following CollectionType example here.  I guess it's because the antlr parser
        // takes care of building list literals, because they're not strings
        // (in which case, when would this ever be called?)
        try
        {
            return ByteBufferUtil.hexToBytes(source);
        }
        catch (NumberFormatException e)
        {
            throw new MarshalException(String.format("cannot parse '%s' as hex bytes", source), e);
        }
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String)
            parsed = Json.decodeJson((String) parsed);

        if (!(parsed instanceof List))
            throw new MarshalException(String.format(
            "Expected a list, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        List list = (List) parsed;
        var vector = new float[list.size()];
        int i = 0;
        for (Object element : list)
        {
            if (element == null)
                throw new MarshalException("Invalid null element in float32 vector");
            var n = (Number) element;
            vector[i++] = n.floatValue();
        }

        return new Constants.Value(Serializer.instance.serialize(vector));
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.DENSE_F32;
    }

    public static class Serializer extends TypeSerializer<float[]>
    {
        public static final Serializer instance = new Serializer();

        private Serializer() {}

        @Override
        public ByteBuffer serialize(float[] value)
        {
            if (value == null)
                return ByteBufferUtil.EMPTY_BYTE_BUFFER;

            var bb = ByteBuffer.allocate(4 + 4 * value.length);
            bb.putInt(value.length);
            for (var v : value)
                bb.putFloat(v);
            bb.position(0);
            return bb;
        }

        @Override
        public <V> float[] deserialize(V value, ValueAccessor<V> accessor)
        {
            if (accessor.isEmpty(value))
                return null;

            var length = accessor.getInt(value, 0);
            var vector = new float[length];
            for (int i = 0, offset = 4; i < length; offset += 4, i++)
                vector[i] = accessor.getFloat(value, offset);
            return vector;
        }

        @Override
        public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
        {
            int size = accessor.size(value);
            if (size == 0)
                return;
            if (size < 4)
                throw new MarshalException(String.format("Expected at least 4 bytes for a float32 dense vector (found %d)", size));
            int length = accessor.getInt(value, 0);
            if (size != 4 * (1 + length))
                throw new MarshalException(String.format("Expected %d bytes for a float32 dense vector (found %d)", 4 + 4 * length, size));
        }

        @Override
        public String toString(float[] value)
        {
            return Arrays.toString(value);
        }

        @Override
        public Class<float[]> getType()
        {
            return float[].class;
        }
    }
}

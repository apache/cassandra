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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class VectorType extends AbstractType<float[]>
{
    private static final ConcurrentHashMap<Integer, VectorType> instances = new ConcurrentHashMap<>();

    private final int dimensions;


    public static VectorType getInstance(int dimensions)
    {
        VectorType type = instances.get(dimensions);
        return null == type
               ? instances.computeIfAbsent(dimensions, k -> new VectorType(dimensions))
               : type;
    }


    private VectorType(int dimensions)
    {
        super(ComparisonType.BYTE_ORDER);
        this.dimensions = dimensions;
    }

    /**
     * @return num of dimensions of current vector type
     */
    public int getDimensions()
    {
        return dimensions;
    }

    @Override
    public boolean isVector()
    {
        return true;
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return new CQL3Type.Vector(dimensions);
    }

    @Override
    public ByteBuffer fromString(String source) throws MarshalException
    {
        return null;
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        return null;
    }

    @Override
    public TypeSerializer<float[]> getSerializer()
    {
        return Serializer.instance;
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

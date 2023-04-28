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

import java.nio.BufferUnderflowException;
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

    public final int dimensions;
    public final Serializer serializer;

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
        this.serializer = new Serializer(dimensions);
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
        return serializer;
    }

    public static class Serializer extends TypeSerializer<float[]>
    {
        private final int dimensions;

        private Serializer(int dimensions)
        {
            this.dimensions = dimensions;
        }

        @Override
        public ByteBuffer serialize(float[] value)
        {
            if (value == null)
                return ByteBufferUtil.EMPTY_BYTE_BUFFER;

            var bb = ByteBuffer.allocate(4 * value.length);
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

            var vector = new float[dimensions];
            for (int i = 0, offset = 0; i < dimensions; offset += 4, i++)
                vector[i] = accessor.getFloat(value, offset);
            return vector;
        }

        @Override
        public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
        {
            int offset = 0;
            try
            {
                for (int index = 0; index < dimensions; offset += 4, index++)
                {
                    accessor.getFloat(value, offset);
                }
                if (!accessor.isEmptyFromOffset(value, offset))
                    throw new MarshalException("Unexpected extraneous bytes after vector value");
            }
            catch (BufferUnderflowException | IndexOutOfBoundsException e)
            {
                throw new MarshalException(String.format("Not enough bytes to read a vector of %s dimensions", dimensions));
            }
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

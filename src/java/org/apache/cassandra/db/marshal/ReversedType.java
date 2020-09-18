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
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;

public class ReversedType<T> extends AbstractType<T>
{
    // interning instances
    private static final Map<AbstractType<?>, ReversedType> instances = new ConcurrentHashMap<>();

    public final AbstractType<T> baseType;

    public static <T> ReversedType<T> getInstance(TypeParser parser)
    {
        List<AbstractType<?>> types = parser.getTypeParameters();
        if (types.size() != 1)
            throw new ConfigurationException("ReversedType takes exactly one argument, " + types.size() + " given");
        return getInstance((AbstractType<T>) types.get(0));
    }

    public static <T> ReversedType<T> getInstance(AbstractType<T> baseType)
    {
        ReversedType<T> t = instances.get(baseType);
        return null == t
             ? instances.computeIfAbsent(baseType, ReversedType::new)
             : t;
    }

    private ReversedType(AbstractType<T> baseType)
    {
        super(ComparisonType.CUSTOM);
        this.baseType = baseType;
    }

    public boolean isEmptyValueMeaningless()
    {
        return baseType.isEmptyValueMeaningless();
    }

    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        return baseType.compare(right, accessorR, left, accessorL);
    }

    @Override
    public int compareForCQL(ByteBuffer v1, ByteBuffer v2)
    {
        return baseType.compare(v1, v2);
    }

    public <V> String getString(V value, ValueAccessor<V> accessor)
    {
        return baseType.getString(value, accessor);
    }

    public ByteBuffer fromString(String source)
    {
        return baseType.fromString(source);
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        return baseType.fromJSONObject(parsed);
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return baseType.toJSONString(buffer, protocolVersion);
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> otherType)
    {
        if (!(otherType instanceof ReversedType))
            return false;

        return this.baseType.isCompatibleWith(((ReversedType) otherType).baseType);
    }

    @Override
    public boolean isValueCompatibleWith(AbstractType<?> otherType)
    {
        return this.baseType.isValueCompatibleWith(otherType);
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return baseType.asCQL3Type();
    }

    public TypeSerializer<T> getSerializer()
    {
        return baseType.getSerializer();
    }

    @Override
    public <V> boolean referencesUserType(V name, ValueAccessor<V> accessor)
    {
        return baseType.referencesUserType(name, accessor);
    }

    @Override
    public AbstractType<?> expandUserTypes()
    {
        return getInstance(baseType.expandUserTypes());
    }

    @Override
    public ReversedType<?> withUpdatedUserType(UserType udt)
    {
        if (!referencesUserType(udt.name))
            return this;

        instances.remove(baseType);

        return getInstance(baseType.withUpdatedUserType(udt));
    }

    @Override
    public int valueLengthIfFixed()
    {
        return baseType.valueLengthIfFixed();
    }

    @Override
    public boolean isReversed()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return getClass().getName() + "(" + baseType + ")";
    }
}

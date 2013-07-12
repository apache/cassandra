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
import java.util.*;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.SetSerializer;
import org.apache.cassandra.utils.Pair;

public class SetType<T> extends CollectionType<Set<T>>
{
    // interning instances
    private static final Map<AbstractType<?>, SetType> instances = new HashMap<AbstractType<?>, SetType>();

    public final AbstractType<T> elements;
    private final SetSerializer<T> serializer;

    public static SetType<?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 1)
            throw new ConfigurationException("SetType takes exactly 1 type parameter");

        return getInstance(l.get(0));
    }

    public static synchronized <T> SetType<T> getInstance(AbstractType<T> elements)
    {
        SetType<T> t = instances.get(elements);
        if (t == null)
        {
            t = new SetType<T>(elements);
            instances.put(elements, t);
        }
        return t;
    }

    public SetType(AbstractType<T> elements)
    {
        super(Kind.SET);
        this.elements = elements;
        this.serializer = SetSerializer.getInstance(elements.getSerializer());
    }

    public AbstractType<T> nameComparator()
    {
        return elements;
    }

    public AbstractType<?> valueComparator()
    {
        return EmptyType.instance;
    }

    public TypeSerializer<Set<T>> getSerializer()
    {
        return serializer;
    }

    protected void appendToStringBuilder(StringBuilder sb)
    {
        sb.append(getClass().getName()).append(TypeParser.stringifyTypeParameters(Collections.<AbstractType<?>>singletonList(elements)));
    }

    public ByteBuffer serialize(List<Pair<ByteBuffer, Column>> columns)
    {
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(columns.size());
        int size = 0;
        for (Pair<ByteBuffer, Column> p : columns)
        {
            bbs.add(p.left);
            size += 2 + p.left.remaining();
        }
        return pack(bbs, columns.size(), size);
    }
}

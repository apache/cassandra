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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class SetType extends CollectionType
{
    // interning instances
    private static final Map<AbstractType<?>, SetType> instances = new HashMap<AbstractType<?>, SetType>();

    public final AbstractType<?> elements;

    public static SetType getInstance(TypeParser parser) throws ConfigurationException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 1)
            throw new ConfigurationException("SetType takes exactly 1 type parameter");

        return getInstance(l.get(0));
    }

    public static synchronized SetType getInstance(AbstractType<?> elements)
    {
        SetType t = instances.get(elements);
        if (t == null)
        {
            t = new SetType(elements);
            instances.put(elements, t);
        }
        return t;
    }

    public SetType(AbstractType<?> elements)
    {
        super(Kind.SET);
        this.elements = elements;
    }

    public AbstractType<?> nameComparator()
    {
        return elements;
    }

    public AbstractType<?> valueComparator()
    {
        return EmptyType.instance;
    }

    protected void appendToStringBuilder(StringBuilder sb)
    {
        sb.append(getClass().getName()).append(TypeParser.stringifyTypeParameters(Collections.<AbstractType<?>>singletonList(elements)));
    }

    public ByteBuffer serializeForThrift(List<Pair<ByteBuffer, IColumn>> columns)
    {
        // We're using a list for now, since json doesn't have maps
        List<Object> l = new ArrayList<Object>(columns.size());
        for (Pair<ByteBuffer, IColumn> p : columns)
            l.add(elements.compose(p.left));
        return ByteBufferUtil.bytes(FBUtilities.json(l));
    }
}

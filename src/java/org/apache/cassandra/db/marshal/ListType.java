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

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class ListType extends CollectionType
{
    // interning instances
    private static final Map<AbstractType<?>, ListType> instances = new HashMap<AbstractType<?>, ListType>();

    public final AbstractType<?> elements;

    public static ListType getInstance(TypeParser parser) throws ConfigurationException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 1)
            throw new ConfigurationException("ListType takes exactly 1 type parameter");

        return getInstance(l.get(0));
    }

    public static synchronized ListType getInstance(AbstractType<?> elements)
    {
        ListType t = instances.get(elements);
        if (t == null)
        {
            t = new ListType(elements);
            instances.put(elements, t);
        }
        return t;
    }

    private ListType(AbstractType<?> elements)
    {
        super(Kind.LIST);
        this.elements = elements;
    }

    public AbstractType<?> nameComparator()
    {
        return TimeUUIDType.instance;
    }

    public AbstractType<?> valueComparator()
    {
        return elements;
    }

    protected void appendToStringBuilder(StringBuilder sb)
    {
        sb.append(getClass().getName()).append(TypeParser.stringifyTypeParameters(Collections.<AbstractType<?>>singletonList(elements)));
    }

    public ByteBuffer serializeForThrift(List<Pair<ByteBuffer, IColumn>> columns)
    {
        List<Object> l = new ArrayList<Object>(columns.size());
        for (Pair<ByteBuffer, IColumn> p : columns)
            l.add(elements.compose(p.right.value()));
        return ByteBufferUtil.bytes(FBUtilities.json(l));
    }
}

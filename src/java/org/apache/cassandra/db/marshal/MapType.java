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
import java.util.HashMap;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class MapType extends CollectionType
{
    // interning instances
    private static final Map<Pair<AbstractType<?>, AbstractType<?>>, MapType> instances = new HashMap<Pair<AbstractType<?>, AbstractType<?>>, MapType>();

    public final AbstractType<?> keys;
    public final AbstractType<?> values;

    public static MapType getInstance(TypeParser parser) throws ConfigurationException
    {
        List<AbstractType<?>> l = parser.getTypeParameters();
        if (l.size() != 2)
            throw new ConfigurationException("MapType takes exactly 2 type parameters");

        return getInstance(l.get(0), l.get(1));
    }

    public static synchronized MapType getInstance(AbstractType<?> keys, AbstractType<?> values)
    {
        Pair<AbstractType<?>, AbstractType<?>> p = Pair.<AbstractType<?>, AbstractType<?>>create(keys, values);
        MapType t = instances.get(p);
        if (t == null)
        {
            t = new MapType(keys, values);
            instances.put(p, t);
        }
        return t;
    }

    private MapType(AbstractType<?> keys, AbstractType<?> values)
    {
        super(Kind.MAP);
        this.keys = keys;
        this.values = values;
    }

    public AbstractType<?> nameComparator()
    {
        return keys;
    }

    public AbstractType<?> valueComparator()
    {
        return values;
    }

    protected void appendToStringBuilder(StringBuilder sb)
    {
        sb.append(getClass().getName()).append(TypeParser.stringifyTypeParameters(Arrays.asList(keys, values)));
    }

    public ByteBuffer serializeForThrift(List<Pair<ByteBuffer, IColumn>> columns)
    {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        for (Pair<ByteBuffer, IColumn> p : columns)
            m.put(keys.getString(p.left), values.compose(p.right.value()));
        return ByteBufferUtil.bytes(FBUtilities.json(m));
    }
}

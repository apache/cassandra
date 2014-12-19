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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.StringUtils;

public class ColumnGroupMap
{
    public static ColumnGroupMap EMPTY = new ColumnGroupMap(null, false);

    private final ByteBuffer[] fullPath;
    private final Map<ByteBuffer, Value> map = new HashMap<ByteBuffer, Value>();
    public final boolean isStatic; // Whether or not the group correspond to "static" cells

    private ColumnGroupMap(ByteBuffer[] fullPath, boolean isStatic)
    {
        this.fullPath = fullPath;
        this.isStatic = isStatic;
    }

    private void add(ByteBuffer[] fullName, int idx, Column column)
    {
        ByteBuffer columnName = fullName[idx];
        if (fullName.length == idx + 2)
        {
            // It's a collection
            Value v = map.get(columnName);
            if (v == null)
            {
                v = new Collection();
                map.put(columnName, v);
            }
            assert v instanceof Collection;

            ((Collection)v).add(Pair.create(fullName[idx + 1], column));
        }
        else
        {
            assert !map.containsKey(columnName);
            map.put(columnName, new Simple(column));
        }
    }

    public ByteBuffer getKeyComponent(int pos)
    {
        return fullPath == null ? null : fullPath[pos];
    }

    public Column getSimple(ByteBuffer key)
    {
        Value v = map.get(key);
        if (v == null)
            return null;

        assert v instanceof Simple;
        return ((Simple)v).column;
    }

    public List<Pair<ByteBuffer, Column>> getCollection(ByteBuffer key)
    {
        Value v = map.get(key);
        if (v == null)
            return null;

        assert v instanceof Collection;
        return (List<Pair<ByteBuffer, Column>>)v;
    }

    public boolean hasValueFor(ByteBuffer key)
    {
        return map.containsKey(key);
    }

    @Override
    public String toString()
    {
        List<String> fullStringPath = new ArrayList<>(fullPath.length);
        for (ByteBuffer buffer : fullPath)
            fullStringPath.add(ByteBufferUtil.bytesToHex(buffer));

        List<String> stringMap = new ArrayList<>(fullPath.length);
        for (Map.Entry<ByteBuffer, Value> entry : map.entrySet())
            stringMap.add(ByteBufferUtil.bytesToHex(entry.getKey()) + ": " + entry.getValue());

        return Objects.toStringHelper(this)
                .add("fullPath", "[" + StringUtils.join(fullStringPath, ", ") + "]")
                .add("map", "{" + StringUtils.join(stringMap, ", ") + "}")
                .add("isStatic", isStatic)
                .toString();
    }

    private interface Value {};

    private static class Simple implements Value
    {
        public final Column column;

        Simple(Column column)
        {
            this.column = column;
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this).add("column", ByteBufferUtil.bytesToHex(column.name())).toString();
        }
    }

    private static class Collection extends ArrayList<Pair<ByteBuffer, Column>> implements Value {}

    public static class Builder
    {
        private final CompositeType composite;
        private final int idx;
        private final long now;
        private ByteBuffer[] previous;

        private final List<ColumnGroupMap> groups = new ArrayList<ColumnGroupMap>();
        private ColumnGroupMap currentGroup;

        public Builder(CompositeType composite, boolean hasCollections, long now)
        {
            this.composite = composite;
            this.idx = composite.types.size() - (hasCollections ? 2 : 1);
            this.now = now;
        }

        public void add(Column c)
        {
            if (c.isMarkedForDelete(now))
                return;

            ByteBuffer[] current = composite.split(c.name());

            if (currentGroup == null)
            {
                currentGroup = new ColumnGroupMap(current, CompositeType.isStaticName(c.name()));
                currentGroup.add(current, idx, c);
                previous = current;
                return;
            }

            if ((currentGroup.isStatic && !CompositeType.isStaticName(c.name())) || !isSameGroup(current))
            {
                groups.add(currentGroup);
                // Note that we know that only the first group built can be static
                currentGroup = new ColumnGroupMap(current, false);
            }
            currentGroup.add(current, idx, c);
            previous = current;
        }

        /**
         * For sparse composite, returns wheter the column belong to the same
         * cqlRow than the previously added, based on the full list of component
         * in the name.
         * Two columns do belong together if they differ only by the last
         * component.
         */
        private boolean isSameGroup(ByteBuffer[] c)
        {
            for (int i = 0; i < idx; i++)
            {
                AbstractType<?> comp = composite.types.get(i);
                if (comp.compare(c[i], previous[i]) != 0)
                    return false;
            }
            return true;
        }

        public List<ColumnGroupMap> groups()
        {
            if (currentGroup != null)
            {
                groups.add(currentGroup);
                currentGroup = null;
            }
            return groups;
        }

        public boolean isEmpty()
        {
            return currentGroup == null && groups.isEmpty();
        }

        public ColumnGroupMap firstGroup()
        {
            if (currentGroup != null)
            {
                groups.add(currentGroup);
                currentGroup = null;
            }
            return groups.get(0);
        }

        public void discardFirst()
        {
            groups.remove(0);
        }
    }
}

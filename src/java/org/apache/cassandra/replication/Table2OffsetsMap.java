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
package org.apache.cassandra.replication;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;

import org.apache.cassandra.schema.TableId;

/**
 * A mapping of {@link TableId} to {@link Offsets.Mutable}.
 */
public class Table2OffsetsMap
{
    private final Map<TableId, Offsets.Mutable> offsetsMap;

    public Table2OffsetsMap()
    {
        this.offsetsMap = new HashMap<>();
    }

    public Table2OffsetsMap(Map<TableId, Offsets.Mutable> initial)
    {
        this.offsetsMap = new HashMap<>(initial);
    }

    public static Table2OffsetsMap copy(Table2OffsetsMap other)
    {
        Table2OffsetsMap copy = new Table2OffsetsMap();
        other.offsetsMap.forEach((tableId, offsets) -> copy.offsetsMap.put(tableId, Offsets.Mutable.copy(offsets)));
        return copy;
    }

    public void add(TableId tableId, Offsets offsets)
    {
        if (offsets == null || offsets.isEmpty())
            return;
        offsetsMap.computeIfAbsent(tableId, id -> new Offsets.Mutable(offsets.logId))
                  .addAll(offsets);
    }

    @Nonnull
    public Offsets.Mutable get(TableId tableId)
    {
        return offsetsMap.computeIfAbsent(tableId, id -> new Offsets.Mutable(CoordinatorLogId.NONE));
    }

    public Offsets.Mutable getNullable(TableId tableId)
    {
        return offsetsMap.get(tableId);
    }

    public boolean contains(TableId tableId, int offset)
    {
        Offsets offsets = offsetsMap.get(tableId);
        return offsets != null && offsets.contains(offset);
    }

    public boolean isEmpty()
    {
        return offsetsMap.isEmpty();
    }

    public int size()
    {
        return offsetsMap.size();
    }

    public void forEach(BiConsumer<TableId, Offsets.Mutable> consumer)
    {
        offsetsMap.forEach(consumer);
    }

    public Map<UUID, List<Integer>> toPrimitiveMap()
    {
        if (offsetsMap.isEmpty())
            return Collections.emptyMap();

        Map<UUID, List<Integer>> map = new HashMap<>(offsetsMap.size());
        offsetsMap.forEach((tableId, offsets) -> map.put(tableId.asUUID(), offsets.asList()));
        return map;
    }

    public static Table2OffsetsMap fromPrimitiveMap(CoordinatorLogId logId, Map<UUID, List<Integer>> from)
    {
        Table2OffsetsMap map = new Table2OffsetsMap();
        if (from != null)
        {
            from.forEach((uuid, list) -> {
                TableId tableId = TableId.fromUUID(uuid);
                map.offsetsMap.put(tableId, Offsets.fromList(logId, list));
            });
        }
        return map;
    }

    public Immutable copyImmutable()
    {
        Map<TableId, Offsets.Immutable> immutableMap = new HashMap<>(offsetsMap.size());
        offsetsMap.forEach((tableId, offsets) -> immutableMap.put(tableId, Offsets.Immutable.copy(offsets)));
        return new Immutable(immutableMap);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof Table2OffsetsMap)) return false;
        Table2OffsetsMap that = (Table2OffsetsMap) o;
        return Objects.equals(offsetsMap, that.offsetsMap);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(offsetsMap);
    }

    @Override
    public String toString()
    {
        return "Table2OffsetsMap{" + offsetsMap + '}';
    }

    /**
     * An immutable view/snapshot of Table2OffsetsMap.
     */
    public static class Immutable
    {
        public static final Immutable EMPTY = new Immutable(Collections.emptyMap());

        private final Map<TableId, Offsets.Immutable> offsetsMap;

        public Immutable(Map<TableId, Offsets.Immutable> offsetsMap)
        {
            this.offsetsMap = Collections.unmodifiableMap(new HashMap<>(offsetsMap));
        }

        public Offsets.Immutable get(TableId tableId)
        {
            return offsetsMap.get(tableId);
        }

        public boolean contains(TableId tableId, int offset)
        {
            Offsets.Immutable offsets = offsetsMap.get(tableId);
            return offsets != null && offsets.contains(offset);
        }

        public boolean isDurablyFlushed(TableId tableId, Offsets offsets)
        {
            if (offsets == null || offsets.isEmpty())
                return true;
            Offsets.Immutable durable = offsetsMap.get(tableId);
            if (durable == null)
                return false;
            return Offsets.Immutable.difference(offsets, durable).isEmpty();
        }

        public boolean isDurablyFlushed(Offsets offsets)
        {
            if (offsets == null || offsets.isEmpty())
                return true;
            for (Offsets.Immutable durable : offsetsMap.values())
            {
                if (Offsets.Immutable.difference(offsets, durable).isEmpty())
                    return true;
            }
            return false;
        }

        public Map<TableId, Offsets.Immutable> asMap()
        {
            return offsetsMap;
        }

        public boolean isEmpty()
        {
            return offsetsMap.isEmpty();
        }

        public int size()
        {
            return offsetsMap.size();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof Immutable)) return false;
            Immutable that = (Immutable) o;
            return Objects.equals(offsetsMap, that.offsetsMap);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(offsetsMap);
        }

        @Override
        public String toString()
        {
            return "Table2OffsetsMap.Immutable{" + offsetsMap + '}';
        }
    }
}

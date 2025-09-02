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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.replication.MutationSummary.CoordinatorSummary;

public abstract class Log2OffsetsMap<T extends Offsets> implements Iterable<ShortMutationId>
{
    abstract Long2ObjectHashMap<T> asMap();

    @Override
    public Iterator<ShortMutationId> iterator()
    {
        return Iterables.concat(asMap().values()).iterator();
    }

    public Collection<T> offsets()
    {
        return asMap().values();
    }

    public int idCount()
    {
        int count = 0;
        for (T offsets : offsets())
            count += offsets.offsetCount();
        return count;
    }

    @Override
    public String toString()
    {

        StringBuilder builder = new StringBuilder("Log2OffsetsMap{");
        boolean isFirst = true;
        for (Map.Entry<Long, T> entry : asMap().entrySet())
        {
            if (!isFirst)
                builder.append(", ");

            CoordinatorLogId logId = new CoordinatorLogId(entry.getKey());
            T offsets = entry.getValue();
            builder.append(logId);
            builder.append("->");
            builder.append(offsets);
            isFirst = false;
        }
        for (T offsets : offsets())
        {
            if (!isFirst)
                builder.append(", ");
            builder.append(offsets);
            isFirst = false;
        }
        return builder.append('}').toString();
    }

    public boolean isEmpty()
    {
        return Iterables.all(offsets(), Offsets::isEmpty);
    }

    private static abstract class AbstractMutable<T extends Offsets.AbstractMutable<T>> extends Log2OffsetsMap<T>
    {
        protected final Long2ObjectHashMap<T> offsetMap = new Long2ObjectHashMap<>();

        @Override
        Long2ObjectHashMap<T> asMap()
        {
            return offsetMap;
        }

        protected abstract T createOrNull(Offsets.RangeIterator iterator);

        protected abstract T create(CoordinatorLogId logId);

        protected abstract T copy(Offsets offsets);

        protected T create(long logId)
        {
            return create(new CoordinatorLogId(logId));
        }

        public void add(ShortMutationId id)
        {
            T offsets = offsetMap.computeIfAbsent(id.logId(), this::create);
            offsets.add(id.offset());
        }

        public void add(Offsets offsets)
        {
            if (offsets.isEmpty())
                return;

            T existing = offsetMap.get(offsets.logId().asLong());
            if (existing == null)
            {
                offsetMap.put(offsets.logId().asLong(), copy(offsets));
                return;
            }

            existing.addAll(offsets);
        }

        public void addAll(Log2OffsetsMap<?> that)
        {
            for (Offsets offsets : that.asMap().values())
                add(offsets);
        }

        public void addAll(MutationSummary summary)
        {
            for (int i = 0, size = summary.size(); i < size; i++)
                addAll(summary.get(i));
        }

        public void addAll(CoordinatorSummary summary)
        {
            add(summary.reconciled);
            add(summary.unreconciled);
        }

        public void remove(Offsets offsets)
        {
            T existing = offsetMap.get(offsets.logId().asLong());
            if (existing == null)
                return;

            if (existing.isEmpty())
            {
                offsetMap.remove(offsets.logId().asLong());
                return;
            }

            // TODO (expected): ? shouldn't we updating Offsets in-place here? where did Offsets.remove() go?
            T next = createOrNull(Offsets.difference(existing.rangeIterator(), offsets.rangeIterator()));
            if (next == null)
                offsetMap.remove(offsets.logId().asLong());
            else
                offsetMap.put(offsets.logId().asLong(), next);
        }

        public void remove(ShortMutationId id)
        {
            T existing = offsetMap.get(id.logId());
            if (existing == null)
                return;

            existing.remove(id.offset());

            if (existing.isEmpty())
                offsetMap.remove(id.logId());
        }

        public void removeAll(Log2OffsetsMap<?> that)
        {
            for (Offsets offsets : that.asMap().values())
                remove(offsets);
        }

        public void removeAll(Iterable<ShortMutationId> ids)
        {
            for (ShortMutationId id : ids)
                remove(id);
        }

        public void removeAll(MutationSummary summary)
        {
            for (int i = 0, size = summary.size(); i < size; i++)
                removeAll(summary.get(i));
        }

        public void removeAll(CoordinatorSummary summary)
        {
            remove(summary.reconciled);
            remove(summary.unreconciled);
        }
    }

    public static class Mutable extends AbstractMutable<Offsets.Mutable>
    {
        @Override
        protected Offsets.Mutable createOrNull(Offsets.RangeIterator iterator)
        {
            return Offsets.Mutable.createOrNull(iterator);
        }

        @Override
        protected Offsets.Mutable create(CoordinatorLogId logId)
        {
            return new Offsets.Mutable(logId);
        }

        @Override
        protected Offsets.Mutable copy(Offsets offsets)
        {
            return Offsets.Mutable.copy(offsets);
        }
    }

    public static class Immutable extends Log2OffsetsMap<Offsets.Immutable>
    {
        private final Long2ObjectHashMap<Offsets.Immutable> offsetMap;

        private Immutable(Long2ObjectHashMap<Offsets.Immutable> offsetMap)
        {
            this.offsetMap = offsetMap;
        }

        @Override
        Long2ObjectHashMap<Offsets.Immutable> asMap()
        {
            return offsetMap;
        }

        public static class Builder extends AbstractMutable<Offsets.Immutable.Builder>
        {
            @Override
            protected Offsets.Immutable.Builder createOrNull(Offsets.RangeIterator iterator)
            {
                return Offsets.Immutable.Builder.createOrNull(iterator);
            }

            @Override
            protected Offsets.Immutable.Builder create(CoordinatorLogId logId)
            {
                return new Offsets.Immutable.Builder(logId);
            }

            @Override
            protected Offsets.Immutable.Builder copy(Offsets offsets)
            {
                return Offsets.Immutable.Builder.copy(offsets);
            }

            public Log2OffsetsMap.Immutable build()
            {
                Long2ObjectHashMap<Offsets.Immutable> result = new Long2ObjectHashMap<>();
                offsetMap.forEachLong((key, builder) -> result.put(key, builder.build()));
                return new Immutable(result);
            }
        }

        public static final IVersionedSerializer<Log2OffsetsMap.Immutable> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(Log2OffsetsMap.Immutable mo, DataOutputPlus out, int version) throws IOException
            {
                out.writeInt(mo.offsetMap.size());
                for (Offsets.Immutable offsets : mo.asMap().values())
                    Offsets.serializer.serialize(offsets, out, version);
            }

            @Override
            public Log2OffsetsMap.Immutable deserialize(DataInputPlus in, int version) throws IOException
            {
                Long2ObjectHashMap<Offsets.Immutable> offsetMap = new Long2ObjectHashMap<>();
                int size = in.readInt();
                for (int i=0; i<size; i++)
                {
                    Offsets.Immutable offsets = Offsets.serializer.deserialize(in, version);
                    long key = offsets.logId().asLong();
                    Preconditions.checkState(!offsetMap.containsKey(key));
                    offsetMap.put(key, offsets);
                }
                return new Immutable(offsetMap);
            }

            @Override
            public long serializedSize(Log2OffsetsMap.Immutable mo, int version)
            {
                long size = TypeSizes.INT_SIZE;
                for (Offsets.Immutable offsets : mo.offsetMap.values())
                    size += Offsets.serializer.serializedSize(offsets, version);
                return size;
            }
        };
    }
}

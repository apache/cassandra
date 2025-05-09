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
import java.util.Arrays;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public abstract class MultiOffsets<T extends Offsets>
{
    abstract Long2ObjectHashMap<T> offsetMap();

    public int idCount()
    {
        int count = 0;
        for (T offsets : offsetMap().values())
            count += offsets.offsetCount();
        return count;
    }

    public void forEachId(Consumer<ShortMutationId> consumer)
    {
        offsetMap().values().forEach(offsets -> offsets.forEachOffset(((logId, offset) -> {
            consumer.accept(new ShortMutationId(logId, offset));
        })));
    }

    public boolean isEmpty()
    {
        for (T offsets : offsetMap().values())
            if (!offsets.isEmpty())
                return false;
        return true;
    }

    private static abstract class AbstractMutable<T extends Offsets.AbstractMutable<T>> extends MultiOffsets<T>
    {
        protected final Long2ObjectHashMap<T> offsetMap = new Long2ObjectHashMap<>();

        @Override
        Long2ObjectHashMap<T> offsetMap()
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

        public void addAll(MultiOffsets<?> that)
        {
            for (Offsets offsets : that.offsetMap().values())
                add(offsets);
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

            T next = createOrNull(Offsets.difference(existing.rangeIterator(), offsets.rangeIterator()));
            if (next == null)
                offsetMap.remove(offsets.logId().asLong());
            else
                offsetMap.put(offsets.logId().asLong(), next);
        }

        public void removeAll(MultiOffsets<?> that)
        {
            for (Offsets offsets : that.offsetMap().values())
                remove(offsets);
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

    public static class Immutable extends MultiOffsets<Offsets.Immutable>
    {
        private final Long2ObjectHashMap<Offsets.Immutable> offsetMap;

        private Immutable(Long2ObjectHashMap<Offsets.Immutable> offsetMap)
        {
            this.offsetMap = offsetMap;
        }

        @Override
        Long2ObjectHashMap<Offsets.Immutable> offsetMap()
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

            public MultiOffsets.Immutable build()
            {
                Long2ObjectHashMap<Offsets.Immutable> result = new Long2ObjectHashMap<>();
                offsetMap.forEachLong((key, builder) -> result.put(key, builder.build()));
                return new Immutable(result);
            }
        }

        public static final IVersionedSerializer<MultiOffsets.Immutable> serializer = new IVersionedSerializer<MultiOffsets.Immutable>()
        {
            @Override
            public void serialize(MultiOffsets.Immutable mo, DataOutputPlus out, int version) throws IOException
            {
                out.writeInt(mo.offsetMap.size());
                for (Offsets.Immutable offsets : mo.offsetMap().values())
                    Offsets.serializer.serialize(offsets, out, version);
            }

            @Override
            public MultiOffsets.Immutable deserialize(DataInputPlus in, int version) throws IOException
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
            public long serializedSize(MultiOffsets.Immutable mo, int version)
            {
                long size = TypeSizes.INT_SIZE;
                for (Offsets.Immutable offsets : mo.offsetMap.values())
                    size += Offsets.serializer.serializedSize(offsets, version);
                return size;
            }
        };
    }
}

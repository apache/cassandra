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
        return idCount() == 0;
    }

    public static class Mutable extends MultiOffsets<Offsets.Mutable>
    {
        private final Long2ObjectHashMap<Offsets.Mutable> offsetMap = new Long2ObjectHashMap<>();

        @Override
        Long2ObjectHashMap<Offsets.Mutable> offsetMap()
        {
            return offsetMap;
        }

        public Immutable immutableCopy()
        {
            return Immutable.copyOf(this);
        }

        public void add(ShortMutationId id)
        {
            Offsets.Mutable offsets = offsetMap.computeIfAbsent(id.logId(), l -> new Offsets.Mutable(new CoordinatorLogId(l)));
            offsets.add(id.offset());
        }

        public void add(Offsets offsets, boolean copy)
        {
            if (offsets.isEmpty())
                return;

            Offsets.Mutable existing = offsetMap.get(offsets.logId().asLong());
            if (existing == null)
            {
                Offsets.Mutable mutable = copy ? offsets.mutableCopy() : offsets.mutable();
                offsetMap.put(offsets.logId().asLong(), mutable);
                return;
            }

            existing.addAll(offsets);
        }

        public void addAll(MultiOffsets<?> that)
        {
            for (Offsets offsets : that.offsetMap().values())
                add(offsets, true);
        }

        public void remove(Offsets offsets)
        {
            Offsets existing = offsetMap.get(offsets.logId().asLong());
            if (existing == null)
                return;

            if (existing.isEmpty())
            {
                offsetMap.remove(offsets.logId().asLong());
                return;
            }

            Offsets.Immutable next = Offsets.difference(existing, offsets);
            if (next.isEmpty())
                offsetMap.remove(offsets.logId().asLong());
            else
                offsetMap.put(offsets.logId().asLong(), next.mutableCopy());
        }

        public void removeAll(MultiOffsets<?> that)
        {
            for (Offsets offsets : that.offsetMap().values())
                remove(offsets);
        }
    }

    public static class Immutable extends MultiOffsets<Offsets.Immutable>
    {
        private final Long2ObjectHashMap<Offsets.Immutable> offsetMap;

        private static Long2ObjectHashMap<Offsets.Immutable> copyOffsets(Long2ObjectHashMap<? extends Offsets> src)
        {
            Long2ObjectHashMap<Offsets.Immutable> dst = new Long2ObjectHashMap<>();
            src.forEachLong((key, value) -> dst.put(key, value.immutable()));
            return dst;
        }

        private Immutable(Long2ObjectHashMap<Offsets.Immutable> offsetMap)
        {
            this.offsetMap = offsetMap;
        }

        public static Immutable copyOf(Long2ObjectHashMap<Offsets.Immutable> src)
        {
            return new Immutable(copyOffsets(src));
        }

        public static Immutable copyOf(MultiOffsets.Mutable src)
        {
            return new Immutable(copyOffsets(src.offsetMap));
        }

        @Override
        Long2ObjectHashMap<Offsets.Immutable> offsetMap()
        {
            return offsetMap;
        }

        private static class KeySink implements Consumer<Long>
        {
            int idx = 0;
            final long[] keys;

            public KeySink(int size)
            {
                this.keys = new long[size];
            }

            @Override
            public void accept(Long v)
            {
                keys[idx++] = v;
            }

            public void sort()
            {
                Arrays.sort(keys);
            }
        }

        public static final IVersionedSerializer<MultiOffsets.Immutable> serializer = new IVersionedSerializer<MultiOffsets.Immutable>()
        {
            @Override
            public void serialize(MultiOffsets.Immutable mo, DataOutputPlus out, int version) throws IOException
            {
                int size = mo.offsetMap.size();
                KeySink keys = new KeySink(size);
                mo.offsetMap.keySet().forEach(keys);
                keys.sort();

                out.writeInt(size);
                for (int i=0; i<size; i++)
                    Offsets.serializer.serialize(mo.offsetMap.get(keys.keys[i]), out, version);
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

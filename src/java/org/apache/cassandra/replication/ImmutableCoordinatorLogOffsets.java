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
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.Iterators;

import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.vint.VIntCoding;

public class ImmutableCoordinatorLogOffsets implements CoordinatorLogOffsets<Offsets.Immutable>
{
    private final Long2ObjectHashMap<Offsets.Immutable> ids;

    @Override
    public Offsets.Immutable offsets(long logId)
    {
        Offsets.Immutable offsets = ids.get(logId);
        if (offsets == null)
            return new Offsets.Immutable(new CoordinatorLogId(logId));
        return offsets;
    }

    @Override
    public int size()
    {
        return ids.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    @Override
    public Iterator<Long> iterator()
    {
        return Iterators.unmodifiableIterator(ids.keySet().iterator());
    }

    public Iterable<Map.Entry<Long, Offsets.Immutable>> entries()
    {
        return ids.entrySet();
    }


    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        ImmutableCoordinatorLogOffsets longs = (ImmutableCoordinatorLogOffsets) o;
        return Objects.equals(ids, longs.ids);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(ids);
    }

    public ImmutableCoordinatorLogOffsets(Builder builder)
    {
        // Important to set shouldAvoidAllocation=false, otherwise iterators are cached and not thread safe, even when
        // immutable and read-only
        this.ids = new Long2ObjectHashMap<>(builder.ids.size(), 0.9f, false);

        for (Map.Entry<Long, Offsets.Immutable.Builder> entry : builder.ids.entrySet())
            ids.put(entry.getKey(), entry.getValue().build());
    }

    public void forEach(BiConsumer<CoordinatorLogId, Offsets.Immutable> consumer)
    {
        ids.forEach((logId, offsets) -> consumer.accept(new CoordinatorLogId(logId), offsets));
    }

    @NotThreadSafe
    public static class Builder
    {
        private final Long2ObjectHashMap<Offsets.Immutable.Builder> ids;

        public Builder()
        {
            this(16);
        }

        public Builder(int size)
        {
            this.ids = new Long2ObjectHashMap<>(size, 0.9f, false);
        }

        public Builder add(MutationId mutationId)
        {
            if (mutationId.isNone())
                return this;
            ids.computeIfAbsent(mutationId.logId(), logId -> new Offsets.Immutable.Builder(new CoordinatorLogId(logId)))
               .add(mutationId.offset());
            return this;
        }

        public Builder addAll(CoordinatorLogOffsets<?> logOffsets)
        {
            for (long log : logOffsets)
            {
                Offsets offsets = logOffsets.offsets(log);
                ids.computeIfAbsent(log, logId -> new Offsets.Immutable.Builder(new CoordinatorLogId(logId)))
                   .addAll(offsets);
            }
            return this;
        }

        public Builder addAll(Offsets.Immutable offsets)
        {
            ids.computeIfAbsent(offsets.logId.asLong(), logId -> new Offsets.Immutable.Builder(new CoordinatorLogId(logId)))
               .addAll(offsets);
            return this;
        }

        public ImmutableCoordinatorLogOffsets build()
        {
            return new ImmutableCoordinatorLogOffsets(this);
        }
    }

    public static class Serializer implements IVersionedSerializer<ImmutableCoordinatorLogOffsets>
    {
        @Override
        public void serialize(ImmutableCoordinatorLogOffsets logOffsets, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_61)
                return;
            out.writeUnsignedVInt32(logOffsets.size());
            for (long logId : logOffsets)
                Offsets.serializer.serialize(logOffsets.offsets(logId), out, version);
        }

        @Override
        public ImmutableCoordinatorLogOffsets deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_61)
                return ImmutableCoordinatorLogOffsets.NONE;
            int size = in.readUnsignedVInt32();
            ImmutableCoordinatorLogOffsets.Builder builder = new ImmutableCoordinatorLogOffsets.Builder(size);
            for (int i = 0; i < size; i++)
            {
                Offsets.Immutable offsets = Offsets.serializer.deserialize(in, version);
                builder.addAll(offsets);
            }
            return builder.build();
        }

        @Override
        public long serializedSize(ImmutableCoordinatorLogOffsets logOffsets, int version)
        {
            if (version < MessagingService.VERSION_61)
                return 0;
            long size = 0;
            size += VIntCoding.computeUnsignedVIntSize(logOffsets.size());
            for (long logId : logOffsets)
                size += Offsets.serializer.serializedSize(logOffsets.offsets(logId), version);
            return size;
        }
    }

    public static final Serializer serializer = new Serializer();
}

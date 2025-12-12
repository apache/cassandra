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
import java.util.function.Predicate;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.vint.VIntCoding;

public class ImmutableCoordinatorLogOffsets implements CoordinatorLogOffsets<Offsets.Immutable>
{
    private static final Logger logger = LoggerFactory.getLogger(ImmutableCoordinatorLogOffsets.class);

    private final ImmutableMutations mutations;
    private final ActivatedTransfers transfers;

    private ImmutableCoordinatorLogOffsets(Builder builder)
    {
        // Important to set shouldAvoidAllocation=false, otherwise iterators are cached and not thread safe, even when immutable and read-only
        Long2ObjectHashMap<Offsets.Immutable> ids = new Long2ObjectHashMap<>(builder.ids.size(), 0.9f, false);

        for (Map.Entry<Long, Offsets.Immutable.Builder> entry : builder.ids.entrySet())
            ids.put(entry.getKey(), entry.getValue().build());

        this.mutations = new ImmutableMutations(ids);
        this.transfers = ActivatedTransfers.copyOf(builder.transfers);
    }

    @Override
    public Mutations<Offsets.Immutable> mutations()
    {
        return mutations;
    }

    @Override
    public ActivatedTransfers transfers()
    {
        return transfers == null ? ActivatedTransfers.EMPTY : transfers;
    }

    public Iterable<Map.Entry<Long, Offsets.Immutable>> entries()
    {
        return mutations.ids.entrySet();
    }

    public boolean isEmpty()
    {
        return mutations().isEmpty() && transfers().isEmpty();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        ImmutableCoordinatorLogOffsets other = (ImmutableCoordinatorLogOffsets) o;
        return Objects.equals(mutations, other.mutations) && Objects.equals(transfers, other.transfers);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(mutations, transfers);
    }

    @Override
    public String toString()
    {
        return "ImmutableCoordinatorLogOffsets{" +
               "mutations=" + mutations +
               ", transfers=" + transfers +
               '}';
    }

    @NotThreadSafe
    public static class Builder
    {
        private final Long2ObjectHashMap<Offsets.Immutable.Builder> ids;
        private ActivatedTransfers transfers;

        public Builder()
        {
            this(16);
        }

        public Builder(int size)
        {
            this.ids = new Long2ObjectHashMap<>(size, 0.9f, false);
            this.transfers = null;
        }

        public Builder add(MutationId mutationId)
        {
            if (mutationId.isNone())
                return this;
            ids.computeIfAbsent(mutationId.logId(), logId -> new Offsets.Immutable.Builder(new CoordinatorLogId(logId)))
               .add(mutationId.offset());
            return this;
        }

        private Builder addAll(CoordinatorLogOffsets.Mutations<? extends Offsets> mutations)
        {
            for (long log : mutations)
            {
                Offsets offsets = mutations.offsets(log);
                ids.computeIfAbsent(log, logId -> new Offsets.Immutable.Builder(new CoordinatorLogId(logId)))
                   .addAll(offsets);
            }
            return this;
        }

        public Builder addAll(CoordinatorLogOffsets<?> logOffsets)
        {
            addAll(logOffsets.mutations());
            ActivatedTransfers newTransfers = logOffsets.transfers();
            if (transfers == null)
                transfers = newTransfers;
            else
                transfers.addAll(newTransfers);
            return this;
        }

        public Builder addAll(Offsets.Immutable offsets)
        {
            ids.computeIfAbsent(offsets.logId.asLong(), logId -> new Offsets.Immutable.Builder(new CoordinatorLogId(logId)))
               .addAll(offsets);
            return this;
        }

        public Builder addTransfer(ShortMutationId transferId, Bounds<Token> bounds)
        {
            if (transferId.isNone())
                return this;
            if (transfers == null)
                transfers = new ActivatedTransfers();
            transfers.add(transferId, bounds);
            return this;
        }

        public Builder addTransfers(ActivatedTransfers other)
        {
            if (other.isEmpty())
                return this;
            if (transfers == null)
                transfers = other;
            else
                transfers.addAll(other);
            return this;
        }

        /**
         * Removes expired transfers
         */
        public void purgeTransfers(Predicate<ShortMutationId> predicate)
        {
            int purged = 0;
            if (transfers != null)
            {
                Iterator<ShortMutationId> iter = transfers.iterator();
                while (iter.hasNext())
                {
                    ShortMutationId id = iter.next();
                    if (predicate.test(id))
                    {
                        iter.remove();
                        purged++;
                        logger.debug("Purging activation {}", id);
                    }
                }
            }
            if (purged > 0)
                logger.info("Purged {} transfers", purged);
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
            ImmutableMutations.serializer.serialize(logOffsets.mutations, out, version);
            ActivatedTransfers.serializer.serialize(logOffsets.transfers(), out, version);
        }

        @Override
        public ImmutableCoordinatorLogOffsets deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_61)
                return ImmutableCoordinatorLogOffsets.NONE;
            Builder builder = new Builder();
            ImmutableMutations mutations = ImmutableMutations.serializer.deserialize(in, version);
            mutations.ids.forEach((id, offsets) -> builder.addAll(offsets));
            ActivatedTransfers transfers = ActivatedTransfers.serializer.deserialize(in, version);
            if (!transfers.isEmpty())
                builder.addTransfers(transfers);
            return builder.build();
        }

        @Override
        public long serializedSize(ImmutableCoordinatorLogOffsets logOffsets, int version)
        {
            if (version < MessagingService.VERSION_61)
                return 0;
            long size = 0;
            size += ImmutableMutations.serializer.serializedSize(logOffsets.mutations, version);
            size += ActivatedTransfers.serializer.serializedSize(logOffsets.transfers(), version);
            return size;
        }
    }

    public static final Serializer serializer = new Serializer();

    public static class ImmutableMutations implements Mutations<Offsets.Immutable>
    {
        final private Long2ObjectHashMap<Offsets.Immutable> ids;

        private ImmutableMutations(Long2ObjectHashMap<Offsets.Immutable> ids)
        {
            this.ids = ids;
        }

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

        @Override
        public Iterator<Long> iterator()
        {
            return Iterators.unmodifiableIterator(ids.keySet().iterator());
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            ImmutableMutations longs = (ImmutableMutations) o;
            return Objects.equals(ids, longs.ids);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(ids);
        }

        @Override
        public String toString()
        {
            return "ImmutableMutations{" +
                   "ids=" + ids +
                   '}';
        }

        private static final IVersionedSerializer<ImmutableMutations> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(ImmutableMutations mutations, DataOutputPlus out, int version) throws IOException
            {
                out.writeUnsignedVInt32(mutations.size());
                for (long logId : mutations)
                    Offsets.serializer.serialize(mutations.offsets(logId), out, version);
            }

            @Override
            public ImmutableMutations deserialize(DataInputPlus in, int version) throws IOException
            {
                int size = in.readUnsignedVInt32();
                Long2ObjectHashMap<Offsets.Immutable> ids = new Long2ObjectHashMap<>(size, 0.9f, false);
                for (int i = 0; i < size; i++)
                {
                    Offsets.Immutable offsets = Offsets.serializer.deserialize(in, version);
                    ids.put(offsets.logId.asLong(), offsets);
                }
                return new ImmutableMutations(ids);
            }

            @Override
            public long serializedSize(ImmutableMutations mutations, int version)
            {
                long size = VIntCoding.computeUnsignedVIntSize(mutations.size());
                for (long logId : mutations)
                    size += Offsets.serializer.serializedSize(mutations.offsets(logId), version);
                return size;
            }
        };
    }
}

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Comparators;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.IntervalTree;

/**
 * A collection of activated bulk transfers. Bulk transfers are accessed for overlapping {@link ShortMutationId} for a
 * given {@link Bounds}, to find the set of transfer IDs that intersect with a given read, for example. Tracked reads
 * must include intersecting transfers to ensure that summaries reflect transferred SSTables.
 * <p>
 * We expect to have very few bulk transfers (typically 0) so they're kept in an un-indexed set. If we have more
 * transfers in the future, we could transition this to an {@link IntervalTree}.
 */
public class ActivatedTransfers implements Iterable<ShortMutationId>
{
    public static final ActivatedTransfers EMPTY = new ActivatedTransfers();

    private final Set<ActivatedTransfer> transfers;

    public ActivatedTransfers()
    {
        this(new HashSet<>(1));
    }

    private ActivatedTransfers(Collection<ActivatedTransfer> transfers)
    {
        this.transfers = new HashSet<>(transfers);
    }

    public static ActivatedTransfers copyOf(ActivatedTransfers other)
    {
        return other == null ? new ActivatedTransfers() : new ActivatedTransfers(other.transfers);
    }

    @VisibleForTesting
    static final class ActivatedTransfer
    {
        final ShortMutationId id;
        final Bounds<Token> bounds;

        @VisibleForTesting
        ActivatedTransfer(ShortMutationId id, Bounds<Token> bounds)
        {
            this.id = id;
            this.bounds = bounds;
        }

        private ActivatedTransfer(ShortMutationId id, Collection<SSTableReader> sstables)
        {
            this(id, covering(sstables));
        }

        public static final IVersionedSerializer<ActivatedTransfer> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(ActivatedTransfer transfer, DataOutputPlus out, int version) throws IOException
            {
                ShortMutationId.serializer.serialize(transfer.id, out, version);
                Token.serializer.serialize(transfer.bounds.left, out, version);
                Token.serializer.serialize(transfer.bounds.right, out, version);
            }

            @Override
            public ActivatedTransfer deserialize(DataInputPlus in, int version) throws IOException
            {
                ShortMutationId id = ShortMutationId.serializer.deserialize(in, version);
                Token left = Token.serializer.deserialize(in, version);
                Token right = Token.serializer.deserialize(in, version);
                return new ActivatedTransfer(id, new Bounds<Token>(left, right));
            }

            @Override
            public long serializedSize(ActivatedTransfer transfer, int version)
            {
                long size = 0;
                size += ShortMutationId.serializer.serializedSize(transfer.id, version);
                size += Token.serializer.serializedSize(transfer.bounds.left, version);
                size += Token.serializer.serializedSize(transfer.bounds.right, version);
                return size;
            }
        };

        @Override
        public String toString()
        {
            return "ActivatedTransfer{" +
                   "id=" + id +
                   ", bounds=" + bounds +
                   '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            ActivatedTransfer that = (ActivatedTransfer) o;
            return Objects.equals(id, that.id) && Objects.equals(bounds, that.bounds);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, bounds);
        }
    }

    public void removeOffset(int offset)
    {
        transfers.removeIf(transfer -> transfer.id.offset() == offset);
    }

    @VisibleForTesting
    public void add(ShortMutationId transferId, Bounds<Token> bounds)
    {
        transfers.add(new ActivatedTransfer(transferId, bounds));
    }

    public void add(ShortMutationId transferId, Collection<SSTableReader> sstables)
    {
        transfers.add(new ActivatedTransfer(transferId, sstables));
    }

    public void addAll(ActivatedTransfers other)
    {
        transfers.addAll(other.transfers);
    }

    public void forEachIntersecting(AbstractBounds<PartitionPosition> range, Consumer<ShortMutationId> consumer)
    {
        for (ActivatedTransfer transfer : transfers)
            if (intersects(transfer.bounds, range))
                consumer.accept(transfer.id);
    }

    public void forEachIntersecting(Token token, Consumer<ShortMutationId> consumer)
    {
        for (ActivatedTransfer transfer : transfers)
            if (transfer.bounds.contains(token))
                consumer.accept(transfer.id);
    }

    @Override
    public Iterator<ShortMutationId> iterator()
    {
        return Iterators.transform(transfers.iterator(), transfer -> transfer.id);
    }

    public boolean isEmpty()
    {
        return transfers.isEmpty();
    }

    private static Bounds<Token> covering(Collection<SSTableReader> sstables)
    {
        Preconditions.checkArgument(!sstables.isEmpty());
        Iterator<SSTableReader> iter = sstables.iterator();
        SSTableReader next = iter.next();
        Token left = next.getFirst().getToken();
        Token right = next.getLast().getToken();
        while (iter.hasNext())
        {
            next = iter.next();
            left = Comparators.min(left, next.getFirst().getToken());
            right = Comparators.max(right, next.getLast().getToken());
        }
        return new Bounds<>(left, right);
    }

    private static boolean intersects(Bounds<Token> bounds, AbstractBounds<PartitionPosition> range)
    {
        Preconditions.checkArgument(!AbstractBounds.strictlyWrapsAround(bounds.left, bounds.right));
        if (range instanceof Range && ((Range<?>) range).isTrulyWrapAround())
        {
            List<? extends AbstractBounds<PartitionPosition>> unwrapped = range.unwrap();
            return Iterables.any(unwrapped, unwrap -> intersects(bounds, unwrap));
        }

        if (range.right.getToken().isMinimum())
        {
            /*
            bounds:       []
            range:      ?----|
            */
            boolean overlapsPastBoundary = bounds.right.compareTo(range.left.getToken()) > 0;
            /*
            bounds:     []
            range:      [----|
            */
            boolean overlapsAtBoundary = bounds.right.equals(range.left.getToken()) && range.inclusiveLeft();
            return overlapsPastBoundary || overlapsAtBoundary;
        }

        if ((range.left.getToken().compareTo(bounds.right) < 0) && (bounds.left.compareTo(range.right.getToken()) < 0))
            return true;

        if (range.inclusiveLeft() && bounds.contains(range.left.getToken()))
            return true;
        if (range.inclusiveRight() && bounds.contains(range.right.getToken()))
            return true;
        return false;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        ActivatedTransfers that = (ActivatedTransfers) o;
        return Objects.equals(transfers, that.transfers);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(transfers);
    }

    @Override
    public String toString()
    {
        return "ActivatedTransfers{" +
               "transfers=" + transfers +
               '}';
    }

    public static final IVersionedSerializer<ActivatedTransfers> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(ActivatedTransfers transfers, DataOutputPlus out, int version) throws IOException
        {
            CollectionSerializers.serializeCollection(transfers.transfers, out, version, ActivatedTransfer.serializer);
        }

        @Override
        public ActivatedTransfers deserialize(DataInputPlus in, int version) throws IOException
        {
            return new ActivatedTransfers(CollectionSerializers.deserializeSet(in, version, ActivatedTransfer.serializer));
        }

        @Override
        public long serializedSize(ActivatedTransfers transfers, int version)
        {
            return CollectionSerializers.serializedCollectionSize(transfers.transfers, version, ActivatedTransfer.serializer);
        }
    };
}

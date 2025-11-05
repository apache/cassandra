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

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableId;

/**
 * Tracks unreconciled local mutations - the subset of all unreconciled mutations
 * that have been witnessed, or are currently being written to, on the local node.
 */
public class UnreconciledMutations
{
    private static final Logger logger = LoggerFactory.getLogger(UnreconciledMutations.class);

    // Mutations (single-partition)
    private final Int2ObjectHashMap<Entry> statesMap = new Int2ObjectHashMap<>();
    private final SortedSet<Entry> statesSet = new TreeSet<>(Entry.comparator);

    // Transfers (partition-range)
    private final ActivatedTransfers transfers = new ActivatedTransfers();

    enum Visibility
    {
        PENDING, // written to the journal, but not yet to LSM
        VISIBLE, // written to both the journal and LSM
    }

    private static final class Entry
    {
        private static final Comparator<Entry> comparator = (left, right) ->
        {
            int cmp = left.token.compareTo(right.token);
            return (cmp != 0) ? cmp : Integer.compare(left.offset, right.offset);
        };

        final Token token;
        final int offset;
        final Object tableOrTables;
        private Visibility visibility;

        Entry(Token token, int offset, Object tableOrTables, Visibility visibility)
        {
            this.token = token;
            this.offset = offset;
            this.tableOrTables = tableOrTables;
            this.visibility = visibility;
        }

        static Entry create(Mutation mutation)
        {
            Collection<TableId> ids = mutation.getTableIds();
            Preconditions.checkArgument(!ids.isEmpty());
            return new Entry(mutation.key().getToken(), mutation.id().offset(), tableOrTables(mutation), Visibility.PENDING);
        }

        private static Object tableOrTables(Mutation mutation)
        {
            Collection<TableId> ids = mutation.getTableIds();
            Preconditions.checkArgument(!ids.isEmpty());
            return ids.size() == 1 ? ids.iterator().next() : Sets.newHashSet(mutation.getTableIds());
        }

        boolean contains(TableId tableId)
        {
            return tableOrTables instanceof Set
                 ? ((Set<?>) tableOrTables).contains(tableId)
                 : tableId.equals(tableOrTables);
        }

        boolean isVisible()
        {
            return visibility == Visibility.VISIBLE;
        }

        static Entry start(Token token, boolean isInclusive)
        {
            return new Entry(token, isInclusive ? 0 : Integer.MAX_VALUE, null, null);
        }

        static Entry end(Token token, boolean isInclusive)
        {
            return new Entry(token, isInclusive ? Integer.MAX_VALUE : 0, null, null);
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof Entry))
                return false;
            Entry that = (Entry) o;
            return this.offset == that.offset && this.token.equals(that.token);
        }
    }

    public void startWriting(Mutation mutation)
    {
        Entry entry = Entry.create(mutation);
        statesMap.put(entry.offset, entry);
        statesSet.add(entry);
    }

    public void finishWriting(Mutation mutation)
    {
        Preconditions.checkArgument(!mutation.id().isNone());
        Entry entry = statesMap.get(mutation.id().offset());
        if (entry == null)
            return;
        entry.visibility = Visibility.VISIBLE;
    }

    public void remove(int offset)
    {
        Entry state = statesMap.remove(offset);
        if (state == null)
            transfers.removeOffset(offset);
        else
            statesSet.remove(state);
    }

    public void activatedTransfer(ShortMutationId id, Collection<SSTableReader> sstables)
    {
        transfers.add(id, sstables);
    }

    public UnreconciledMutations copy()
    {
        UnreconciledMutations copy = new UnreconciledMutations();
        copy.statesMap.putAll(statesMap);
        copy.statesSet.addAll(statesSet);
        copy.transfers.addAll(transfers);
        return copy;
    }

    public boolean collect(AbstractBounds<PartitionPosition> range, TableId tableId, boolean includePending, Offsets.OffsetReciever into)
    {
        Entry start = Entry.start(range.left.getToken(), range.left.kind() != PartitionPosition.Kind.MAX_BOUND);
        Entry end = Entry.end(range.right.getToken(), range.right.kind() != PartitionPosition.Kind.MIN_BOUND);
        transfers.forEachIntersecting(range, id -> into.add(id.offset()));
        return collect(start, end, tableId, includePending, into);
    }

    public boolean collect(Token token, TableId tableId, boolean includePending, Offsets.OffsetReciever into)
    {
        SortedSet<Entry> subset = statesSet.subSet(Entry.start(token, true), Entry.end(token, true));
        transfers.forEachIntersecting(token, id -> into.add(id.offset()));
        return collect(subset, tableId, includePending, into);
    }

    private boolean collect(SortedSet<Entry> subset, TableId tableId, boolean includePending, Offsets.OffsetReciever into)
    {
        boolean found = false;
        for (Entry entry : subset)
        {
            if (entry.contains(tableId) && (includePending || entry.isVisible()))
            {
                into.add(entry.offset);
                found = true;
            }
        }
        return found;
    }

    private boolean collect(Entry start, Entry end, TableId tableId, boolean includePending, Offsets.OffsetReciever into)
    {
        int cmp = start.token.compareTo(end.token);
        if (cmp == 0)
        {
            // When start and end tokens are equal, check if this is a single-token range
            // Single-token ranges have start inclusive (offset=0) and end inclusive (offset=MAX_VALUE)
            if (start.offset == 0 && end.offset == Integer.MAX_VALUE)
            {
                // Single token range - collect only mutations for this specific token
                SortedSet<Entry> subset = statesSet.subSet(Entry.start(start.token, true), Entry.end(end.token, true));
                return collect(subset, tableId, includePending, into);
            }
            else
            {
                // Full range - collect all mutations
                return collect(statesSet, tableId, includePending, into);
            }
        }
        else if (cmp > 0)
        {
            // wrap around range
            boolean lFound = collect(statesSet.headSet(end), tableId, includePending, into);
            boolean rFound = collect(statesSet.tailSet(start), tableId, includePending, into);
            return lFound || rFound;
        }
        else
        {
            // contiguous range
            return collect(statesSet.subSet(start, end), tableId, includePending, into);
        }
    }

    @VisibleForTesting
    boolean equalsForTesting(UnreconciledMutations other)
    {
        return this.statesMap.equals(other.statesMap) && this.statesSet.equals(other.statesSet) && this.transfers.equals(other.transfers);
    }

    @VisibleForTesting
    void addDirectly(Mutation mutation)
    {
        Entry entry = Entry.create(mutation);
        entry.visibility = Visibility.VISIBLE;
        statesMap.put(entry.offset, entry);
        statesSet.add(entry);
    }

    @VisibleForTesting
    boolean isEmpty()
    {
        return statesMap.isEmpty();
    }

    public int size()
    {
        return statesMap.size();
    }

    static UnreconciledMutations loadFromJournal(Node2OffsetsMap witnessedOffsets, int localNodeId)
    {
        UnreconciledMutations result = new UnreconciledMutations();

        Offsets.Mutable witnessed = witnessedOffsets.get(localNodeId);
        Offsets.Mutable reconciled = witnessedOffsets.intersection();

        // difference between locally witnessed offsets and fully reconciled ones is all the ids
        // that need to be loaded into UnreconciledMutations index
        Offsets.RangeIterator iter = Offsets.difference(witnessed.rangeIterator(), reconciled.rangeIterator());
        while (iter.tryAdvance())
        {
            for (int offset = iter.start(), end = iter.end(); offset <= end; offset++)
            {
                ShortMutationId id = new ShortMutationId(witnessed.logId, offset);
                Mutation mutation = MutationJournal.instance.read(id);
                if (mutation != null)
                {
                    result.addDirectly(mutation);
                    continue;
                }
                CoordinatedTransfer transfer = LocalTransfers.instance().getActivatedTransfer(id);
                if (transfer != null)
                {
                    result.transfers.add(transfer.id(), transfer.sstables);
                    continue;
                }

                logger.error("Cannot load unknown mutation ID {}", id);
            }
        }

        // Transfers are never present in the journal, since they're added as SSTables directly

        return result;
    }
}

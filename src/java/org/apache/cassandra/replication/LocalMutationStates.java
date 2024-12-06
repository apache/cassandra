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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableId;

/**
 * Tracks unreconciled local mutations - the subset of all unreconciled mutations
 * that have been witnessed, or are currently being written to, on the local node.
 */
class LocalMutationStates
{
    private final Int2ObjectHashMap<Entry> statesMap = new Int2ObjectHashMap<>();
    private final SortedSet<Entry> statesSet = new TreeSet<>(Entry.comparator);

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

    void startWriting(Mutation mutation)
    {
        Entry entry = Entry.create(mutation);
        statesMap.put(entry.offset, entry);
        statesSet.add(entry);
    }

    void finishWriting(Mutation mutation)
    {
        Entry entry = statesMap.get(mutation.id().offset());
        Preconditions.checkNotNull(entry);
        entry.visibility = Visibility.VISIBLE;
    }

    void remove(int offset)
    {
        Entry state = statesMap.remove(offset);
        Preconditions.checkNotNull(state);
        statesSet.remove(state);
    }

    boolean collect(Token token, TableId tableId, boolean includePending, Offsets into)
    {
        SortedSet<Entry> subset = statesSet.subSet(Entry.start(token, true), Entry.end(token, true));
        return collect(subset, tableId, includePending, into);
    }

    boolean collect(AbstractBounds<PartitionPosition> range, TableId tableId, boolean includePending, Offsets into)
    {
        Entry start = Entry.start(range.left.getToken(), range.left.kind() != PartitionPosition.Kind.MAX_BOUND);
        Entry end = Entry.end(range.right.getToken(), range.right.kind() != PartitionPosition.Kind.MIN_BOUND);
        return collect(start, end, tableId, includePending, into);
    }

    private boolean collect(SortedSet<Entry> subset, TableId tableId, boolean includePending, Offsets into)
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

    private boolean collect(Entry start, Entry end, TableId tableId, boolean includePending, Offsets into)
    {
        int cmp = start.token.compareTo(end.token);
        if (cmp == 0)
        {
            // full range
            return collect(statesSet, tableId, includePending, into);
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
}

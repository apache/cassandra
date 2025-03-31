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
import org.apache.cassandra.dht.Range;
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

        Entry(Token token, int offset, Object tableOrTables)
        {
            this.token = token;
            this.offset = offset;
            this.tableOrTables = tableOrTables;
        }

        static Entry create(Mutation mutation)
        {
            Collection<TableId> ids = mutation.getTableIds();
            Preconditions.checkArgument(!ids.isEmpty());
            return new Entry(mutation.key().getToken(), mutation.id().offset(), tableOrTables(mutation));
        }

        private static Object tableOrTables(Mutation mutation)
        {
            Collection<TableId> ids = mutation.getTableIds();
            Preconditions.checkArgument(!ids.isEmpty());
            return ids.size() == 1 ? ids.iterator().next() : Sets.newHashSet(mutation.getTableIds());
        }

        private boolean contains(TableId tableId)
        {
            return tableOrTables instanceof Set
                 ? ((Set<?>) tableOrTables).contains(tableId)
                 : tableId.equals(tableOrTables);
        }

        static Entry start(Token token, boolean isInclusive)
        {
            return new Entry(token, isInclusive ? 0 : Integer.MAX_VALUE, null);
        }

        static Entry end(Token token, boolean isInclusive)
        {
            return new Entry(token, isInclusive ? Integer.MAX_VALUE : 0, null);
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

    void add(Mutation mutation)
    {
        Entry entry = Entry.create(mutation);
        statesMap.put(entry.offset, entry);
        statesSet.add(entry);
    }

    boolean lookUp(Token token, TableId tableId, Offsets into)
    {
        SortedSet<Entry> subset = statesSet.subSet(Entry.start(token, true), Entry.end(token, true));
        return addSubset(subset, tableId, into);
    }

    private boolean addSubset(SortedSet<Entry> subset, TableId tableId, Offsets into)
    {
        boolean found = false;
        for (Entry entry : subset)
        {
            if (entry.contains(tableId))
            {
                into.add(entry.offset);
                found = true;
            }
        }
        return found;
    }

    private boolean lookUp(Entry start, Entry end, TableId tableId, Offsets into)
    {
        int cmp = start.token.compareTo(end.token);
        if (cmp == 0)
        {
            // full range
            return addSubset(statesSet, tableId, into);
        }
        else if (cmp > 0)
        {
            // wrap around range
            boolean lFound = addSubset(statesSet.headSet(end), tableId, into);
            boolean rFound = addSubset(statesSet.tailSet(start), tableId, into);
            return lFound || rFound;
        }
        else
        {
            // contiguous range
            return addSubset(statesSet.subSet(start, end), tableId, into);
        }
    }

    boolean lookUp(Range<Token> range, TableId tableId, Offsets into)
    {
        return lookUp(Entry.start(range.left, false), Entry.end(range.right, true), tableId, into);
    }

    boolean lookUp(AbstractBounds<PartitionPosition> range, TableId tableId, Offsets into)
    {
        Entry start = Entry.start(range.left.getToken(), range.left.kind() != PartitionPosition.Kind.MAX_BOUND);
        Entry end = Entry.end(range.right.getToken(), range.right.kind() != PartitionPosition.Kind.MIN_BOUND);
        return lookUp(start, end, tableId, into);
    }

    void remove(int offset)
    {
        Entry state = statesMap.remove(offset);
        if (state != null)
            statesSet.remove(new Entry(state.token, offset, null));
    }
}

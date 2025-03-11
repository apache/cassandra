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
package org.apache.cassandra.service.tracking;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

class IdTokenIndex
{
    private final Long2ObjectHashMap<Token> idToToken = new Long2ObjectHashMap<>();
    // FIXME: BTreeSet is missing some methods used by this class.
//    private final BTreeSet<Entry> tokenToIds = BTreeSet.empty(Entry.comparator);
    private final SortedSet<Entry> tokenToIds = new TreeSet<>(Entry.comparator);

    private static final class Entry
    {
        private static final Comparator<Entry> comparator = (left, right) ->
        {
            int cmp = left.token.compareTo(right.token);
            return (cmp != 0) ? cmp : Long.compare(left.sequenceId, right.sequenceId);
        };

        final Token token;
        final long sequenceId;

        Entry(Token token, long sequenceId)
        {
            this.token = token;
            this.sequenceId = sequenceId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof Entry))
                return false;
            Entry that = (Entry) o;
            return this.sequenceId == that.sequenceId && this.token.equals(that.token);
        }
    }

    void update(long sequenceId, Token token)
    {
        idToToken.put(sequenceId, token);
        tokenToIds.add(new Entry(token, sequenceId));
    }

    boolean lookUp(Token token, SequenceIds into)
    {
        boolean found = false;
        SortedSet<Entry> subset = tokenToIds.subSet(new Entry(token, 0), new Entry(token, Long.MAX_VALUE));
        for (Entry entry : subset)
        {
            into.append(entry.sequenceId);
            found = true;
        }
        return found;
    }

    // TODO (expected): handle wrap-around ranges
    boolean lookUp(Range<Token> range, SequenceIds into)
    {
        boolean found = false;
        SortedSet<Entry> subset = tokenToIds.subSet(new Entry(range.left, 0), new Entry(range.right, Long.MAX_VALUE));
        for (Entry entry : subset)
        {
            into.append(entry.sequenceId);
            found = true;
        }
        return found;
    }

    boolean lookUp(AbstractBounds<PartitionPosition> range, SequenceIds into)
    {
        boolean found = false;
        Entry start = new Entry(range.left.getToken(), range.inclusiveLeft() ? 0 : Long.MAX_VALUE);
        Entry end = new Entry(range.right.getToken(), range.inclusiveRight() ? Long.MAX_VALUE : 0);
        SortedSet<Entry> subset = tokenToIds.subSet(start, end);
        for (Entry entry : subset)
        {
            into.append(entry.sequenceId);
            found = true;
        }
        return found;
    }

    void invalidate(long sequenceId)
    {
        Token token = idToToken.remove(sequenceId);
        if (token != null)
            tokenToIds.remove(new Entry(token, sequenceId));
    }
}

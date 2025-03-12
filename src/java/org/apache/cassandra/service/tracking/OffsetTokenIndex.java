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

import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

class OffsetTokenIndex
{
    private final Int2ObjectHashMap<Token> offsetToToken = new Int2ObjectHashMap<>();
    private final SortedSet<Entry> tokenToOffsets = new TreeSet<>(Entry.comparator);

    private static final class Entry
    {
        private static final Comparator<Entry> comparator = (left, right) ->
        {
            int cmp = left.token.compareTo(right.token);
            return (cmp != 0) ? cmp : Integer.compare(left.offset, right.offset);
        };

        final Token token;
        final int offset;

        Entry(Token token, int offset)
        {
            this.token = token;
            this.offset = offset;
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

    void update(int offset, Token token)
    {
        offsetToToken.put(offset, token);
        tokenToOffsets.add(new Entry(token, offset));
    }

    boolean lookUp(Token token, Offsets into)
    {
        boolean found = false;
        SortedSet<Entry> subset = tokenToOffsets.subSet(new Entry(token, 0), new Entry(token, Integer.MAX_VALUE));
        for (Entry entry : subset)
        {
            into.append(entry.offset);
            found = true;
        }
        return found;
    }

    // TODO (expected): handle wrap-around ranges
    boolean lookUp(Range<Token> range, Offsets into)
    {
        boolean found = false;
        SortedSet<Entry> subset = tokenToOffsets.subSet(new Entry(range.left, 0), new Entry(range.right, Integer.MAX_VALUE));
        for (Entry entry : subset)
        {
            into.append(entry.offset);
            found = true;
        }
        return found;
    }

    boolean lookUp(AbstractBounds<PartitionPosition> range, Offsets into)
    {
        boolean found = false;
        Entry start = new Entry(range.left.getToken(), range.inclusiveLeft() ? 0 : Integer.MAX_VALUE);
        Entry end = new Entry(range.right.getToken(), range.inclusiveRight() ? Integer.MAX_VALUE : 0);
        SortedSet<Entry> subset = tokenToOffsets.subSet(start, end);
        for (Entry entry : subset)
        {
            into.append(entry.offset);
            found = true;
        }
        return found;
    }

    void invalidate(int offset)
    {
        Token token = offsetToToken.remove(offset);
        if (token != null)
            tokenToOffsets.remove(new Entry(token, offset));
    }
}

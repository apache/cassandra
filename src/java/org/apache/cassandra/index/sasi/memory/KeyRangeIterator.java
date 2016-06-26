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
package org.apache.cassandra.index.sasi.memory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.cassandra.db.*;
import org.apache.cassandra.index.sasi.disk.*;
import org.apache.cassandra.index.sasi.utils.AbstractIterator;
import org.apache.cassandra.index.sasi.utils.CombinedValue;
import org.apache.cassandra.index.sasi.utils.RangeIterator;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.cassandra.utils.*;

import com.google.common.collect.PeekingIterator;
import org.apache.commons.lang.NotImplementedException;

public class KeyRangeIterator extends RangeIterator<Long, Token>
{
    private final DKIterator iterator;

    public KeyRangeIterator(ConcurrentSkipListSet<Pair<DecoratedKey, ClusteringPrefix>> keys)
    {
        super((Long) keys.first().left.getToken().getTokenValue(), (Long) keys.last().left.getToken().getTokenValue(), keys.size());
        this.iterator = new DKIterator(keys.iterator());
    }

    protected Token computeNext()
    {
        return iterator.hasNext() ? new DKToken(iterator.next()) : endOfData();
    }

    protected void performSkipTo(Long nextToken)
    {
        while (iterator.hasNext())
        {
            Pair<DecoratedKey, ClusteringPrefix> key = iterator.peek();
            // TODO: (ifesdjeen) fix comparison
            if (Long.compare((long) key.left.getToken().getTokenValue(), nextToken) >= 0)
                break;

            // consume smaller key
            iterator.next();
        }
    }

    public void close() throws IOException
    {}

    private static class DKIterator extends AbstractIterator<Pair<DecoratedKey, ClusteringPrefix>> implements PeekingIterator<Pair<DecoratedKey, ClusteringPrefix>>
    {
        private final Iterator<Pair<DecoratedKey, ClusteringPrefix>> keys;

        public DKIterator(Iterator<Pair<DecoratedKey, ClusteringPrefix>> keys)
        {
            this.keys = keys;
        }

        protected Pair<DecoratedKey, ClusteringPrefix> computeNext()
        {
            return keys.hasNext() ? keys.next() : endOfData();
        }
    }

    private static class DKToken extends Token
    {
        private final SortedSet<Pair<DecoratedKey, ClusteringPrefix>> keys;

        public DKToken(Pair<DecoratedKey, ClusteringPrefix> key)
        {
            super((long) key.left.getToken().getTokenValue());

            keys = new TreeSet<Pair<DecoratedKey, ClusteringPrefix>>(TokenTree.OnDiskToken.comparator)
            {{
                add(key);
            }};
        }

        public Set<RowOffset> getOffsets()
        {
            throw new IllegalStateException("DecoratedKey tokens are used in memtables and do not have on-disk offsets");
        }

        public void merge(CombinedValue<Long> other)
        {
            if (!(other instanceof Token))
                return;

            Token o = (Token) other;
            assert o.get().equals(token);

            if (o instanceof DKToken)
            {
                keys.addAll(((DKToken) o).keys);
            }
            else
            {
                for (Pair<DecoratedKey, ClusteringPrefix> key : o)
                    keys.add(key);
            }
        }

        public Iterator<Pair<DecoratedKey, ClusteringPrefix>> iterator()
        {
            return keys.iterator();
        }
    }
}
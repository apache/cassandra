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
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.utils.CombinedValue;
import org.apache.cassandra.index.sasi.utils.RangeIterator;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.cassandra.utils.AbstractGuavaIterator;

import com.google.common.collect.PeekingIterator;

public class KeyRangeIterator extends RangeIterator<Long, Token>
{
    private final DKIterator iterator;

    public KeyRangeIterator(ConcurrentSkipListSet<DecoratedKey> keys, int size)
    {
        super((Long) keys.first().getToken().getTokenValue(), (Long) keys.last().getToken().getTokenValue(), size);
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
            DecoratedKey key = iterator.peek();
            if (Long.compare((long) key.getToken().getTokenValue(), nextToken) >= 0)
                break;

            // consume smaller key
            iterator.next();
        }
    }

    public void close() throws IOException
    {}

    private static class DKIterator extends AbstractGuavaIterator<DecoratedKey> implements PeekingIterator<DecoratedKey>
    {
        private final Iterator<DecoratedKey> keys;

        public DKIterator(Iterator<DecoratedKey> keys)
        {
            this.keys = keys;
        }

        protected DecoratedKey computeNext()
        {
            return keys.hasNext() ? keys.next() : endOfData();
        }
    }

    private static class DKToken extends Token
    {
        private final SortedSet<DecoratedKey> keys;

        public DKToken(final DecoratedKey key)
        {
            super((long) key.getToken().getTokenValue());

            keys = new TreeSet<DecoratedKey>(DecoratedKey.comparator)
            {{
                add(key);
            }};
        }

        public LongSet getOffsets()
        {
            LongSet offsets = new LongHashSet(4);
            for (DecoratedKey key : keys)
                offsets.add((long) key.getToken().getTokenValue());

            return offsets;
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
                for (DecoratedKey key : o)
                    keys.add(key);
            }
        }

        public Iterator<DecoratedKey> iterator()
        {
            return keys.iterator();
        }
    }
}

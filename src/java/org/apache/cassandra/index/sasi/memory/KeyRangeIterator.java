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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sasi.disk.*;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.utils.AbstractIterator;
import org.apache.cassandra.index.sasi.utils.CombinedValue;
import org.apache.cassandra.index.sasi.utils.RangeIterator;

import com.google.common.collect.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyRangeIterator extends RangeIterator<Long, Token>
{
    private final DKIterator iterator;

    public KeyRangeIterator(ConcurrentSkipListSet<RowKey> keys)
    {
        super((Long) keys.first().decoratedKey.getToken().getTokenValue(), (Long) keys.last().decoratedKey.getToken().getTokenValue(), keys.size());
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
            RowKey key = iterator.peek();
            if (Long.compare((Long) key.decoratedKey.getToken().getTokenValue(), nextToken) >= 0)
                break;

            // consume smaller key
            iterator.next();
        }
    }

    public void close() throws IOException
    {}

    private static class DKIterator extends AbstractIterator<RowKey> implements PeekingIterator<RowKey>
    {
        private final Iterator<RowKey> keys;

        public DKIterator(Iterator<RowKey> keys)
        {
            this.keys = keys;
        }

        protected RowKey computeNext()
        {
            return keys.hasNext() ? keys.next() : endOfData();
        }
    }

    private static class DKToken extends Token
    {
        private final SortedSet<RowKey> keys;

        public DKToken(RowKey key)
        {
            super((Long) key.decoratedKey.getToken().getTokenValue());

            keys = new TreeSet<RowKey>(RowKey.COMPARATOR)
            {{
                add(key);
            }};
        }

        public KeyOffsets getOffsets()
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
                for (RowKey key : o)
                    keys.add(key);
            }
        }

        public Iterator<RowKey> iterator()
        {
            return keys.iterator();
        }
    }
}

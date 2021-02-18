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
package org.apache.cassandra.index.sai.memory;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.SortedSet;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sai.Token;
import org.apache.cassandra.index.sai.utils.RangeIterator;

public class KeyRangeIterator extends RangeIterator
{
    private final PriorityQueue<DecoratedKey> keys;
    private final boolean uniqueKeys;
    private DecoratedKey lastKey;

    /**
     * An in-memory {@link RangeIterator} that uses a {@link SortedSet} which has no duplication as its backing store.
     */
    public KeyRangeIterator(SortedSet<DecoratedKey> keys)
    {
        super((Long) keys.first().getToken().getTokenValue(), (Long) keys.last().getToken().getTokenValue(), keys.size());
        this.keys = new PriorityQueue<>(keys);
        this.uniqueKeys = true;
    }

    /**
     * An in-memory {@link RangeIterator} that uses a {@link PriorityQueue} which may
     * contain duplicated keys as its backing store.
     */
    public KeyRangeIterator(Long min, Long max, PriorityQueue<DecoratedKey> keys)
    {
        super(min, max, keys.size());
        this.keys = keys;
        this.uniqueKeys = false;
    }

    protected Token computeNext()
    {
        DecoratedKey key = computeNextKey();
        return key == null ? endOfData() : new InMemoryToken(key.getToken().getLongValue(), key);
    }

    private DecoratedKey computeNextKey()
    {
        DecoratedKey next = null;

        while (!keys.isEmpty())
        {
            DecoratedKey key = keys.poll();
            if (uniqueKeys)
                return key;

            if (lastKey == null || lastKey.compareTo(key) != 0)
            {
                next = key;
                lastKey = key;
                break;
            }
        }

        return next;
    }

    protected void performSkipTo(Long nextToken)
    {
        while (!keys.isEmpty())
        {
            DecoratedKey key = keys.peek();
            if ((long) key.getToken().getTokenValue() >= nextToken)
                break;

            // consume smaller key
            keys.poll();
        }
    }

    public void close() throws IOException
    {}
}
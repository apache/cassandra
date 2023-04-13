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

import java.util.PriorityQueue;
import java.util.SortedSet;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

@NotThreadSafe
public class InMemoryKeyRangeIterator extends KeyRangeIterator
{
    private final PriorityQueue<PrimaryKey> keys;
    private final boolean uniqueKeys;
    private PrimaryKey lastKey;

    /**
     * An in-memory {@link KeyRangeIterator} that uses a {@link PriorityQueue} built from a {@link SortedSet}
     * which has no duplication as its backing store.
     */
    public InMemoryKeyRangeIterator(SortedSet<PrimaryKey> keys)
    {
        super(keys.first(), keys.last(), keys.size());
        this.keys = new PriorityQueue<>(keys);
        this.uniqueKeys = true;
    }

    /**
     * An in-memory {@link KeyRangeIterator} that uses a {@link PriorityQueue} which may
     * contain duplicated keys as its backing store.
     */
    public InMemoryKeyRangeIterator(PrimaryKey min, PrimaryKey max, PriorityQueue<PrimaryKey> keys)
    {
        super(min, max, keys.size());
        this.keys = keys;
        this.uniqueKeys = false;
    }

    @Override
    protected PrimaryKey computeNext()
    {
        PrimaryKey key = computeNextKey();
        return key == null ? endOfData() : key;
    }

    protected PrimaryKey computeNextKey()
    {
        PrimaryKey next = null;

        while (!keys.isEmpty())
        {
            PrimaryKey key = keys.poll();
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

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        while (!keys.isEmpty())
        {
            PrimaryKey key = keys.peek();
            if (key.compareTo(nextKey) >= 0)
                break;

            // consume smaller key
            keys.poll();
        }
    }

    @Override
    public void close()
    {}
}

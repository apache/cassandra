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

package org.apache.cassandra.index.sai.utils;

import java.util.List;
import java.util.SortedSet;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * A {@link RangeIterator} that iterates over a collection of {@link PrimaryKey}s without modifying the underlying list.
 */
public class CollectionRangeIterator extends RangeIterator
{
    private final PeekingIterator<PrimaryKey> keyQueue;

    /**
     * Create a new {@link CollectionRangeIterator} that iterates over the provided list of keys.
     * @param minimumKey the minimum key for the provided list of keys
     * @param maximumKey the maximum key for the provided list of keys
     * @param keys the list of keys to iterate over
     */
    public CollectionRangeIterator(PrimaryKey minimumKey, PrimaryKey maximumKey, List<PrimaryKey> keys)
    {
        super(minimumKey, maximumKey, keys.size());
        this.keyQueue = Iterators.peekingIterator(keys.iterator());
    }

    /**
     * Create a new {@link CollectionRangeIterator} that iterates over the provided set of keys.
     * @param keys the sorted set of keys to iterate over
     */
    public CollectionRangeIterator(SortedSet<PrimaryKey> keys)
    {
        super(keys.first(), keys.last(), keys.size());
        this.keyQueue = Iterators.peekingIterator(keys.iterator());
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        while (keyQueue.hasNext())
        {
            if (keyQueue.peek().compareTo(nextKey) >= 0)
                break;
            keyQueue.next();
        }
    }

    @Override
    public void close() {}

    @Override
    protected PrimaryKey computeNext()
    {
        return keyQueue.hasNext() ? keyQueue.next() : endOfData();
    }
}

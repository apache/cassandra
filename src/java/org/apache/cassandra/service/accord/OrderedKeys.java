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
package org.apache.cassandra.service.accord;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.cassandra.utils.BulkIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;

/**
 * Maintains an ordered set of keys backed by a BTree, buffering additions and removals for batch updates.
 */
public class OrderedKeys<K> implements Iterable<K>
{
    private final Comparator<K> comparator;

    private Object[] map;
    private final Object[] buffer = new Object[32];
    private int bufferRemovals;  // Bit field: 1 = removal, 0 = addition
    private int bufferCount;

    public OrderedKeys(Comparator<K> comparator)
    {
        this.comparator = comparator;
        this.map = BTree.empty();
    }

    public OrderedKeys(Comparator<K> comparator, Collection<K> init)
    {
        this.comparator = comparator;
        BTree.Builder<K> builder = BTree.builder(comparator);
        for (K k : init)
            builder.add(k);
        this.map = builder.build();
    }

    public void add(K key, int removalBit)
    {
        if (bufferCount == buffer.length)
            flush();
        bufferRemovals |= removalBit << bufferCount;
        buffer[bufferCount++] = key;
    }

    public void add(K key)
    {
        add(key, 0);
    }

    public void remove(K key)
    {
        add(key, 1);
    }

    /**
     * Flushes all buffered additions and removals to the underlying BTree.
     */
    public void flush()
    {
        if (bufferCount == 0)
            return;

        // Partition the buffer: additions first, then removals
        int additionEnd = 0, removalStart = bufferCount;
        while (additionEnd != removalStart)
        {
            if (0 == (bufferRemovals & (1 << additionEnd))) ++additionEnd;
            else if (0 != (bufferRemovals & (1 << (removalStart - 1)))) --removalStart;
            else
            {
                Object tmp = buffer[additionEnd];
                buffer[additionEnd++] = buffer[--removalStart];
                buffer[removalStart] = tmp;
            }
        }

        // Sort both partitions
        Arrays.sort(buffer, 0, removalStart, (Comparator) comparator);
        Arrays.sort(buffer, removalStart, bufferCount, (Comparator) comparator);

        int i = 0, j = removalStart;

        // Merge additions and removals
        additionEnd = 0;
        int removalEnd = removalStart;
        while (i < removalStart && j < bufferCount)
        {
            int c = comparator.compare((K) buffer[i], (K) buffer[j]);
            if (c == 0)
            {
                // matched pairs can be ignored
                ++i; ++j;
            }
            else if (c < 0)
            {
                // Addition is smaller
                if (additionEnd != i)
                    buffer[additionEnd] = buffer[i];
                ++additionEnd;
                ++i;
            }
            else
            {
                // Removal is smaller
                if (removalEnd != j)
                    buffer[removalEnd] = buffer[j];
                ++removalEnd;
                ++j;
            }
        }

        // Copy remaining additions
        if (i != removalStart)
        {
            if (additionEnd == i) additionEnd = removalStart;
            else
            {
                int count = removalStart - i;
                System.arraycopy(buffer, i, buffer, additionEnd, count);
                additionEnd += count;
            }
        }

        // Copy remaining removals
        if (j != bufferCount)
        {
            if (removalEnd == j) removalEnd = bufferCount;
            else
            {
                int count = bufferCount - j;
                System.arraycopy(buffer, j, buffer, removalEnd, count);
                removalEnd += count;
            }
        }

        // Apply additions to BTree
        if (additionEnd != 0)
            map = BTree.update(map, BTree.build(BulkIterator.of(buffer, 0), additionEnd, UpdateFunction.noOp()), comparator);

        // Apply removals from BTree
        if (removalEnd != removalStart)
            map = BTree.subtract(map, BTree.build(BulkIterator.of(buffer, removalStart), removalEnd - removalStart, UpdateFunction.noOp()), comparator);

        // Clear buffer
        Arrays.fill(buffer, 0, bufferCount, null);
        bufferRemovals = 0;
        bufferCount = 0;
    }

    public Iterable<K> between(K start, boolean startInclusive, K end, boolean endInclusive)
    {
        return () -> {
            flush();
            return BTree.slice(map, comparator, start, startInclusive, end, endInclusive, BTree.Dir.ASC);
        };
    }

    public Iterator<K> iterator()
    {
        flush();
        return BTree.iterator(map);
    }

    public int size()
    {
        flush();
        return BTree.size(map);
    }

    public int bufferSize()
    {
        return bufferCount;
    }
}

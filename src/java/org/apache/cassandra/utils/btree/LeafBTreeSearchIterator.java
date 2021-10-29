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
package org.apache.cassandra.utils.btree;

import java.util.Arrays;
import java.util.Comparator;
import java.util.NoSuchElementException;

import static org.apache.cassandra.utils.btree.BTree.size;

public class LeafBTreeSearchIterator<K, V> implements BTreeSearchIterator<K, V>
{
    private final boolean forwards;
    private final K[] keys;
    private final Comparator<? super K> comparator;
    private int nextPos;
    private final int lowerBound, upperBound; // inclusive
    private boolean hasNext;
    private boolean hasCurrent;

    public LeafBTreeSearchIterator(Object[] btree, Comparator<? super K> comparator, BTree.Dir dir)
    {
        this(btree, comparator, dir, 0, size(btree) -1);
    }

    LeafBTreeSearchIterator(Object[] btree, Comparator<? super K> comparator, BTree.Dir dir, int lowerBound, int upperBound)
    {
        this.keys = (K[]) btree;
        this.forwards = dir == BTree.Dir.ASC;
        this.comparator = comparator;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        rewind();
    }

    public void rewind()
    {
        nextPos = forwards ? lowerBound : upperBound;
        hasNext = nextPos >= lowerBound && nextPos <= upperBound;
    }

    public V next()
    {
        if (!hasNext)
            throw new NoSuchElementException();
        final V elem = (V) keys[nextPos];
        nextPos += forwards ? 1 : -1;
        hasNext = nextPos >= lowerBound && nextPos <= upperBound;
        hasCurrent = true;
        return elem;
    }

    public boolean hasNext()
    {
        return hasNext;
    }

    private int searchNext(K key)
    {
        int lb = forwards ? nextPos : lowerBound; // inclusive
        int ub = forwards ? upperBound : nextPos; // inclusive

        return Arrays.binarySearch(keys, lb, ub + 1, key, comparator);
    }

    private void updateHasNext()
    {
        hasNext = nextPos >= lowerBound && nextPos <= upperBound;
    }

    public V next(K key)
    {
        if (!hasNext)
            return null;
        V result = null;

        // first check the current position in case of sequential access
        if (comparator.compare(keys[nextPos], key) == 0)
        {
            hasCurrent = true;
            result = (V) keys[nextPos];
            nextPos += forwards ? 1 : -1;
        }
        updateHasNext();

        if (result != null || !hasNext)
            return result;

        // otherwise search against the remaining values
        int find = searchNext(key);
        if (find >= 0)
        {
            hasCurrent = true;
            result = (V) keys[find];
            nextPos = find + (forwards ? 1 : -1);
        }
        else
        {
            nextPos = (forwards ? -1 : -2) - find;
            hasCurrent = false;
        }
        updateHasNext();
        return result;
    }

    public V current()
    {
        if (!hasCurrent)
            throw new NoSuchElementException();
        int current = forwards ? nextPos - 1 : nextPos + 1;
        return (V) keys[current];
    }

    public int indexOfCurrent()
    {
        if (!hasCurrent)
            throw new NoSuchElementException();
        int current = forwards ? nextPos - 1 : nextPos + 1;
        return forwards ? current - lowerBound : upperBound - current;
    }
}

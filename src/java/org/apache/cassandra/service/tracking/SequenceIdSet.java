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


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.MutationId;

import java.util.Arrays;
import java.util.Objects;

/**
 * Sorted set of sequence of ids
 */
public class SequenceIdSet
{
    public static final int INITIAL_CAPACITY = 8;
    private static final int MAX_CAPACITY = 2147483639;

    // ordered set of sequence ids
    private long[] ids;
    private int size;
    private int expectedMaxSize;

    public SequenceIdSet(int initialSize, int expectedMaxSize)
    {
        this.ids = new long[initialSize];
        this.size = 0;
        this.expectedMaxSize = expectedMaxSize;
    }

    public SequenceIdSet(int initialSize)
    {
        this(initialSize, MAX_CAPACITY);
    }

    public SequenceIdSet()
    {
        this(INITIAL_CAPACITY);
    }

    public SequenceIdSet(long[] ids)
    {
        if (ids.length > 1)
        {
            long last = ids[0];
            for (int i = 1; i < ids.length; i++)
            {
                long next = ids[i];
                if (next <= last)
                    throw new AssertionError(String.format("%s <= %s", next, last));
                last = next;
            }
        }
        this.ids = Arrays.copyOf(ids, ids.length);
        this.expectedMaxSize = MAX_CAPACITY;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        SequenceIdSet that = (SequenceIdSet) o;
        return size == that.size && Arrays.equals(ids, 0, size, that.ids, 0, size);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(Arrays.hashCode(ids), size);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("{");
        int i = 0;
        while (i < size)
        {
            long sequence = ids[i++];
            builder.append('<')
                   .append(MutationId.offset(sequence))
                   .append(',')
                   .append(MutationId.timestamp(sequence))
                   .append('>');
            if (i < size) builder.append(',');
        }
        builder.append('}');
        return builder.toString();
    }

    @VisibleForTesting
    int capacity()
    {
        return ids.length;
    }

    @VisibleForTesting
    long[] toArray()
    {
        return Arrays.copyOf(ids, size);
    }

    public int size()
    {
        return size;
    }

    public void digest(Digest digest)
    {
        digest.updateWithInt(size);
        for (int i = 0; i < size; i++)
            digest.updateWithLong(ids[i]);
    }

    public void ensureAdditionalCapacity(int additional)
    {
        expectedMaxSize = Math.max(expectedMaxSize, size + additional);
        ensureCapacity(size + additional);
    }

    private void ensureCapacity(int minCapacity)
    {
        int currentCapacity = ids.length;
        if (currentCapacity >= minCapacity)
            return;

        if (minCapacity > MAX_CAPACITY)
            throw new IllegalStateException("max capacity: " + MAX_CAPACITY);

        int newCapacity = Math.max(currentCapacity, INITIAL_CAPACITY);
        newCapacity *= 2;
        if (newCapacity < 0 || newCapacity > MAX_CAPACITY)
            newCapacity = MAX_CAPACITY;

        if (currentCapacity < expectedMaxSize && newCapacity > expectedMaxSize)
            newCapacity = expectedMaxSize;

        long[] newIds = new long[newCapacity];
        System.arraycopy(ids, 0, newIds, 0, size);
        ids = newIds;
    }

    public void append(long id)
    {
        Preconditions.checkArgument(size == 0 || id > ids[size - 1]);
        ensureCapacity(size + 1);
        ids[size++] = id;
    }

    public boolean add(long id)
    {
        int idx = Arrays.binarySearch(ids, 0, size, id);
        if (idx >= 0)
            return false;

        ensureCapacity(size + 1);
        idx = -idx - 1;
        if (idx < size)
            System.arraycopy(ids, idx, ids, idx + 1, size - idx);
        size++;
        ids[idx] = id;
        return true;
    }
}

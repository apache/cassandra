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

import java.util.Arrays;

import static org.apache.cassandra.service.tracking.MutationId.offset;
import static org.apache.cassandra.service.tracking.MutationId.timestamp;

public class SequenceIds
{
    private static final int INITIAL_CAPACITY = 16;

    private long[] bounds;
    private int size;

    public SequenceIds()
    {
        size = 0;
        bounds = new long[INITIAL_CAPACITY];
    }

    public int rangeCount()
    {
        return size / 2;
    }

    public int idCount()
    {
        int count = 0, i = 0;
        while (i < size)
        {
            long start = bounds[i++];
            long   end = bounds[i++];
            count += offset(end) - offset(start) + 1;
        }
        return count;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("{");
        int i = 0;
        while (i < size)
        {
            long start = bounds[i++];
            long   end = bounds[i++];
            builder.append('[')
                   .append('<').append(offset(start)).append(',').append(timestamp(start)).append('>')
                   .append(',')
                   .append('<').append(offset(end)).append(',').append(timestamp(end)).append('>')
                   .append(']');
            if (i < size) builder.append(',');
        }
        builder.append('}');
        return builder.toString();
    }

    public boolean contains(long id)
    {
        if (size == 0)
            return false;

        int pos = Arrays.binarySearch(bounds, 0, size, id);
        if (pos >= 0) return true; // matches one of the bounds

        pos = -pos - 1;
        return ((pos - 1) % 2 == 0); // id falls within bounds of an existing range if the bound to the left is an open one
    }

    public void addAll(SequenceIds other, RangeConsumer onAdded)
    {
        for (int i = 0; i < other.size; i += 2)
            add(other.bounds[i], other.bounds[i + 1], onAdded);
    }

    public boolean add(long id)
    {
        if (size == 0)
        {
            append(id, id);
            return true;
        }

        int pos = Arrays.binarySearch(bounds, 0, size, id);
        if (pos >= 0) return false; // matches one of the bounds

        pos = -pos - 1;
        if (pos == size) // after all existing ranges
        {
            if (offset(bounds[size - 1]) == offset(id) - 1)
                bounds[size - 1] = id; // extend the last range
            else
                append(id, id); // append a new single-id range

            return true;
        }
        else if (pos == 0) // before all existing ranges
        {
            if (offset(bounds[0]) == offset(id) + 1)
                bounds[0] = id; // extend the first range
            else
                insert(0, id, id); // prepend a new single-id range

            return true;
        }
        else if ((pos - 1) % 2 == 0) // id falls within bounds of an existing range (bound to the left is an open bound)
        {
            return false;
        }

        // between two existing ranges
        boolean extendsPrev = offset(bounds[pos - 1]) == offset(id) - 1;
        boolean extendsNext = offset(bounds[pos]) == offset(id) + 1;

        if (extendsPrev && extendsNext) // closes the gap between two adjacent ranges
        {
            bounds[pos - 1] = bounds[pos + 1];
            System.arraycopy(bounds, pos + 2, bounds, pos, size - pos - 2);
            bounds[--size] = 0;
            bounds[--size] = 0;
        }
        else if (extendsPrev)
        {
            bounds[pos - 1] = id;
        }
        else if (extendsNext)
        {
            bounds[pos] = id;
        }
        else
        {
            insert(pos, id, id);
        }

        return true;
    }

    public boolean add(long start, long end, RangeConsumer onAdded)
    {
        // TODO (expected): implement once we have positions broadcasting going
        throw new UnsupportedOperationException();
    }

    private void insert(int pos, long start, long end)
    {
        if (bounds.length == size)
        {
            long[] newBounds = new long[bounds.length * 2];
            System.arraycopy(bounds, 0, newBounds, 0, pos);
            System.arraycopy(bounds, pos, newBounds, pos + 2, size - pos);
            bounds = newBounds;
        }
        else
        {
            System.arraycopy(bounds, pos, bounds, pos + 2, size - pos);
        }
        bounds[pos] = start;
        bounds[pos + 1] = end;
        size += 2;
    }

    private void append(long start, long end)
    {
        if (bounds.length == size)
        {
            long[] newBounds = new long[bounds.length * 2];
            System.arraycopy(bounds, 0, newBounds, 0, bounds.length);
            bounds = newBounds;
        }
        bounds[size++] = start;
        bounds[size++] = end;
    }

    public interface RangeConsumer
    {
        void consume(long start, long end);
    }
}

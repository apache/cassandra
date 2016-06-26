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

package org.apache.cassandra.index.sasi.disk;

import java.util.*;

import org.apache.commons.lang3.ArrayUtils;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongObjectCursor;

public class KeyOffsets extends LongObjectOpenHashMap<long[]>
{
    public static final long NO_OFFSET = Long.MIN_VALUE;

    public KeyOffsets() {
        super(4);
    }

    public KeyOffsets(int initialCapacity) {
        super(initialCapacity);
    }

    public void put(long currentPartitionOffset, long currentRowOffset)
    {
        if (containsKey(currentPartitionOffset))
            super.put(currentPartitionOffset, append(get(currentPartitionOffset), currentRowOffset));
        else
            super.put(currentPartitionOffset, asArray(currentRowOffset));
    }

    public long[] put(long currentPartitionOffset, long[] currentRowOffset)
    {
        if (containsKey(currentPartitionOffset))
            return super.put(currentPartitionOffset, merge(get(currentPartitionOffset), currentRowOffset));
        else
            return super.put(currentPartitionOffset, currentRowOffset);
    }

    public boolean equals(Object obj)
    {
        if (!(obj instanceof KeyOffsets))
            return false;

        KeyOffsets other = (KeyOffsets) obj;
        if (other.size() != this.size())
            return false;

        for (LongObjectCursor<long[]> cursor : this)
            if (!Arrays.equals(cursor.value, other.get(cursor.key)))
                return false;

        return true;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("KeyOffsets { ");
        forEach((a, b) -> {
            sb.append(a).append(": ").append(Arrays.toString(b));
        });
        sb.append(" }");
        return sb.toString();
    }

    // primitive array creation
    public static long[] asArray(long... vals)
    {
        return vals;
    }

    private static long[] merge(long[] arr1, long[] arr2)
    {
        long[] copy = new long[arr2.length];
        int written = 0;
        for (long l : arr2)
        {
            if (!ArrayUtils.contains(arr1, l))
                copy[written++] = l;
        }

        if (written == 0)
            return arr1;

        long[] merged = new long[arr1.length + written];
        System.arraycopy(arr1, 0, merged, 0, arr1.length);
        System.arraycopy(copy, 0, merged, arr1.length, written);
        return merged;
    }

    private static long[] append(long[] arr1, long v)
    {
        if (ArrayUtils.contains(arr1, v))
            return arr1;
        else
            return ArrayUtils.add(arr1, v);
    }
}

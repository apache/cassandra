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
package org.apache.cassandra.io.sstable;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.MemoryInputStream;
import org.apache.cassandra.io.util.MemoryOutputStream;
import org.apache.cassandra.utils.FBUtilities;

public class IndexSummary implements Closeable
{
    public static final IndexSummarySerializer serializer = new IndexSummarySerializer();
    private final int indexInterval;
    private final IPartitioner partitioner;
    private final int summary_size;
    private final Memory bytes;

    public IndexSummary(IPartitioner partitioner, Memory memory, int summary_size, int indexInterval)
    {
        this.partitioner = partitioner;
        this.indexInterval = indexInterval;
        this.summary_size = summary_size;
        this.bytes = memory;
    }

    // binary search is notoriously more difficult to get right than it looks; this is lifted from
    // Harmony's Collections implementation
    public int binarySearch(RowPosition key)
    {
        int low = 0, mid = summary_size, high = mid - 1, result = -1;
        while (low <= high)
        {
            mid = (low + high) >> 1;
            result = -DecoratedKey.compareTo(partitioner, ByteBuffer.wrap(getKey(mid)), key);
            if (result > 0)
            {
                low = mid + 1;
            }
            else if (result == 0)
            {
                return mid;
            }
            else
            {
                high = mid - 1;
            }
        }

        return -mid - (result < 0 ? 1 : 2);
    }

    public int getIndex(int index)
    {
        // multiply by 4.
        return bytes.getInt(index << 2);
    }

    public byte[] getKey(int index)
    {
        long start = getIndex(index);
        int keySize = (int) (caclculateEnd(index) - start - 8L);
        byte[] key = new byte[keySize];
        bytes.getBytes(start, key, 0, keySize);
        return key;
    }

    public long getPosition(int index)
    {
        return bytes.getLong(caclculateEnd(index) - 8);
    }

    private long caclculateEnd(int index)
    {
        return index == (summary_size - 1) ? bytes.size() : getIndex(index + 1);
    }

    public int getIndexInterval()
    {
        return indexInterval;
    }

    public int size()
    {
        return summary_size;
    }

    /**
     * Returns the amount of memory in bytes used off heap.
     * @return the amount of memory in bytes used off heap
     */
    public long offHeapSize()
    {
        return bytes.size();
    }

    public static class IndexSummarySerializer
    {
        public void serialize(IndexSummary t, DataOutputStream out) throws IOException
        {
            out.writeInt(t.indexInterval);
            out.writeInt(t.summary_size);
            out.writeLong(t.bytes.size());
            FBUtilities.copy(new MemoryInputStream(t.bytes), out, t.bytes.size());
        }

        public IndexSummary deserialize(DataInputStream in, IPartitioner partitioner) throws IOException
        {
            int indexInterval = in.readInt();
            int summary_size = in.readInt();
            long offheap_size = in.readLong();
            Memory memory = Memory.allocate(offheap_size);
            FBUtilities.copy(in, new MemoryOutputStream(memory), offheap_size);
            return new IndexSummary(partitioner, memory, summary_size, indexInterval);
        }
    }

    @Override
    public void close() throws IOException
    {
        bytes.free();
    }
}

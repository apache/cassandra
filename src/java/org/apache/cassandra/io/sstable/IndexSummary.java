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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.ByteBufferUtil;

public class IndexSummary
{
    public static final IndexSummarySerializer serializer = new IndexSummarySerializer();

    private final long[] positions;
    private final byte[][] keys;
    private final IPartitioner partitioner;

    public IndexSummary(IPartitioner partitioner, byte[][] keys, long[] positions)
    {
        this.partitioner = partitioner;
        assert keys != null && keys.length > 0;
        assert keys.length == positions.length;

        this.keys = keys;
        this.positions = positions;
    }

    public byte[][] getKeys()
    {
        return keys;
    }

    // binary search is notoriously more difficult to get right than it looks; this is lifted from
    // Harmony's Collections implementation
    public int binarySearch(RowPosition key)
    {
        int low = 0, mid = keys.length, high = mid - 1, result = -1;

        while (low <= high)
        {
            mid = (low + high) >> 1;
            result = -partitioner.decorateKey(ByteBuffer.wrap(keys[mid])).compareTo(key);

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

    public byte[] getKey(int index)
    {
        return keys[index];
    }

    public long getPosition(int index)
    {
        return positions[index];
    }

    public int size()
    {
        return positions.length;
    }

    public static class IndexSummarySerializer
    {
        public void serialize(IndexSummary t, DataOutput out) throws IOException
        {
            out.writeInt(DatabaseDescriptor.getIndexInterval());
            out.writeInt(t.keys.length);
            for (int i = 0; i < t.keys.length; i++)
            {
                out.writeLong(t.getPosition(i));
                ByteBufferUtil.writeWithLength(t.keys[i], out);
            }
        }

        public IndexSummary deserialize(DataInput in, IPartitioner partitioner) throws IOException
        {
            if (in.readInt() != DatabaseDescriptor.getIndexInterval())
                throw new IOException("Cannot read the saved summary because Index Interval changed.");

            int size = in.readInt();
            long[] positions = new long[size];
            byte[][] keys = new byte[size][];

            for (int i = 0; i < size; i++)
            {
                positions[i] = in.readLong();
                keys[i] = ByteBufferUtil.readBytes(in, in.readInt());
            }

            return new IndexSummary(partitioner, keys, positions);
        }
    }
}

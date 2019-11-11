/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemoryUtil;
import org.apache.cassandra.utils.memory.NativeAllocator;

public class NativeClustering extends AbstractClusteringPrefix implements Clustering
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new NativeClustering());

    private final long peer;

    private NativeClustering() { peer = 0; }

    public NativeClustering(NativeAllocator allocator, OpOrder.Group writeOp, Clustering clustering)
    {
        int count = clustering.size();
        int metadataSize = (count * 2) + 4;
        int dataSize = clustering.dataSize();
        int bitmapSize = ((count + 7) >>> 3);

        assert count < 64 << 10;
        assert dataSize < 64 << 10;

        peer = allocator.allocate(metadataSize + dataSize + bitmapSize, writeOp);
        long bitmapStart = peer + metadataSize;
        MemoryUtil.setShort(peer, (short) count);
        MemoryUtil.setShort(peer + (metadataSize - 2), (short) dataSize); // goes at the end of the other offsets

        MemoryUtil.setByte(bitmapStart, bitmapSize, (byte) 0);
        long dataStart = peer + metadataSize + bitmapSize;
        int dataOffset = 0;
        for (int i = 0 ; i < count ; i++)
        {
            MemoryUtil.setShort(peer + 2 + i * 2, (short) dataOffset);

            ByteBuffer value = clustering.get(i);
            if (value == null)
            {
                long boffset = bitmapStart + (i >>> 3);
                int b = MemoryUtil.getByte(boffset);
                b |= 1 << (i & 7);
                MemoryUtil.setByte(boffset, (byte) b);
                continue;
            }

            assert value.order() == ByteOrder.BIG_ENDIAN;

            int size = value.remaining();
            MemoryUtil.setBytes(dataStart + dataOffset, value);
            dataOffset += size;
        }
    }

    public Kind kind()
    {
        return Kind.CLUSTERING;
    }

    public int size()
    {
        return MemoryUtil.getShort(peer);
    }

    public ByteBuffer get(int i)
    {
        // offset at which we store the dataOffset
        int size = size();
        if (i >= size)
            throw new IndexOutOfBoundsException();

        int metadataSize = (size * 2) + 4;
        int bitmapSize = ((size + 7) >>> 3);
        long bitmapStart = peer + metadataSize;
        int b = MemoryUtil.getByte(bitmapStart + (i >>> 3));
        if ((b & (1 << (i & 7))) != 0)
            return null;

        int startOffset = MemoryUtil.getShort(peer + 2 + i * 2);
        int endOffset = MemoryUtil.getShort(peer + 4 + i * 2);
        return MemoryUtil.getByteBuffer(bitmapStart + bitmapSize + startOffset,
                                        endOffset - startOffset,
                                        ByteOrder.BIG_ENDIAN);
    }

    public ByteBuffer[] getRawValues()
    {
        ByteBuffer[] values = new ByteBuffer[size()];
        for (int i = 0 ; i < values.length ; i++)
            values[i] = get(i);
        return values;
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE;
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE;
    }

    public ClusteringPrefix minimize()
    {
        return this;
    }
}

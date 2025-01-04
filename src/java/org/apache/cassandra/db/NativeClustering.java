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

import org.apache.cassandra.db.marshal.AddressBasedNativeData;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.NativeAccessor;
import org.apache.cassandra.db.marshal.NativeData;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.MemoryUtil;
import org.apache.cassandra.utils.memory.NativeAllocator;

public class NativeClustering implements Clustering<NativeData>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new NativeClustering());

    private final long peer;

    private NativeClustering() { peer = 0; }

    public NativeClustering(NativeAllocator allocator, OpOrder.Group writeOp, Clustering<?> clustering)
    {
        int count = clustering.size();
        int metadataSize = (count * 2) + 4;
        int dataSize = clustering.dataSize();
        int bitmapSize = ((count + 7) >>> 3);

        assert count < 64 << 10;
        assert dataSize <= FBUtilities.MAX_UNSIGNED_SHORT : String.format("Data size %d >= %d", dataSize, FBUtilities.MAX_UNSIGNED_SHORT + 1);

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

            ByteBuffer value = clustering.bufferAt(i);
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

    public ClusteringPrefix<NativeData> clustering()
    {
        return this;
    }

    public int size()
    {
        return MemoryUtil.getShort(peer);
    }

    public int dataSize()
    {
        int dataSizeOffset = (size() * 2) + 2; // metadataSize - 2
        return MemoryUtil.getShort(peer + dataSizeOffset);
    }


    private static class NativeClusteringValue implements NativeData {
        private final long peer;
        private final int i;

        private NativeClusteringValue(long peer, int i)
        {
            this.peer = peer;
            this.i = i;
        }

        @Override
        public int nativeDataSize()
        {
            int size = parentSize();
            return NativeClustering.nativeDataSize(peer, size, i);
        }

        @Override
        public ByteBuffer asByteBuffer()
        {
            return getByteBuffer((address, length) -> MemoryUtil.getByteBuffer(address, length, ByteOrder.BIG_ENDIAN));
        }

        @Override
        public NativeData slice(int offset, int sliceLength)
        {
            int clusteringSize = parentSize();
            if (i >= clusteringSize)
                throw new IndexOutOfBoundsException();

            int metadataSize = (clusteringSize * 2) + 4;
            int bitmapSize = ((clusteringSize + 7) >>> 3);
            long bitmapStart = peer + metadataSize;
            int b = MemoryUtil.getByte(bitmapStart + (i >>> 3));
            if ((b & (1 << (i & 7))) != 0)
                return AddressBasedNativeData.EMPTY;

            int startOffset = MemoryUtil.getShort(peer + 2 + i * 2);
            int endOffset = MemoryUtil.getShort(peer + 4 + i * 2);
            long address = bitmapStart + bitmapSize + startOffset;
            int length =  endOffset - startOffset;

            if (offset < 0 || offset > length)
                throw new IllegalArgumentException("offset must but be >= 0 and < parent length");
            if (sliceLength < 0 || offset + sliceLength > length) {
                throw new IllegalArgumentException("length must but be >= 0 and offset + length > parent length");
            }

            if (length == 0) {
                return AddressBasedNativeData.EMPTY;
            }
            return new AddressBasedNativeData(address + offset, sliceLength);
        }

        private int parentSize() {
            return MemoryUtil.getShort(peer);
        }

        private ByteBuffer getByteBuffer(ByteBufferProvider provider)
        {
            int size = parentSize();
            if (i >= size)
                throw new IndexOutOfBoundsException();

            return NativeClustering.getByteBuffer(peer, i, size, provider);
        }

        private interface ByteBufferProvider
        {
            ByteBuffer get(long address, int length);
        }

        @Override
        public int compareTo(NativeData right)
        {
            int leftSize = this.nativeDataSize();
            int rightSize = right.nativeDataSize();
            return FastByteOperations.compareUnsigned(this.getAddress(), leftSize, right.getAddress(), rightSize);
        }

        @Override
        public long getAddress()
        {
            int parentSize = parentSize();
            if (i >= parentSize)
                throw new IndexOutOfBoundsException();

            int metadataSize = (parentSize * 2) + 4;
            int bitmapSize = ((parentSize + 7) >>> 3);
            long bitmapStart = peer + metadataSize;
            int b = MemoryUtil.getByte(bitmapStart + (i >>> 3));
            if ((b & (1 << (i & 7))) != 0)
                return -1;

            int startOffset = MemoryUtil.getShort(peer + 2 + i * 2);
            long address = bitmapStart + bitmapSize + startOffset;
            return  address;
        }

        @Override
        public int compareTo(ByteBuffer right)
        {
            int leftSize = this.nativeDataSize();
            return -FastByteOperations.compareUnsigned(right, this.getAddress(), leftSize);
        }
    }

    public NativeData get(int i)
    {
        if (isNull(i))
            return null;

        return new NativeClusteringValue(peer, i);
    }

    public boolean isNull(int i)
    {
        int size = size();
        return isNull(peer, size, i);
    }

    static boolean isNull(long peer, int parentSize, int i)
    {
        if (i >= parentSize)
            throw new IndexOutOfBoundsException();

        int metadataSize = (parentSize * 2) + 4;
        long bitmapStart = peer + metadataSize;
        int b = MemoryUtil.getByte(bitmapStart + (i >>> 3));
        return ((b & (1 << (i & 7))) != 0);
    }

    public boolean isEmpty(int i)
    {
        return nativeDataSize(peer, size(), i) == 0;
    }

    static int nativeDataSize(long peer, int parentSize, int i)
    {
        if (isNull(peer, parentSize, i))
            return 0;

        int startOffset = MemoryUtil.getShort(peer + 2 + i * 2);
        int endOffset = MemoryUtil.getShort(peer + 4 + i * 2);
        return (endOffset - startOffset);
    }

    private ByteBuffer getByteBuffer(int i)
    {
        int size = size();
        if (i >= size)
            throw new IndexOutOfBoundsException();

        return getByteBuffer(peer, i, size, (address, length) -> MemoryUtil.getByteBuffer(address, length, ByteOrder.BIG_ENDIAN));
    }

    static ByteBuffer getByteBuffer(long peer, int i, int size, NativeClusteringValue.ByteBufferProvider provider)
    {
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
        return provider.get(bitmapStart + bitmapSize + startOffset,
                            endOffset - startOffset);
    }

    public NativeData[] getRawValues()
    {
        NativeData[] values = new NativeData[size()];
        for (int i = 0 ; i < values.length ; i++)
            values[i] = get(i);
        return values;
    }

    public ByteBuffer[] getBufferArray()
    {
        ByteBuffer[] values = new ByteBuffer[size()];
        for (int i = 0 ; i < values.length ; i++)
            values[i] = getByteBuffer(i);
        return values;
    }

    public ValueAccessor<NativeData> accessor()
    {
        return NativeAccessor.instance;
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE;
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE;
    }

    @Override
    public final int hashCode()
    {
        return ClusteringPrefix.hashCode(this);
    }

    @Override
    public final boolean equals(Object o)
    {
        return ClusteringPrefix.equals(this, o);
    }

    // data are copied to heap byte buffers to detach from a NativeAllocator lifecycle
    @Override
    public ClusteringPrefix<?> retainable()
    {
        assert kind() == Kind.CLUSTERING; // tombstones are never stored natively

        // always extract
        ByteBuffer[] values = new ByteBuffer[size()];
        for (int i = 0; i < values.length; ++i)
        {
            ByteBuffer value = getByteBuffer(i);
            values[i] = value != null ? HeapCloner.instance.clone(value) : null;
        }

        return ByteBufferAccessor.instance.factory().clustering(values);
    }
}

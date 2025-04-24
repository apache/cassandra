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
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.MemoryUtil;
import org.apache.cassandra.utils.memory.NativeEndianMemoryUtil;
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
        NativeEndianMemoryUtil.setShort(peer, (short) count);
        NativeEndianMemoryUtil.setShort(peer + (metadataSize - 2), (short) dataSize); // goes at the end of the other offsets

        NativeEndianMemoryUtil.setByte(bitmapStart, bitmapSize, (byte) 0);
        long dataStart = peer + metadataSize + bitmapSize;
        int dataOffset = 0;
        for (int i = 0 ; i < count ; i++)
        {
            NativeEndianMemoryUtil.setShort(peer + 2 + i * 2, (short) dataOffset);

            ByteBuffer value = clustering.bufferAt(i);
            if (value == null)
            {
                long boffset = bitmapStart + (i >>> 3);
                int b = NativeEndianMemoryUtil.getByte(boffset);
                b |= 1 << (i & 7);
                NativeEndianMemoryUtil.setByte(boffset, (byte) b);
                continue;
            }

            assert value.order() == ByteOrder.BIG_ENDIAN;

            int size = value.remaining();
            NativeEndianMemoryUtil.setBytes(dataStart + dataOffset, value);
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
        return NativeEndianMemoryUtil.getUnsignedShort(peer);
    }

    public int dataSize()
    {
        int dataSizeOffset = (size() * 2) + 2; // metadataSize - 2
        return NativeEndianMemoryUtil.getUnsignedShort(peer + dataSizeOffset);
    }

    public NativeData get(int i)
    {
        return buildDataObject(i, AddressBasedNativeData::new);
    }

    public boolean isNull(int i)
    {
        return isNull(peer, size(), i);
    }

    private static boolean isNull(long peer, int size, int i)
    {
        if (i >= size)
            throw new IndexOutOfBoundsException();

        int metadataSize = (size * 2) + 4;
        long bitmapStart = peer + metadataSize;
        int b = NativeEndianMemoryUtil.getByte(bitmapStart + (i >>> 3));
        return ((b & (1 << (i & 7))) != 0);
    }

    public boolean isEmpty(int i)
    {
        int size = size();
        if (isNull(peer, size, i))
            return true;

        int startOffset = NativeEndianMemoryUtil.getUnsignedShort(peer + 2 + i * 2);
        int endOffset = NativeEndianMemoryUtil.getUnsignedShort(peer + 4 + i * 2);
        return (endOffset - startOffset) == 0;
    }


    private ByteBuffer getByteBuffer(int i)
    {
        return buildDataObject(i, (long address, int length) -> MemoryUtil.getByteBuffer(address, length, ByteOrder.BIG_ENDIAN));
    }

    private interface DataObjectBuilder<D> {
        D build(long address, int length);
    }

    private <D> D buildDataObject(int i, DataObjectBuilder<D> builder)
    {
        int size = size();
        if (i >= size)
            throw new IndexOutOfBoundsException();

        int metadataSize = (size * 2) + 4;
        int bitmapSize = ((size + 7) >>> 3);
        long bitmapStart = peer + metadataSize;
        int b = NativeEndianMemoryUtil.getByte(bitmapStart + (i >>> 3));
        if ((b & (1 << (i & 7))) != 0)
            return null;

        int startOffset = NativeEndianMemoryUtil.getUnsignedShort(peer + 2 + i * 2);
        int endOffset = NativeEndianMemoryUtil.getUnsignedShort(peer + 4 + i * 2);

        long address = bitmapStart + bitmapSize + startOffset;
        int length = endOffset - startOffset;
        return builder.build(address, length);
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
    public Clustering<?> ensureAccessorFactorySupport()
    {
        return retainable();
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
    public Clustering<?> retainable()
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

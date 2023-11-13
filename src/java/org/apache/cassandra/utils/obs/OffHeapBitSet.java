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
package org.apache.cassandra.utils.obs;

import java.io.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.MemoryOutputStream;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * Off-heap bitset,
 * file compatible with OpeBitSet
 */
public class OffHeapBitSet implements IBitSet
{
    private final Memory bytes;

    public OffHeapBitSet(long numBits)
    {
        /** returns the number of 64 bit words it would take to hold numBits */
        long wordCount = (((numBits - 1) >>> 6) + 1);
        if (wordCount > Integer.MAX_VALUE)
            throw new UnsupportedOperationException("Bloom filter size is > 16GB, reduce the bloom_filter_fp_chance");
        try
        {
            long byteCount = wordCount * 8L;
            bytes = Memory.allocate(byteCount);
        }
        catch (OutOfMemoryError e)
        {
            throw new RuntimeException("Out of native memory occured, You can avoid it by increasing the system ram space or by increasing bloom_filter_fp_chance.");
        }
        // flush/clear the existing memory.
        clear();
    }

    private OffHeapBitSet(Memory bytes)
    {
        this.bytes = bytes;
    }

    public long capacity()
    {
        return bytes.size() * 8;
    }

    @Override
    public long offHeapSize()
    {
        return bytes.size();
    }

    public void addTo(Ref.IdentityCollection identities)
    {
        identities.add(bytes);
    }

    public boolean get(long index)
    {
        long i = index >> 3;
        long bit = index & 0x7;
        int bitmask = 0x1 << bit;
        return (bytes.getByte(i) & bitmask) != 0;
    }

    public void set(long index)
    {
        long i = index >> 3;
        long bit = index & 0x7;
        int bitmask = 0x1 << bit;
        bytes.setByte(i, (byte) (bitmask | bytes.getByte(i)));
    }

    public void set(long offset, byte b)
    {
        bytes.setByte(offset, b);
    }

    public void clear(long index)
    {
        long i = index >> 3;
        long bit = index & 0x7;
        int bitmask = 0x1 << bit;
        int nativeByte = (bytes.getByte(i) & 0xFF);
        nativeByte &= ~bitmask;
        bytes.setByte(i, (byte) nativeByte);
    }

    public void clear()
    {
        bytes.setMemory(0, bytes.size(), (byte) 0);
    }

    public void serialize(DataOutputPlus out) throws IOException
    {
        out.writeInt((int) (bytes.size() / 8));
        out.write(bytes, 0, bytes.size());
    }

    @VisibleForTesting
    public void serializeOldBfFormat(DataOutputPlus out) throws IOException
    {
        out.writeInt((int) (bytes.size() / 8));
        for (long i = 0; i < bytes.size(); )
        {
            long value = ((bytes.getByte(i++) & 0xff) << 0)
                         + ((bytes.getByte(i++) & 0xff) << 8)
                         + ((bytes.getByte(i++) & 0xff) << 16)
                         + ((long) (bytes.getByte(i++) & 0xff) << 24)
                         + ((long) (bytes.getByte(i++) & 0xff) << 32)
                         + ((long) (bytes.getByte(i++) & 0xff) << 40)
                         + ((long) (bytes.getByte(i++) & 0xff) << 48)
                         + ((long) bytes.getByte(i++) << 56);
            out.writeLong(value);
        }
    }

    public long serializedSize()
    {
        return TypeSizes.sizeof((int) bytes.size()) + bytes.size();
    }

    public static <I extends InputStream & DataInput> OffHeapBitSet deserialize(I in, boolean oldBfFormat) throws IOException
    {
        long byteCount = in.readInt() * 8L;
        Memory memory = Memory.allocate(byteCount);
        if (oldBfFormat)
        {
            for (long i = 0; i < byteCount; )
            {
                long v = in.readLong();
                memory.setByte(i++, (byte) (v >>> 0));
                memory.setByte(i++, (byte) (v >>> 8));
                memory.setByte(i++, (byte) (v >>> 16));
                memory.setByte(i++, (byte) (v >>> 24));
                memory.setByte(i++, (byte) (v >>> 32));
                memory.setByte(i++, (byte) (v >>> 40));
                memory.setByte(i++, (byte) (v >>> 48));
                memory.setByte(i++, (byte) (v >>> 56));
            }
        }
        else
        {
            FBUtilities.copy(in, new MemoryOutputStream(memory), byteCount);
        }
        return new OffHeapBitSet(memory);
    }

    public void close()
    {
        bytes.free();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof OffHeapBitSet))
            return false;
        OffHeapBitSet b = (OffHeapBitSet) o;
        return bytes.equals(b.bytes);
    }

    @Override
    public int hashCode()
    {
        // Similar to open bitset.
        long h = 0;
        for (long i = bytes.size(); --i >= 0;)
        {
            h ^= bytes.getByte(i);
            h = (h << 1) | (h >>> 63); // rotate left
        }
        return (int) ((h >> 32) ^ h) + 0x98761234;
    }

    public String toString()
    {
        return "[OffHeapBitSet]";
    }
}

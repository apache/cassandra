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
package org.apache.cassandra.index.sasi.utils;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;

import com.google.common.annotations.VisibleForTesting;

public class MappedBuffer implements Closeable
{
    private final MappedByteBuffer[] pages;

    private long position, limit;
    private final long capacity;
    private final int pageSize, sizeBits;

    private MappedBuffer(MappedBuffer other)
    {
        this.sizeBits = other.sizeBits;
        this.pageSize = other.pageSize;
        this.position = other.position;
        this.limit = other.limit;
        this.capacity = other.capacity;
        this.pages = other.pages;
    }

    public MappedBuffer(RandomAccessReader file)
    {
        this(file.getChannel(), 30);
    }

    public MappedBuffer(ChannelProxy file)
    {
        this(file, 30);
    }

    @VisibleForTesting
    protected MappedBuffer(ChannelProxy file, int numPageBits)
    {
        if (numPageBits > Integer.SIZE - 1)
            throw new IllegalArgumentException("page size can't be bigger than 1G");

        sizeBits = numPageBits;
        pageSize = 1 << sizeBits;
        position = 0;
        limit = capacity = file.size();
        pages = new MappedByteBuffer[(int) (file.size() / pageSize) + 1];

        try
        {
            long offset = 0;
            for (int i = 0; i < pages.length; i++)
            {
                long pageSize = Math.min(this.pageSize, (capacity - offset));
                pages[i] = file.map(MapMode.READ_ONLY, offset, pageSize);
                offset += pageSize;
            }
        }
        finally
        {
            file.close();
        }
    }

    public int comparePageTo(long offset, int length, AbstractType<?> comparator, ByteBuffer other)
    {
        return comparator.compare(getPageRegion(offset, length), other);
    }

    public long capacity()
    {
        return capacity;
    }

    public long position()
    {
        return position;
    }

    public MappedBuffer position(long newPosition)
    {
        if (newPosition < 0 || newPosition > limit)
            throw new IllegalArgumentException("position: " + newPosition + ", limit: " + limit);

        position = newPosition;
        return this;
    }

    public long limit()
    {
        return limit;
    }

    public MappedBuffer limit(long newLimit)
    {
        if (newLimit < position || newLimit > capacity)
            throw new IllegalArgumentException();

        limit = newLimit;
        return this;
    }

    public long remaining()
    {
        return limit - position;
    }

    public boolean hasRemaining()
    {
        return remaining() > 0;
    }

    public byte get()
    {
        return get(position++);
    }

    public byte get(long pos)
    {
        return pages[getPage(pos)].get(getPageOffset(pos));
    }

    public short getShort()
    {
        short value = getShort(position);
        position += 2;
        return value;
    }

    public short getShort(long pos)
    {
        if (isPageAligned(pos, 2))
            return pages[getPage(pos)].getShort(getPageOffset(pos));

        int ch1 = get(pos)     & 0xff;
        int ch2 = get(pos + 1) & 0xff;
        return (short) ((ch1 << 8) + ch2);
    }

    public int getInt()
    {
        int value = getInt(position);
        position += 4;
        return value;
    }

    public int getInt(long pos)
    {
        if (isPageAligned(pos, 4))
            return pages[getPage(pos)].getInt(getPageOffset(pos));

        int ch1 = get(pos)     & 0xff;
        int ch2 = get(pos + 1) & 0xff;
        int ch3 = get(pos + 2) & 0xff;
        int ch4 = get(pos + 3) & 0xff;

        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4);
    }

    public long getLong()
    {
        long value = getLong(position);
        position += 8;
        return value;
    }


    public long getLong(long pos)
    {
        // fast path if the long could be retrieved from a single page
        // that would avoid multiple expensive look-ups into page array.
        return (isPageAligned(pos, 8))
                ? pages[getPage(pos)].getLong(getPageOffset(pos))
                : ((long) (getInt(pos)) << 32) + (getInt(pos + 4) & 0xFFFFFFFFL);
    }

    public ByteBuffer getPageRegion(long position, int length)
    {
        if (!isPageAligned(position, length))
            throw new IllegalArgumentException(String.format("range: %s-%s wraps more than one page", position, length));

        ByteBuffer slice = pages[getPage(position)].duplicate();

        int pageOffset = getPageOffset(position);
        slice.position(pageOffset).limit(pageOffset + length);

        return slice;
    }

    public MappedBuffer duplicate()
    {
        return new MappedBuffer(this);
    }

    public void close()
    {
        if (!FileUtils.isCleanerAvailable)
            return;

        /*
         * Try forcing the unmapping of pages using undocumented unsafe sun APIs.
         * If this fails (non Sun JVM), we'll have to wait for the GC to finalize the mapping.
         * If this works and a thread tries to access any page, hell will unleash on earth.
         */
        try
        {
            for (MappedByteBuffer segment : pages)
                FileUtils.clean(segment);
        }
        catch (Exception e)
        {
            // This is not supposed to happen
        }
    }

    private int getPage(long position)
    {
        return (int) (position >> sizeBits);
    }

    private int getPageOffset(long position)
    {
        return (int) (position & pageSize - 1);
    }

    private boolean isPageAligned(long position, int length)
    {
        return pageSize - (getPageOffset(position) + length) > 0;
    }
}

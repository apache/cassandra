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

package org.apache.cassandra.index.sai.memory;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.ByteBlockPool;

/**
 * This is a copy of {@code org.apache.lucene.index.ByteSliceReader} done to
 * make it visible in the {@code org.apache.cassandra.index.sai.memory} package.
 *
 * IndexInput that knows how to read the byte slices written by RAMPostingSlices.
 * We read the bytes in each slice until we hit the end of that slice at which
 * point we read the forwarding address of the next slice and then jump to it.
 */
@NotThreadSafe
final class ByteSliceReader extends DataInput
{
    private ByteBlockPool pool;
    private int bufferUpto;
    private byte[] buffer;
    private int upto;
    private int limit;
    private int level;
    private int bufferOffset;
    private int endIndex;

    public void init(ByteBlockPool pool, int startIndex, int endIndex)
    {
        assert endIndex - startIndex >= 0 : "startIndex=" + startIndex + " endIndex=" + endIndex;
        assert startIndex >= 0;
        assert endIndex >= 0;

        this.pool = pool;
        this.endIndex = endIndex;

        level = 0;
        bufferUpto = startIndex / ByteBlockPool.BYTE_BLOCK_SIZE;
        bufferOffset = bufferUpto * ByteBlockPool.BYTE_BLOCK_SIZE;
        buffer = pool.buffers[bufferUpto];
        upto = startIndex & ByteBlockPool.BYTE_BLOCK_MASK;

        final int firstSize = ByteBlockPool.LEVEL_SIZE_ARRAY[0];

        if (startIndex + firstSize >= endIndex)
        {
            // There is only this one slice to read
            limit = endIndex & ByteBlockPool.BYTE_BLOCK_MASK;
        }
        else
            limit = upto + firstSize - 4;
    }

    public boolean eof()
    {
        assert upto + bufferOffset <= endIndex;
        return upto + bufferOffset == endIndex;
    }

    @Override
    public byte readByte()
    {
        assert !eof();
        assert upto <= limit;
        if (upto == limit)
            nextSlice();
        return buffer[upto++];
    }

    public void nextSlice()
    {
        // Skip to our next slice
        final int nextIndex = (int) BitUtil.VH_LE_INT.get(buffer, limit);

        level = ByteBlockPool.NEXT_LEVEL_ARRAY[level];
        final int newSize = ByteBlockPool.LEVEL_SIZE_ARRAY[level];

        bufferUpto = nextIndex / ByteBlockPool.BYTE_BLOCK_SIZE;
        bufferOffset = bufferUpto * ByteBlockPool.BYTE_BLOCK_SIZE;

        buffer = pool.buffers[bufferUpto];
        upto = nextIndex & ByteBlockPool.BYTE_BLOCK_MASK;

        if (nextIndex + newSize >= endIndex)
        {
            // We are advancing to the final slice
            assert endIndex - nextIndex > 0;
            limit = endIndex - bufferOffset;
        }
        else
        {
            // This is not the final slice (subtract 4 for the
            // forwarding address at the end of this new slice)
            limit = upto + newSize - 4;
        }
    }

    @Override
    public void skipBytes(long l)
    {
        throw new UnsupportedOperationException("skipBytes is not supported by ByteSliceReader");
    }

    @Override
    public void readBytes(byte[] b, int offset, int len)
    {
        throw new UnsupportedOperationException("readBytes is not supported by ByteSliceReader");
    }
}

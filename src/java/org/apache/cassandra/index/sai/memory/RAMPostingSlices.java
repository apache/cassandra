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

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.mutable.MutableValueInt;

/**
 * Encodes postings as variable integers into slices
 */
@NotThreadSafe
class RAMPostingSlices
{
    static final int DEFAULT_TERM_DICT_SIZE = 1024;

    private final ByteBlockPool postingsPool;
    private int[] postingStarts = new int[DEFAULT_TERM_DICT_SIZE];
    private int[] postingUptos = new int[DEFAULT_TERM_DICT_SIZE];
    private int[] sizes = new int[DEFAULT_TERM_DICT_SIZE];

    RAMPostingSlices(Counter memoryUsage)
    {
        postingsPool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(memoryUsage));
    }

    PostingList postingList(int termID, final ByteSliceReader reader)
    {
        initReader(reader, termID);

        final MutableValueInt lastSegmentRowId = new MutableValueInt();

        return new PostingList()
        {
            @Override
            public long nextPosting() throws IOException
            {
                if (reader.eof())
                {
                    return PostingList.END_OF_STREAM;
                }
                else
                {
                    lastSegmentRowId.value += reader.readVInt();
                    return lastSegmentRowId.value;
                }
            }

            @Override
            public long size()
            {
                return sizes[termID];
            }

            @Override
            public long advance(long targetRowID)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    void initReader(ByteSliceReader reader, int termID)
    {
        final int upto = postingUptos[termID];
        reader.init(postingsPool, postingStarts[termID], upto);
    }

    void createNewSlice(int termID)
    {
        if (termID >= postingStarts.length - 1)
        {
            postingStarts = ArrayUtil.grow(postingStarts, termID + 1);
            postingUptos = ArrayUtil.grow(postingUptos, termID + 1);
            sizes = ArrayUtil.grow(sizes, termID + 1);
        }

        // the slice will not fit in the current block, create a new block
        if ((ByteBlockPool.BYTE_BLOCK_SIZE - postingsPool.byteUpto) < ByteBlockPool.FIRST_LEVEL_SIZE)
        {
            postingsPool.nextBuffer();
        }

        final int upto = postingsPool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
        postingStarts[termID] = upto + postingsPool.byteOffset;
        postingUptos[termID] = upto + postingsPool.byteOffset;
    }

    void writeVInt(int termID, int i)
    {
        while ((i & ~0x7F) != 0)
        {
            writeByte(termID, (byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        writeByte(termID, (byte) i);
        sizes[termID]++;
    }

    private void writeByte(int termID, byte b)
    {
        int upto = postingUptos[termID];
        byte[] block = postingsPool.buffers[upto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
        assert block != null;
        int offset = upto & ByteBlockPool.BYTE_BLOCK_MASK;
        if (block[offset] != 0)
        {
            // End of slice; allocate a new one
            offset = postingsPool.allocSlice(block, offset);
            block = postingsPool.buffer;
            postingUptos[termID] = offset + postingsPool.byteOffset;
        }
        block[offset] = b;
        postingUptos[termID]++;
    }
}

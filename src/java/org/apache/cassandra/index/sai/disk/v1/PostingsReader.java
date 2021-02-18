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
package org.apache.cassandra.index.sai.disk.v1;


import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;


/**
 * Reads, decompresses and decodes postings lists written by {@link PostingsWriter}.
 *
 * Holds exactly one postings block in memory at a time. Does binary search over skip table to find a postings block to
 * load.
 */
@NotThreadSafe
public class PostingsReader implements OrdinalPostingList
{
    protected final IndexInput input;
    private final int blockSize;
    private final long numPostings;
    private final LongArray blockOffsets;
    private final LongArray blockMaxValues;
    private final SeekingRandomAccessInput seekingInput;
    private final QueryEventListener.PostingListEventListener listener;

    // TODO: Expose more things through the summary, now that it's an actual field?
    private final BlocksSummary summary;

    private int postingsBlockIdx;
    private int blockIdx; // position in block
    private long totalPostingsRead;
    private long actualSegmentRowId;

    private long currentPosition;
    private DirectReaders.Reader currentFORValues;

    @VisibleForTesting
    PostingsReader(IndexInput input, long summaryOffset, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        this(input, new BlocksSummary(input, summaryOffset, () -> {}), listener);
    }

    @VisibleForTesting
    public PostingsReader(IndexInput input, BlocksSummary summary, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        this.input = input;
        this.seekingInput = new SeekingRandomAccessInput(input);
        this.blockOffsets = summary.offsets;
        this.blockSize = summary.blockSize;
        this.numPostings = summary.numPostings;
        this.blockMaxValues = summary.maxValues;
        this.listener = listener;

        this.summary = summary;

        reBuffer();
    }

    @Override
    public long getOrdinal()
    {
        return totalPostingsRead;
    }

    interface InputCloser
    {
        void close() throws IOException;
    }

    @VisibleForTesting
    public static class BlocksSummary
    {
        final int blockSize;
        final int numPostings;
        final LongArray offsets;
        final LongArray maxValues;

        private final InputCloser runOnClose;

        @VisibleForTesting
        public BlocksSummary(IndexInput input, long offset) throws IOException
        {
            this(input, offset, input::close);
        }

        BlocksSummary(IndexInput input, long offset, InputCloser runOnClose) throws IOException
        {
            this.runOnClose = runOnClose;

            input.seek(offset);
            this.blockSize = input.readVInt();
            //TODO This should need to change because we can potentially end up with postings of more than Integer.MAX_VALUE?
            this.numPostings = input.readVInt();

            final SeekingRandomAccessInput randomAccessInput = new SeekingRandomAccessInput(input);
            final int numBlocks = input.readVInt();
            final long maxBlockValuesLength = input.readVLong();
            final long maxBlockValuesOffset = input.getFilePointer() + maxBlockValuesLength;

            final byte offsetBitsPerValue = input.readByte();
            if (offsetBitsPerValue > 64)
            {
                throw new CorruptIndexException(
                        String.format("Postings list header is corrupted: Bits per value for block offsets must be no more than 64 and is %d.", offsetBitsPerValue), input);
            }
            this.offsets = new LongArrayReader(randomAccessInput, DirectReaders.getReaderForBitsPerValue(offsetBitsPerValue), input.getFilePointer(), numBlocks);

            input.seek(maxBlockValuesOffset);
            final byte valuesBitsPerValue = input.readByte();
            if (valuesBitsPerValue > 64)
            {
                throw new CorruptIndexException(
                        String.format("Postings list header is corrupted: Bits per value for values samples must be no more than 64 and is %d.", valuesBitsPerValue), input);
            }
            this.maxValues = new LongArrayReader(randomAccessInput, DirectReaders.getReaderForBitsPerValue(valuesBitsPerValue), input.getFilePointer(), numBlocks);
        }

        void close() throws IOException
        {
            runOnClose.close();
        }

        private static class LongArrayReader implements LongArray
        {
            private final RandomAccessInput input;
            private final DirectReaders.Reader reader;
            private final long offset;
            private final int length;

            private LongArrayReader(RandomAccessInput input, DirectReaders.Reader reader, long offset, int length)
            {
                this.input = input;
                this.reader = reader;
                this.offset = offset;
                this.length = length;
            }

            @Override
            public long findTokenRowID(long value)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public long get(long idx)
            {
                return reader.get(input, offset, idx);
            }

            @Override
            public long length()
            {
                return length;
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            input.close();
        }
        finally
        {
            summary.close();
        }
    }

    @Override
    public long size()
    {
        return numPostings;
    }

    /**
     * Advances to the first row ID beyond the current that is greater than or equal to the
     * target, and returns that row ID. Exhausts the iterator and returns {@link #END_OF_STREAM} if
     * the target is greater than the highest row ID.
     *
     * Does binary search over the skip table to find the next block to load into memory.
     *
     * Note: Callers must use the return value of this method before calling {@link #nextPosting()}, as calling
     * that method will return the next posting, not the one to which we have just advanced.
     *
     * @param targetRowID target row ID to advance to
     *
     * @return first segment row ID which is >= the target row ID or {@link PostingList#END_OF_STREAM} if one does not exist
     */
    @Override
    public long advance(long targetRowID) throws IOException
    {
        listener.onAdvance();
        int block = binarySearchBlock(targetRowID);

        if (block < 0)
        {
            block = -block - 1;
        }

        if (postingsBlockIdx == block + 1)
        {
            // we're in the same block, just iterate through
            return slowAdvance(targetRowID);
        }
        assert block > 0;
        // Even if there was an exact match, block might contain duplicates.
        // We iterate to the target token from the beginning.
        lastPosInBlock(block - 1);
        return slowAdvance(targetRowID);
    }

    private long slowAdvance(long targetRowID) throws IOException
    {
        while (totalPostingsRead < numPostings)
        {
            long segmentRowId = peekNext();

            advanceOnePosition(segmentRowId);

            if (segmentRowId >= targetRowID)
            {
                return segmentRowId;
            }
        }
        return END_OF_STREAM;
    }

    private int binarySearchBlock(long targetRowID)
    {
        int low = postingsBlockIdx - 1;
        int high = Math.toIntExact(blockMaxValues.length()) - 1;

        // in current block
        if (low <= high && targetRowID <= blockMaxValues.get(low))
            return low;

        while (low <= high)
        {
            int mid = low + ((high - low) >> 1) ;

            long midVal = blockMaxValues.get(mid);

            if (midVal < targetRowID)
            {
                low = mid + 1;
            }
            else if (midVal > targetRowID)
            {
                high = mid - 1;
            }
            else
            {
                // target found, but we need to check for duplicates
                if (mid > 0 && blockMaxValues.get(mid - 1L) == targetRowID)
                {
                    // there are duplicates, pivot left
                    high = mid - 1;
                }
                else
                {
                    // no duplicates
                    return mid;
                }
            }
        }
        return -(low + 1);  // target not found
    }

    private void lastPosInBlock(int block)
    {
        // blockMaxValues is integer only
        actualSegmentRowId = blockMaxValues.get(block);
        //upper bound, since we might've advanced to the last block, but upper bound is enough
        totalPostingsRead += (blockSize - blockIdx) + (block - postingsBlockIdx + 1) * blockSize;

        postingsBlockIdx = block + 1;
        blockIdx = blockSize;
    }

    @Override
    public long nextPosting() throws IOException
    {
        final long next = peekNext();
        if (next != END_OF_STREAM)
        {
            advanceOnePosition(next);
        }
        return next;
    }

    @VisibleForTesting
    int getBlockSize()
    {
        return blockSize;
    }

    private long peekNext() throws IOException
    {
        if (totalPostingsRead >= numPostings)
        {
            return END_OF_STREAM;
        }
        if (blockIdx == blockSize)
        {
            reBuffer();
        }

        return actualSegmentRowId + nextRowID();
    }

    private int nextRowID()
    {
        // currentFORValues is null when the all the values in the block are the same
        if (currentFORValues == null)
        {
            return 0;
        }
        else
        {
            final long id = currentFORValues.get(seekingInput, currentPosition, blockIdx);
            listener.onPostingDecoded();
            return Math.toIntExact(id);
        }
    }

    private void advanceOnePosition(long nextRowID)
    {
        actualSegmentRowId = nextRowID;
        totalPostingsRead++;
        blockIdx++;
    }

    private void reBuffer() throws IOException
    {
        final long pointer = blockOffsets.get(postingsBlockIdx);

        input.seek(pointer);

        final long left = numPostings - totalPostingsRead;
        assert left > 0;

        readFoRBlock(input);

        postingsBlockIdx++;
        blockIdx = 0;
    }

    private void readFoRBlock(IndexInput in) throws IOException
    {
        final byte bitsPerValue = in.readByte();

        currentPosition = in.getFilePointer();

        if (bitsPerValue == 0)
        {
            // currentFORValues is null when the all the values in the block are the same
            currentFORValues = null;
            return;
        }
        else if (bitsPerValue > 64)
        {
            throw new CorruptIndexException(
                    String.format("Postings list #%s block is corrupted. Bits per value should be no more than 64 and is %d.", postingsBlockIdx, bitsPerValue), input);
        }
        currentFORValues = DirectReaders.getReaderForBitsPerValue(bitsPerValue);
    }
}

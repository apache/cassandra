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
package org.apache.cassandra.index.sai.disk.v1.postings;


import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.index.sai.disk.io.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.postings.OrdinalPostingList;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;


/**
 * Reads, decompresses and decodes postings lists written by {@link PostingsWriter}.
 * <p>
 * Holds exactly one posting block in memory at a time. Does binary search over skip table to find a postings block to
 * load.
 */
@NotThreadSafe
public class PostingsReader implements OrdinalPostingList
{
    private final IndexInput input;
    private final SeekingRandomAccessInput seekingInput;
    private final QueryEventListener.PostingListEventListener listener;
    private final BlocksSummary summary;

    // Current block index
    private int blockIndex;
    // Current posting index within block
    private int postingIndex;
    private long totalPostingsRead;
    private long actualPosting;

    private LongValues currentFoRValues;
    private long postingsDecoded = 0;

    @VisibleForTesting
    public PostingsReader(IndexInput input, long summaryOffset, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        this(input, new BlocksSummary(input, summaryOffset), listener);
    }

    public PostingsReader(IndexInput input, BlocksSummary summary, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        this.input = input;
        this.seekingInput = new SeekingRandomAccessInput(input);
        this.listener = listener;
        this.summary = summary;

        reBuffer();
    }

    @Override
    public long getOrdinal()
    {
        return totalPostingsRead;
    }

    public static class BlocksSummary
    {
        private final IndexInput input;
        final int blockSize;
        final int numPostings;
        final LongArray offsets;
        final LongArray maxValues;

        public BlocksSummary(IndexInput input, long offset) throws IOException
        {
            this.input = input;
            input.seek(offset);
            this.blockSize = input.readVInt();
            //TODO This should need to change because we can potentially end up with postings of more than Integer.MAX_VALUE?
            this.numPostings = input.readVInt();

            SeekingRandomAccessInput randomAccessInput = new SeekingRandomAccessInput(input);
            int numBlocks = input.readVInt();
            long maxBlockValuesLength = input.readVLong();
            long maxBlockValuesOffset = input.getFilePointer() + maxBlockValuesLength;

            byte offsetBitsPerValue = input.readByte();
            DirectReaders.checkBitsPerValue(offsetBitsPerValue, input, () -> "Postings list header");
            LongValues lvOffsets = offsetBitsPerValue == 0 ? LongValues.ZEROES : DirectReader.getInstance(randomAccessInput, offsetBitsPerValue, input.getFilePointer());
            this.offsets = new LongArrayReader(lvOffsets, numBlocks);

            input.seek(maxBlockValuesOffset);
            byte valuesBitsPerValue = input.readByte();
            DirectReaders.checkBitsPerValue(valuesBitsPerValue, input, () -> "Postings list header");
            LongValues lvValues = valuesBitsPerValue == 0 ? LongValues.ZEROES : DirectReader.getInstance(randomAccessInput, valuesBitsPerValue, input.getFilePointer());
            this.maxValues = new LongArrayReader(lvValues, numBlocks);
        }

        void close()
        {
            FileUtils.closeQuietly(input);
        }

        private static class LongArrayReader implements LongArray
        {
            private final LongValues reader;
            private final int length;

            private LongArrayReader(LongValues reader, int length)
            {
                this.reader = reader;
                this.length = length;
            }

            @Override
            public long get(long idx)
            {
                return reader.get(idx);
            }

            @Override
            public long length()
            {
                return length;
            }

            @Override
            public long indexOf(long value)
            {
                throw new UnsupportedOperationException();
            }
        }
    }

    @Override
    public void close()
    {
        listener.postingDecoded(postingsDecoded);
        FileUtils.closeQuietly(input);
        summary.close();
    }

    @Override
    public long size()
    {
        return summary.numPostings;
    }

    /**
     * Advances to the first row ID beyond the current that is greater than or equal to the
     * target, and returns that row ID. Exhausts the iterator and returns {@link #END_OF_STREAM} if
     * the target is greater than the highest row ID.
     * <p>
     * Does binary search over the skip table to find the next block to load into memory.
     * <p>
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
        int block = binarySearchBlocks(targetRowID);

        if (block < 0)
        {
            block = -block - 1;
        }

        if (blockIndex == block + 1)
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
        while (totalPostingsRead < summary.numPostings)
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

    // Perform a binary search of the blocks to the find the block index
    // containing the targetRowID, or, in the case of a duplicate value
    // crossing blocks, the preceeding block index
    private int binarySearchBlocks(long targetRowID)
    {
        int lowBlockIndex = blockIndex - 1;
        int highBlockIndex = Math.toIntExact(summary.maxValues.length()) - 1;

        // in current block
        if (lowBlockIndex <= highBlockIndex && targetRowID <= summary.maxValues.get(lowBlockIndex))
            return lowBlockIndex;

        while (lowBlockIndex <= highBlockIndex)
        {
            int midBlockIndex = lowBlockIndex + ((highBlockIndex - lowBlockIndex) >> 1) ;

            long maxValueOfMidBlock = summary.maxValues.get(midBlockIndex);

            if (maxValueOfMidBlock < targetRowID)
            {
                lowBlockIndex = midBlockIndex + 1;
            }
            else if (maxValueOfMidBlock > targetRowID)
            {
                highBlockIndex = midBlockIndex - 1;
            }
            else
            {
                // At this point the maximum value of the midway block matches our target.
                //
                // This following check is to see if we have a duplicate value in the last entry of the
                // preceeding block. This check is only going to be successful if the entire current
                // block is full of duplicates.
                if (midBlockIndex > 0 && summary.maxValues.get(midBlockIndex - 1) == targetRowID)
                {
                    // there is a duplicate in the preceeding block so restrict search to finish
                    // at that block
                    highBlockIndex = midBlockIndex - 1;
                }
                else
                {
                    // no duplicates
                    return midBlockIndex;
                }
            }
        }
        return -(lowBlockIndex + 1);  // target not found
    }

    private void lastPosInBlock(int block)
    {
        // blockMaxValues is integer only
        actualPosting = summary.maxValues.get(block);
        //upper bound, since we might've advanced to the last block, but upper bound is enough
        totalPostingsRead += (summary.blockSize - postingIndex) + (block - blockIndex + 1) * (long)summary.blockSize;

        blockIndex = block + 1;
        postingIndex = summary.blockSize;
    }

    @Override
    public long nextPosting() throws IOException
    {
        long next = peekNext();
        if (next != END_OF_STREAM)
        {
            advanceOnePosition(next);
        }
        return next;
    }

    private long peekNext() throws IOException
    {
        if (totalPostingsRead >= summary.numPostings)
        {
            return END_OF_STREAM;
        }
        if (postingIndex == summary.blockSize)
        {
            reBuffer();
        }

        return actualPosting + nextFoRValue();
    }

    private int nextFoRValue()
    {
        long id = currentFoRValues.get(postingIndex);
        postingsDecoded++;
        return Math.toIntExact(id);
    }

    private void advanceOnePosition(long nextPosting)
    {
        actualPosting = nextPosting;
        totalPostingsRead++;
        postingIndex++;
    }

    private void reBuffer() throws IOException
    {
        long pointer = summary.offsets.get(blockIndex);
        if (pointer < 4)
        {
            // the first 4 bytes must be CODEC_MAGIC
            throw new CorruptIndexException(String.format("Invalid block offset %d for postings block idx %d", pointer, blockIndex), input);
        }
        input.seek(pointer);

        long left = summary.numPostings - totalPostingsRead;
        assert left > 0;

        readFoRBlock(input);

        blockIndex++;
        postingIndex = 0;
    }

    private void readFoRBlock(IndexInput in) throws IOException
    {
        if (blockIndex == 0)
            actualPosting = in.readVLong();

        byte bitsPerValue = in.readByte();

        long currentPosition = in.getFilePointer();

        if (bitsPerValue == 0)
        {
            // If bitsPerValue is 0 then all the values in the block are the same
            currentFoRValues = LongValues.ZEROES;
            return;
        }
        else if (bitsPerValue > 64)
        {
            throw new CorruptIndexException(
            String.format("Postings list #%s block is corrupted. Bits per value should be no more than 64 and is %d.", blockIndex, bitsPerValue), input);
        }
        currentFoRValues = DirectReader.getInstance(seekingInput, bitsPerValue, currentPosition);
    }
}

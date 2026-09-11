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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;

import org.apache.cassandra.index.sai.disk.io.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.postings.OrdinalPostingList;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.io.util.FileUtils;


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

    // Block range [startBlock, endBlock) this reader is scoped to, and the number of postings to read.
    // For a full (V1) posting list these are 0, numBlocks and numPostings respectively.
    private final int startBlock;
    private final int endBlock;
    private final long limit;

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
        this(input, summary, listener, 0, summary.numBlocks(), summary.numPostings);
    }

    /**
     * Creates a reader scoped to a single block-aligned section of a posting list.
     *
     * @param startBlock    first block (inclusive) of the section; its {@code firstPosting} VLong is read fresh
     * @param endBlock      last block (exclusive) of the section, used to bound skip-table binary search
     * @param postingsCount number of postings to read from the section
     */
    private PostingsReader(IndexInput input, BlocksSummary summary, QueryEventListener.PostingListEventListener listener,
                           int startBlock, int endBlock, long postingsCount) throws IOException
    {
        this.input = input;
        this.seekingInput = new SeekingRandomAccessInput(input);
        this.listener = listener;
        this.summary = summary;
        this.startBlock = startBlock;
        this.endBlock = endBlock;
        this.limit = postingsCount;
        this.blockIndex = startBlock;

        if (postingsCount > 0)
            reBuffer();
    }

    /**
     * Opens a reader over the exact-match section ({@code [0, prefixIndex)}) of a V2 posting list.
     */
    public static PostingsReader exactSection(IndexInput input, BlocksSummary summary,
                                              QueryEventListener.PostingListEventListener listener) throws IOException
    {
        int exactBlocks = blocksFor(summary.prefixIndex, summary.blockSize);
        return new PostingsReader(input, summary, listener, 0, exactBlocks, summary.prefixIndex);
    }

    /**
     * Opens a reader over the prefix section ({@code [prefixIndex, suffixIndex)}) of a V2 posting list.
     * Returns null if there are no prefix postings.
     */
    public static PostingsReader prefixSection(IndexInput input, BlocksSummary summary,
                                               QueryEventListener.PostingListEventListener listener) throws IOException
    {
        int prefixCount = summary.suffixIndex - summary.prefixIndex;
        if (prefixCount <= 0)
            return null;
        int exactBlocks = blocksFor(summary.prefixIndex, summary.blockSize);
        return new PostingsReader(input, summary, listener, exactBlocks, summary.numBlocks(), prefixCount);
    }

    /**
     * Opens a reader over both exact and prefix sections ({@code [0, suffixIndex)}) of a V2 posting list.
     * This reads exact and prefix postings in a single contiguous read, which is more efficient than
     * two separate I/O operations. Returns null if there are no postings in either section.
     */
    public static PostingsReader combinedExactAndPrefixSections(IndexInput input, BlocksSummary summary,
                                                                QueryEventListener.PostingListEventListener listener) throws IOException
    {
        int combinedCount = summary.suffixIndex; // Exact + prefix postings
        if (combinedCount <= 0)
            return null;
        int combinedBlocks = blocksFor(summary.suffixIndex, summary.blockSize);
        return new PostingsReader(input, summary, listener, 0, combinedBlocks, combinedCount);
    }

    private static int blocksFor(int postings, int blockSize)
    {
        return (postings + blockSize - 1) / blockSize;
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
        public final int numPostings;
        // V2 section boundaries expressed as posting counts. For V1 / exact-only lists both equal numPostings.
        public final int prefixIndex;
        public final int suffixIndex;
        final LongArray offsets;
        final LongArray maxValues;

        public BlocksSummary(IndexInput input, long offset) throws IOException
        {
            this(input, offset, false);
        }

        public BlocksSummary(IndexInput input, long offset, boolean isV2) throws IOException
        {
            this.input = input;
            input.seek(offset);

            int pIdx = -1;
            int sIdx = -1;
            if (isV2)
            {
                pIdx = input.readVInt();
                sIdx = input.readVInt();
            }

            this.blockSize = input.readVInt();
            //TODO This should need to change because we can potentially end up with postings of more than Integer.MAX_VALUE?
            this.numPostings = input.readVInt();

            if (!isV2)
            {
                pIdx = numPostings;
                sIdx = numPostings;
            }
            this.prefixIndex = pIdx;
            this.suffixIndex = sIdx;

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

        int numBlocks()
        {
            return Math.toIntExact(offsets.length());
        }

        public void close()
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
        return limit;
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
        while (totalPostingsRead < limit)
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
        int lowBlockIndex = Math.max(blockIndex - 1, startBlock);
        int highBlockIndex = endBlock - 1;

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
                if (midBlockIndex > startBlock && summary.maxValues.get(midBlockIndex - 1) == targetRowID)
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
        if (totalPostingsRead >= limit)
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

        long left = limit - totalPostingsRead;
        assert left > 0;

        readFoRBlock(input);

        blockIndex++;
        postingIndex = 0;
    }

    private void readFoRBlock(IndexInput in) throws IOException
    {
        if (blockIndex == startBlock)
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

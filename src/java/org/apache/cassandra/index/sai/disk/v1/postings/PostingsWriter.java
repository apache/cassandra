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


import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.disk.ResettableByteBuffersIndexOutput;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectWriter;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;

/**
 * Encodes, compresses and writes postings lists to disk.
 * <p>
 * All postings in the posting list are delta encoded, then deltas are divided into blocks for compression.
 * The deltas are based on the final value of the previous block. For the first block in the posting list
 * the first value in the block is written as a VLong prior to block delta encodings.
 * <p>
 * In packed blocks, longs are encoded with the same bit width (FoR compression). The block size (i.e. number of
 * longs inside block) is fixed (currently 128). Additionally blocks that are all the same value are encoded in an
 * optimized way.
 * </p>
 * <p>
 * In VLong blocks, longs are compressed with {@link DataOutput#writeVLong}. The block size is variable.
 * </p>
 *
 * <p>
 * Packed blocks are favoured, meaning when the postings are long enough, {@link PostingsWriter} will try
 * to encode most data as a packed block. Take a term with 259 postings as an example, the first 256 postings are encoded
 * as two packed blocks, while the remaining 3 are encoded as one VLong block.
 * </p>
 * <p>
 * Each posting list ends with a block summary containing metadata and a skip table, written right after all postings
 * blocks. Skip interval is the same as block size, and each skip entry points to the end of each block.
 * Skip table consist of block offsets and last values of each block, compressed as two FoR blocks.
 * </p>
 *
 * Visual representation of the disk format:
 * <pre>
 *
 * +========+========================+=====+==============+===============+===============+=====+========================+========+
 * | HEADER | POSTINGS LIST (TERM 1)                                                      | ... | POSTINGS LIST (TERM N) | FOOTER |
 * +========+========================+=====+==============+===============+===============+=====+========================+========+
 *          | FIRST VALUE| FOR BLOCK (1)| ... | FOR BLOCK (N)| BLOCK SUMMARY              |
 *          +---------------------------+-----+--------------+---------------+------------+
 *                                                           | BLOCK SIZE    |            |
 *                                                           | LIST SIZE     | SKIP TABLE |
 *                                                           +---------------+------------+
 *                                                                           | BLOCKS POS.|
 *                                                                           | MAX VALUES |
 *                                                                           +------------+
 *
 *  </pre>
 */
@NotThreadSafe
public class PostingsWriter implements Closeable
{
    // import static org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat.BLOCK_SIZE;
    private final static int BLOCK_SIZE = 128;

    private static final String POSTINGS_MUST_BE_SORTED_ERROR_MSG = "Postings must be sorted ascending, got [%s] after [%s]";

    private final IndexOutput dataOutput;
    private final int blockSize;
    private final long[] deltaBuffer;
    private final LongArrayList blockOffsets = new LongArrayList();
    private final LongArrayList blockMaximumPostings = new LongArrayList();
    private final ResettableByteBuffersIndexOutput inMemoryOutput = new ResettableByteBuffersIndexOutput("blockOffsets");

    private final long startOffset;

    private int bufferUpto;
    private long firstPosting = Long.MIN_VALUE;
    private long lastPosting = Long.MIN_VALUE;
    private long maxDelta;
    private long totalPostings;

    public PostingsWriter(IndexDescriptor indexDescriptor, IndexIdentifier indexIdentifier) throws IOException
    {
        this(indexDescriptor, indexIdentifier, BLOCK_SIZE);
    }

    public PostingsWriter(IndexOutputWriter dataOutput) throws IOException
    {
        this(dataOutput, BLOCK_SIZE);
    }

    @VisibleForTesting
    PostingsWriter(IndexDescriptor indexDescriptor, IndexIdentifier indexIdentifier, int blockSize) throws IOException
    {
        this(indexDescriptor.openPerIndexOutput(IndexComponent.POSTING_LISTS, indexIdentifier, true), blockSize);
    }

    private PostingsWriter(IndexOutputWriter dataOutput, int blockSize) throws IOException
    {
        this.blockSize = blockSize;
        this.dataOutput = dataOutput;
        startOffset = dataOutput.getFilePointer();
        deltaBuffer = new long[blockSize];
        SAICodecUtils.writeHeader(dataOutput);
    }

    /**
     * @return current file pointer
     */
    public long getFilePointer()
    {
        return dataOutput.getFilePointer();
    }

    /**
     * @return file pointer where index structure begins (before header)
     */
    public long getStartOffset()
    {
        return startOffset;
    }

    /**
     * write footer to the postings
     */
    public void complete() throws IOException
    {
        SAICodecUtils.writeFooter(dataOutput);
    }

    @Override
    public void close() throws IOException
    {
        dataOutput.close();
    }

    /**
     * Encodes, compresses and flushes given posting list to disk.
     *
     * @param postings posting list to write to disk
     *
     * @return file offset to the summary block of this posting list
     */
    public long write(PostingList postings) throws IOException
    {
        checkArgument(postings != null, "Expected non-null posting list.");
        checkArgument(postings.size() > 0, "Expected non-empty posting list.");

        lastPosting = Long.MIN_VALUE;
        resetBlockCounters();
        blockOffsets.clear();
        blockMaximumPostings.clear();

        long posting;
        // When postings list are merged, we don't know exact size, just an upper bound.
        // We need to count how many postings we added to the block ourselves.
        int size = 0;
        while ((posting = postings.nextPosting()) != PostingList.END_OF_STREAM)
        {
            writePosting(posting);
            size++;
            totalPostings++;
        }

        assert size > 0 : "No postings were written";

        finish();

        final long summaryOffset = dataOutput.getFilePointer();
        writeSummary(size);
        return summaryOffset;
    }

    public long getTotalPostings()
    {
        return totalPostings;
    }

    private void writePosting(long posting) throws IOException
    {
        if (lastPosting == Long.MIN_VALUE)
        {
            firstPosting = posting;
            deltaBuffer[bufferUpto++] = 0;
        }
        else
        {
            if (posting < lastPosting)
                throw new IllegalArgumentException(String.format(POSTINGS_MUST_BE_SORTED_ERROR_MSG, posting, lastPosting));
            long delta = posting - lastPosting;
            maxDelta = max(maxDelta, delta);
            deltaBuffer[bufferUpto++] = delta;
        }
        lastPosting = posting;

        if (bufferUpto == blockSize)
        {
            addBlockToSkipTable();
            writePostingsBlock();
            resetBlockCounters();
        }
    }

    private void finish() throws IOException
    {
        if (bufferUpto > 0)
        {
            addBlockToSkipTable();
            writePostingsBlock();
        }
    }

    private void resetBlockCounters()
    {
        firstPosting = Long.MIN_VALUE;
        bufferUpto = 0;
        maxDelta = 0;
    }

    private void addBlockToSkipTable()
    {
        blockOffsets.add(dataOutput.getFilePointer());
        blockMaximumPostings.add(lastPosting);
    }

    private void writeSummary(int exactSize) throws IOException
    {
        dataOutput.writeVInt(blockSize);
        dataOutput.writeVInt(exactSize);
        writeSkipTable();
    }

    private void writeSkipTable() throws IOException
    {
        assert blockOffsets.size() == blockMaximumPostings.size();
        dataOutput.writeVInt(blockOffsets.size());

        // compressing offsets in memory first, to know the exact length (with padding)
        inMemoryOutput.reset();

        writeSortedFoRBlock(blockOffsets, inMemoryOutput);
        dataOutput.writeVLong(inMemoryOutput.getFilePointer());
        inMemoryOutput.copyTo(dataOutput);
        writeSortedFoRBlock(blockMaximumPostings, dataOutput);
    }

    private void writePostingsBlock() throws IOException
    {
        final int bitsPerValue = maxDelta == 0 ? 0 : DirectWriter.unsignedBitsRequired(maxDelta);

        // If we have a first posting, indicating that this is the first block in the posting list
        // then write it prior to the deltas.
        if (firstPosting != Long.MIN_VALUE)
            dataOutput.writeVLong(firstPosting);

        dataOutput.writeByte((byte) bitsPerValue);
        if (bitsPerValue > 0)
        {
            final DirectWriter writer = DirectWriter.getInstance(dataOutput, blockSize, bitsPerValue);
            for (int index = 0; index < bufferUpto; ++index)
            {
                writer.add(deltaBuffer[index]);
            }
            if (bufferUpto < blockSize)
            {
                // Pad the rest of the block with 0, so we don't write invalid
                // values from previous blocks
                for (int index = bufferUpto; index < blockSize; index++)
                {
                    writer.add(0);
                }
            }
            writer.finish();
        }
    }

    private void writeSortedFoRBlock(LongArrayList values, IndexOutput output) throws IOException
    {
        final long maxValue = values.getLong(values.size() - 1);

        assert values.size() > 0;
        final int bitsPerValue = maxValue == 0 ? 0 : DirectWriter.unsignedBitsRequired(maxValue);
        output.writeByte((byte) bitsPerValue);
        if (bitsPerValue > 0)
        {
            final DirectWriter writer = DirectWriter.getInstance(output, values.size(), bitsPerValue);
            for (int i = 0; i < values.size(); ++i)
            {
                writer.add(values.getLong(i));
            }
            writer.finish();
        }
    }
}

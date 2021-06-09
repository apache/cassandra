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


import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.IntArrayList;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.io.RAMIndexOutput;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectWriter;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;
import static org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat.BLOCK_SIZE;

/**
 * Encodes, compresses and writes postings lists to disk.
 *
 * All row IDs in the posting list are delta encoded, then deltas are divided into blocks for compression.
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
 * to encode most data as a packed block. Take a term with 259 row IDs as an example, the first 256 IDs are encoded
 * as two packed blocks, while the remaining 3 are encoded as one VLong block.
 * </p>
 * <p>
 * Each posting list ends with a meta section and a skip table, that are written right after all postings blocks. Skip
 * interval is the same as block size, and each skip entry points to the end of each block.  Skip table consist of
 * block offsets and last values of each block, compressed as two FoR blocks.
 * </p>
 *
 * Visual representation of the disk format:
 * <pre>
 *
 * +========+========================+=====+==============+===============+============+=====+========================+========+
 * | HEADER | POSTINGS LIST (TERM 1)                                                   | ... | POSTINGS LIST (TERM N) | FOOTER |
 * +========+========================+=====+==============+===============+============+=====+========================+========+
 *          | FOR BLOCK (1)          | ... | FOR BLOCK (N)| BLOCK SUMMARY              |
 *          +------------------------+-----+--------------+---------------+------------+
 *                                                        | BLOCK SIZE    |            |
 *                                                        | LIST SIZE     | SKIP TABLE |
 *                                                        +---------------+------------+
 *                                                                        | BLOCKS POS.|
 *                                                                        | MAX VALUES |
 *                                                                        +------------+
 *
 *  </pre>
 */
@NotThreadSafe
//TODO Review this for DSP-19608
public class PostingsWriter implements Closeable
{
    private final static String POSTINGS_MUST_BE_SORTED_ERROR_MSG = "Postings must be sorted ascending, got [%s] after [%s]";

    private final IndexOutput dataOutput;
    private final int blockSize;
    private final long[] deltaBuffer;
    private final LongArrayList blockOffsets = new LongArrayList();
    private final LongArrayList blockMaxIDs = new LongArrayList();
    private final RAMIndexOutput inMemoryOutput = new RAMIndexOutput("blockOffsets");

    private final long startOffset;

    private int bufferUpto;
    private long lastSegmentRowId;
    private long maxDelta;
    private long totalPostings;

    @VisibleForTesting
    public PostingsWriter(IndexComponents components, boolean segmented) throws IOException
    {
        this(components, BLOCK_SIZE, segmented);
    }

    PostingsWriter(IndexOutput dataOutput) throws IOException
    {
        this(dataOutput, BLOCK_SIZE);
    }

    PostingsWriter(IndexComponents components, int blockSize, boolean segmented) throws IOException
    {
        this(components.createOutput(components.postingLists, true, segmented), blockSize);
    }

    private PostingsWriter(IndexOutput dataOutput, int blockSize) throws IOException
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
     * @return file pointer where index structure begins
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

        resetBlockCounters();
        blockOffsets.clear();
        blockMaxIDs.clear();

        long segmentRowId;
        // When postings list are merged, we don't know exact size, just an upper bound.
        // We need to count how many postings we added to the block ourselves.
        int size = 0;
        while ((segmentRowId = postings.nextPosting()) != PostingList.END_OF_STREAM)
        {
            writePosting(segmentRowId);
            size++;
            totalPostings++;
        }
        if (size == 0)
            return -1;

        finish();

        final long summaryOffset = dataOutput.getFilePointer();
        writeSummary(size);
        return summaryOffset;
    }

    public long getTotalPostings()
    {
        return totalPostings;
    }

    private void writePosting(long segmentRowId) throws IOException
    {
        if (!(segmentRowId >= lastSegmentRowId || lastSegmentRowId == 0))
            throw new IllegalArgumentException(String.format(POSTINGS_MUST_BE_SORTED_ERROR_MSG, segmentRowId, lastSegmentRowId));

        final long delta = segmentRowId - lastSegmentRowId;
        maxDelta = max(maxDelta, delta);
        deltaBuffer[bufferUpto++] = delta;

        if (bufferUpto == blockSize)
        {
            addBlockToSkipTable(segmentRowId);
            writePostingsBlock(maxDelta, bufferUpto);
            resetBlockCounters();
        }
        lastSegmentRowId = segmentRowId;
    }

    private void finish() throws IOException
    {
        if (bufferUpto > 0)
        {
            addBlockToSkipTable(lastSegmentRowId);

            writePostingsBlock(maxDelta, bufferUpto);
        }
    }

    private void resetBlockCounters()
    {
        bufferUpto = 0;
        lastSegmentRowId = 0;
        maxDelta = 0;
    }

    private void addBlockToSkipTable(long maxSegmentRowID)
    {
        blockOffsets.add(dataOutput.getFilePointer());
        blockMaxIDs.add(maxSegmentRowID);
    }

    private void writeSummary(int exactSize) throws IOException
    {
        dataOutput.writeVInt(blockSize);
        dataOutput.writeVInt(exactSize);
        writeSkipTable();
    }

    private void writeSkipTable() throws IOException
    {
        assert blockOffsets.size() == blockMaxIDs.size();
        dataOutput.writeVInt(blockOffsets.size());

        // compressing offsets in memory first, to know the exact length (with padding)
        inMemoryOutput.reset();

        writeSortedFoRBlock(blockOffsets, inMemoryOutput);
        dataOutput.writeVLong(inMemoryOutput.getFilePointer());
        inMemoryOutput.writeTo(dataOutput);
        writeSortedFoRBlock(blockMaxIDs, dataOutput);
    }

    private void writePostingsBlock(long maxValue, int blockSize) throws IOException
    {
        final int bitsPerValue = maxValue == 0 ? 0 : DirectWriter.unsignedBitsRequired(maxValue);

        assert bitsPerValue < Byte.MAX_VALUE;

        dataOutput.writeByte((byte) bitsPerValue);
        if (bitsPerValue > 0)
        {
            final DirectWriter writer = DirectWriter.getInstance(dataOutput, blockSize, bitsPerValue);
            for (int i = 0; i < blockSize; ++i)
            {
                writer.add(deltaBuffer[i]);
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

    private void writeSortedFoRBlock(IntArrayList values, IndexOutput output) throws IOException
    {
        final int maxValue = values.getInt(values.size() - 1);

        assert values.size() > 0;
        final int bitsPerValue = maxValue == 0 ? 0 : DirectWriter.unsignedBitsRequired(maxValue);
        output.writeByte((byte) bitsPerValue);
        if (bitsPerValue > 0)
        {
            final DirectWriter writer = DirectWriter.getInstance(output, values.size(), bitsPerValue);
            for (int i = 0; i < values.size(); ++i)
            {
                writer.add(values.getInt(i));
            }
            writer.finish();
        }
    }
}

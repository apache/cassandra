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
package org.apache.cassandra.index.sai.disk.v1.bitpack;

import java.io.IOException;

import org.apache.cassandra.index.sai.disk.ResettableByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectWriter;

import static org.apache.cassandra.index.sai.disk.v1.SAICodecUtils.checkBlockSize;

/**
 * Modified copy of {@code org.apache.lucene.util.packed.AbstractBlockPackedWriter} to use {@link DirectWriter} for
 * optimised reads that doesn't require seeking through the whole file to open a thread-exclusive reader.
 */
public abstract class AbstractBlockPackedWriter
{
    static final int MIN_BLOCK_SIZE = 64;
    static final int MAX_BLOCK_SIZE = 1 << (30 - 3);

    protected final IndexOutput indexOutput;
    protected final long[] blockValues;
    // This collects metadata specific to the block packed writer being used during the
    // writing of the block packed data. This cached metadata is then written to the end
    // of the data file when the block packed writer is finished.
    protected final ResettableByteBuffersIndexOutput blockMetaWriter;

    protected int blockIndex;
    protected boolean finished;

    AbstractBlockPackedWriter(IndexOutput indexOutput, int blockSize)
    {
        checkBlockSize(blockSize, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
        this.indexOutput = indexOutput;
        this.blockMetaWriter = new ResettableByteBuffersIndexOutput(blockSize, "BlockPackedMeta");
        blockValues = new long[blockSize];
    }


    /**
     * Append a new long.
     */
    public void add(long l) throws IOException
    {
        checkNotFinished();
        if (blockIndex == blockValues.length)
        {
            flush();
        }
        blockValues[blockIndex++] = l;
    }

    /**
     * Flush all buffered data to disk. This instance is not usable anymore
     * after this method has been called.
     *
     * @return a file offset to the block metadata
     */
    public long finish() throws IOException
    {
        checkNotFinished();
        if (blockIndex > 0)
        {
            flush();
        }
        final long fp = indexOutput.getFilePointer();
        blockMetaWriter.copyTo(indexOutput);
        finished = true;
        return fp;
    }

    protected abstract void flushBlock() throws IOException;

    void writeValues(int numValues, int bitsPerValue) throws IOException
    {
        final DirectWriter writer = DirectWriter.getInstance(indexOutput, numValues, bitsPerValue);
        for (int i = 0; i < numValues; ++i)
        {
            writer.add(blockValues[i]);
        }
        writer.finish();
    }

    void writeVLong(IndexOutput out, long i) throws IOException
    {
        int k = 0;
        while ((i & ~0x7FL) != 0L && k++ < 8)
        {
            out.writeByte((byte) ((i & 0x7FL) | 0x80L));
            i >>>= 7;
        }
        out.writeByte((byte) i);
    }

    private void flush() throws IOException
    {
        flushBlock();
        blockIndex = 0;
    }

    private void checkNotFinished()
    {
        if (finished)
        {
            throw new IllegalStateException(String.format("[%s] Writer already finished!", indexOutput.getName()));
        }
    }
}

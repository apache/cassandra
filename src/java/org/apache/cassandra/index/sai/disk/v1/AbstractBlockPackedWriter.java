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

import org.apache.cassandra.index.sai.disk.io.RAMIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectWriter;

import static org.apache.cassandra.index.sai.utils.SAICodecUtils.checkBlockSize;

/**
 * Modified copy of {@link org.apache.lucene.util.packed.AbstractBlockPackedWriter} to use {@link DirectWriter} for
 * optimised reads that doesn't require seeking through the whole file to open a thread-exclusive reader.
 */
abstract class AbstractBlockPackedWriter
{
    static final int MIN_BLOCK_SIZE = 64;
    static final int MAX_BLOCK_SIZE = 1 << (30 - 3);
    static final int MIN_VALUE_EQUALS_0 = 1;
    static final int BPV_SHIFT = 1;

    protected final IndexOutput out;
    protected final long[] values;
    protected int off;
    protected boolean finished;
    
    final RAMIndexOutput blockMetaWriter;

    AbstractBlockPackedWriter(IndexOutput out, int blockSize)
    {
        checkBlockSize(blockSize, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
        this.out = out;
        this.blockMetaWriter = new RAMIndexOutput("NumericValuesMeta");
        values = new long[blockSize];
    }

    private void checkNotFinished()
    {
        if (finished)
        {
            throw new IllegalStateException(String.format("[%s] Writer already finished!", out.getName()));
        }
    }

    /**
     * Append a new long.
     */
    public void add(long l) throws IOException
    {
        checkNotFinished();
        if (off == values.length)
        {
            flush();
        }
        values[off++] = l;
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
        if (off > 0)
        {
            flush();
        }
        final long fp = out.getFilePointer();
        blockMetaWriter.writeTo(out);
        finished = true;
        return fp;
    }

    protected abstract void flush() throws IOException;

    void writeValues(int numValues, int bitsPerValue) throws IOException
    {
        final DirectWriter writer = DirectWriter.getInstance(out, numValues, bitsPerValue);
        for (int i = 0; i < numValues; ++i)
        {
            writer.add(values[i]);
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
}

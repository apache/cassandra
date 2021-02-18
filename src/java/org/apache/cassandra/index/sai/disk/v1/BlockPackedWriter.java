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

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectWriter;

import static org.apache.lucene.util.BitUtil.zigZagEncode;

/**
 * A writer for large sequences of longs.
 *
 * Modified copy of {@link org.apache.lucene.util.packed.BlockPackedWriter} to use {@link DirectWriter}
 * for optimised reads that doesn't require seeking through the whole file to open a thread-exclusive reader.
 */
public class BlockPackedWriter extends AbstractBlockPackedWriter
{
    public BlockPackedWriter(IndexOutput out, int blockSize)
    {
        super(out, blockSize);
    }

    @Override
    protected void flush() throws IOException
    {
        assert off > 0;
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (int i = 0; i < off; ++i)
        {
            min = Math.min(values[i], min);
            max = Math.max(values[i], max);
        }

        final long delta = max - min;
        int bitsRequired = delta == 0 ? 0 : DirectWriter.unsignedBitsRequired(delta);

        final int token = (bitsRequired << BPV_SHIFT) | (min == 0 ? MIN_VALUE_EQUALS_0 : 0);
        blockMetaWriter.writeByte((byte) token);

        if (min != 0)
        {
            // TODO: the min values can be delta encoded since they are read linearly
            // TODO: buffer the min values so they may be written as a single block
            writeVLong(blockMetaWriter, zigZagEncode(min) - 1);
        }

        if (bitsRequired > 0)
        {
            if (min != 0)
            {
                for (int i = 0; i < off; ++i)
                {
                    values[i] -= min;
                }
            }
            blockMetaWriter.writeVLong(out.getFilePointer());
            writeValues(off, bitsRequired);
        }

        off = 0;
    }
}

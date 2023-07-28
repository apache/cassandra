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
    static final int BPV_SHIFT = 1;
    static final int MIN_VALUE_EQUALS_0 = 1;

    public BlockPackedWriter(IndexOutput out, int blockSize)
    {
        super(out, blockSize);
    }

    @Override
    protected void flushBlock() throws IOException
    {
        long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
        for (int i = 0; i < blockIndex; ++i)
        {
            min = Math.min(blockValues[i], min);
            max = Math.max(blockValues[i], max);
        }

        long delta = max - min;
        int bitsRequired = delta == 0 ? 0 : DirectWriter.unsignedBitsRequired(delta);

        int shiftedBitsRequired = (bitsRequired << BPV_SHIFT) | (min == 0 ? MIN_VALUE_EQUALS_0 : 0);
        blockMetaWriter.writeByte((byte) shiftedBitsRequired);

        if (min != 0)
        {
            writeVLong(blockMetaWriter, zigZagEncode(min) - 1);
        }

        if (bitsRequired > 0)
        {
            if (min != 0)
            {
                for (int i = 0; i < blockIndex; ++i)
                {
                    blockValues[i] -= min;
                }
            }
            blockMetaWriter.writeVLong(indexOutput.getFilePointer());
            writeValues(blockIndex, bitsRequired);
        }
    }
}

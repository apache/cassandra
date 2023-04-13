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

/**
 * A writer for large monotonically increasing sequences of positive longs.
 *
 * The writer is optimised for monotonic sequences and stores values as a series of deltas
 * from an expected value. The expected value is calculated from the minimum value in the block and the average
 * delta for the block. This means that stored values are generally smaller and can be packed
 * into a smaller number of bits, allowing for larger block sizes.
 *
 * Modified copy of {@link org.apache.lucene.util.packed.MonotonicBlockPackedWriter} to use {@link DirectWriter} for
 * optimised reads that doesn't require seeking through the whole file to open a thread-exclusive reader.
 */
public class MonotonicBlockPackedWriter extends AbstractBlockPackedWriter
{
    public MonotonicBlockPackedWriter(IndexOutput out, int blockSize)
    {
        super(out, blockSize);
    }

    @Override
    public void add(long l) throws IOException
    {
        assert l >= 0;
        super.add(l);
    }

    @Override
    protected void flushBlock() throws IOException
    {
        final float averageDelta = blockIndex == 1 ? 0f : (float) (blockValues[blockIndex - 1] - blockValues[0]) / (blockIndex - 1);
        long minimumValue = blockValues[0];
        // adjust minimumValue so that all deltas will be positive
        for (int index = 1; index < blockIndex; ++index)
        {
            long actual = blockValues[index];
            long expected = MonotonicBlockPackedReader.expected(minimumValue, averageDelta, index);
            if (expected > actual)
            {
                minimumValue -= (expected - actual);
            }
        }

        long maxDelta = 0;
        for (int i = 0; i < blockIndex; ++i)
        {
            blockValues[i] = blockValues[i] - MonotonicBlockPackedReader.expected(minimumValue, averageDelta, i);
            maxDelta = Math.max(maxDelta, blockValues[i]);
        }

        blockMetaWriter.writeZLong(minimumValue);
        blockMetaWriter.writeInt(Float.floatToIntBits(averageDelta));
        if (maxDelta == 0)
        {
            blockMetaWriter.writeVInt(0);
        }
        else
        {
            final int bitsRequired = DirectWriter.bitsRequired(maxDelta);
            blockMetaWriter.writeVInt(bitsRequired);
            blockMetaWriter.writeVLong(indexOutput.getFilePointer());
            writeValues(blockIndex, bitsRequired);
        }
    }
}

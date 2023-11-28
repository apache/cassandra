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

import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.cassandra.index.sai.disk.v1.SAICodecUtils.checkBlockSize;
import static org.apache.cassandra.index.sai.disk.v1.SAICodecUtils.numBlocks;

/**
 * Provides non-blocking, random access to a stream written with {@link MonotonicBlockPackedWriter}.
 */
public class MonotonicBlockPackedReader implements LongArray.Factory
{
    private final FileHandle file;
    private final int blockShift;
    private final int blockMask;
    private final long valueCount;
    private final byte[] blockBitsPerValue;
    private final PackedLongValues blockOffsets;
    private final PackedLongValues minValues;
    private final float[] averages;

    public MonotonicBlockPackedReader(FileHandle file, NumericValuesMeta meta) throws IOException
    {
        this.valueCount = meta.valueCount;
        blockShift = checkBlockSize(meta.blockSize, AbstractBlockPackedWriter.MIN_BLOCK_SIZE, AbstractBlockPackedWriter.MAX_BLOCK_SIZE);
        blockMask = meta.blockSize - 1;
        int numBlocks = numBlocks(valueCount, meta.blockSize);
        PackedLongValues.Builder minValuesBuilder = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        PackedLongValues.Builder blockOffsetsBuilder = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        averages = new float[numBlocks];
        blockBitsPerValue = new byte[numBlocks];
        this.file = file;

        try (RandomAccessReader reader = this.file.createReader();
             IndexInputReader in = IndexInputReader.create(reader))
        {
            SAICodecUtils.validate(in);

            in.seek(meta.blockMetaOffset);
            for (int i = 0; i < numBlocks; ++i)
            {
                minValuesBuilder.add(in.readZLong());
                averages[i] = Float.intBitsToFloat(in.readInt());
                final int bitsPerValue = in.readVInt();
                DirectReaders.checkBitsPerValue(bitsPerValue, in, () -> "Postings list header");
                blockBitsPerValue[i] = (byte) bitsPerValue;
                // when bitsPerValue is 0, block offset won't be used
                blockOffsetsBuilder.add(bitsPerValue == 0 ? -1 : in.readVLong());
            }
        }

        blockOffsets = blockOffsetsBuilder.build();
        minValues = minValuesBuilder.build();
    }

    @Override
    public LongArray open()
    {
        final IndexInput indexInput = IndexFileUtils.instance.openInput(file);
        return new AbstractBlockPackedReader(indexInput, blockBitsPerValue, blockShift, blockMask, valueCount)
        {
            @Override
            long delta(int block, int idx)
            {
                return expected(minValues.get(block), averages[block], idx);
            }

            @Override
            public void close() throws IOException
            {
                indexInput.close();
            }

            @Override
            protected long blockOffsetAt(int block)
            {
                return blockOffsets.get(block);
            }

            @Override
            public long indexOf(long value)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static long expected(long origin, float average, int index)
    {
        return origin + (long)(average * index);
    }
}

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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;

import static org.apache.cassandra.index.sai.utils.SAICodecUtils.checkBlockSize;
import static org.apache.cassandra.index.sai.utils.SAICodecUtils.numBlocks;
import static org.apache.cassandra.index.sai.utils.SAICodecUtils.readVLong;
import static org.apache.lucene.util.BitUtil.zigZagDecode;

/**
 * Provides non-blocking, random access to a stream written with {@link BlockPackedWriter}.
 */
public class BlockPackedReader implements LongArray.Factory
{
    private final IndexComponents components;
    private final FileHandle file;
    private final int blockShift, blockMask;
    private final long valueCount;
    private final byte[] blockBitsPerValue;
    private final long[] blockOffsets;
    private final long[] minValues;

    public BlockPackedReader(FileHandle file, Component component, IndexComponents components, MetadataSource source) throws IOException
    {
        this(file, components, new NumericValuesMeta(source.get(component.name())));
    }

    @SuppressWarnings("resource")
    public BlockPackedReader(FileHandle file, IndexComponents components, NumericValuesMeta meta) throws IOException
    {
        this.components = components;
        this.file = file;

        this.valueCount = meta.valueCount;

        blockShift = checkBlockSize(meta.blockSize, AbstractBlockPackedWriter.MIN_BLOCK_SIZE, AbstractBlockPackedWriter.MAX_BLOCK_SIZE);
        blockMask = meta.blockSize - 1;
        final int numBlocks = numBlocks(valueCount, meta.blockSize);
        blockBitsPerValue = new byte[numBlocks];
        blockOffsets = new long[numBlocks];
        minValues = new long[numBlocks];

        try (final RandomAccessReader reader = this.file.createReader())
        {
            final IndexInputReader in = IndexInputReader.create(reader);
            SAICodecUtils.validate(in);
            in.seek(meta.blockMetaOffset);

            for (int i = 0; i < numBlocks; ++i)
            {
                final int token = in.readByte() & 0xFF;
                final int bitsPerValue = token >>> AbstractBlockPackedWriter.BPV_SHIFT;
                if (bitsPerValue > 64)
                {
                    throw new CorruptIndexException(String.format("Block %d is corrupted. Bits per value should be no more than 64 and is %d.", i, bitsPerValue), in);
                }
                if ((token & AbstractBlockPackedWriter.MIN_VALUE_EQUALS_0) == 0)
                {
                    long val = zigZagDecode(1L + readVLong(in));
                    minValues[i] = val;
                }
                else
                {
                    minValues[i] = 0L;
                }

                blockBitsPerValue[i] = (byte) bitsPerValue;

                if (bitsPerValue > 0)
                {
                    blockOffsets[i] = in.readVLong();
                }
                else
                {
                    blockOffsets[i] = -1;
                }
            }
        }
    }

    @VisibleForTesting
    @Override
    public LongArray open()
    {
        return openTokenReader(0, null);
    }

    @Override
    @SuppressWarnings("resource")
    public LongArray openTokenReader(long sstableRowId, SSTableQueryContext context)
    {
        final IndexInput indexInput = components.openInput(file);
        return new AbstractBlockPackedReader(indexInput, blockBitsPerValue, blockShift, blockMask, sstableRowId, valueCount)
        {
            @Override
            protected long blockOffsetAt(int block)
            {
                return blockOffsets[block];
            }

            @Override
            long delta(int block, int idx)
            {
                return minValues[block];
            }

            @Override
            public void close() throws IOException
            {
                indexInput.close();
            }
        };
    }
}

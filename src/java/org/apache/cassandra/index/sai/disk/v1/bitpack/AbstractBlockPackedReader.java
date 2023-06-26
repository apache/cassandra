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

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.io.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;

@NotThreadSafe
public abstract class AbstractBlockPackedReader implements LongArray
{
    private final int blockShift;
    private final int blockMask;
    private final long valueCount;
    private final byte[] blockBitsPerValue;
    private final SeekingRandomAccessInput input;

    AbstractBlockPackedReader(IndexInput indexInput, byte[] blockBitsPerValue, int blockShift, int blockMask, long valueCount)
    {
        this.blockShift = blockShift;
        this.blockMask = blockMask;
        this.valueCount = valueCount;
        this.input = new SeekingRandomAccessInput(indexInput);
        this.blockBitsPerValue = blockBitsPerValue;
    }

    protected abstract long blockOffsetAt(int block);

    @Override
    public long get(final long valueIndex)
    {
        if (valueIndex < 0 || valueIndex >= valueCount)
        {
            throw new IndexOutOfBoundsException(String.format("Index should be between [0, %d), but was %d.", valueCount, valueIndex));
        }

        int blockIndex = (int) (valueIndex >>> blockShift);
        int inBlockIndex = (int) (valueIndex & blockMask);
        byte bitsPerValue = blockBitsPerValue[blockIndex];
        final LongValues subReader = bitsPerValue == 0 ? LongValues.ZEROES
                                                       : DirectReader.getInstance(input, bitsPerValue, blockOffsetAt(blockIndex));
        return delta(blockIndex, inBlockIndex) + subReader.get(inBlockIndex);
    }

    @Override
    public long length()
    {
        return valueCount;
    }

    abstract long delta(int block, int idx);
}

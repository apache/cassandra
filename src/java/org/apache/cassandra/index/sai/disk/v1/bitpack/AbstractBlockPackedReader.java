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

    private long previousValue = Long.MIN_VALUE;
    private long lastIndex; // the last index visited by token -> row ID searches

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

    @Override
    public long indexOf(long value)
    {
        // If we are searching backwards, we need to reset the lastIndex. This is not normal since we normally move
        // forwards when searching for tokens. We only (may) search backwards in vector searchs where we need the
        // primary key ranges presented as row IDs.
        if (value < previousValue)
            lastIndex = 0;

        // already out of range
        if (lastIndex >= valueCount)
            return -1;

        previousValue = value;

        int blockIndex = binarySearchBlockMinValues(value);

        // We need to check next block's min value on an exact match.
        boolean exactMatch = blockIndex >= 0;

        if (blockIndex < 0)
        {
            // A non-exact match, which is the negative index of the first value greater than the target.
            // For example, searching for 4 against min values [3,3,5,7] produces -2, which we convert to 2.
            blockIndex = -blockIndex;
        }

        if (blockIndex > 0)
        {
            // Start at the previous block, because there could be duplicate values in the previous block.
            // For example, with block 1: [1,2,3,3] & block 2: [3,3,5,7], binary search for 3 would find
            // block 2, but we need to start from block 1 and search both.
            // In case non-exact match, we need to pivot left as target is less than next block's min.
            blockIndex--;
        }

        // Find the global (not block-specific) index of the target token, which is equivalent to its row ID:
        lastIndex = findBlockRowID(value, blockIndex, exactMatch);
        return lastIndex >= valueCount ? -1 : lastIndex;
    }

    /**
     *
     * @return a positive block index for an exact match, or a negative one for a non-exact match
     */
    private int binarySearchBlockMinValues(long targetValue)
    {
        int high = Math.toIntExact(blockBitsPerValue.length) - 1;

        // Assume here that we'll never move backward through the blocks:
        int low = Math.toIntExact(lastIndex >> blockShift);

        // Short-circuit the search if the target is in current block:
        if (low + 1 <= high)
        {
            long cmp = Long.compare(targetValue, delta(low + 1, 0));

            if (cmp == 0)
            {
                // We have an exact match, so return the index of the next block, which means we'll start
                // searching from the current one and also inspect the first value of the next block.
                return low + 1;
            }
            else if (cmp < 0)
            {
                // We're in the same block. Indicate a non-exact match, and this value will be both
                // negated and then decremented to wind up at the current value of "low" here.
                return -low - 1;
            }

            // The target is greater than the next block's min value, so advance to that
            // block before starting the usual search...
            low++;
        }

        while (low <= high)
        {
            int mid = low + ((high - low) >> 1);

            long midVal = delta(mid, 0);

            if (midVal < targetValue)
            {
                low = mid + 1;
            }
            else if (midVal > targetValue)
            {
                high = mid - 1;
            }
            else
            {
                // target found, but we need to check for duplicates
                if (mid > 0 && delta(mid - 1, 0) == targetValue)
                {
                    // there are duplicates, pivot left
                    high = mid - 1;
                }
                else
                {
                    // no duplicates
                    return mid;
                }
            }
        }

        return -low; // no exact match found
    }

    private long findBlockRowID(long targetValue, long blockIdx, boolean exactMatch)
    {
        // Calculate the global offset for the selected block:
        long offset = blockIdx << blockShift;

        // Resume from previous index if it's larger than offset
        long low = Math.max(lastIndex, offset);

        // The high is either the last local index in the block, or something smaller if the block isn't full:
        long high = Math.min(offset + blockMask + (exactMatch ? 1 : 0), valueCount - 1);

        return binarySearchBlock(targetValue, low, high);
    }

    /**
     * binary search target value between low and high.
     *
     * @return index if exact match is found, or *positive* insertion point if no exact match is found.
     */
    private long binarySearchBlock(long target, long low, long high)
    {
        while (low <= high)
        {
            long mid = low + ((high - low) >> 1);

            long midVal = get(mid);

            if (midVal < target)
            {
                low = mid + 1;
                // future rowId cannot be smaller than mid as long as next token not smaller than current token.
                lastIndex = mid;
            }
            else if (midVal > target)
            {
                high = mid - 1;
            }
            else
            {
                // target found, but we need to check for duplicates
                if (mid > 0 && get(mid - 1) == target)
                {
                    // there are duplicates, pivot left
                    high = mid - 1;
                }
                else
                {
                    // exact match and no duplicates
                    return mid;
                }
            }
        }

        // target not found
        return low;
    }

    abstract long delta(int block, int idx);
}

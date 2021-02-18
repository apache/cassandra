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

import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.lucene.store.IndexInput;

public abstract class AbstractBlockPackedReader implements LongArray
{
    private final int blockShift, blockMask;
    private final int blockSize;
    private final long valueCount;
    final byte[] blockBitsPerValue; // package protected for test access
    private final SeekingRandomAccessInput input;

    private long prevTokenValue = Long.MIN_VALUE;
    private long lastIndex; // the last index visited by token -> row ID searches

    AbstractBlockPackedReader(IndexInput indexInput, byte[] blockBitsPerValue, int blockShift, int blockMask, long sstableRowId, long valueCount)
    {
        this.blockShift = blockShift;
        this.blockMask = blockMask;
        this.blockSize = blockMask + 1;
        this.valueCount = valueCount;
        this.input = new SeekingRandomAccessInput(indexInput);
        this.blockBitsPerValue = blockBitsPerValue;
        // start searching tokens from current index segment
        this.lastIndex = sstableRowId;
    }

    protected abstract long blockOffsetAt(int block);

    @Override
    public long get(final long index)
    {
        if (index < 0 || index >= valueCount)
        {
            throw new IndexOutOfBoundsException(String.format("Index should be between [0, %d), but was %d.", valueCount, index));
        }

        final int block = (int) (index >>> blockShift);
        final int idx = (int) (index & blockMask);
        final DirectReaders.Reader subReader = DirectReaders.getReaderForBitsPerValue(blockBitsPerValue[block]);
        return delta(block, idx) + subReader.get(input, blockOffsetAt(block), idx);
    }

    @Override
    public long findTokenRowID(long targetValue)
    {
        // already out of range
        if (lastIndex >= valueCount)
            return -1;

        // We keep track previous returned value in lastIndex, so searching backward will not return correct result.
        // Also it's logically wrong to search backward during token iteration in PostingListRangeIterator.
        if (targetValue < prevTokenValue)
            throw new IllegalArgumentException(String.format("%d is smaller than prev token value %d", targetValue, prevTokenValue));
        prevTokenValue = targetValue;

        int blockIndex = binarySearchBlockMinValues(targetValue);

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
        lastIndex = findBlockRowID(targetValue, blockIndex, exactMatch);
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
        long high = Math.min(offset + blockSize - 1 + (exactMatch ? 1 : 0), valueCount - 1);

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

    @Override
    public long length()
    {
        return valueCount;
    }

    abstract long delta(int block, int idx);
}

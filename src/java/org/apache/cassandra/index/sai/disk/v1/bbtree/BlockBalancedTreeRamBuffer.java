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
package org.apache.cassandra.index.sai.disk.v1.bbtree;

import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * On-heap buffer for values that provides a sorted view of itself as a {@link BlockBalancedTreeIterator}.
 */
@NotThreadSafe
public class BlockBalancedTreeRamBuffer
{
    private final InMemoryTrie<PackedLongValues.Builder> trieBuffer;
    private final RowIDReducer reducer;
    private final int bytesPerValue;
    private int numRows;

    public BlockBalancedTreeRamBuffer(int bytesPerValue)
    {
        trieBuffer = new InMemoryTrie<>(TrieMemtable.BUFFER_TYPE);
        reducer = new RowIDReducer();
        this.bytesPerValue = bytesPerValue;
    }

    public int numRows()
    {
        return numRows;
    }

    public long memoryUsed()
    {
        return trieBuffer.sizeOnHeap() + reducer.heapAllocations();
    }

    public long add(int segmentRowId, byte[] value)
    {
        final long initialSizeOnHeap = trieBuffer.sizeOnHeap();
        final long reducerHeapSize = reducer.heapAllocations();

        try
        {
            trieBuffer.putRecursive(v -> ByteSource.fixedLength(value), segmentRowId, reducer);
        }
        catch (InMemoryTrie.SpaceExhaustedException e)
        {
            throw Throwables.unchecked(e);
        }

        numRows++;
        return (trieBuffer.sizeOnHeap() - initialSizeOnHeap) + (reducer.heapAllocations() - reducerHeapSize);
    }

    public BlockBalancedTreeIterator iterator()
    {
        return BlockBalancedTreeIterator.fromTrieIterator(trieBuffer.entrySet().iterator(), bytesPerValue);
    }

    private static class RowIDReducer implements InMemoryTrie.UpsertTransformer<PackedLongValues.Builder, Integer>
    {
        private final LongAdder heapAllocations = new LongAdder();

        @Override
        public PackedLongValues.Builder apply(PackedLongValues.Builder existing, Integer neww)
        {
            if (existing == null)
            {
                existing = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
                heapAllocations.add(existing.ramBytesUsed());
            }
            long ramBefore = existing.ramBytesUsed();
            existing.add(neww);
            heapAllocations.add(existing.ramBytesUsed() - ramBefore);
            return existing;
        }

        long heapAllocations()
        {
            return heapAllocations.longValue();
        }
    }
}

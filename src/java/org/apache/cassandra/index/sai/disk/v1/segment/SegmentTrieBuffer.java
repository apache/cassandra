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
package org.apache.cassandra.index.sai.disk.v1.segment;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.IndexEntry;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * On-heap buffer for values that provides a sorted view of itself as an {@link Iterator}.
 */
@NotThreadSafe
public class SegmentTrieBuffer
{
    private static final int MAX_RECURSIVE_TERM_LENGTH = 128;

    private final InMemoryTrie<PackedLongValues.Builder> trie;
    private final PostingsAccumulator postingsAccumulator;
    private int numRows;

    public SegmentTrieBuffer()
    {
        trie = new InMemoryTrie<>(DatabaseDescriptor.getMemtableAllocationType().toBufferType());
        postingsAccumulator = new PostingsAccumulator();
    }

    public int numRows()
    {
        return numRows;
    }

    public long memoryUsed()
    {
        return trie.sizeOnHeap() + postingsAccumulator.heapAllocations();
    }

    public long add(ByteComparable term, int termLength, int segmentRowId)
    {
        final long initialSizeOnHeap = trie.sizeOnHeap();
        final long reducerHeapSize = postingsAccumulator.heapAllocations();

        try
        {
            trie.putSingleton(term, segmentRowId, postingsAccumulator, termLength <= MAX_RECURSIVE_TERM_LENGTH);
        }
        catch (InMemoryTrie.SpaceExhaustedException e)
        {
            throw Throwables.unchecked(e);
        }

        numRows++;
        return (trie.sizeOnHeap() - initialSizeOnHeap) + (postingsAccumulator.heapAllocations() - reducerHeapSize);
    }

    public Iterator<IndexEntry> iterator()
    {
        var iterator = trie.entrySet().iterator();

        return new Iterator<>()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public IndexEntry next()
            {
                Map.Entry<ByteComparable, PackedLongValues.Builder> entry = iterator.next();
                PackedLongValues postings = entry.getValue().build();
                PackedLongValues.Iterator postingsIterator = postings.iterator();
                return IndexEntry.create(entry.getKey(), new PostingList()
                {
                    @Override
                    public long nextPosting()
                    {
                        if (postingsIterator.hasNext())
                            return postingsIterator.next();
                        return END_OF_STREAM;
                    }

                    @Override
                    public long size()
                    {
                        return postings.size();
                    }

                    @Override
                    public long advance(long targetRowID)
                    {
                        throw new UnsupportedOperationException();
                    }
                });
            }
        };
    }

    private static class PostingsAccumulator implements InMemoryTrie.UpsertTransformer<PackedLongValues.Builder, Integer>
    {
        private final LongAdder heapAllocations = new LongAdder();

        @Override
        public PackedLongValues.Builder apply(PackedLongValues.Builder existing, Integer rowID)
        {
            if (existing == null)
            {
                existing = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
                heapAllocations.add(existing.ramBytesUsed());
            }
            long ramBefore = existing.ramBytesUsed();
            existing.add(rowID);
            heapAllocations.add(existing.ramBytesUsed() - ramBefore);
            return existing;
        }

        long heapAllocations()
        {
            return heapAllocations.longValue();
        }
    }
}

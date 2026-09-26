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
import java.util.function.IntPredicate;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.lucene.util.packed.PackedLongValues;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.IndexEntry;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * On-heap buffer for indexed terms and row IDs backed by an {@link InMemoryTrie} that provides a sorted view of
 * itself as an {@link Iterator}.
 * <p>
 * When a non-null {@code prefixAtDepth} policy is provided, each {@link #add} call accumulates intermediate-node
 * (prefix) postings at every depth where {@code prefixAtDepth.test(depth)} is true; the terminal node always
 * receives an exact posting. Each trie node stores a {@link PackedLongValuesList.Builder} holding one section per
 * {@link PostingType}. {@link #iterator()} yields all entries (leaf + intermediate nodes that received any
 * postings) in sorted order; each entry's {@link PostingList} emits {@code exactCount}, {@code totalCount}, then
 * all exact row IDs followed by all prefix row IDs.
 */
@NotThreadSafe
public class SegmentTrieBuffer
{
    private static final int MAX_RECURSIVE_TERM_LENGTH = 128;

    private final InMemoryTrie<PackedLongValuesList.Builder> trie;
    private final PostingsAccumulator postingsAccumulator;
    private final IntPredicate prefixAtDepth; // null = no intermediate (prefix) accumulation
    private int numRows;

    /** V1 — no intermediate (prefix) accumulation. */
    public SegmentTrieBuffer()
    {
        this(null);
    }

    /**
     * @param prefixAtDepth nullable depth policy; when non-null, prefix postings are accumulated at every depth
     *                      for which {@code prefixAtDepth.test(depth)} returns true. Null means V1 (exact-only).
     */
    public SegmentTrieBuffer(IntPredicate prefixAtDepth)
    {
        trie = new InMemoryTrie<>(DatabaseDescriptor.getMemtableAllocationType().toBufferType());
        postingsAccumulator = new PostingsAccumulator();
        this.prefixAtDepth = prefixAtDepth;
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
            trie.putSingleton(term, segmentRowId, postingsAccumulator, termLength <= MAX_RECURSIVE_TERM_LENGTH, prefixAtDepth);
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
        Iterator<Map.Entry<ByteComparable, PackedLongValuesList.Builder>> iterator = trie.entrySet().iterator();

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
                Map.Entry<ByteComparable, PackedLongValuesList.Builder> entry = iterator.next();
                PackedLongValuesList list = entry.getValue().build();
                return IndexEntry.create(entry.getKey(), prefixAtDepth == null ? rawPostings(list)
                                                                               : sectionedPostings(list));
            }
        };
    }

    /** V1 posting list: raw exact row IDs only (numeric and non-prefix literal indexes). */
    private static PostingList rawPostings(PackedLongValuesList list)
    {
        PackedLongValues.Iterator exactIterator = list.exactIterator();
        return new PostingList()
        {
            @Override
            public long nextPosting()
            {
                return exactIterator.hasNext() ? exactIterator.next() : END_OF_STREAM;
            }

            @Override
            public long size()
            {
                return list.exactCount();
            }

            @Override
            public long advance(long targetRowID)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    /** V2 posting list: emits exactCount, totalCount, then exact rows followed by prefix rows. */
    private static PostingList sectionedPostings(PackedLongValuesList list)
    {
        PackedLongValuesList.Iterator listIterator = list.iterator();
        return new PostingList()
        {
            @Override
            public long nextPosting()
            {
                return listIterator.hasNext() ? listIterator.next() : END_OF_STREAM;
            }

            @Override
            public long size()
            {
                // FILTER_TYPES header values followed by the actual postings.
                return PackedLongValuesList.FILTER_TYPES + list.totalCount();
            }

            @Override
            public long advance(long targetRowID)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static class PostingsAccumulator implements InMemoryTrie.UpsertTransformer<PackedLongValuesList.Builder, Integer>
    {
        private final LongAdder heapAllocations = new LongAdder();

        @Override
        public PackedLongValuesList.Builder apply(PackedLongValuesList.Builder existing, Integer rowID)
        {
            return applyWithType(existing, rowID, PostingType.EXACT);
        }

        @Override
        public PackedLongValuesList.Builder applyIntermediate(PackedLongValuesList.Builder existing, Integer rowID)
        {
            return applyWithType(existing, rowID, PostingType.PREFIX);
        }

        private PackedLongValuesList.Builder applyWithType(PackedLongValuesList.Builder existing, int rowID, PostingType type)
        {
            if (existing == null)
            {
                existing = new PackedLongValuesList.Builder();
                heapAllocations.add(existing.ramBytesUsed());
            }
            long ramBefore = existing.ramBytesUsed();
            existing.add(rowID, type);
            heapAllocations.add(existing.ramBytesUsed() - ramBefore);
            return existing;
        }

        long heapAllocations()
        {
            return heapAllocations.longValue();
        }
    }
}

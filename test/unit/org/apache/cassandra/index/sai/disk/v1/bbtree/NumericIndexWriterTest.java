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

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentTrieBuffer;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.memory.MemtableTermsIterator;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.index.sai.utils.TermsIterator;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.AbstractGuavaIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.NumericUtils;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NumericIndexWriterTest extends SAIRandomizedTester
{
    private IndexDescriptor indexDescriptor;
    private IndexTermType indexTermType;
    private IndexIdentifier indexIdentifier;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        indexTermType = SAITester.createIndexTermType(Int32Type.instance);
        indexIdentifier = SAITester.createIndexIdentifier("test", "test", newIndex());
    }

    @Test
    public void shouldFlushFromRamBuffer() throws Exception
    {
        final SegmentTrieBuffer ramBuffer = new SegmentTrieBuffer();
        final int numRows = 120;
        int currentValue = numRows;
        for (int i = 0; i < numRows; ++i)
        {
            ramBuffer.add(integerToByteComparable(currentValue--), Integer.BYTES, i);
        }

        SegmentMetadata.ComponentMetadataMap indexMetas;

        NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                           indexIdentifier,
                                                           Integer.BYTES);
        indexMetas = writer.writeCompleteSegment(ramBuffer.iterator());

        final FileHandle treeHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.BALANCED_TREE, indexIdentifier, null);
        final FileHandle treePostingsHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, indexIdentifier, null);

        try (BlockBalancedTreeReader reader = new BlockBalancedTreeReader(indexIdentifier,
                                                                          treeHandle,
                                                                          indexMetas.get(IndexComponent.BALANCED_TREE).root,
                                                                          treePostingsHandle,
                                                                          indexMetas.get(IndexComponent.POSTING_LISTS).root))
        {
            final Counter visited = Counter.newCounter();
            try (final PostingList ignored = reader.intersect(new BlockBalancedTreeReader.IntersectVisitor()
            {
                @Override
                public boolean contains(byte[] packedValue)
                {
                    // we should read point values in reverse order after sorting
                    assertEquals(1 + visited.get(), NumericUtils.sortableBytesToInt(packedValue, 0));
                    visited.addAndGet(1);
                    return true;
                }

                @Override
                public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
                {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
            }, mockEventListener(), mock(QueryContext.class)))
            {
                assertEquals(numRows, visited.get());
            }
        }
    }

    @Test
    public void shouldFlushFromMemtable() throws Exception
    {
        final int maxSegmentRowId = 100;
        final TermsIterator termEnum = buildTermEnum(0, maxSegmentRowId);

        SegmentMetadata.ComponentMetadataMap indexMetas;
        NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                           indexIdentifier,
                                                           indexTermType.fixedSizeOf());
        indexMetas = writer.writeCompleteSegment(termEnum);

        final FileHandle treeHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.BALANCED_TREE, indexIdentifier, null);
        final FileHandle treePostingsHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, indexIdentifier, null);

        try (BlockBalancedTreeReader reader = new BlockBalancedTreeReader(indexIdentifier,
                                                                          treeHandle,
                                                                          indexMetas.get(IndexComponent.BALANCED_TREE).root,
                                                                          treePostingsHandle,
                                                                          indexMetas.get(IndexComponent.POSTING_LISTS).root
        ))
        {
            final Counter visited = Counter.newCounter();
            try (final PostingList ignored = reader.intersect(new BlockBalancedTreeReader.IntersectVisitor()
            {
                @Override
                public boolean contains(byte[] packedValue)
                {
                    final ByteComparable actualTerm = ByteComparable.fixedLength(packedValue);
                    final ByteComparable expectedTerm = ByteComparable.of(Math.toIntExact(visited.get()));
                    assertEquals("Point value mismatch after visiting " + visited.get() + " entries.", 0,
                                 ByteComparable.compare(actualTerm, expectedTerm, ByteComparable.Version.OSS50));

                    visited.addAndGet(1);
                    return true;
                }

                @Override
                public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
                {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
            }, mockEventListener(), mock(QueryContext.class)))
            {
                assertEquals(maxSegmentRowId, visited.get());
            }
        }
    }

    private QueryEventListener.BalancedTreeEventListener mockEventListener()
    {
        QueryEventListener.BalancedTreeEventListener balancedTreeEventListener = mock(QueryEventListener.BalancedTreeEventListener.class);
        when(balancedTreeEventListener.postingListEventListener()).thenReturn(mock(QueryEventListener.PostingListEventListener.class));
        return balancedTreeEventListener;
    }

    private TermsIterator buildTermEnum(int startTermInclusive, int endTermExclusive)
    {
        final ByteBuffer minTerm = Int32Type.instance.decompose(startTermInclusive);
        final ByteBuffer maxTerm = Int32Type.instance.decompose(endTermExclusive);

        final AbstractGuavaIterator<Pair<ByteComparable, LongArrayList>> iterator = new AbstractGuavaIterator<>()
        {
            private int currentTerm = startTermInclusive;
            private int currentRowId = 0;

            @Override
            protected Pair<ByteComparable, LongArrayList> computeNext()
            {
                if (currentTerm >= endTermExclusive)
                {
                    return endOfData();
                }
                final ByteBuffer term = Int32Type.instance.decompose(currentTerm++);
                final LongArrayList postings = new LongArrayList();
                postings.add(currentRowId++);
                final ByteSource encoded = Int32Type.instance.asComparableBytes(term, ByteComparable.Version.OSS50);
                return Pair.create(v -> encoded, postings);
            }
        };

        return new MemtableTermsIterator(minTerm, maxTerm, iterator);
    }
}

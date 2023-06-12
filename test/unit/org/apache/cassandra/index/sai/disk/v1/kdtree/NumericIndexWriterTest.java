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
package org.apache.cassandra.index.sai.disk.v1.kdtree;

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.memory.MemtableTermsIterator;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.index.sai.utils.TermsIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.AbstractGuavaIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.NumericUtils;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NumericIndexWriterTest extends SAIRandomizedTester
{
    private IndexDescriptor indexDescriptor;
    private IndexContext indexContext;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        indexContext = SAITester.createIndexContext(newIndex(), Int32Type.instance);
    }

    @Test
    public void shouldFlushFromRamBuffer() throws Exception
    {
        final BKDTreeRamBuffer ramBuffer = new BKDTreeRamBuffer(Integer.BYTES);
        final int numRows = 120;
        int currentValue = numRows;
        for (int i = 0; i < numRows; ++i)
        {
            byte[] scratch = new byte[Integer.BYTES];
            NumericUtils.intToSortableBytes(currentValue--, scratch, 0);
            ramBuffer.addPackedValue(i, new BytesRef(scratch));
        }

        final MutableOneDimPointValues pointValues = ramBuffer.asPointValues();

        int docCount = pointValues.getDocCount();

        SegmentMetadata.ComponentMetadataMap indexMetas;

        NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                           indexContext,
                                                           Integer.BYTES,
                                                           docCount);
        indexMetas = writer.writeAll(pointValues);

        final FileHandle kdtreeHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE, indexContext);
        final FileHandle kdtreePostingsHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, indexContext);

        try (BKDReader reader = new BKDReader(indexContext,
                                              kdtreeHandle,
                                              indexMetas.get(IndexComponent.KD_TREE).root,
                                              kdtreePostingsHandle,
                                              indexMetas.get(IndexComponent.POSTING_LISTS).root))
        {
            final Counter visited = Counter.newCounter();
            try (final PostingList ignored = reader.intersect(new BKDReader.IntersectVisitor()
            {
                @Override
                public boolean visit(byte[] packedValue)
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
        final ImmutableOneDimPointValues pointValues = ImmutableOneDimPointValues
                                                       .fromTermEnum(termEnum, Int32Type.instance);

        SegmentMetadata.ComponentMetadataMap indexMetas;
        NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                           indexContext,
                                                           TypeUtil.fixedSizeOf(Int32Type.instance),
                                                           maxSegmentRowId);
        indexMetas = writer.writeAll(pointValues);

        final FileHandle kdtreeHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE, indexContext);
        final FileHandle kdtreePostingsHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, indexContext);

        try (BKDReader reader = new BKDReader(indexContext,
                                              kdtreeHandle,
                                              indexMetas.get(IndexComponent.KD_TREE).root,
                                              kdtreePostingsHandle,
                                              indexMetas.get(IndexComponent.POSTING_LISTS).root
        ))
        {
            final Counter visited = Counter.newCounter();
            try (final PostingList ignored = reader.intersect(new BKDReader.IntersectVisitor()
            {
                @Override
                public boolean visit(byte[] packedValue)
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

    private QueryEventListener.BKDIndexEventListener mockEventListener()
    {
        QueryEventListener.BKDIndexEventListener bkdIndexEventListener = mock(QueryEventListener.BKDIndexEventListener.class);
        when(bkdIndexEventListener.postingListEventListener()).thenReturn(mock(QueryEventListener.PostingListEventListener.class));
        return bkdIndexEventListener;
    }

    private TermsIterator buildTermEnum(int startTermInclusive, int endTermExclusive)
    {
        final ByteBuffer minTerm = Int32Type.instance.decompose(startTermInclusive);
        final ByteBuffer maxTerm = Int32Type.instance.decompose(endTermExclusive);

        final AbstractGuavaIterator<Pair<ByteComparable, LongArrayList>> iterator = new AbstractGuavaIterator<Pair<ByteComparable, LongArrayList>>()
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

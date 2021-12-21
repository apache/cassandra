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

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.MergeOneDimPointValues;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.assertj.core.util.Lists;

import static org.apache.cassandra.index.sai.metrics.QueryEventListeners.NO_OP_BKD_LISTENER;
import static org.apache.lucene.index.PointValues.Relation.CELL_CROSSES_QUERY;
import static org.apache.lucene.index.PointValues.Relation.CELL_INSIDE_QUERY;
import static org.apache.lucene.index.PointValues.Relation.CELL_OUTSIDE_QUERY;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class BKDReaderTest extends SaiRandomizedTest
{
    private final BKDReader.IntersectVisitor NONE_MATCH = new BKDReader.IntersectVisitor()
    {
        @Override
        public boolean visit(byte[] packedValue)
        {
            return false;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            return CELL_OUTSIDE_QUERY;
        }
    };

    private final BKDReader.IntersectVisitor ALL_MATCH = new BKDReader.IntersectVisitor()
    {
        @Override
        public boolean visit(byte[] packedValue)
        {
            return true;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            return CELL_INSIDE_QUERY;
        }
    };

    private final BKDReader.IntersectVisitor ALL_MATCH_WITH_FILTERING = new BKDReader.IntersectVisitor()
    {
        @Override
        public boolean visit(byte[] packedValue)
        {
            return true;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            return CELL_CROSSES_QUERY;
        }
    };

    private IndexDescriptor indexDescriptor;
    private String index;
    private IndexContext indexContext;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        index = newIndex();
        indexContext = SAITester.createIndexContext(index, Int32Type.instance);
    }

    @Test
    public void testInts1D() throws IOException
    {
        doTestInts1D();
    }

    @Test
    public void testMerge() throws Exception
    {
        // Start by testing that the iteratorState returns rowIds in order
        BKDReader reader1 = createReader(10);
        BKDReader.IteratorState it1 = reader1.iteratorState();
        Long expectedRowId = 0L;
        while (it1.hasNext())
        {
            assertEquals(expectedRowId++, it1.next());
        }
        it1.close();

        // Next test that an intersection only returns the query values
        List<Long> expected = Lists.list(8L, 9L);
        int expectedCount = 0;
        PostingList intersection = reader1.intersect(buildQuery(8, 9), (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext());
        for (Long id = intersection.nextPosting(); id != PostingList.END_OF_STREAM; id = intersection.nextPosting())
        {
            assertEquals(expected.get(expectedCount++), id);
        }
        intersection.close();
        reader1.close();

        // Finally test that merger returns the correct values
        expected = Lists.list(8L, 9L, 18L, 19L);
        expectedCount = 0;

        reader1 = createReader(10);
        BKDReader reader2 = createReader(10);

        List<BKDReader.IteratorState> iterators = ImmutableList.of(reader1.iteratorState(), reader2.iteratorState((rowID) -> rowID + 10));
        MergeOneDimPointValues merger = new MergeOneDimPointValues(iterators, Int32Type.instance);
        final BKDReader reader = finishAndOpenReaderOneDim(2, merger, 20);

        final int queryMin = 8;
        final int queryMax = 9;

        intersection = reader.intersect(buildQuery(queryMin, queryMax), (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext());

        for (Long id = intersection.nextPosting(); id != PostingList.END_OF_STREAM; id = intersection.nextPosting())
        {
            assertEquals(expected.get(expectedCount++), id);
        }

        intersection.close();

        for (BKDReader.IteratorState iterator : iterators)
        {
            iterator.close();
        }

        reader1.close();
        reader2.close();
        reader.close();
    }

    private PostingList intersect(List<PostingList.PeekablePostingList> postings)
    {
        if (postings == null || postings.isEmpty())
            return null;

        PriorityQueue<PostingList.PeekablePostingList> queue = new PriorityQueue<>(Comparator.comparingLong(PostingList.PeekablePostingList::peek));
        queue.addAll(postings);

        return MergePostingList.merge(queue, () -> postings.forEach(posting -> FileUtils.closeQuietly(posting)));
    }

    private BKDReader createReader(int numRows) throws IOException
    {
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);
        byte[] scratch = new byte[4];
        for (int docID = 0; docID < numRows; docID++)
        {
            NumericUtils.intToSortableBytes(docID, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }
        return finishAndOpenReaderOneDim(2, buffer);
    }

    private void doTestInts1D() throws IOException
    {
        final int numRows = between(100, 400);
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);

        byte[] scratch = new byte[4];
        for (int docID = 0; docID < numRows; docID++)
        {
            NumericUtils.intToSortableBytes(docID, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }

        final BKDReader reader = finishAndOpenReaderOneDim(2, buffer);

        try (BKDReader.IteratorState iterator = reader.iteratorState())
        {
            while (iterator.hasNext())
            {
                int value = NumericUtils.sortableBytesToInt(iterator.scratch, 0);
                System.out.println("term=" + value);
                iterator.next();
            }
        }

        try (PostingList intersection = reader.intersect(NONE_MATCH, (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext()))
        {
            assertNull(intersection);
        }

        try (PostingList collectAllIntersection = reader.intersect(ALL_MATCH, (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext());
             PostingList filteringIntersection = reader.intersect(ALL_MATCH_WITH_FILTERING, (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext()))
        {
            assertEquals(numRows, collectAllIntersection.size());
            assertEquals(numRows, filteringIntersection.size());

            for (int docID = 0; docID < numRows; docID++)
            {
                assertEquals(docID, collectAllIntersection.nextPosting());
                assertEquals(docID, filteringIntersection.nextPosting());
            }

            assertEquals(PostingList.END_OF_STREAM, collectAllIntersection.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, filteringIntersection.nextPosting());
        }

        // Simple 1D range query:
        final int queryMin = 42;
        final int queryMax = 87;

        final PostingList intersection = reader.intersect(buildQuery(queryMin, queryMax), (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext());

        assertThat(intersection, is(instanceOf(MergePostingList.class)));
        long expectedRowID = queryMin;
        for (long id = intersection.nextPosting(); id != PostingList.END_OF_STREAM; id = intersection.nextPosting())
        {
            assertEquals(expectedRowID++, id);
        }
        assertEquals(queryMax - queryMin + 1, intersection.size());

        intersection.close();
        reader.close();
    }

    @Test
    public void testAdvance() throws IOException
    {
        doTestAdvance(false);
    }

    @Test
    public void testAdvanceCrypto() throws IOException
    {
        doTestAdvance(true);
    }

    private void doTestAdvance(boolean crypto) throws IOException
    {
        final int numRows = between(1000, 2000);
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);

        byte[] scratch = new byte[4];
        for (int docID = 0; docID < numRows; docID++)
        {
            NumericUtils.intToSortableBytes(docID, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }

        final BKDReader reader = finishAndOpenReaderOneDim(2, buffer);

        PostingList intersection = reader.intersect(NONE_MATCH, (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext());
        assertNull(intersection);

        intersection = reader.intersect(ALL_MATCH, (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext());
        assertEquals(numRows, intersection.size());
        assertEquals(100, intersection.advance(100));
        assertEquals(200, intersection.advance(200));
        assertEquals(300, intersection.advance(300));
        assertEquals(400, intersection.advance(400));
        assertEquals(401, intersection.advance(401));
        long expectedRowID = 402;
        for (long id = intersection.nextPosting(); expectedRowID < 500; id = intersection.nextPosting())
        {
            assertEquals(expectedRowID++, id);
        }
        assertEquals(PostingList.END_OF_STREAM, intersection.advance(numRows + 1));

        intersection.close();
    }

    @Test
    public void testResourcesReleaseWhenQueryDoesntMatchAnything() throws Exception
    {
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);
        byte[] scratch = new byte[4];
        for (int docID = 0; docID < 1000; docID++)
        {
            NumericUtils.intToSortableBytes(docID, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }
        // add a gap between 1000 and 1100
        for (int docID = 1000; docID < 2000; docID++)
        {
            NumericUtils.intToSortableBytes(docID + 100, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }

        final BKDReader reader = finishAndOpenReaderOneDim(50, buffer);

        final PostingList intersection = reader.intersect(buildQuery(1017, 1096), (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext());
        assertNull(intersection);
    }

    private BKDReader.IntersectVisitor buildQuery(int queryMin, int queryMax)
    {
        return new BKDReader.IntersectVisitor()
        {
            @Override
            public boolean visit(byte[] packedValue)
            {
                int x = NumericUtils.sortableBytesToInt(packedValue, 0);
                boolean bb = x >= queryMin && x <= queryMax;
                return bb;
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
            {
                int min = NumericUtils.sortableBytesToInt(minPackedValue, 0);
                int max = NumericUtils.sortableBytesToInt(maxPackedValue, 0);
                assert max >= min;

                if (max < queryMin || min > queryMax)
                {
                    return Relation.CELL_OUTSIDE_QUERY;
                }
                else if (min >= queryMin && max <= queryMax)
                {
                    return CELL_INSIDE_QUERY;
                }
                else
                {
                    return CELL_CROSSES_QUERY;
                }
            }
        };
    }

    private BKDReader finishAndOpenReaderOneDim(int maxPointsPerLeaf, BKDTreeRamBuffer buffer) throws IOException
    {
        final NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                                 indexContext,
                                                                 maxPointsPerLeaf,
                                                                 Integer.BYTES,
                                                                 Math.toIntExact(buffer.numRows()),
                                                                 buffer.numRows(),
                                                                 new IndexWriterConfig("test", 2, 8),
                                                                 false);

        final SegmentMetadata.ComponentMetadataMap metadata = writer.writeAll(buffer.asPointValues());
        final long bkdPosition = metadata.get(IndexComponent.KD_TREE).root;
        assertThat(bkdPosition, is(greaterThan(0L)));
        final long postingsPosition = metadata.get(IndexComponent.KD_TREE_POSTING_LISTS).root;
        assertThat(postingsPosition, is(greaterThan(0L)));

        FileHandle kdtreeHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE, indexContext);
        FileHandle kdtreePostingsHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE_POSTING_LISTS, indexContext);
        return new BKDReader(indexContext,
                             kdtreeHandle,
                             bkdPosition,
                             kdtreePostingsHandle,
                             postingsPosition);
    }

    private BKDReader finishAndOpenReaderOneDim(int maxPointsPerLeaf, MutableOneDimPointValues values, int numRows) throws IOException
    {
        final NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                                 indexContext,
                                                                 maxPointsPerLeaf,
                                                                 Integer.BYTES,
                                                                 Math.toIntExact(numRows),
                                                                 numRows,
                                                                 new IndexWriterConfig("test", 2, 8),
                                                                 false);

        final SegmentMetadata.ComponentMetadataMap metadata = writer.writeAll(values);
        final long bkdPosition = metadata.get(IndexComponent.KD_TREE).root;
        assertThat(bkdPosition, is(greaterThan(0L)));
        final long postingsPosition = metadata.get(IndexComponent.KD_TREE_POSTING_LISTS).root;
        assertThat(postingsPosition, is(greaterThan(0L)));

        FileHandle kdtreeHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE, indexContext);
        FileHandle kdtreePostingsHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE_POSTING_LISTS, indexContext);
        return new BKDReader(indexContext,
                             kdtreeHandle,
                             bkdPosition,
                             kdtreePostingsHandle,
                             postingsPosition);
    }
}

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

import static org.apache.lucene.index.PointValues.Relation.CELL_CROSSES_QUERY;
import static org.apache.lucene.index.PointValues.Relation.CELL_INSIDE_QUERY;
import static org.apache.lucene.index.PointValues.Relation.CELL_OUTSIDE_QUERY;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BlockBalancedTreeReaderTest extends SAIRandomizedTester
{
    private final BlockBalancedTreeReader.IntersectVisitor NONE_MATCH = new BlockBalancedTreeReader.IntersectVisitor()
    {
        @Override
        public boolean contains(byte[] packedValue)
        {
            return false;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            return CELL_OUTSIDE_QUERY;
        }
    };

    private final BlockBalancedTreeReader.IntersectVisitor ALL_MATCH = new BlockBalancedTreeReader.IntersectVisitor()
    {
        @Override
        public boolean contains(byte[] packedValue)
        {
            return true;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            return CELL_INSIDE_QUERY;
        }
    };

    private IndexDescriptor indexDescriptor;
    private StorageAttachedIndex index;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        index = SAITester.createMockIndex(newIndex(), Int32Type.instance);
    }

    @Test
    public void testFilteringIntersection() throws Exception
    {
        int numRows = 1000;

        final BlockBalancedTreeRamBuffer buffer = new BlockBalancedTreeRamBuffer(Integer.BYTES);

        byte[] scratch = new byte[4];
        for (int rowID = 0; rowID < numRows; rowID++)
        {
            NumericUtils.intToSortableBytes(rowID, scratch, 0);
            buffer.add(rowID, scratch);
        }

        try (BlockBalancedTreeReader reader = finishAndOpenReader(4, buffer))
        {
            assertRange(reader, 445, 555);
        }
    }

    @Test
    public void testAdvance() throws Exception
    {
        final int numRows = between(1000, 2000);
        final BlockBalancedTreeRamBuffer buffer = new BlockBalancedTreeRamBuffer(Integer.BYTES);

        byte[] scratch = new byte[4];
        for (int rowID = 0; rowID < numRows; rowID++)
        {
            NumericUtils.intToSortableBytes(rowID, scratch, 0);
            buffer.add(rowID, scratch);
        }

        try (BlockBalancedTreeReader reader = finishAndOpenReader(2, buffer))
        {
            PostingList intersection = performIntersection(reader, NONE_MATCH);
            assertNull(intersection);

            intersection = performIntersection(reader, ALL_MATCH);
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
    }

    @Test
    public void testSameValuesInLeaf() throws Exception
    {
        // While a bit synthetic this test is designed to test that the
        // BlockBalancedTreeReader.FilteringIntersection.buildPostingsFilterForSingleValueLeaf
        // method is exercised in a test. To do this we need to ensure that
        // we have at least one leaf that has all the same value and that
        // all of that leaf is requested in a query.
        final BlockBalancedTreeRamBuffer buffer = new BlockBalancedTreeRamBuffer(Integer.BYTES);
        byte[] scratch = new byte[4];

        for (int rowID = 0; rowID < 10; rowID++)
        {
            NumericUtils.intToSortableBytes(rowID, scratch, 0);
            buffer.add(rowID, scratch);
        }

        for (int rowID = 10; rowID < 20; rowID++)
        {
            NumericUtils.intToSortableBytes(10, scratch, 0);
            buffer.add(rowID, scratch);
        }

        for (int rowID = 20; rowID < 30; rowID++)
        {
            NumericUtils.intToSortableBytes(rowID, scratch, 0);
            buffer.add(rowID, scratch);
        }

        try (BlockBalancedTreeReader reader = finishAndOpenReader(5, buffer))
        {
            PostingList postingList = performIntersection(reader, buildQuery(8, 15));

            assertEquals(8, postingList.nextPosting());
            assertEquals(9, postingList.nextPosting());
            assertEquals(10, postingList.nextPosting());
            assertEquals(11, postingList.nextPosting());
            assertEquals(12, postingList.nextPosting());
            assertEquals(13, postingList.nextPosting());
            assertEquals(14, postingList.nextPosting());
            assertEquals(15, postingList.nextPosting());
            assertEquals(16, postingList.nextPosting());
            assertEquals(17, postingList.nextPosting());
            assertEquals(18, postingList.nextPosting());
            assertEquals(19, postingList.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, postingList.nextPosting());
        }
    }

    @Test
    public void testResourcesReleaseWhenQueryDoesntMatchAnything() throws Exception
    {
        final BlockBalancedTreeRamBuffer buffer = new BlockBalancedTreeRamBuffer(Integer.BYTES);
        byte[] scratch = new byte[4];
        for (int rowID = 0; rowID < 1000; rowID++)
        {
            NumericUtils.intToSortableBytes(rowID, scratch, 0);
            buffer.add(rowID, scratch);
        }
        // add a gap between 1000 and 1100
        for (int rowID = 1000; rowID < 2000; rowID++)
        {
            NumericUtils.intToSortableBytes(rowID + 100, scratch, 0);
            buffer.add(rowID, scratch);
        }

        try (BlockBalancedTreeReader reader = finishAndOpenReader(50, buffer))
        {
            final PostingList intersection = performIntersection(reader, buildQuery(1017, 1096));
            assertNull(intersection);
        }
    }

    @Test
    public void testConcurrentIntersectionsOnSameReader() throws Exception
    {
        int numRows = 1000;

        final BlockBalancedTreeRamBuffer buffer = new BlockBalancedTreeRamBuffer(Integer.BYTES);

        byte[] scratch = new byte[4];
        for (int rowID = 0; rowID < numRows; rowID++)
        {
            NumericUtils.intToSortableBytes(rowID, scratch, 0);
            buffer.add(rowID, scratch);
        }

        try (BlockBalancedTreeReader reader = finishAndOpenReader(4, buffer))
        {
            int concurrency = 100;

            ExecutorService executor = Executors.newFixedThreadPool(concurrency);
            List<Future<?>> results = new ArrayList<>();
            for (int thread = 0; thread < concurrency; thread++)
            {
                results.add(executor.submit(() -> assertRange(reader, 445, 555)));
            }
            FBUtilities.waitOnFutures(results);
            executor.shutdown();
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void assertRange(BlockBalancedTreeReader reader, long lowerBound, long upperBound)
    {
        Expression expression = Expression.create(index);
        expression.add(Operator.GT, Int32Type.instance.decompose(444));
        expression.add(Operator.LT, Int32Type.instance.decompose(555));

        try
        {
            PostingList intersection = performIntersection(reader, BlockBalancedTreeQueries.balancedTreeQueryFrom(expression, 4));
            assertNotNull(intersection);
            assertEquals(upperBound - lowerBound, intersection.size());
            for (long posting = lowerBound; posting < upperBound; posting++)
                assertEquals(posting, intersection.nextPosting());
        }
        catch (IOException e)
        {
            throw Throwables.unchecked(e);
        }
    }

    private PostingList performIntersection(BlockBalancedTreeReader reader, BlockBalancedTreeReader.IntersectVisitor visitor)
    {
        QueryEventListener.BalancedTreeEventListener balancedTreeEventListener = mock(QueryEventListener.BalancedTreeEventListener.class);
        when(balancedTreeEventListener.postingListEventListener()).thenReturn(mock(QueryEventListener.PostingListEventListener.class));
        return reader.intersect(visitor, balancedTreeEventListener, mock(QueryContext.class));
    }

    private BlockBalancedTreeReader.IntersectVisitor buildQuery(int queryMin, int queryMax)
    {
        return new BlockBalancedTreeReader.IntersectVisitor()
        {
            @Override
            public boolean contains(byte[] packedValue)
            {
                int x = NumericUtils.sortableBytesToInt(packedValue, 0);
                return x >= queryMin && x <= queryMax;
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

    private BlockBalancedTreeReader finishAndOpenReader(int maxPointsPerLeaf, BlockBalancedTreeRamBuffer buffer) throws Exception
    {
        setBDKPostingsWriterSizing(8, 2);
        final NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                                 index.identifier(),
                                                                 maxPointsPerLeaf,
                                                                 Integer.BYTES,
                                                                 Math.toIntExact(buffer.numRows()));

        final SegmentMetadata.ComponentMetadataMap metadata = writer.writeCompleteSegment(buffer.iterator());
        final long treePosition = metadata.get(IndexComponent.BALANCED_TREE).root;
        assertThat(treePosition, is(greaterThan(0L)));
        final long postingsPosition = metadata.get(IndexComponent.POSTING_LISTS).root;
        assertThat(postingsPosition, is(greaterThan(0L)));

        FileHandle treeHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.BALANCED_TREE, index.identifier());
        FileHandle treePostingsHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, index.identifier());
        return new BlockBalancedTreeReader(index.identifier(),
                                           treeHandle,
                                           treePosition,
                                           treePostingsHandle,
                                           postingsPosition);
    }
}

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
package org.apache.cassandra.db.partitions;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.Predicate;

import com.google.common.collect.Iterators;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringPrefix.Kind;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public final class PurgeFunctionTest
{
    private static final String KEYSPACE = "PurgeFunctionTest";
    private static final String TABLE = "table";

    private CFMetaData metadata;
    private DecoratedKey key;

    private static UnfilteredPartitionIterator withoutPurgeableTombstones(UnfilteredPartitionIterator iterator, int gcBefore)
    {
        class WithoutPurgeableTombstones extends PurgeFunction
        {
            private WithoutPurgeableTombstones()
            {
                super(iterator.isForThrift(), FBUtilities.nowInSeconds(), gcBefore, Integer.MAX_VALUE, false, false);
            }

            protected Predicate<Long> getPurgeEvaluator()
            {
                return time -> true;
            }
        }

        return Transformation.apply(iterator, new WithoutPurgeableTombstones());
    }

    @Before
    public void setUp()
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        metadata =
            CFMetaData.Builder
                      .create(KEYSPACE, TABLE)
                      .addPartitionKey("pk", UTF8Type.instance)
                      .addClusteringColumn("ck", UTF8Type.instance)
                      .build();
        key = Murmur3Partitioner.instance.decorateKey(bytes("key"));
    }

    @Test
    public void testNothingIsPurgeableASC()
    {
        UnfilteredPartitionIterator original = iter(false
        , bound(Kind.INCL_START_BOUND, 0L, 0, "a")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0L, 0, 1L, 1, "b")
        , bound(Kind.INCL_END_BOUND, 1L, 1, "c")
        );
        UnfilteredPartitionIterator purged = withoutPurgeableTombstones(original, 0);

        UnfilteredPartitionIterator expected = iter(false
        , bound(Kind.INCL_START_BOUND, 0L, 0, "a")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0L, 0, 1L, 1, "b")
        , bound(Kind.INCL_END_BOUND, 1L, 1, "c")
        );
        assertIteratorsEqual(expected, purged);
    }

    @Test
    public void testNothingIsPurgeableDESC()
    {
        UnfilteredPartitionIterator original = iter(true
        , bound(Kind.INCL_END_BOUND, 1L, 1, "c")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0L, 0, 1L, 1, "b")
        , bound(Kind.INCL_START_BOUND, 0L, 0, "a")
        );
        UnfilteredPartitionIterator purged = withoutPurgeableTombstones(original, 0);

        UnfilteredPartitionIterator expected = iter(true
        , bound(Kind.INCL_END_BOUND, 1L, 1, "c")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0L, 0, 1L, 1, "b")
        , bound(Kind.INCL_START_BOUND, 0L, 0, "a")
        );
        assertIteratorsEqual(expected, purged);
    }

    @Test
    public void testEverythingIsPurgeableASC()
    {
        UnfilteredPartitionIterator original = iter(false
        , bound(Kind.INCL_START_BOUND, 0L, 0, "a")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0L, 0, 1L, 1, "b")
        , bound(Kind.INCL_END_BOUND, 1L, 1, "c")
        );
        UnfilteredPartitionIterator purged = withoutPurgeableTombstones(original, 2);

        assertTrue(!purged.hasNext());
    }

    @Test
    public void testEverythingIsPurgeableDESC()
    {
        UnfilteredPartitionIterator original = iter(false
        , bound(Kind.INCL_END_BOUND, 1L, 1, "c")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0L, 0, 1L, 1, "b")
        , bound(Kind.INCL_START_BOUND, 0L, 0, "a")
        );
        UnfilteredPartitionIterator purged = withoutPurgeableTombstones(original, 2);

        assertTrue(!purged.hasNext());
    }

    @Test
    public void testFirstHalfIsPurgeableASC()
    {
        UnfilteredPartitionIterator original = iter(false
        , bound(Kind.INCL_START_BOUND, 0L, 0, "a")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0L, 0, 1L, 1, "b")
        , bound(Kind.INCL_END_BOUND, 1L, 1, "c")
        );
        UnfilteredPartitionIterator purged = withoutPurgeableTombstones(original, 1);

        UnfilteredPartitionIterator expected = iter(false
        , bound(Kind.INCL_START_BOUND, 1L, 1, "b")
        , bound(Kind.INCL_END_BOUND, 1L, 1, "c")
        );
        assertIteratorsEqual(expected, purged);
    }

    @Test
    public void testFirstHalfIsPurgeableDESC()
    {
        UnfilteredPartitionIterator original = iter(true
        , bound(Kind.INCL_END_BOUND, 1L, 1, "c")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 0L, 0, 1L, 1, "b")
        , bound(Kind.INCL_START_BOUND, 0L, 0, "a")
        );
        UnfilteredPartitionIterator purged = withoutPurgeableTombstones(original, 1);

        UnfilteredPartitionIterator expected = iter(false
        , bound(Kind.INCL_END_BOUND, 1L, 1, "c")
        , bound(Kind.INCL_START_BOUND, 1L, 1, "b")
        );
        assertIteratorsEqual(expected, purged);
    }

    @Test
    public void testSecondHalfIsPurgeableASC()
    {
        UnfilteredPartitionIterator original = iter(false
        , bound(Kind.INCL_START_BOUND, 1L, 1, "a")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 1L, 1, 0L, 0, "b")
        , bound(Kind.INCL_END_BOUND, 0L, 0, "c")
        );
        UnfilteredPartitionIterator purged = withoutPurgeableTombstones(original, 1);

        UnfilteredPartitionIterator expected = iter(false
        , bound(Kind.INCL_START_BOUND, 1L, 1, "a")
        , bound(Kind.EXCL_END_BOUND, 1L, 1, "b")
        );
        assertIteratorsEqual(expected, purged);
    }

    @Test
    public void testSecondHalfIsPurgeableDESC()
    {
        UnfilteredPartitionIterator original = iter(true
        , bound(Kind.INCL_END_BOUND, 0L, 0, "c")
        , boundary(Kind.EXCL_END_INCL_START_BOUNDARY, 1L, 1, 0L, 0, "b")
        , bound(Kind.INCL_START_BOUND, 1L, 1, "a")
        );
        UnfilteredPartitionIterator purged = withoutPurgeableTombstones(original, 1);

        UnfilteredPartitionIterator expected = iter(true
        , bound(Kind.EXCL_END_BOUND, 1L, 1, "b")
        , bound(Kind.INCL_START_BOUND, 1L, 1, "a")
        );
        assertIteratorsEqual(expected, purged);
    }

    private UnfilteredPartitionIterator iter(boolean isReversedOrder, Unfiltered... unfiltereds)
    {
        Iterator<Unfiltered> iterator = Iterators.forArray(unfiltereds);

        UnfilteredRowIterator rowIter =
            new AbstractUnfilteredRowIterator(metadata,
                                              key,
                                              DeletionTime.LIVE,
                                              metadata.partitionColumns(),
                                              Rows.EMPTY_STATIC_ROW,
                                              isReversedOrder,
                                              EncodingStats.NO_STATS)
        {
            protected Unfiltered computeNext()
            {
                return iterator.hasNext() ? iterator.next() : endOfData();
            }
        };

        return new SingletonUnfilteredPartitionIterator(rowIter, false);
    }

    private RangeTombstoneBoundMarker bound(ClusteringPrefix.Kind kind,
                                            long timestamp,
                                            int localDeletionTime,
                                            Object clusteringValue)
    {
        ByteBuffer[] clusteringByteBuffers =
            new ByteBuffer[] { decompose(metadata.clusteringColumns().get(0).type, clusteringValue) };

        return new RangeTombstoneBoundMarker(ClusteringBound.create(kind, clusteringByteBuffers),
                                             new DeletionTime(timestamp, localDeletionTime));
    }

    private RangeTombstoneBoundaryMarker boundary(ClusteringPrefix.Kind kind,
                                                  long closeTimestamp,
                                                  int closeLocalDeletionTime,
                                                  long openTimestamp,
                                                  int openDeletionTime,
                                                  Object clusteringValue)
    {
        ByteBuffer[] clusteringByteBuffers =
            new ByteBuffer[] { decompose(metadata.clusteringColumns().get(0).type, clusteringValue) };

        return new RangeTombstoneBoundaryMarker(ClusteringBoundary.create(kind, clusteringByteBuffers),
                                                new DeletionTime(closeTimestamp, closeLocalDeletionTime),
                                                new DeletionTime(openTimestamp, openDeletionTime));
    }

    @SuppressWarnings("unchecked")
    private static <T> ByteBuffer decompose(AbstractType<?> type, T value)
    {
        return ((AbstractType<T>) type).decompose(value);
    }

    private void assertIteratorsEqual(UnfilteredPartitionIterator iter1, UnfilteredPartitionIterator iter2)
    {
        while (iter1.hasNext())
        {
            assertTrue(iter2.hasNext());

            try (UnfilteredRowIterator partition1 = iter1.next())
            {
                try (UnfilteredRowIterator partition2 = iter2.next())
                {
                    assertIteratorsEqual(partition1, partition2);
                }
            }
        }

        assertTrue(!iter2.hasNext());
    }

    private void assertIteratorsEqual(UnfilteredRowIterator iter1, UnfilteredRowIterator iter2)
    {
        while (iter1.hasNext())
        {
            assertTrue(iter2.hasNext());

            assertEquals(iter1.next(), iter2.next());
        }
        assertTrue(!iter2.hasNext());
    }
}

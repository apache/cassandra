/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.internal;

import java.nio.ByteBuffer;
import java.util.SortedSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.internal.composites.CollectionValueIndex;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.btree.BTreeSet;

public abstract class CassandraIndexSearcher implements Index.Searcher
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraIndexSearcher.class);

    private final RowFilter.Expression expression;
    protected final CassandraIndex index;
    protected final ReadCommand command;

    public CassandraIndexSearcher(ReadCommand command,
                                  RowFilter.Expression expression,
                                  CassandraIndex index)
    {
        this.command = command;
        this.expression = expression;
        this.index = index;
    }

    @Override
    public ReadCommand command()
    {
        return command;
    }

    // of this method.
    public UnfilteredPartitionIterator search(ReadExecutionController executionController)
    {
        // the value of the index expression is the partition key in the index table
        DecoratedKey indexKey = index.getBackingTable().get().decorateKey(expression.getIndexValue());
        UnfilteredRowIterator indexIter = queryIndex(indexKey, command, executionController);
        try
        {
            return queryDataFromIndex(indexKey, UnfilteredRowIterators.filter(indexIter, command.nowInSec()), command, executionController);
        }
        catch (RuntimeException | Error e)
        {
            indexIter.close();
            throw e;
        }
    }

    private UnfilteredRowIterator queryIndex(DecoratedKey indexKey, ReadCommand command, ReadExecutionController executionController)
    {
        ClusteringIndexFilter filter = makeIndexFilter(command);
        ColumnFamilyStore indexCfs = index.getBackingTable().get();
        TableMetadata indexMetadata = indexCfs.metadata();
        return SinglePartitionReadCommand.create(indexMetadata, command.nowInSec(), indexKey, ColumnFilter.all(indexMetadata), filter)
                                         .queryMemtableAndDisk(indexCfs, executionController.indexReadController());
    }

    private ClusteringIndexFilter makeIndexFilter(ReadCommand command)
    {
        if (command instanceof SinglePartitionReadCommand)
        {
            SinglePartitionReadCommand sprc = (SinglePartitionReadCommand)command;
            ByteBuffer pk = sprc.partitionKey().getKey();
            ClusteringIndexFilter filter = sprc.clusteringIndexFilter();

            if (filter instanceof ClusteringIndexNamesFilter)
            {
                if (index instanceof CollectionValueIndex)
                {
                    // Collection value indexes have an extra clustering key for the path, but we cannot construct an
                    // index names filter from the filter on the backing table, because it has no path information.
                    // Instead, we construct a slice from the clustering keys that are provided.
                    Slices slices = filter.getSlices(index.baseCfs.metadata());
                    ClusteringBound<?> start = BufferClusteringBound.BOTTOM;
                    ClusteringBound<?> end = BufferClusteringBound.TOP;

                    if (slices.size() > 0)
                        start = slices.get(0).start();
                    if (slices.size() > 0)
                        end = slices.get(slices.size() - 1).end();

                    Slice slice = Slice.make(makeIndexBound(pk, start), makeIndexBound(pk, end));
                    return new ClusteringIndexSliceFilter(Slices.with(index.getIndexComparator(), slice), filter.isReversed());
                }

                SortedSet<Clustering<?>> requested = ((ClusteringIndexNamesFilter) filter).requestedRows();
                // The partition key from the base table must be the first element of al clusterings of the index table.
                BTreeSet<Clustering<?>> clusterings = BTreeSet.copy(requested, index.getIndexComparator(), clustering -> makeIndexClustering(pk, clustering));
                return new ClusteringIndexNamesFilter(clusterings, filter.isReversed());
            }
            else
            {
                Slices requested = ((ClusteringIndexSliceFilter)filter).requestedSlices();
                Slices.Builder builder = new Slices.Builder(index.getIndexComparator());
                for (Slice slice : requested)
                    builder.add(makeIndexBound(pk, slice.start()), makeIndexBound(pk, slice.end()));
                return new ClusteringIndexSliceFilter(builder.build(), filter.isReversed());
            }
        }
        else
        {
            DataRange dataRange = command.dataRange();
            AbstractBounds<PartitionPosition> range = dataRange.keyRange();

            Slice slice = Slice.ALL;

            /*
             * XXX: If the range requested is a token range, we'll have to start at the beginning (and stop at the end) of
             * the indexed row unfortunately (which will be inefficient), because we have no way to intuit the smallest possible
             * key having a given token. A potential fix would be to actually store the token along the key in the indexed row.
             */
            if (range.left instanceof DecoratedKey)
            {
                // the right hand side of the range may not be a DecoratedKey (for instance if we're paging),
                // but if it is, we can optimise slightly by restricting the slice
                if (range.right instanceof DecoratedKey)
                {

                    DecoratedKey startKey = (DecoratedKey) range.left;
                    DecoratedKey endKey = (DecoratedKey) range.right;

                    ClusteringBound<?> start = BufferClusteringBound.BOTTOM;
                    ClusteringBound<?> end = BufferClusteringBound.TOP;

                    /*
                     * For index queries over a range, we can't do a whole lot better than querying everything for the
                     * key range, though for slice queries where we can slightly restrict the beginning and end. We can
                     * not do this optimisation for static column indexes.
                     */
                    if (!dataRange.isNamesQuery() && !index.indexedColumn.isStatic())
                    {
                        ClusteringIndexSliceFilter startSliceFilter = ((ClusteringIndexSliceFilter) dataRange.clusteringIndexFilter(startKey));
                        ClusteringIndexSliceFilter endSliceFilter = ((ClusteringIndexSliceFilter) dataRange.clusteringIndexFilter(endKey));

                        // We can't effectively support reversed queries when we have a range, so we don't support it
                        // (or through post-query reordering) and shouldn't get there.
                        assert !startSliceFilter.isReversed() && !endSliceFilter.isReversed();

                        Slices startSlices = startSliceFilter.requestedSlices();
                        Slices endSlices = endSliceFilter.requestedSlices();

                        if (startSlices.size() > 0)
                            start = startSlices.get(0).start();

                        if (endSlices.size() > 0)
                            end = endSlices.get(endSlices.size() - 1).end();
                    }

                    slice = Slice.make(makeIndexBound(startKey.getKey(), start),
                                       makeIndexBound(endKey.getKey(), end));
                }
                else
                {
                    // otherwise, just start the index slice from the key we do have
                    slice = Slice.make(makeIndexBound(((DecoratedKey)range.left).getKey(), BufferClusteringBound.BOTTOM),
                                       BufferClusteringBound.TOP);
                }
            }
            return new ClusteringIndexSliceFilter(Slices.with(index.getIndexComparator(), slice), false);
        }
    }

    private ClusteringBound<?> makeIndexBound(ByteBuffer rowKey, ClusteringBound<?> bound)
    {
        return index.buildIndexClusteringPrefix(rowKey, bound, null)
                                 .buildBound(bound.isStart(), bound.isInclusive());
    }

    protected Clustering<?> makeIndexClustering(ByteBuffer rowKey, Clustering<?> clustering)
    {
        return index.buildIndexClusteringPrefix(rowKey, clustering, null).build();
    }

    protected abstract UnfilteredPartitionIterator queryDataFromIndex(DecoratedKey indexKey,
                                                                      RowIterator indexHits,
                                                                      ReadCommand command,
                                                                      ReadExecutionController executionController);
}

package org.apache.cassandra.index.internal;

import java.nio.ByteBuffer;
import java.util.NavigableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.Index;
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

    @SuppressWarnings("resource") // Both the OpOrder and 'indexIter' are closed on exception, or through the closing of the result
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
        CFMetaData indexCfm = indexCfs.metadata;
        return SinglePartitionReadCommand.create(indexCfm, command.nowInSec(), indexKey, ColumnFilter.all(indexCfm), filter)
                                         .queryMemtableAndDisk(indexCfs, executionController.indexReadOpOrderGroup());
    }

    private ClusteringIndexFilter makeIndexFilter(ReadCommand command)
    {
        if (command instanceof SinglePartitionReadCommand)
        {
            // Note: as yet there's no route to get here - a 2i query *always* uses a
            // PartitionRangeReadCommand. This is here in preparation for coming changes
            // in SelectStatement.
            SinglePartitionReadCommand sprc = (SinglePartitionReadCommand)command;
            ByteBuffer pk = sprc.partitionKey().getKey();
            ClusteringIndexFilter filter = sprc.clusteringIndexFilter();

            if (filter instanceof ClusteringIndexNamesFilter)
            {
                NavigableSet<Clustering> requested = ((ClusteringIndexNamesFilter)filter).requestedRows();
                BTreeSet.Builder<Clustering> clusterings = BTreeSet.builder(index.getIndexComparator());
                for (Clustering c : requested)
                    clusterings.add(makeIndexClustering(pk, c));
                return new ClusteringIndexNamesFilter(clusterings.build(), filter.isReversed());
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

            DataRange dataRange = ((PartitionRangeReadCommand)command).dataRange();
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

                    Slice.Bound start = Slice.Bound.BOTTOM;
                    Slice.Bound end = Slice.Bound.TOP;

                    /*
                     * For index queries over a range, we can't do a whole lot better than querying everything for the key range, though for
                     * slice queries where we can slightly restrict the beginning and end.
                     */
                    if (!dataRange.isNamesQuery())
                    {
                        ClusteringIndexSliceFilter startSliceFilter = ((ClusteringIndexSliceFilter) dataRange.clusteringIndexFilter(
                                                                                                                                   startKey));
                        ClusteringIndexSliceFilter endSliceFilter = ((ClusteringIndexSliceFilter) dataRange.clusteringIndexFilter(
                                                                                                                                 endKey));

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
                    slice = Slice.make(makeIndexBound(((DecoratedKey)range.left).getKey(), Slice.Bound.BOTTOM),
                                       Slice.Bound.TOP);
                }
            }
            return new ClusteringIndexSliceFilter(Slices.with(index.getIndexComparator(), slice), false);
        }
    }

    private Slice.Bound makeIndexBound(ByteBuffer rowKey, Slice.Bound bound)
    {
        return index.buildIndexClusteringPrefix(rowKey, bound, null)
                                 .buildBound(bound.isStart(), bound.isInclusive());
    }

    protected Clustering makeIndexClustering(ByteBuffer rowKey, Clustering clustering)
    {
        return index.buildIndexClusteringPrefix(rowKey, clustering, null).build();
    }

    protected abstract UnfilteredPartitionIterator queryDataFromIndex(DecoratedKey indexKey,
                                                                      RowIterator indexHits,
                                                                      ReadCommand command,
                                                                      ReadExecutionController executionController);
}

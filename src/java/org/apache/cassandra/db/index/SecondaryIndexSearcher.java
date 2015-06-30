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
package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.FBUtilities;

public abstract class SecondaryIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(SecondaryIndexSearcher.class);

    protected final SecondaryIndexManager indexManager;
    protected final Set<ColumnDefinition> columns;
    protected final ColumnFamilyStore baseCfs;

    public SecondaryIndexSearcher(SecondaryIndexManager indexManager, Set<ColumnDefinition> columns)
    {
        this.indexManager = indexManager;
        this.columns = columns;
        this.baseCfs = indexManager.baseCfs;
    }

    public SecondaryIndex highestSelectivityIndex(RowFilter filter)
    {
        RowFilter.Expression expr = highestSelectivityPredicate(filter, false);
        return expr == null ? null : indexManager.getIndexForColumn(expr.column());
    }

    public RowFilter.Expression primaryClause(ReadCommand command)
    {
        return highestSelectivityPredicate(command.rowFilter(), false);
    }

    @SuppressWarnings("resource") // Both the OpOrder and 'indexIter' are closed on exception, or through the closing of the result
                                  // of this method.
    public UnfilteredPartitionIterator search(ReadCommand command, ReadOrderGroup orderGroup)
    {
        RowFilter.Expression primary = highestSelectivityPredicate(command.rowFilter(), true);
        assert primary != null;

        AbstractSimplePerColumnSecondaryIndex index = (AbstractSimplePerColumnSecondaryIndex)indexManager.getIndexForColumn(primary.column());
        assert index != null && index.getIndexCfs() != null;

        if (logger.isDebugEnabled())
            logger.debug("Most-selective indexed predicate is {}", primary);

        DecoratedKey indexKey = index.getIndexKeyFor(primary.getIndexValue());

        UnfilteredRowIterator indexIter = queryIndex(index, indexKey, command, orderGroup);
        try
        {
            return queryDataFromIndex(index, indexKey, UnfilteredRowIterators.filter(indexIter, command.nowInSec()), command, orderGroup);
        }
        catch (RuntimeException | Error e)
        {
            indexIter.close();
            throw e;
        }
    }

    private UnfilteredRowIterator queryIndex(AbstractSimplePerColumnSecondaryIndex index, DecoratedKey indexKey, ReadCommand command, ReadOrderGroup orderGroup)
    {
        ClusteringIndexFilter filter = makeIndexFilter(index, command);
        CFMetaData indexMetadata = index.getIndexCfs().metadata;
        return SinglePartitionReadCommand.create(indexMetadata, command.nowInSec(), indexKey, ColumnFilter.all(indexMetadata), filter)
                                         .queryMemtableAndDisk(index.getIndexCfs(), orderGroup.indexReadOpOrderGroup());
    }

    private ClusteringIndexFilter makeIndexFilter(AbstractSimplePerColumnSecondaryIndex index, ReadCommand command)
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
                    clusterings.add(index.makeIndexClustering(pk, c, (Cell)null));
                return new ClusteringIndexNamesFilter(clusterings.build(), filter.isReversed());
            }
            else
            {
                Slices requested = ((ClusteringIndexSliceFilter)filter).requestedSlices();
                Slices.Builder builder = new Slices.Builder(index.getIndexComparator());
                for (Slice slice : requested)
                    builder.add(index.makeIndexBound(pk, slice.start()), index.makeIndexBound(pk, slice.end()));
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

                    slice = Slice.make(index.makeIndexBound(startKey.getKey(), start),
                                       index.makeIndexBound(endKey.getKey(), end));
                }
                else
                {
                   // otherwise, just start the index slice from the key we do have
                   slice = Slice.make(index.makeIndexBound(((DecoratedKey)range.left).getKey(), Slice.Bound.BOTTOM),
                                      Slice.Bound.TOP);
                }
            }
            return new ClusteringIndexSliceFilter(Slices.with(index.getIndexComparator(), slice), false);
        }
    }

    protected abstract UnfilteredPartitionIterator queryDataFromIndex(AbstractSimplePerColumnSecondaryIndex index,
                                                                      DecoratedKey indexKey,
                                                                      RowIterator indexHits,
                                                                      ReadCommand command,
                                                                      ReadOrderGroup orderGroup);

    protected RowFilter.Expression highestSelectivityPredicate(RowFilter filter, boolean includeInTrace)
    {
        RowFilter.Expression best = null;
        int bestMeanCount = Integer.MAX_VALUE;
        Map<SecondaryIndex, Integer> candidates = new HashMap<>();

        for (RowFilter.Expression expression : filter)
        {
            // skip columns belonging to a different index type
            if (!columns.contains(expression.column()))
                continue;

            SecondaryIndex index = indexManager.getIndexForColumn(expression.column());
            if (index == null || index.getIndexCfs() == null || !index.supportsOperator(expression.operator()))
                continue;

            int columns = index.getIndexCfs().getMeanColumns();
            candidates.put(index, columns);
            if (columns < bestMeanCount)
            {
                best = expression;
                bestMeanCount = columns;
            }
        }

        if (includeInTrace)
        {
            if (best == null)
                Tracing.trace("No applicable indexes found");
            else if (Tracing.isTracing())
                // pay for an additional threadlocal get() rather than build the strings unnecessarily
                Tracing.trace("Candidate index mean cardinalities are {}. Scanning with {}.", FBUtilities.toString(candidates), indexManager.getIndexForColumn(best.column()).getIndexName());
        }
        return best;
    }

    /**
     * Post-process the result of an index query. This is done by the coordinator node after it has reconciled
     * the replica responses.
     *
     * @param command The {@code ReadCommand} use for the query.
     * @param result The index query results to be post-processed
     * @return The post-processed results
     */
    public PartitionIterator postReconciliationProcessing(RowFilter filter, PartitionIterator result)
    {
        return result;
    }
}

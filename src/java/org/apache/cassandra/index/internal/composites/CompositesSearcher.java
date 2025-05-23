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
package org.apache.cassandra.index.internal.composites;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadableView;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseablePeekingIterator;
import org.apache.cassandra.utils.btree.BTreeSet;


public class CompositesSearcher extends CassandraIndexSearcher<IndexEntry>
{
    public CompositesSearcher(ReadCommand command,
                              RowFilter.Expression expression,
                              CassandraIndex index)
    {
        super(command, expression, index);
    }

    @Override
    public MatchIndexer<IndexEntry> matchIndexer()
    {
        return new AbstractMatchIndexer<IndexEntry>()
        {
            @Override
            protected IndexEntry createMatch(DecoratedKey rowKey, Clustering<?> clustering, Cell<?> cell, LivenessInfo info)
            {
                return index.createIndexEntry(rowKey, clustering, cell, info);
            }
        };
    }

    @Override
    public MatchComparator<IndexEntry> matchComparator()
    {
        return (left, right, strict) -> IndexEntry.compare(index.getIndexCfs().metadata(), command.metadata(), left, right);
    }

    private boolean isMatchingEntry(DecoratedKey partitionKey, IndexEntry entry, ReadCommand command)
    {
        return command.selectsKey(partitionKey) && command.selectsClustering(partitionKey, entry.indexedEntryClustering);
    }

    private boolean isStaticColumn()
    {
        return index.getIndexedColumn().isStatic();
    }

    @Override
    public CloseablePeekingIterator<IndexEntry> matchIterator(ReadExecutionController executionController)
    {
        RowIterator indexHits = queryIndex(indexedKey, executionController);
        try
        {
            Preconditions.checkState(indexHits.staticRow() == Rows.EMPTY_STATIC_ROW);
            return new AbstractIterator<IndexEntry>()
            {
                @Override
                protected IndexEntry computeNext()
                {
                    while (indexHits.hasNext())
                    {
                        IndexEntry nextEntry = index.decodeEntry(indexedKey, indexHits.next());
                        DecoratedKey partitionKey = nextEntry.indexedKey;
                        if (!isMatchingEntry(partitionKey, nextEntry, command))
                            continue;

                        return nextEntry;
                    }
                    return endOfData();
                }

                @Override
                public void close()
                {
                    if (indexHits != null)
                        indexHits.close();
                }
            };
        }
        catch (Throwable e)
        {
            if (indexHits != null)
                indexHits.close();
            throw e;
        }
    }

    @Override
    public UnfilteredRowIterator queryNextMatches(ReadExecutionController executionController, DecoratedKey partitionKey, ReadableView view, PeekingIterator<IndexEntry> matches)
    {
        Preconditions.checkArgument(matches.hasNext());
        SinglePartitionReadCommand dataCmd;
        List<IndexEntry> entries = new ArrayList<>();
        if (isStaticColumn())
        {

            // If the index is on a static column, we just need to do a full read on the partition.
            // Note that we want to re-use the command.columnFilter() in case of future change.
            dataCmd = SinglePartitionReadCommand.create(index.baseCfs.metadata(),
                                                        command.nowInSec(),
                                                        command.columnFilter(),
                                                        RowFilter.none(),
                                                        DataLimits.NONE,
                                                        partitionKey,
                                                        command.clusteringIndexFilter(partitionKey));
            entries.add(matches.next());
        }
        else
        {
            // Gather all index hits belonging to the same partition and query the data for those hits.
            // TODO: it's much more efficient to do 1 read for all hits to the same partition than doing
            // 1 read per index hit. However, this basically mean materializing all hits for a partition
            // in memory so we should consider adding some paging mechanism. However, index hits should
            // be relatively small so it's much better than the previous code that was materializing all
            // *data* for a given partition.
            BTreeSet.Builder<Clustering<?>> clusterings = BTreeSet.builder(index.baseCfs.getComparator());
            while (matches.hasNext() && partitionKey.equals(matches.peek().indexedKey))
            {
                // We're queried a slice of the index, and some hits may not match some of the clustering column constraints,
                // but they will have been filtered out upstream
                IndexEntry nextEntry = matches.next();
                clusterings.add(nextEntry.indexedEntryClustering);
                entries.add(nextEntry);
            }

            // since non-matching entries will have been filtered out by matchIterator, it should not be possible to have empty clusterings
            Preconditions.checkArgument(!clusterings.isEmpty());

            // Query the gathered index hits. We still need to filter stale hits from the resulting query.
            ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings.build(), false);
            dataCmd = SinglePartitionReadCommand.create(index.baseCfs.metadata(),
                                                        command.nowInSec(),
                                                        command.columnFilter(),
                                                        command.rowFilter(),
                                                        DataLimits.NONE,
                                                        partitionKey,
                                                        filter,
                                                        (Index.QueryPlan) null);
        }

        // by the next caller of next, or through closing this iterator is this come before.
        return filterStaleEntries(dataCmd.queryMemtableAndDisk(view, index.baseCfs, executionController),
                                  indexedKey.getKey(),
                                  entries,
                                  executionController.getWriteContext(),
                                  command.nowInSec());
    }

    private void deleteAllEntries(final List<IndexEntry> entries, final WriteContext ctx, final long nowInSec)
    {
        entries.forEach(entry ->
            index.deleteStaleEntry(entry.indexValue,
                                   entry.indexClustering,
                                   DeletionTime.build(entry.timestamp, nowInSec),
                                   ctx));
    }

    // We assume all rows in dataIter belong to the same partition.
    private UnfilteredRowIterator filterStaleEntries(UnfilteredRowIterator dataIter,
                                                     final ByteBuffer indexValue,
                                                     final List<IndexEntry> entries,
                                                     final WriteContext ctx,
                                                     final long nowInSec)
    {
        // collect stale index entries and delete them when we close this iterator
        final List<IndexEntry> staleEntries = new ArrayList<>();

        // if there is a partition level delete in the base table, we need to filter
        // any index entries which would be shadowed by it
        if (!dataIter.partitionLevelDeletion().isLive())
        {
            DeletionTime deletion = dataIter.partitionLevelDeletion();
            entries.forEach(e -> {
                if (deletion.deletes(e.timestamp))
                    staleEntries.add(e);
            });
        }

        UnfilteredRowIterator iteratorToReturn = null;
        if (isStaticColumn())
        {
            if (entries.size() != 1)
                throw new AssertionError("A partition should have at most one index within a static column index");

            iteratorToReturn = dataIter;
            if (index.isStale(dataIter.staticRow(), indexValue, nowInSec))
            {
                // The entry is staled, we return no rows in this partition.
                staleEntries.addAll(entries);
                iteratorToReturn = UnfilteredRowIterators.noRowsIterator(dataIter.metadata(),
                                                                         dataIter.partitionKey(),
                                                                         Rows.EMPTY_STATIC_ROW,
                                                                         dataIter.partitionLevelDeletion(),
                                                                         dataIter.isReverseOrder());
            }
            deleteAllEntries(staleEntries, ctx, nowInSec);
        }
        else
        {
            ClusteringComparator comparator = dataIter.metadata().comparator;

            class Transform extends Transformation
            {
                private int entriesIdx;

                @Override
                public Row applyToRow(Row row)
                {
                    IndexEntry entry = findEntry(row.clustering());
                    if (!index.isStale(row, indexValue, nowInSec))
                        return row;

                    staleEntries.add(entry);
                    return null;
                }

                private IndexEntry findEntry(Clustering<?> clustering)
                {
                    assert entriesIdx < entries.size();
                    while (entriesIdx < entries.size())
                    {
                        IndexEntry entry = entries.get(entriesIdx++);
                        Clustering<?> indexedEntryClustering = entry.indexedEntryClustering;
                        // The entries are in clustering order. So that the requested entry should be the
                        // next entry, the one at 'entriesIdx'. However, we can have stale entries, entries
                        // that have no corresponding row in the base table typically because of a range
                        // tombstone or partition level deletion. Delete such stale entries.
                        // For static column, we only need to compare the partition key, otherwise we compare
                        // the whole clustering.
                        int cmp = comparator.compare(indexedEntryClustering, clustering);
                        assert cmp <= 0; // this would means entries are not in clustering order, which shouldn't happen
                        if (cmp == 0)
                            return entry;

                        // COMPACT COMPOSITE tables support null values in there clustering key but
                        // those tables do not support static columns. By consequence if a table
                        // has some static columns and all its clustering key elements are null
                        // it means that the partition exists and contains only static data
                        if (!dataIter.metadata().hasStaticColumns() || !containsOnlyNullValues(indexedEntryClustering))
                            staleEntries.add(entry);
                    }
                    // entries correspond to the rows we've queried, so we shouldn't have a row that has no corresponding entry.
                    throw new AssertionError();
                }

                private boolean containsOnlyNullValues(Clustering<?> indexedEntryClustering)
                {
                    int i = 0;
                    for (; i < indexedEntryClustering.size() && indexedEntryClustering.get(i) == null; i++);
                    return i == indexedEntryClustering.size();
                }

                @Override
                public void onPartitionClose()
                {
                    deleteAllEntries(staleEntries, ctx, nowInSec);
                }
            }
            iteratorToReturn = Transformation.apply(dataIter, new Transform());
        }

        return iteratorToReturn;
    }
}

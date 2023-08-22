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

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.utils.btree.BTreeSet;


public class CompositesSearcher extends CassandraIndexSearcher
{
    public CompositesSearcher(ReadCommand command,
                              RowFilter.Expression expression,
                              CassandraIndex index)
    {
        super(command, expression, index);
    }

    private boolean isMatchingEntry(DecoratedKey partitionKey, IndexEntry entry, ReadCommand command)
    {
        return command.selectsKey(partitionKey) && command.selectsClustering(partitionKey, entry.indexedEntryClustering);
    }

    private boolean isStaticColumn()
    {
        return index.getIndexedColumn().isStatic();
    }

    protected UnfilteredPartitionIterator queryDataFromIndex(final DecoratedKey indexKey,
                                                             final RowIterator indexHits,
                                                             final ReadCommand command,
                                                             final ReadExecutionController executionController)
    {
        assert indexHits.staticRow() == Rows.EMPTY_STATIC_ROW;

        return new UnfilteredPartitionIterator()
        {
            private IndexEntry nextEntry;

            private UnfilteredRowIterator next;

            public TableMetadata metadata()
            {
                return command.metadata();
            }

            public boolean hasNext()
            {
                return prepareNext();
            }

            public UnfilteredRowIterator next()
            {
                if (next == null)
                    prepareNext();

                UnfilteredRowIterator toReturn = next;
                next = null;
                return toReturn;
            }

            private boolean prepareNext()
            {
                while (true)
                {
                    if (next != null)
                        return true;

                    if (nextEntry == null)
                    {
                        if (!indexHits.hasNext())
                            return false;

                        nextEntry = index.decodeEntry(indexKey, indexHits.next());
                    }

                    SinglePartitionReadCommand dataCmd;
                    DecoratedKey partitionKey = index.baseCfs.decorateKey(nextEntry.indexedKey);
                    List<IndexEntry> entries = new ArrayList<>();
                    if (isStaticColumn())
                    {
                        // The index hit may not match the commad key constraint
                        if (!isMatchingEntry(partitionKey, nextEntry, command)) {
                            nextEntry = indexHits.hasNext() ? index.decodeEntry(indexKey, indexHits.next()) : null;
                            continue;
                        }

                        // If the index is on a static column, we just need to do a full read on the partition.
                        // Note that we want to re-use the command.columnFilter() in case of future change.
                        dataCmd = SinglePartitionReadCommand.create(index.baseCfs.metadata(),
                                                                    command.nowInSec(),
                                                                    command.columnFilter(),
                                                                    RowFilter.none(),
                                                                    DataLimits.NONE,
                                                                    partitionKey,
                                                                    command.clusteringIndexFilter(partitionKey));
                        entries.add(nextEntry);
                        nextEntry = indexHits.hasNext() ? index.decodeEntry(indexKey, indexHits.next()) : null;
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
                        while (nextEntry != null && partitionKey.getKey().equals(nextEntry.indexedKey))
                        {
                            // We're queried a slice of the index, but some hits may not match some of the clustering column constraints
                            if (isMatchingEntry(partitionKey, nextEntry, command))
                            {
                                clusterings.add(nextEntry.indexedEntryClustering);
                                entries.add(nextEntry);
                            }

                            nextEntry = indexHits.hasNext() ? index.decodeEntry(indexKey, indexHits.next()) : null;
                        }

                        // Because we've eliminated entries that don't match the clustering columns, it's possible we added nothing
                        if (clusterings.isEmpty())
                            continue;

                        // Query the gathered index hits. We still need to filter stale hits from the resulting query.
                        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings.build(), false);
                        dataCmd = SinglePartitionReadCommand.create(index.baseCfs.metadata(),
                                                                    command.nowInSec(),
                                                                    command.columnFilter(),
                                                                    command.rowFilter(),
                                                                    DataLimits.NONE,
                                                                    partitionKey,
                                                                    filter,
                                                                    null);
                    }

                    // by the next caller of next, or through closing this iterator is this come before.
                    UnfilteredRowIterator dataIter =
                        filterStaleEntries(dataCmd.queryMemtableAndDisk(index.baseCfs, executionController),
                                           indexKey.getKey(),
                                           entries,
                                           executionController.getWriteContext(),
                                           command.nowInSec());

                    if (dataIter.isEmpty())
                    {
                        dataIter.close();
                        continue;
                    }

                    next = dataIter;
                    return true;
                }
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public void close()
            {
                indexHits.close();
                if (next != null)
                    next.close();
            }
        };
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

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
package org.apache.cassandra.db.index.composites;

import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.*;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.concurrent.OpOrder;


public class CompositesSearcher extends SecondaryIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(CompositesSearcher.class);

    public CompositesSearcher(SecondaryIndexManager indexManager, Set<ColumnDefinition> columns)
    {
        super(indexManager, columns);
    }

    private boolean isMatchingEntry(DecoratedKey partitionKey, CompositesIndex.IndexedEntry entry, ReadCommand command)
    {
        return command.selects(partitionKey, entry.indexedEntryClustering);
    }

    protected UnfilteredPartitionIterator queryDataFromIndex(AbstractSimplePerColumnSecondaryIndex secondaryIdx,
                                                             final DecoratedKey indexKey,
                                                             final RowIterator indexHits,
                                                             final ReadCommand command,
                                                             final ReadOrderGroup orderGroup)
    {
        assert indexHits.staticRow() == Rows.EMPTY_STATIC_ROW;

        assert secondaryIdx instanceof CompositesIndex;
        final CompositesIndex index = (CompositesIndex)secondaryIdx;

        return new UnfilteredPartitionIterator()
        {
            private CompositesIndex.IndexedEntry nextEntry;

            private UnfilteredRowIterator next;

            public boolean isForThrift()
            {
                return command.isForThrift();
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
                if (next != null)
                    return true;

                if (nextEntry == null)
                {
                    if (!indexHits.hasNext())
                        return false;

                    nextEntry = index.decodeEntry(indexKey, indexHits.next());
                }

                // Gather all index hits belonging to the same partition and query the data for those hits.
                // TODO: it's much more efficient to do 1 read for all hits to the same partition than doing
                // 1 read per index hit. However, this basically mean materializing all hits for a partition
                // in memory so we should consider adding some paging mechanism. However, index hits should
                // be relatively small so it's much better than the previous code that was materializing all
                // *data* for a given partition.
                BTreeSet.Builder<Clustering> clusterings = BTreeSet.builder(baseCfs.getComparator());
                List<CompositesIndex.IndexedEntry> entries = new ArrayList<>();
                DecoratedKey partitionKey = baseCfs.partitioner.decorateKey(nextEntry.indexedKey);

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
                    return prepareNext();

                // Query the gathered index hits. We still need to filter stale hits from the resulting query.
                ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings.build(), false);
                SinglePartitionReadCommand dataCmd = new SinglePartitionNamesCommand(baseCfs.metadata,
                                                                                     command.nowInSec(),
                                                                                     command.columnFilter(),
                                                                                     command.rowFilter(),
                                                                                     DataLimits.NONE,
                                                                                     partitionKey,
                                                                                     filter);
                @SuppressWarnings("resource") // We close right away if empty, and if it's assign to next it will be called either
                                              // by the next caller of next, or through closing this iterator is this come before.
                UnfilteredRowIterator dataIter = filterStaleEntries(dataCmd.queryMemtableAndDisk(baseCfs, orderGroup.baseReadOpOrderGroup()),
                                                                    index,
                                                                    indexKey.getKey(),
                                                                    entries,
                                                                    orderGroup.writeOpOrderGroup(),
                                                                    command.nowInSec());
                if (dataIter.isEmpty())
                {
                    dataIter.close();
                    return prepareNext();
                }

                next = dataIter;
                return true;
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

    private UnfilteredRowIterator filterStaleEntries(UnfilteredRowIterator dataIter,
                                                     final CompositesIndex index,
                                                     final ByteBuffer indexValue,
                                                     final List<CompositesIndex.IndexedEntry> entries,
                                                     final OpOrder.Group writeOp,
                                                     final int nowInSec)
    {
        return new AlteringUnfilteredRowIterator(dataIter)
        {
            private int entriesIdx;

            @Override
            protected Row computeNext(Row row)
            {
                CompositesIndex.IndexedEntry entry = findEntry(row.clustering(), writeOp, nowInSec);
                if (!index.isStale(row, indexValue, nowInSec))
                    return row;

                // The entry is stale: delete the entry and ignore otherwise
                index.delete(entry, writeOp, nowInSec);
                return null;
            }

            private CompositesIndex.IndexedEntry findEntry(Clustering clustering, OpOrder.Group writeOp, int nowInSec)
            {
                assert entriesIdx < entries.size();
                while (entriesIdx < entries.size())
                {
                    CompositesIndex.IndexedEntry entry = entries.get(entriesIdx++);
                    // The entries are in clustering order. So that the requested entry should be the
                    // next entry, the one at 'entriesIdx'. However, we can have stale entries, entries
                    // that have no corresponding row in the base table typically because of a range
                    // tombstone or partition level deletion. Delete such stale entries.
                    int cmp = metadata().comparator.compare(entry.indexedEntryClustering, clustering);
                    assert cmp <= 0; // this would means entries are not in clustering order, which shouldn't happen
                    if (cmp == 0)
                        return entry;
                    else
                        index.delete(entry, writeOp, nowInSec);
                }
                // entries correspond to the rows we've queried, so we shouldn't have a row that has no corresponding entry.
                throw new AssertionError();
            }
        };
    }
}

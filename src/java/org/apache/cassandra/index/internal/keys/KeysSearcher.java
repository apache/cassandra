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
package org.apache.cassandra.index.internal.keys;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadableView;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseablePeekingIterator;

public class KeysSearcher extends CassandraIndexSearcher<IndexEntry>
{
    public KeysSearcher(ReadCommand command,
                        RowFilter.Expression expression,
                        CassandraIndex indexer)
    {
        super(command, expression, indexer);
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
                        Row hit = indexHits.next();
                        DecoratedKey key = index.baseCfs.decorateKey(hit.clustering().bufferAt(0));
                        if (!command.selectsKey(key))
                            continue;

                        return new IndexEntry(indexedKey, hit.clustering(), hit.primaryKeyLivenessInfo().timestamp(), key, Clustering.EMPTY);
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
    public UnfilteredRowIterator queryNextMatches(ReadExecutionController executionController, DecoratedKey key, ReadableView view, PeekingIterator<IndexEntry> matches)
    {
        Preconditions.checkArgument(matches.hasNext());

        IndexEntry entry = matches.next();

        ColumnFilter extendedFilter = getExtendedFilter(command.columnFilter());
        SinglePartitionReadCommand dataCmd = SinglePartitionReadCommand.create(index.baseCfs.metadata(),
                                                                               command.nowInSec(),
                                                                               extendedFilter,
                                                                               command.rowFilter(),
                                                                               DataLimits.NONE,
                                                                               key,
                                                                               command.clusteringIndexFilter(key));

        // Otherwise, we close right away if empty, and if it's assigned to next it will be called either
        // by the next caller of next, or through closing this iterator is this come before.
        return filterIfStale(dataCmd.queryMemtableAndDisk(index.baseCfs, executionController),
                             entry.timestamp,
                             indexedKey.getKey(),
                             executionController.getWriteContext(),
                             command.nowInSec());
    }

    private ColumnFilter getExtendedFilter(ColumnFilter initialFilter)
    {
        if (command.columnFilter().fetches(index.getIndexedColumn()))
            return initialFilter;

        ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
        builder.addAll(initialFilter.fetchedColumns());
        builder.add(index.getIndexedColumn());
        return builder.build();
    }

    private UnfilteredRowIterator filterIfStale(UnfilteredRowIterator iterator,
                                                long timestamp,
                                                ByteBuffer indexedValue,
                                                WriteContext ctx,
                                                long nowInSec)
    {
        Row data = iterator.staticRow();
        if (index.isStale(data, indexedValue, nowInSec))
        {
            // Index is stale, remove the index entry and ignore
            index.deleteStaleEntry(index.getIndexCfs().decorateKey(indexedValue),
                                   makeIndexClustering(iterator.partitionKey().getKey(), Clustering.EMPTY),
                                   DeletionTime.build(timestamp, nowInSec),
                                   ctx);
            iterator.close();
            return null;
        }
        else
        {
            return iterator;
        }
    }
}

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class KeysSearcher extends CassandraIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(KeysSearcher.class);

    public KeysSearcher(ReadCommand command,
                        RowFilter.Expression expression,
                        CassandraIndex indexer)
    {
        super(command, expression, indexer);
    }

    protected UnfilteredPartitionIterator queryDataFromIndex(final DecoratedKey indexKey,
                                                             final RowIterator indexHits,
                                                             final ReadCommand command,
                                                             final ReadExecutionController executionController)
    {
        assert indexHits.staticRow() == Rows.EMPTY_STATIC_ROW;

        return new UnfilteredPartitionIterator()
        {
            private UnfilteredRowIterator next;

            public boolean isForThrift()
            {
                return command.isForThrift();
            }

            public CFMetaData metadata()
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
                while (next == null && indexHits.hasNext())
                {
                    Row hit = indexHits.next();
                    DecoratedKey key = index.baseCfs.decorateKey(hit.clustering().get(0));
                    if (!command.selectsKey(key))
                        continue;

                    ColumnFilter extendedFilter = getExtendedFilter(command.columnFilter());
                    SinglePartitionReadCommand dataCmd = SinglePartitionReadCommand.create(isForThrift(),
                                                                                           index.baseCfs.metadata,
                                                                                           command.nowInSec(),
                                                                                           extendedFilter,
                                                                                           command.rowFilter(),
                                                                                           DataLimits.NONE,
                                                                                           key,
                                                                                           command.clusteringIndexFilter(key),
                                                                                           null);

                    @SuppressWarnings("resource") // filterIfStale closes it's iterator if either it materialize it or if it returns null.
                                                  // Otherwise, we close right away if empty, and if it's assigned to next it will be called either
                                                  // by the next caller of next, or through closing this iterator is this come before.
                    UnfilteredRowIterator dataIter = filterIfStale(dataCmd.queryMemtableAndDisk(index.baseCfs, executionController),
                                                                   hit,
                                                                   indexKey.getKey(),
                                                                   executionController.writeOpOrderGroup(),
                                                                   isForThrift(),
                                                                   command.nowInSec());

                    if (dataIter != null)
                    {
                        if (dataIter.isEmpty())
                            dataIter.close();
                        else
                            next = dataIter;
                    }
                }
                return next != null;
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
                                                Row indexHit,
                                                ByteBuffer indexedValue,
                                                OpOrder.Group writeOp,
                                                boolean isForThrift,
                                                int nowInSec)
    {
        if (isForThrift)
        {
            // The data we got has gone though ThrifResultsMerger, so we're looking for the row whose clustering
            // is the indexed name and so we need to materialize the partition.
            ImmutableBTreePartition result = ImmutableBTreePartition.create(iterator);
            iterator.close();
            Row data = result.getRow(Clustering.make(index.getIndexedColumn().name.bytes));
            if (data == null)
                return null;

            // for thrift tables, we need to compare the index entry against the compact value column,
            // not the column actually designated as the indexed column so we don't use the index function
            // lib for the staleness check like we do in every other case
            Cell baseData = data.getCell(index.baseCfs.metadata.compactValueColumn());
            if (baseData == null || !baseData.isLive(nowInSec) || index.getIndexedColumn().type.compare(indexedValue, baseData.value()) != 0)
            {
                // Index is stale, remove the index entry and ignore
                index.deleteStaleEntry(index.getIndexCfs().decorateKey(indexedValue),
                                         Clustering.make(index.getIndexedColumn().name.bytes),
                                         new DeletionTime(indexHit.primaryKeyLivenessInfo().timestamp(), nowInSec),
                                         writeOp);
                return null;
            }
            else
            {
                if (command.columnFilter().fetches(index.getIndexedColumn()))
                    return result.unfilteredIterator();

                // The query on the base table used an extended column filter to ensure that the
                // indexed column was actually read for use in the staleness check, before
                // returning the results we must filter the base table partition so that it
                // contains only the originally requested columns. See CASSANDRA-11523
                ClusteringComparator comparator = result.metadata().comparator;
                Slices.Builder slices = new Slices.Builder(comparator);
                for (ColumnDefinition selected : command.columnFilter().fetchedColumns())
                    slices.add(Slice.make(comparator, selected.name.bytes));
                return result.unfilteredIterator(ColumnFilter.all(command.metadata()), slices.build(), false);
            }
        }
        else
        {
            if (!iterator.metadata().isCompactTable())
            {
                logger.warn("Non-composite index was used on the table '{}' during the query. Starting from Cassandra 4.0, only " +
                            "composite indexes will be supported. If compact flags were dropped for this table, drop and re-create " +
                            "the index.", iterator.metadata().cfName);
            }

            Row data = iterator.staticRow();
            if (index.isStale(data, indexedValue, nowInSec))
            {
                // Index is stale, remove the index entry and ignore
                index.deleteStaleEntry(index.getIndexCfs().decorateKey(indexedValue),
                                         makeIndexClustering(iterator.partitionKey().getKey(), Clustering.EMPTY),
                                         new DeletionTime(indexHit.primaryKeyLivenessInfo().timestamp(), nowInSec),
                                         writeOp);
                iterator.close();
                return null;
            }
            else
            {
                return iterator;
            }
        }
    }
}

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
package org.apache.cassandra.index.sasi.plan;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.index.sasi.disk.*;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.plan.Operation.*;
import org.apache.cassandra.utils.btree.*;

public class QueryPlan
{
    private final QueryController controller;

    public QueryPlan(ColumnFamilyStore cfs, ReadCommand command, long executionQuotaMs)
    {
        this.controller = new QueryController(cfs, (PartitionRangeReadCommand) command, executionQuotaMs);
    }

    /**
     * Converts expressions into operation tree (which is currently just a single AND).
     *
     * Operation tree allows us to do a couple of important optimizations
     * namely, group flattening for AND operations (query rewrite), expression bounds checks,
     * "satisfies by" checks for resulting rows with an early exit.
     *
     * @return root of the operations tree.
     */
    private Operation analyze()
    {
        try
        {
            Operation.Builder and = new Operation.Builder(OperationType.AND, controller);
            controller.getExpressions().forEach(and::add);
            return and.complete();
        }
        catch (Exception | Error e)
        {
            controller.finish();
            throw e;
        }
    }

    public UnfilteredPartitionIterator execute(ReadExecutionController executionController) throws RequestTimeoutException
    {
        return new ResultIterator(analyze(), controller, executionController);
    }

    private static class ResultIterator implements UnfilteredPartitionIterator
    {
        private final AbstractBounds<PartitionPosition> keyRange;
        private final Operation operationTree;
        private final QueryController controller;
        private final ReadExecutionController executionController;

        private Iterator<RowKey> currentKeys = null;
        private UnfilteredRowIterator nextPartition = null;
        private DecoratedKey lastPartitionKey = null;

        public ResultIterator(Operation operationTree, QueryController controller, ReadExecutionController executionController)
        {
            this.keyRange = controller.dataRange().keyRange();
            this.operationTree = operationTree;
            this.controller = controller;
            this.executionController = executionController;
            if (operationTree != null)
                operationTree.skipTo((Long) keyRange.left.getToken().getTokenValue());
        }

        public boolean hasNext()
        {
            return prepareNext();
        }

        public UnfilteredRowIterator next()
        {
            if (nextPartition == null)
                prepareNext();

            UnfilteredRowIterator toReturn = nextPartition;
            nextPartition = null;
            return toReturn;
        }

        private boolean prepareNext()
        {
            if (operationTree == null)
                return false;

            if (nextPartition != null)
                nextPartition.close();

            for (;;)
            {
                if (currentKeys == null || !currentKeys.hasNext())
                {
                    if (!operationTree.hasNext())
                        return false;

                    Token token = operationTree.next();
                    currentKeys = token.iterator();
                }

                CFMetaData metadata = controller.metadata();
                BTreeSet.Builder<Clustering> clusterings = BTreeSet.builder(metadata.comparator);
                // results have static clustering, the whole partition has to be read
                boolean fetchWholePartition = false;

                while (true)
                {
                    if (!currentKeys.hasNext())
                    {
                        // No more keys for this token.
                        // If no clusterings were collected yet, exit this inner loop so the operation
                        // tree iterator can move on to the next token.
                        // If some clusterings were collected, build an iterator for those rows
                        // and return.
                        if ((clusterings.isEmpty() && !fetchWholePartition) || lastPartitionKey == null)
                            break;

                        UnfilteredRowIterator partition = fetchPartition(lastPartitionKey, clusterings.build(), fetchWholePartition);
                        // Prepare for next partition, reset partition key and clusterings
                        lastPartitionKey = null;
                        clusterings = BTreeSet.builder(metadata.comparator);

                        if (partition.isEmpty())
                        {
                            partition.close();
                            continue;
                        }

                        nextPartition = partition;
                        return true;
                    }

                    RowKey fullKey = currentKeys.next();
                    DecoratedKey key = fullKey.decoratedKey;

                    if (!keyRange.right.isMinimum() && keyRange.right.compareTo(key) < 0)
                        return false;

                    if (lastPartitionKey != null && metadata.getKeyValidator().compare(lastPartitionKey.getKey(), key.getKey()) != 0)
                    {
                        UnfilteredRowIterator partition = fetchPartition(lastPartitionKey, clusterings.build(), fetchWholePartition);

                        if (partition.isEmpty())
                            partition.close();
                        else
                        {
                            nextPartition = partition;
                            return true;
                        }
                    }

                    lastPartitionKey = key;

                    // We fetch whole partition for versions before AC and in case static column index is queried in AC
                    if (fullKey.clustering == null || fullKey.clustering.clustering().kind() == ClusteringPrefix.Kind.STATIC_CLUSTERING)
                        fetchWholePartition = true;
                    else
                        clusterings.add(fullKey.clustering);

                }
            }
        }

        private UnfilteredRowIterator fetchPartition(DecoratedKey key, NavigableSet<Clustering> clusterings, boolean fetchWholePartition)
        {
            if (fetchWholePartition)
                clusterings = null;

            try (UnfilteredRowIterator partition = controller.getPartition(key, clusterings, executionController))
            {
                Row staticRow = partition.staticRow();
                List<Unfiltered> clusters = new ArrayList<>();

                while (partition.hasNext())
                {
                    Unfiltered row = partition.next();
                    if (operationTree.satisfiedBy(row, staticRow, true))
                        clusters.add(row);
                }

                if (!clusters.isEmpty())
                    return new PartitionIterator(partition, clusters);
                else
                    return UnfilteredRowIterators.noRowsIterator(partition.metadata(),
                                                                 partition.partitionKey(),
                                                                 Rows.EMPTY_STATIC_ROW,
                                                                 partition.partitionLevelDeletion(),
                                                                 partition.isReverseOrder());
            }
        }

        public void close()
        {
            if (nextPartition != null)
                nextPartition.close();
        }

        public boolean isForThrift()
        {
            return controller.isForThrift();
        }

        public CFMetaData metadata()
        {
            return controller.metadata();
        }

        private static class PartitionIterator extends AbstractUnfilteredRowIterator
        {
            private final Iterator<Unfiltered> rows;

            public PartitionIterator(UnfilteredRowIterator partition, Collection<Unfiltered> filteredRows)
            {
                super(partition.metadata(),
                      partition.partitionKey(),
                      partition.partitionLevelDeletion(),
                      partition.columns(),
                      partition.staticRow(),
                      partition.isReverseOrder(),
                      partition.stats());

                rows = filteredRows.iterator();
            }

            @Override
            protected Unfiltered computeNext()
            {
                return rows.hasNext() ? rows.next() : endOfData();
            }
        }
    }
}

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

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.plan.Operation.OperationType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;

public class SASIIndexSearcher implements Index.Searcher
{
    private final ReadCommand command;
    private final QueryController controller;

    public SASIIndexSearcher(ColumnFamilyStore cfs, ReadCommand command, long executionQuotaMs)
    {
        this.command = command;
        this.controller = new QueryController(cfs, (PartitionRangeReadCommand) command, executionQuotaMs);
    }

    @Override
    public ReadCommand command()
    {
        return command;
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

    @Override
    public UnfilteredPartitionIterator search(ReadExecutionController executionController)
    {
        return new ResultIterator(analyze(), controller, executionController);
    }

    private static class ResultIterator extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        private final AbstractBounds<PartitionPosition> keyRange;
        private final Operation operationTree;
        private final QueryController controller;
        private final ReadExecutionController executionController;

        private Iterator<DecoratedKey> currentKeys = null;

        public ResultIterator(Operation operationTree, QueryController controller, ReadExecutionController executionController)
        {
            this.keyRange = controller.dataRange().keyRange();
            this.operationTree = operationTree;
            this.controller = controller;
            this.executionController = executionController;
            if (operationTree != null)
                operationTree.skipTo((Long) keyRange.left.getToken().getTokenValue());
        }

        protected UnfilteredRowIterator computeNext()
        {
            if (operationTree == null)
                return endOfData();

            for (;;)
            {
                if (currentKeys == null || !currentKeys.hasNext())
                {
                    if (!operationTree.hasNext())
                         return endOfData();

                    Token token = operationTree.next();
                    currentKeys = token.iterator();
                }

                while (currentKeys.hasNext())
                {
                    DecoratedKey key = currentKeys.next();

                    if (!keyRange.right.isMinimum() && keyRange.right.compareTo(key) < 0)
                        return endOfData();

                    if (!keyRange.inclusiveLeft() && key.compareTo(keyRange.left) == 0)
                        continue;

                    try (UnfilteredRowIterator partition = controller.getPartition(key, executionController))
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
                    }
                }
            }
        }

        private static class PartitionIterator extends AbstractUnfilteredRowIterator
        {
            private final Iterator<Unfiltered> rows;

            public PartitionIterator(UnfilteredRowIterator partition, Collection<Unfiltered> content)
            {
                super(partition.metadata(),
                      partition.partitionKey(),
                      partition.partitionLevelDeletion(),
                      partition.columns(),
                      partition.staticRow(),
                      partition.isReverseOrder(),
                      partition.stats());

                rows = content.iterator();
            }

            @Override
            protected Unfiltered computeNext()
            {
                return rows.hasNext() ? rows.next() : endOfData();
            }
        }

        public TableMetadata metadata()
        {
            return controller.metadata();
        }

        public void close()
        {
            FileUtils.closeQuietly(operationTree);
            controller.finish();
        }
    }
}

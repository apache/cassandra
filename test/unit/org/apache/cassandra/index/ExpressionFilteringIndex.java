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

package org.apache.cassandra.index;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;


/**
 * An {@link Index} that selects rows whose indexed column value is equals to the requested custom expression.
 * The implementation relies only on {@link #customExpressionFor(TableMetadata, ByteBuffer)}, while the searcher
 * returns all the rows satisfying the key range.
 */
public final class ExpressionFilteringIndex extends StubIndex
{
    private final TableMetadata table;
    private final ColumnMetadata column;
    public final AtomicInteger searches = new AtomicInteger(0);

    public ExpressionFilteringIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
    {
        super(baseCfs, metadata);
        this.table = baseCfs.metadata();
        String columnName = metadata.options.get(IndexTarget.TARGET_OPTION_NAME);
        assert columnName != null;
        column = table.getColumn(UTF8Type.instance.decompose(columnName));
    }

    @Override
    public AbstractType<?> customExpressionValueType()
    {
        return Int32Type.instance;
    }

    @Override
    public RowFilter.CustomExpression customExpressionFor(TableMetadata cfm, ByteBuffer value)
    {
        return new RowFilter.CustomExpression(cfm, getIndexMetadata(), value)
        {
            @Override
            public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row)
            {
                Cell cell = row.getCell(ExpressionFilteringIndex.this.column);
                return cell != null && ByteBufferUtil.compareUnsigned(cell.buffer(), value) == 0;
            }
        };
    }

    @Override
    public Searcher searcherFor(ReadCommand command)
    {
        return new Searcher(command)
        {
            @Override
            public ReadCommand command()
            {
                return command;
            }

            @Override
            public UnfilteredPartitionIterator search(ReadExecutionController executionController)
            {
                searches.incrementAndGet();

                ReadCommand all;
                if (command instanceof SinglePartitionReadCommand)
                {
                    SinglePartitionReadCommand cmd = (SinglePartitionReadCommand) command;
                    all = SinglePartitionReadCommand.create(table,
                                                            cmd.nowInSec(),
                                                            cmd.partitionKey(),
                                                            cmd.clusteringIndexFilter().getSlices(cmd.metadata()));
                }
                else if (command instanceof PartitionRangeReadCommand)
                {
                    PartitionRangeReadCommand cmd = (PartitionRangeReadCommand) command;
                    all = PartitionRangeReadCommand.create(table,
                                                           cmd.nowInSec(),
                                                           ColumnFilter.all(table),
                                                           RowFilter.NONE,
                                                           DataLimits.NONE,
                                                           cmd.dataRange());
                }
                else
                {
                    throw new UnsupportedOperationException();
                }
                return all.executeLocally(ReadExecutionController.empty());
            }
        };
    }
}

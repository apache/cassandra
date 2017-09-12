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
package org.apache.cassandra.db;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;

/**
 * Base class for the {@code ReadQuery} implementations use to query virtual tables.
 */
public abstract class VirtualTableReadQuery extends AbstractReadQuery
{
    protected VirtualTableReadQuery(TableMetadata metadata,
                                    int nowInSec,
                                    ColumnFilter columnFilter,
                                    RowFilter rowFilter,
                                    DataLimits limits)
    {
        super(metadata, nowInSec, columnFilter, rowFilter, limits);
    }

    @Override
    public ReadExecutionController executionController()
    {
        return ReadExecutionController.empty();
    }

    @Override
    public PartitionIterator execute(ConsistencyLevel consistency,
                                     ClientState clientState,
                                     long queryStartNanoTime) throws RequestExecutionException
    {
        return executeInternal(executionController());
    }

    @Override
    @SuppressWarnings("resource")
    public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController)
    {
        UnfilteredPartitionIterator resultIterator = queryVirtualTable();
        return limits().filter(rowFilter().filter(resultIterator, nowInSec()), nowInSec(), selectsFullPartition());
    }

    protected abstract UnfilteredPartitionIterator queryVirtualTable();
}

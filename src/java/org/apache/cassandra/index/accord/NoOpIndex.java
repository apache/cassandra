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

package org.apache.cassandra.index.accord;

import java.util.Optional;
import java.util.concurrent.Callable;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.ClientState;

public class NoOpIndex implements Index
{
    final IndexMetadata indexMetadata;
    public NoOpIndex(IndexMetadata indexMetadata)
    {
        this.indexMetadata = indexMetadata;
    }

    @Override
    public IndexMetadata getIndexMetadata()
    {
        return indexMetadata;
    }

    @Override
    public boolean shouldBuildBlocking()
    {
        return false;
    }

    @Override
    public Optional<ColumnFamilyStore> getBackingTable()
    {
        return Optional.empty();
    }

    @Override
    public Callable<?> getInitializationTask()
    {
        return null;
    }

    @Override
    public Callable<?> getTruncateTask(long truncatedAt)
    {
        return null;
    }

    @Override
    public Callable<?> getBlockingFlushTask()
    {
        return null;
    }

    @Override
    public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata)
    {
        return null;
    }

    @Override
    public void register(IndexRegistry registry)
    {
        registry.registerIndex(this);
    }

    @Override
    public Callable<?> getInvalidateTask()
    {
        return null;
    }

    @Override
    public void validate(PartitionUpdate update, ClientState state) throws InvalidRequestException
    {
    }

    @Override
    public Indexer indexerFor(DecoratedKey key,
                              RegularAndStaticColumns columns,
                              long nowInSec,
                              WriteContext ctx,
                              IndexTransaction.Type transactionType,
                              Memtable memtable)
    {
        return null;
    }

    @Override
    public boolean supportsExpression(ColumnMetadata column, Operator operator)
    {
        return false;
    }

    @Override
    public RowFilter getPostIndexQueryFilter(RowFilter filter)
    {
        return RowFilter.none();
    }

    @Override
    public Searcher searcherFor(ReadCommand command)
    {
        return null;
    }

    @Override
    public boolean dependsOn(ColumnMetadata column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AbstractType<?> customExpressionValueType()
    {
        return UTF8Type.instance;
    }

    @Override
    public long getEstimatedResultRows()
    {
        throw new UnsupportedOperationException();
    }
}

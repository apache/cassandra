/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.cassandra.index;

import java.util.Set;
import java.util.function.Predicate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.schema.TableMetadata;

/**
 * An {@link Index.Group} containing a single {@link Index}, to which it just delegates the calls.
 */
public class SingletonIndexGroup implements Index.Group
{
    private volatile Index delegate;
    private final Set<Index> indexes = Sets.newConcurrentHashSet();

    protected SingletonIndexGroup()
    {
    }

    @Override
    public Set<Index> getIndexes()
    {
        return indexes;
    }

    public Index getIndex()
    {
        return delegate;
    }

    @Override
    public void addIndex(Index index)
    {
        Preconditions.checkState(delegate == null);
        delegate = index;
        indexes.add(index);
    }

    @Override
    public void removeIndex(Index index)
    {
        Preconditions.checkState(containsIndex(index));
        delegate = null;
        indexes.clear();
    }

    @Override
    public boolean containsIndex(Index index)
    {
        return index.equals(delegate);
    }

    @Override
    public Index.Indexer indexerFor(Predicate<Index> indexSelector,
                                    DecoratedKey key,
                                    RegularAndStaticColumns columns,
                                    long nowInSec,
                                    WriteContext ctx,
                                    IndexTransaction.Type transactionType,
                                    Memtable memtable)
    {
        Preconditions.checkNotNull(delegate);
        return indexSelector.test(delegate) ? delegate.indexerFor(key, columns, nowInSec, ctx, transactionType, memtable) : null;
    }

    @Override
    public Index.QueryPlan queryPlanFor(RowFilter rowFilter)
    {
        Preconditions.checkNotNull(delegate);
        return SingletonIndexQueryPlan.create(delegate, rowFilter);
    }

    @Override
    public SSTableFlushObserver getFlushObserver(Descriptor descriptor, LifecycleNewTracker tracker, TableMetadata tableMetadata)
    {
        Preconditions.checkNotNull(delegate);
        return delegate.getFlushObserver(descriptor, tracker);
    }

    @Override
    public Set<Component> getComponents()
    {
        Preconditions.checkNotNull(delegate);
        return delegate.getComponents();
    }
}

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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nullable;

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
 * Basic custom index group implementation for testing.
 */
public class StubIndexGroup implements Index.Group
{
    private final Set<Index> indexes = new HashSet<>();

    @Override
    public Set<Index> getIndexes()
    {
        return indexes;
    }

    @Override
    public void addIndex(Index index)
    {
        indexes.add(index);
    }

    @Override
    public void removeIndex(Index index)
    {
        indexes.remove(index);
    }

    @Override
    public boolean containsIndex(Index index)
    {
        return indexes.contains(index);
    }

    @Override
    public boolean isSingleton()
    {
        return false;
    }

    @Override
    public Index.Indexer indexerFor(Predicate<Index> indexSelector,
                                    DecoratedKey key,
                                    RegularAndStaticColumns columns,
                                    long nowInSec,
                                    WriteContext context,
                                    IndexTransaction.Type transactionType,
                                    Memtable memtable)
    {
        return null;
    }

    @Nullable
    @Override
    public Index.QueryPlan queryPlanFor(RowFilter rowFilter)
    {
        return null;
    }

    @Override
    public SSTableFlushObserver getFlushObserver(Descriptor descriptor, LifecycleNewTracker tracker, TableMetadata tableMetadata)
    {
        return null;
    }

    public Set<Component> getComponents()
    {
        return Collections.emptySet();
    }
}
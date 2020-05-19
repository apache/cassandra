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

import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;

public class SingletonIndexQueryPlan implements Index.QueryPlan
{
    private final Index index;
    private final Set<Index> indexes;
    private final RowFilter postIndexFilter;

    protected SingletonIndexQueryPlan(Index index, RowFilter postIndexFilter)
    {
        this.index = index;
        this.indexes = Collections.singleton(index);
        this.postIndexFilter = postIndexFilter;
    }

    @Nullable
    protected static SingletonIndexQueryPlan create(Index index, RowFilter rowFilter)
    {
        for (RowFilter.Expression e : rowFilter.getExpressions())
        {
            if (index.supportsExpression(e.column(), e.operator()))
                return new SingletonIndexQueryPlan(index, index.getPostIndexQueryFilter(rowFilter));
        }

        return null;
    }

    @Override
    public Set<Index> getIndexes()
    {
        return indexes;
    }

    @Override
    @Nonnull
    public Index getFirst()
    {
        return index;
    }

    @Override
    public long getEstimatedResultRows()
    {
        return index.getEstimatedResultRows();
    }

    @Override
    public Index.Searcher searcherFor(ReadCommand command)
    {
        return index.searcherFor(command);
    }

    @Override
    public RowFilter postIndexQueryFilter()
    {
        return postIndexFilter;
    }
}

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
package org.apache.cassandra.cql3.restrictions;

import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * A <code>RestrictionSet</code> wrapper that can be extended to allow to modify the <code>RestrictionSet</code>
 * behaviour without breaking its immutability. Subclasses should be immutable.
 */
class RestrictionSetWrapper implements Restrictions
{
    /**
     * The wrapped <code>RestrictionSet</code>.
     */
    protected final RestrictionSet restrictions;

    public RestrictionSetWrapper(RestrictionSet restrictions)
    {
        this.restrictions = restrictions;
    }

    public void addToRowFilter(RowFilter filter,
                               IndexRegistry indexRegistry,
                               QueryOptions options)
    {
        restrictions.addToRowFilter(filter, indexRegistry, options);
    }

    public List<ColumnMetadata> columns()
    {
        return restrictions.columns();
    }

    public void addFunctionsTo(List<Function> functions)
    {
        restrictions.addFunctionsTo(functions);
    }

    @Override
    public boolean isRestrictedByEquals(ColumnMetadata column)
    {
        return restrictions.isRestrictedByEquals(column);
    }

    @Override
    public boolean isRestrictedByEqualsOrIN(ColumnMetadata column)
    {
        return restrictions.isRestrictedByEqualsOrIN(column);
    }

    public boolean isEmpty()
    {
        return restrictions.isEmpty();
    }

    public int size()
    {
        return restrictions.size();
    }

    public boolean hasSupportingIndex(IndexRegistry indexRegistry)
    {
        return restrictions.hasSupportingIndex(indexRegistry);
    }

    @Override
    public Index findSupportingIndex(Iterable<Index> indexes)
    {
        return restrictions.findSupportingIndex(indexes);
    }

    @Override
    public boolean needsFiltering(Index.Group indexGroup)
    {
        return restrictions.needsFiltering(indexGroup);
    }

    @Override
    public boolean needsFilteringOrIndexing()
    {
        return restrictions.needsFilteringOrIndexing();
    }

    public ColumnMetadata firstColumn()
    {
        return restrictions.firstColumn();
    }

    public ColumnMetadata lastColumn()
    {
        return restrictions.lastColumn();
    }

    public boolean hasSlice()
    {
        return restrictions.hasSlice();
    }

    @Override
    public boolean hasIN()
    {
        return restrictions.hasIN();
    }

    public boolean hasOnlyEqualityRestrictions()
    {
        for (ColumnMetadata column : columns())
        {
            if (!isRestrictedByEqualsOrIN(column))
                return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}

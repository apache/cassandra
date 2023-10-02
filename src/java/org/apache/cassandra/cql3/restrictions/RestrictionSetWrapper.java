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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.IndexRegistry;

/**
 * A <code>RestrictionSet</code> wrapper that can be extended to allow to modify the <code>RestrictionSet</code>
 * behaviour without breaking its immutability. Sub-classes should be immutables.
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

    public List<ColumnMetadata> getColumnDefs()
    {
        return restrictions.getColumnDefs();
    }

    @Override
    public Collection<ColumnMetadata> getColumnDefinitions()
    {
        return restrictions.getColumnDefinitions();
    }

    public RestrictionSet getRestrictionSet()
    {
        return restrictions;
    }

    public void addFunctionsTo(List<Function> functions)
    {
        restrictions.addFunctionsTo(functions);
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
    public Index findSupportingIndex(IndexRegistry indexRegistry)
    {
        return restrictions.findSupportingIndex(indexRegistry);
    }

    @Override
    public Index findSupportingIndexFromQueryPlan(Index.QueryPlan indexQueryPlan)
    {
        return restrictions.findSupportingIndexFromQueryPlan(indexQueryPlan);
    }

    @Override
    public boolean needsFiltering(Index.Group indexGroup)
    {
        return restrictions.needsFiltering(indexGroup);
    }

    public ColumnMetadata getFirstColumn()
    {
        return restrictions.getFirstColumn();
    }

    public ColumnMetadata getLastColumn()
    {
        return restrictions.getLastColumn();
    }

    public boolean hasIN()
    {
        return restrictions.hasIN();
    }

    public boolean hasContains()
    {
        return restrictions.hasContains();
    }

    public boolean hasSlice()
    {
        return restrictions.hasSlice();
    }

    public boolean hasOnlyEqualityRestrictions()
    {
        return restrictions.hasOnlyEqualityRestrictions();
    }

    public Set<Restriction> getRestrictions(ColumnMetadata columnDef)
    {
        return restrictions.getRestrictions(columnDef);
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}

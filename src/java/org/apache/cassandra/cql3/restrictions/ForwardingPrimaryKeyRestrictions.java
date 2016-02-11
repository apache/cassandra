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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.NavigableSet;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.SecondaryIndexManager;

/**
 * A <code>PrimaryKeyRestrictions</code> which forwards all its method calls to another 
 * <code>PrimaryKeyRestrictions</code>. Subclasses should override one or more methods to modify the behavior 
 * of the backing <code>PrimaryKeyRestrictions</code> as desired per the decorator pattern. 
 */
abstract class ForwardingPrimaryKeyRestrictions implements PrimaryKeyRestrictions
{
    /**
     * Returns the backing delegate instance that methods are forwarded to.
     * @return the backing delegate instance that methods are forwarded to.
     */
    protected abstract PrimaryKeyRestrictions getDelegate();

    @Override
    public Iterable<Function> getFunctions()
    {
        return getDelegate().getFunctions();
    }

    @Override
    public List<ColumnDefinition> getColumnDefs()
    {
        return getDelegate().getColumnDefs();
    }

    @Override
    public ColumnDefinition getFirstColumn()
    {
        return getDelegate().getFirstColumn();
    }

    @Override
    public ColumnDefinition getLastColumn()
    {
        return getDelegate().getLastColumn();
    }

    @Override
    public PrimaryKeyRestrictions mergeWith(Restriction restriction) throws InvalidRequestException
    {
        return getDelegate().mergeWith(restriction);
    }

    @Override
    public boolean hasSupportingIndex(SecondaryIndexManager secondaryIndexManager)
    {
        return getDelegate().hasSupportingIndex(secondaryIndexManager);
    }

    @Override
    public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
    {
        return getDelegate().values(options);
    }

    @Override
    public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
    {
        return getDelegate().appendTo(builder, options);
    }

    @Override
    public NavigableSet<Clustering> valuesAsClustering(QueryOptions options) throws InvalidRequestException
    {
        return getDelegate().valuesAsClustering(options);
    }

    @Override
    public List<ByteBuffer> bounds(Bound bound, QueryOptions options) throws InvalidRequestException
    {
        return getDelegate().bounds(bound, options);
    }

    @Override
    public NavigableSet<Slice.Bound> boundsAsClustering(Bound bound, QueryOptions options) throws InvalidRequestException
    {
        return getDelegate().boundsAsClustering(bound, options);
    }

    @Override
    public MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options)
    {
        return getDelegate().appendBoundTo(builder, bound, options);
    }

    @Override
    public boolean isInclusive(Bound bound)
    {
        return getDelegate().isInclusive(bound.reverse());
    }

    @Override
    public boolean isEmpty()
    {
        return getDelegate().isEmpty();
    }

    @Override
    public int size()
    {
        return getDelegate().size();
    }

    @Override
    public boolean isOnToken()
    {
        return getDelegate().isOnToken();
    }

    @Override
    public boolean isSlice()
    {
        return getDelegate().isSlice();
    }

    @Override
    public boolean isEQ()
    {
        return getDelegate().isEQ();
    }

    @Override
    public boolean isLIKE()
    {
        return getDelegate().isLIKE();
    }

    @Override
    public boolean isIN()
    {
        return getDelegate().isIN();
    }

    @Override
    public boolean isContains()
    {
        return getDelegate().isContains();
    }

    @Override
    public boolean isNotNull()
    {
        return getDelegate().isNotNull();
    }

    @Override
    public boolean isMultiColumn()
    {
        return getDelegate().isMultiColumn();
    }

    @Override
    public boolean hasBound(Bound b)
    {
        return getDelegate().hasBound(b);
    }

    @Override
    public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) throws InvalidRequestException
    {
        getDelegate().addRowFilterTo(filter, indexManager, options);
    }
}

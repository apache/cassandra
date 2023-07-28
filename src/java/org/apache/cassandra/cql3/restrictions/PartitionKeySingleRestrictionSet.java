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
import java.util.*;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.service.ClientState;

/**
 * A set of single restrictions on the partition key.
 * <p>This class can only contains <code>SingleRestriction</code> instances. Token restrictions will be handled by
 * <code>TokenRestriction</code> class or by the <code>TokenFilter</code> class if the query contains a mix of token
 * restrictions and single column restrictions on the partition key.
 */
final class PartitionKeySingleRestrictionSet extends RestrictionSetWrapper implements PartitionKeyRestrictions
{
    /**
     * The composite type.
     */
    private final ClusteringComparator comparator;

    public PartitionKeySingleRestrictionSet(ClusteringComparator comparator)
    {
        super(new RestrictionSet());
        this.comparator = comparator;
    }

    private PartitionKeySingleRestrictionSet(PartitionKeySingleRestrictionSet restrictionSet,
                                       SingleRestriction restriction)
    {
        super(restrictionSet.restrictions.addRestriction(restriction));
        this.comparator = restrictionSet.comparator;
    }

    private List<ByteBuffer> toByteBuffers(SortedSet<? extends ClusteringPrefix> clusterings)
    {
        List<ByteBuffer> l = new ArrayList<>(clusterings.size());
        for (ClusteringPrefix clustering : clusterings)
        {
            // Can not use QueryProcessor.validateKey here to validate each column as that validates that empty are not allowed
            // but composite partition keys actually allow empty!
            clustering.validate();
            l.add(clustering.serializeAsPartitionKey());
        }
        return l;
    }

    @Override
    public PartitionKeyRestrictions mergeWith(Restriction restriction)
    {
        if (restriction.isOnToken())
        {
            if (isEmpty())
                return (PartitionKeyRestrictions) restriction;

            return new TokenFilter(this, (TokenRestriction) restriction);
        }

        return new PartitionKeySingleRestrictionSet(this, (SingleRestriction) restriction);
    }

    @Override
    public List<ByteBuffer> values(QueryOptions options, ClientState state)
    {
        MultiCBuilder builder = MultiCBuilder.create(comparator, hasIN());
        for (SingleRestriction r : restrictions)
        {
            r.appendTo(builder, options);

            if (hasIN() && Guardrails.inSelectCartesianProduct.enabled(state))
                Guardrails.inSelectCartesianProduct.guard(builder.buildSize(), "partition key", false, state);

            if (builder.hasMissingElements())
                break;
        }
        return toByteBuffers(builder.build());
    }

    @Override
    public List<ByteBuffer> bounds(Bound bound, QueryOptions options)
    {
        MultiCBuilder builder = MultiCBuilder.create(comparator, hasIN());
        for (SingleRestriction r : restrictions)
        {
            r.appendBoundTo(builder, bound, options);
            if (builder.hasMissingElements())
                return Collections.emptyList();
        }
        return toByteBuffers(builder.buildBound(bound.isStart(), true));
    }

    @Override
    public boolean hasBound(Bound b)
    {
        if (isEmpty())
            return false;
        return restrictions.lastRestriction().hasBound(b);
    }

    @Override
    public boolean isInclusive(Bound b)
    {
        if (isEmpty())
            return false;
        return restrictions.lastRestriction().isInclusive(b);
    }

    @Override
    public void addToRowFilter(RowFilter filter,
                               IndexRegistry indexRegistry,
                               QueryOptions options)
    {
        for (SingleRestriction restriction : restrictions)
        {
             restriction.addToRowFilter(filter, indexRegistry, options);
        }
    }

    @Override
    public boolean needFiltering(TableMetadata table)
    {
        if (isEmpty())
            return false;

        // slice or has unrestricted key component
        return hasUnrestrictedPartitionKeyComponents(table) || hasSlice() || hasContains();
    }

    @Override
    public boolean hasUnrestrictedPartitionKeyComponents(TableMetadata table)
    {
        return size() < table.partitionKeyColumns().size();
    }

    @Override
    public boolean hasSlice()
    {
        return restrictions.hasSlice();
    }
}

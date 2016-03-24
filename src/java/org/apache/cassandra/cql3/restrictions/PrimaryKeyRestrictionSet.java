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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A set of single column restrictions on a primary key part (partition key or clustering key).
 */
final class PrimaryKeyRestrictionSet extends AbstractPrimaryKeyRestrictions
{
    /**
     * The restrictions.
     */
    private final RestrictionSet restrictions;

    /**
     * <code>true</code> if the restrictions are corresponding to an EQ, <code>false</code> otherwise.
     */
    private boolean eq;

    /**
     * <code>true</code> if the restrictions are corresponding to an IN, <code>false</code> otherwise.
     */
    private boolean in;

    /**
     * <code>true</code> if the restrictions are corresponding to a LIKE, <code>false</code> otherwise.
     */
    private boolean like;

    /**
     * <code>true</code> if the restrictions are corresponding to a Slice, <code>false</code> otherwise.
     */
    private boolean slice;

    /**
     * <code>true</code> if the restrictions are corresponding to a Contains, <code>false</code> otherwise.
     */
    private boolean contains;

    /**
     * <code>true</code> if the restrictions corresponding to a partition key, <code>false</code> if it's clustering columns.
     */
    private boolean isPartitionKey;

    public PrimaryKeyRestrictionSet(ClusteringComparator comparator, boolean isPartitionKey)
    {
        super(comparator);
        this.restrictions = new RestrictionSet();
        this.eq = true;
        this.isPartitionKey = isPartitionKey;
    }

    private PrimaryKeyRestrictionSet(PrimaryKeyRestrictionSet primaryKeyRestrictions,
                                               Restriction restriction) throws InvalidRequestException
    {
        super(primaryKeyRestrictions.comparator);
        this.restrictions = primaryKeyRestrictions.restrictions.addRestriction(restriction);
        this.isPartitionKey = primaryKeyRestrictions.isPartitionKey;

        if (!primaryKeyRestrictions.isEmpty())
        {
            ColumnDefinition lastRestrictionStart = primaryKeyRestrictions.restrictions.lastRestriction().getFirstColumn();
            ColumnDefinition newRestrictionStart = restriction.getFirstColumn();

            checkFalse(primaryKeyRestrictions.isSlice() && newRestrictionStart.position() > lastRestrictionStart.position(),
                       "Clustering column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)",
                       newRestrictionStart.name,
                       lastRestrictionStart.name);

            if (newRestrictionStart.position() < lastRestrictionStart.position() && restriction.isSlice())
                throw invalidRequest("PRIMARY KEY column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)",
                                     restrictions.nextColumn(newRestrictionStart).name,
                                     newRestrictionStart.name);
        }

        if (restriction.isSlice() || primaryKeyRestrictions.isSlice())
            this.slice = true;
        else if (restriction.isContains() || primaryKeyRestrictions.isContains())
            this.contains = true;
        else if (restriction.isIN() || primaryKeyRestrictions.isIN())
            this.in = true;
        else if (restriction.isLIKE() || primaryKeyRestrictions.isLIKE())
            this.like = true;
        else
            this.eq = true;
    }

    private List<ByteBuffer> toByteBuffers(SortedSet<? extends ClusteringPrefix> clusterings)
    {
        // It's currently a tad hard to follow that this is only called for partition key so we should fix that
        List<ByteBuffer> l = new ArrayList<>(clusterings.size());
        for (ClusteringPrefix clustering : clusterings)
            l.add(CFMetaData.serializePartitionKey(clustering));
        return l;
    }

    @Override
    public boolean isSlice()
    {
        return slice;
    }

    @Override
    public boolean isEQ()
    {
        return eq;
    }

    @Override
    public boolean isIN()
    {
        return in;
    }

    @Override
    public boolean isLIKE()
    {
        return like;
    }

    @Override
    public boolean isContains()
    {
        return contains;
    }

    @Override
    public Iterable<Function> getFunctions()
    {
        return restrictions.getFunctions();
    }

    @Override
    public PrimaryKeyRestrictions mergeWith(Restriction restriction) throws InvalidRequestException
    {
        if (restriction.isOnToken())
        {
            if (isEmpty())
                return (PrimaryKeyRestrictions) restriction;

            return new TokenFilter(this, (TokenRestriction) restriction);
        }

        return new PrimaryKeyRestrictionSet(this, restriction);
    }

    // Whether any of the underlying restriction is an IN
    private boolean hasIN()
    {
        if (isIN())
            return true;

        for (Restriction restriction : restrictions)
        {
            if (restriction.isIN())
                return true;
        }
        return false;
    }

    private boolean hasMultiColumnSlice()
    {
        for (Restriction restriction : restrictions)
        {
            if (restriction.isMultiColumn() && restriction.isSlice())
                return true;
        }
        return false;
    }

    @Override
    public NavigableSet<Clustering> valuesAsClustering(QueryOptions options) throws InvalidRequestException
    {
        return appendTo(MultiCBuilder.create(comparator, hasIN()), options).build();
    }

    @Override
    public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options)
    {
        for (Restriction r : restrictions)
        {
            r.appendTo(builder, options);
            if (builder.hasMissingElements())
                break;
        }
        return builder;
    }

    @Override
    public MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<Slice.Bound> boundsAsClustering(Bound bound, QueryOptions options) throws InvalidRequestException
    {
        MultiCBuilder builder = MultiCBuilder.create(comparator, hasIN() || hasMultiColumnSlice());
        int keyPosition = 0;
        for (Restriction r : restrictions)
        {
            ColumnDefinition def = r.getFirstColumn();

            if (keyPosition != def.position() || r.isContains() || r.isLIKE())
                break;

            if (r.isSlice())
            {
                r.appendBoundTo(builder, bound, options);
                return builder.buildBoundForSlice(bound.isStart(),
                                                  r.isInclusive(bound),
                                                  r.isInclusive(bound.reverse()),
                                                  r.getColumnDefs());
            }

            r.appendBoundTo(builder, bound, options);

            if (builder.hasMissingElements())
                return BTreeSet.empty(comparator);

            keyPosition = r.getLastColumn().position() + 1;
        }

        // Everything was an equal (or there was nothing)
        return builder.buildBound(bound.isStart(), true);
    }

    @Override
    public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
    {
        if (!isPartitionKey)
            throw new UnsupportedOperationException();

        return toByteBuffers(valuesAsClustering(options));
    }

    @Override
    public List<ByteBuffer> bounds(Bound b, QueryOptions options) throws InvalidRequestException
    {
        if (!isPartitionKey)
            throw new UnsupportedOperationException();

        return toByteBuffers(boundsAsClustering(b, options));
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
    public boolean hasSupportingIndex(SecondaryIndexManager indexManager)
    {
        return restrictions.hasSupportingIndex(indexManager);
    }

    @Override
    public void addRowFilterTo(RowFilter filter,
                               SecondaryIndexManager indexManager,
                               QueryOptions options) throws InvalidRequestException
    {
        int position = 0;

        for (Restriction restriction : restrictions)
        {
            ColumnDefinition columnDef = restriction.getFirstColumn();

            // We ignore all the clustering columns that can be handled by slices.
            if (!isPartitionKey && !(restriction.isContains() || restriction.isLIKE()) && position == columnDef.position())
            {
                position = restriction.getLastColumn().position() + 1;
                if (!restriction.hasSupportingIndex(indexManager))
                    continue;
            }
            restriction.addRowFilterTo(filter, indexManager, options);
        }
    }

    @Override
    public List<ColumnDefinition> getColumnDefs()
    {
        return restrictions.getColumnDefs();
    }

    @Override
    public ColumnDefinition getFirstColumn()
    {
        return restrictions.firstColumn();
    }

    @Override
    public ColumnDefinition getLastColumn()
    {
        return restrictions.lastColumn();
    }
}

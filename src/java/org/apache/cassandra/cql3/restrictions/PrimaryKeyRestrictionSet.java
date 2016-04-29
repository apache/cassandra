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

    /**
     * If restrictions apply to clustering columns, we need to check whether they can be satisfied by an index lookup
     * as this affects which other restrictions can legally be specified (if an index is present, we are more lenient
     * about what additional filtering can be performed on the results of a lookup - see CASSANDRA-11510).
     *
     * We don't hold a reference to the SecondaryIndexManager itself as this is not strictly a singleton (although
     * we often treat is as one), the field would also require annotation with @Unmetered to avoid blowing up the
     * object size (used when calculating the size of prepared statements for caching). Instead, we refer to the
     * CFMetaData and retrieve the index manager when necessary.
     *
     * There are a couple of scenarios where the CFM can be null (and we make sure and test for null when we use it):
     *  * where an empty set of restrictions are created for use in processing query results - see
     *    SelectStatement.forSelection
     *  * where the restrictions apply to partition keys and not clustering columns e.g.
     *    StatementRestrictions.partitionKeyRestrictions
     *  * in unit tests (in particular PrimaryKeyRestrictionSetTest which is primarily concerned with the correct
     *    generation of bounds when secondary indexes are not used).
     */
    private final CFMetaData cfm;

    public PrimaryKeyRestrictionSet(ClusteringComparator comparator, boolean isPartitionKey)
    {
       this(comparator, isPartitionKey, null);
    }

    public PrimaryKeyRestrictionSet(ClusteringComparator comparator, boolean isPartitionKey, CFMetaData cfm)
    {
        super(comparator);

        if (cfm != null)
            assert !isPartitionKey;

        this.cfm = cfm;
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
        this.cfm = primaryKeyRestrictions.cfm;

        if (!primaryKeyRestrictions.isEmpty() && !hasSupportingIndex(restriction))
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

    private boolean hasSupportingIndex(Restriction restriction)
    {
        return cfm != null
               && restriction.hasSupportingIndex(Keyspace.openAndGetStore(cfm).indexManager);

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
    public boolean isOnToken()
    {
        return false;
    }

    @Override
    public boolean isContains()
    {
        return contains;
    }

    @Override
    public boolean isMultiColumn()
    {
        return false;
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

    @Override
    public NavigableSet<Clustering> valuesAsClustering(QueryOptions options) throws InvalidRequestException
    {
        return appendTo(MultiCBuilder.create(comparator), options).build();
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
        MultiCBuilder builder = MultiCBuilder.create(comparator);
        int keyPosition = 0;
        for (Restriction r : restrictions)
        {
            ColumnDefinition def = r.getFirstColumn();

            if (keyPosition != def.position() || r.isContains())
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
            if (!isPartitionKey && !restriction.isContains()&& position == columnDef.position())
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

    public final boolean needsFiltering()
    {
        // Backported from ClusteringColumnRestrictions from CASSANDRA-11310 for 3.6
        // As that suggests, this should only be called on clustering column
        // and not partition key restrictions.
        int position = 0;
        Restriction slice = null;
        for (Restriction restriction : restrictions)
        {
            if (handleInFilter(restriction, position))
                return true;

            if (slice != null && !slice.getFirstColumn().equals(restriction.getFirstColumn()))
                return true;

            if (slice == null && restriction.isSlice())
                slice = restriction;
            else
                position = restriction.getLastColumn().position() + 1;
        }

        return false;
    }

    private boolean handleInFilter(Restriction restriction, int index)
    {
        return restriction.isContains() || index != restriction.getFirstColumn().position();
    }
}

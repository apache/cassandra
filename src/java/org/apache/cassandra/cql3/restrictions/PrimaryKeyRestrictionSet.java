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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.composites.Composite.EOC;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.exceptions.InvalidRequestException;

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

    public PrimaryKeyRestrictionSet(CType ctype)
    {
        super(ctype);
        this.restrictions = new RestrictionSet();
        this.eq = true;
    }

    private PrimaryKeyRestrictionSet(PrimaryKeyRestrictionSet primaryKeyRestrictions,
                                               Restriction restriction) throws InvalidRequestException
    {
        super(primaryKeyRestrictions.ctype);
        this.restrictions = primaryKeyRestrictions.restrictions.addRestriction(restriction);

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
        else if (restriction.isIN())
            this.in = true;
        else
            this.eq = true;
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
    public boolean usesFunction(String ksName, String functionName)
    {
        return restrictions.usesFunction(ksName, functionName);
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
    public List<Composite> valuesAsComposites(QueryOptions options) throws InvalidRequestException
    {
        return appendTo(new CompositesBuilder(ctype), options).build();
    }

    @Override
    public CompositesBuilder appendTo(CompositesBuilder builder, QueryOptions options)
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
    public CompositesBuilder appendBoundTo(CompositesBuilder builder, Bound bound, QueryOptions options)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Composite> boundsAsComposites(Bound bound, QueryOptions options) throws InvalidRequestException
    {
        CompositesBuilder builder = new CompositesBuilder(ctype);
        // The end-of-component of composite doesn't depend on whether the
        // component type is reversed or not (i.e. the ReversedType is applied
        // to the component comparator but not to the end-of-component itself),
        // it only depends on whether the slice is reversed
        int keyPosition = 0;
        for (Restriction r : restrictions)
        {
            ColumnDefinition def = r.getFirstColumn();

            // In a restriction, we always have Bound.START < Bound.END for the "base" comparator.
            // So if we're doing a reverse slice, we must inverse the bounds when giving them as start and end of the slice filter.
            // But if the actual comparator itself is reversed, we must inversed the bounds too.
            Bound b = !def.isReversedType() ? bound : bound.reverse();
            if (keyPosition != def.position() || r.isContains())
                break;

            if (r.isSlice())
            {
                if (!r.hasBound(b))
                {
                    // There wasn't any non EQ relation on that key, we select all records having the preceding component as prefix.
                    // For composites, if there was preceding component and we're computing the end, we must change the last component
                    // End-Of-Component, otherwise we would be selecting only one record.
                    return builder.buildWithEOC(bound.isEnd() ? EOC.END : EOC.START);
                }

                r.appendBoundTo(builder, b, options);
                Composite.EOC eoc = eocFor(r, bound, b);
                return builder.buildWithEOC(eoc);
            }

            r.appendBoundTo(builder, b, options);

            if (builder.hasMissingElements())
                return Collections.emptyList();

            keyPosition = r.getLastColumn().position() + 1;
        }
        // Means no relation at all or everything was an equal
        // Note: if the builder is "full", there is no need to use the end-of-component bit. For columns selection,
        // it would be harmless to do it. However, we use this method got the partition key too. And when a query
        // with 2ndary index is done, and with the the partition provided with an EQ, we'll end up here, and in that
        // case using the eoc would be bad, since for the random partitioner we have no guarantee that
        // prefix.end() will sort after prefix (see #5240).
        EOC eoc = !builder.hasRemaining() ? EOC.NONE : (bound.isEnd() ? EOC.END : EOC.START);
        return builder.buildWithEOC(eoc);
    }

    @Override
    public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException
    {
        return Composites.toByteBuffers(valuesAsComposites(options));
    }

    @Override
    public List<ByteBuffer> bounds(Bound b, QueryOptions options) throws InvalidRequestException
    {
        return Composites.toByteBuffers(boundsAsComposites(b, options));
    }

    private static Composite.EOC eocFor(Restriction r, Bound eocBound, Bound inclusiveBound)
    {
        if (eocBound.isStart())
            return r.isInclusive(inclusiveBound) ? Composite.EOC.NONE : Composite.EOC.END;

        return r.isInclusive(inclusiveBound) ? Composite.EOC.END : Composite.EOC.START;
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
    public void addIndexExpressionTo(List<IndexExpression> expressions,
                                     SecondaryIndexManager indexManager,
                                     QueryOptions options) throws InvalidRequestException
    {
        Boolean clusteringColumns = null;
        int position = 0;

        for (Restriction restriction : restrictions)
        {
            ColumnDefinition columnDef = restriction.getFirstColumn();

            // PrimaryKeyRestrictionSet contains only one kind of column, either partition key or clustering columns.
            // Therefore we only need to check the column kind once. All the other columns will be of the same kind.
            if (clusteringColumns == null)
                clusteringColumns = columnDef.isClusteringColumn() ? Boolean.TRUE : Boolean.FALSE;

            // We ignore all the clustering columns that can be handled by slices.
            if (clusteringColumns && !restriction.isContains()&& position == columnDef.position())
            {
                position = restriction.getLastColumn().position() + 1;
                if (!restriction.hasSupportingIndex(indexManager))
                    continue;
            }
            restriction.addIndexExpressionTo(expressions, indexManager, options);
        }
    }

    @Override
    public Collection<ColumnDefinition> getColumnDefs()
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

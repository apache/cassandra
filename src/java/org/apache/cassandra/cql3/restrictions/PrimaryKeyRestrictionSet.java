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

import com.google.common.collect.Lists;

import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
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
        else if (restriction.isIN() || primaryKeyRestrictions.isIN())
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
    public List<Composite> valuesAsComposites(QueryOptions options) throws InvalidRequestException
    {
        return filterAndSort(appendTo(new CompositesBuilder(ctype), options).build());
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

            if (keyPosition != def.position() || r.isContains())
                break;

            if (r.isSlice())
            {
                r.appendBoundTo(builder, bound, options);

                // Since CASSANDRA-7281, the composites might not end with the same components and it is possible
                // that one of the composites is an empty one. Unfortunatly, AbstractCType will always sort
                // Composites.EMPTY before all the other components due to its EOC, even if it is not the desired
                // behaviour in some cases. To avoid that problem the code will use normal composites for the empty
                // ones until the composites are properly sorted. They will then be replaced by Composites.EMPTY as
                // it is what is expected by the intra-node serialization.
                // It is clearly a hack but it does not make a lot of sense to refactor 2.2 for that as the problem is
                // already solved in 3.0.
                List<Composite> composites = filterAndSort(setEocs(r, bound, builder.build()));
                return Lists.transform(composites, new com.google.common.base.Function<Composite, Composite>()
                {
                    @Override
                    public Composite apply(Composite composite)
                    {
                        return composite.isEmpty() ? Composites.EMPTY: composite;
                    }
                });
            }

            r.appendBoundTo(builder, bound, options);

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
        return filterAndSort(builder.buildWithEOC(eoc));
    }

    /**
     * Removes duplicates and sort the specified composites.
     *
     * @param composites the composites to filter and sort
     * @return the composites sorted and without duplicates
     */
    private List<Composite> filterAndSort(List<Composite> composites)
    {
        if (composites.size() <= 1)
            return composites;

        TreeSet<Composite> set = new TreeSet<Composite>(ctype);
        set.addAll(composites);

        return new ArrayList<>(set);
    }

    /**
     * Sets EOCs for the composites returned by the specified slice restriction for the given bound.
     *
     * @param r the slice restriction
     * @param bound the bound
     * @param composites the composites
     * @return the composites with their EOCs properly set
     */
    private List<Composite> setEocs(Restriction r, Bound bound, List<Composite> composites)
    {
        List<Composite> list = new ArrayList<>(composites.size());

        // The first column of the slice might not be the first clustering column (e.g. clustering_0 = ? AND (clustering_1, clustering_2) >= (?, ?)
        int offset = r.getFirstColumn().position();

        for (int i = 0, m = composites.size(); i < m; i++)
        {
            Composite composite = composites.get(i);

            // Handle the no bound case
            if (composite.size() == offset)
            {
                list.add(composite.withEOC(bound.isEnd() ? EOC.END : EOC.START));
                continue;
            }

            // In the case of mixed order columns, we will have some extra slices where the columns change directions.
            // For example: if we have clustering_0 DESC and clustering_1 ASC a slice like (clustering_0, clustering_1) > (1, 2)
            // will produce 2 slices: [EMPTY, 1.START] and [1.2.END, 1.END]
            // So, the END bound will return 2 composite with the same values 1
            if (composite.size() <= r.getLastColumn().position() && i < m - 1 && composite.equals(composites.get(i + 1)))
            {
                list.add(composite.withEOC(EOC.START));
                list.add(composites.get(i++).withEOC(EOC.END));
                continue;
            }

            // Handle the normal bounds
            ColumnDefinition column = r.getColumnDefs().get(composite.size() - 1 - offset);
            Bound b = reverseBoundIfNeeded(column, bound);

            Composite.EOC eoc = eocFor(r, bound, b);
            list.add(composite.withEOC(eoc));
        }

        return list;
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

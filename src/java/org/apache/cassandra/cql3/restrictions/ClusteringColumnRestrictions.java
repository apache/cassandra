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

import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A set of restrictions on the clustering key.
 */
final class ClusteringColumnRestrictions extends RestrictionSetWrapper
{
    /**
     * The composite type.
     */
    protected final ClusteringComparator comparator;

    /**
     * <code>true</code> if filtering is allowed for this restriction, <code>false</code> otherwise
     */
    private final boolean allowFiltering;

    public ClusteringColumnRestrictions(CFMetaData cfm)
    {
        this(cfm, false);
    }

    public ClusteringColumnRestrictions(CFMetaData cfm, boolean allowFiltering)
    {
        this(cfm.comparator, new RestrictionSet(), allowFiltering);
    }

    private ClusteringColumnRestrictions(ClusteringComparator comparator,
                                         RestrictionSet restrictionSet,
                                         boolean allowFiltering)
    {
        super(restrictionSet);
        this.comparator = comparator;
        this.allowFiltering = allowFiltering;
    }

    public ClusteringColumnRestrictions mergeWith(Restriction restriction) throws InvalidRequestException
    {
        SingleRestriction newRestriction = (SingleRestriction) restriction;
        RestrictionSet newRestrictionSet = restrictions.addRestriction(newRestriction);

        if (!isEmpty() && !allowFiltering)
        {
            SingleRestriction lastRestriction = restrictions.lastRestriction();
            ColumnDefinition lastRestrictionStart = lastRestriction.getFirstColumn();
            ColumnDefinition newRestrictionStart = restriction.getFirstColumn();

            checkFalse(lastRestriction.isSlice() && newRestrictionStart.position() > lastRestrictionStart.position(),
                       "Clustering column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)",
                       newRestrictionStart.name,
                       lastRestrictionStart.name);

            if (newRestrictionStart.position() < lastRestrictionStart.position() && newRestriction.isSlice())
                throw invalidRequest("PRIMARY KEY column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)",
                                     restrictions.nextColumn(newRestrictionStart).name,
                                     newRestrictionStart.name);
        }

        return new ClusteringColumnRestrictions(this.comparator, newRestrictionSet, allowFiltering);
    }

    private boolean hasMultiColumnSlice()
    {
        for (SingleRestriction restriction : restrictions)
        {
            if (restriction.isMultiColumn() && restriction.isSlice())
                return true;
        }
        return false;
    }

    public NavigableSet<Clustering> valuesAsClustering(QueryOptions options) throws InvalidRequestException
    {
        MultiCBuilder builder = MultiCBuilder.create(comparator, hasIN());
        for (SingleRestriction r : restrictions)
        {
            r.appendTo(builder, options);
            if (builder.hasMissingElements())
                break;
        }
        return builder.build();
    }

    public NavigableSet<ClusteringBound> boundsAsClustering(Bound bound, QueryOptions options) throws InvalidRequestException
    {
        MultiCBuilder builder = MultiCBuilder.create(comparator, hasIN() || hasMultiColumnSlice());
        int keyPosition = 0;

        for (SingleRestriction r : restrictions)
        {
            if (handleInFilter(r, keyPosition))
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

    /**
     * Checks if any of the underlying restriction is a CONTAINS or CONTAINS KEY.
     *
     * @return <code>true</code> if any of the underlying restriction is a CONTAINS or CONTAINS KEY,
     * <code>false</code> otherwise
     */
    public final boolean hasContains()
    {
        for (SingleRestriction restriction : restrictions)
        {
            if (restriction.isContains())
                return true;
        }
        return false;
    }

    /**
     * Checks if any of the underlying restriction is a slice restrictions.
     *
     * @return <code>true</code> if any of the underlying restriction is a slice restrictions,
     * <code>false</code> otherwise
     */
    public final boolean hasSlice()
    {
        for (SingleRestriction restriction : restrictions)
        {
            if (restriction.isSlice())
                return true;
        }
        return false;
    }

    /**
     * Checks if underlying restrictions would require filtering
     *
     * @return <code>true</code> if any underlying restrictions require filtering, <code>false</code>
     * otherwise
     */
    public final boolean needFiltering()
    {
        int position = 0;

        for (SingleRestriction restriction : restrictions)
        {
            if (handleInFilter(restriction, position))
                return true;

            if (!restriction.isSlice())
                position = restriction.getLastColumn().position() + 1;
        }
        return hasContains();
    }

    @Override
    public void addRowFilterTo(RowFilter filter,
                               SecondaryIndexManager indexManager,
                               QueryOptions options) throws InvalidRequestException
    {
        int position = 0;

        for (SingleRestriction restriction : restrictions)
        {
            // We ignore all the clustering columns that can be handled by slices.
            if (handleInFilter(restriction, position) || restriction.hasSupportingIndex(indexManager))
            {
                restriction.addRowFilterTo(filter, indexManager, options);
                continue;
            }

            if (!restriction.isSlice())
                position = restriction.getLastColumn().position() + 1;
        }
    }

    private boolean handleInFilter(SingleRestriction restriction, int index)
    {
        return restriction.isContains() || restriction.isLIKE() || index != restriction.getFirstColumn().position();
    }

    public Iterator<SingleRestriction> iterator()
    {
        return restrictions.iterator();
    }
}

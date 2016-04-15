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

    public ClusteringColumnRestrictions(CFMetaData cfm)
    {
        super(new RestrictionSet());
        this.comparator = cfm.comparator;
    }

    private ClusteringColumnRestrictions(ClusteringComparator comparator, RestrictionSet restrictionSet)
    {
        super(restrictionSet);
        this.comparator = comparator;
    }

    public ClusteringColumnRestrictions mergeWith(Restriction restriction) throws InvalidRequestException
    {
        SingleRestriction newRestriction = (SingleRestriction) restriction;
        RestrictionSet newRestrictionSet = restrictions.addRestriction(newRestriction);

        if (!isEmpty())
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

        return new ClusteringColumnRestrictions(this.comparator, newRestrictionSet);
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

    public NavigableSet<Slice.Bound> boundsAsClustering(Bound bound, QueryOptions options) throws InvalidRequestException
    {
        MultiCBuilder builder = MultiCBuilder.create(comparator, hasIN() || hasMultiColumnSlice());
        int keyPosition = 0;
        for (SingleRestriction r : restrictions)
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

    /**
     * Checks if any of the underlying restriction is a CONTAINS or CONTAINS KEY.
     *
     * @return <code>true</code> if any of the underlying restriction is a CONTAINS or CONTAINS KEY,
     * <code>false</code> otherwise
     */
    public final boolean hasContains()
    {
        return restrictions.stream().anyMatch(SingleRestriction::isContains);
    }

    /**
     * Checks if any of the underlying restriction is a slice restrictions.
     *
     * @return <code>true</code> if any of the underlying restriction is a slice restrictions,
     * <code>false</code> otherwise
     */
    public final boolean hasSlice()
    {
        return restrictions.stream().anyMatch(SingleRestriction::isSlice);
    }

    @Override
    public void addRowFilterTo(RowFilter filter,
                               SecondaryIndexManager indexManager,
                               QueryOptions options) throws InvalidRequestException
    {
        int position = 0;

        for (SingleRestriction restriction : restrictions)
        {
            ColumnDefinition columnDef = restriction.getFirstColumn();

            // We ignore all the clustering columns that can be handled by slices.
            if (!(restriction.isContains() || restriction.isLIKE()) && position == columnDef.position())
            {
                position = restriction.getLastColumn().position() + 1;
                if (!restriction.hasSupportingIndex(indexManager))
                    continue;
            }
            restriction.addRowFilterTo(filter, indexManager, options);
        }
    }
}

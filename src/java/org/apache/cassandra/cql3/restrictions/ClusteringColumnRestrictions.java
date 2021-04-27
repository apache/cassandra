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

import org.apache.cassandra.guardrails.Guardrails;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.service.QueryState;
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

    private ClusteringColumnRestrictions(ClusteringComparator comparator,
                                         RestrictionSet restrictionSet)
    {
        super(restrictionSet);
        this.comparator = comparator;
    }

    public NavigableSet<Clustering<?>> valuesAsClustering(QueryOptions options, QueryState queryState) throws InvalidRequestException
    {
        MultiCBuilder builder = MultiCBuilder.create(comparator, hasIN());
        List<SingleRestriction> restrictions = restrictions();
        for (int i = 0; i < restrictions.size(); i++)
        {
            SingleRestriction r = restrictions.get(i);
            r.appendTo(builder, options);

            if (builder.hasMissingElements())
                break;

            if (hasIN() && Guardrails.inSelectCartesianProduct.enabled(queryState))
                Guardrails.inSelectCartesianProduct.guard(builder.buildSize(), "IN Select", queryState);
        }
        return builder.build();
    }

    public NavigableSet<ClusteringBound<?>> boundsAsClustering(Bound bound, QueryOptions options) throws InvalidRequestException
    {
        List<SingleRestriction> restrictionsList = restrictions();

        MultiCBuilder builder = MultiCBuilder.create(comparator, hasIN() || restrictions.hasMultiColumnSlice());
        int keyPosition = 0;

        for (int i = 0; i < restrictionsList.size(); i++)
        {
            SingleRestriction r = restrictionsList.get(i);
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
     * Checks if underlying restrictions would require filtering
     *
     * @return <code>true</code> if any underlying restrictions require filtering, <code>false</code>
     * otherwise
     */
    public boolean needFiltering()
    {
        int position = 0;

        List<SingleRestriction> restrictions = restrictions();
        for (int i = 0; i < restrictions.size(); i++)
        {
            SingleRestriction restriction = restrictions.get(i);
            if (handleInFilter(restriction, position))
                return true;

            if (!restriction.isSlice())
                position = restriction.getLastColumn().position() + 1;
        }
        return hasContains();
    }

    @Override
    public void addToRowFilter(RowFilter filter,
                               IndexRegistry indexRegistry,
                               QueryOptions options) throws InvalidRequestException
    {
        int position = 0;

        List<SingleRestriction> restrictions = restrictions();
        for (int i = 0; i < restrictions.size(); i++)
        {
            SingleRestriction restriction = restrictions.get(i);
            // We ignore all the clustering columns that can be handled by slices.
            if (handleInFilter(restriction, position) || restriction.hasSupportingIndex(indexRegistry))
            {
                restriction.addToRowFilter(filter, indexRegistry, options);
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

    public static ClusteringColumnRestrictions.Builder builder(TableMetadata table, boolean allowFiltering)
    {
        return new Builder(table, allowFiltering, null);
    }

    public static ClusteringColumnRestrictions.Builder builder(TableMetadata table, boolean allowFiltering, IndexRegistry indexRegistry)
    {
        return new Builder(table, allowFiltering, indexRegistry);
    }

    public static class Builder
    {
        private final TableMetadata table;
        private final boolean allowFiltering;
        private final IndexRegistry indexRegistry;

        private final RestrictionSet.Builder restrictions = RestrictionSet.builder();

        private Builder(TableMetadata table, boolean allowFiltering, IndexRegistry indexRegistry)
        {
            this.table = table;
            this.allowFiltering = allowFiltering;
            this.indexRegistry = indexRegistry;
        }

        public ClusteringColumnRestrictions.Builder addRestriction(Restriction restriction)
        {
            SingleRestriction newRestriction = (SingleRestriction) restriction;
            boolean isEmpty = restrictions.isEmpty();

            if (!isEmpty && !allowFiltering && (indexRegistry == null || !newRestriction.hasSupportingIndex(indexRegistry)))
            {
                SingleRestriction lastRestriction = restrictions.lastRestriction();
                ColumnMetadata lastRestrictionStart = lastRestriction.getFirstColumn();
                ColumnMetadata newRestrictionStart = newRestriction.getFirstColumn();
                restrictions.addRestriction(newRestriction);

                checkFalse(lastRestriction.isSlice() && newRestrictionStart.position() > lastRestrictionStart.position(),
                           "Clustering column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)",
                           newRestrictionStart.name,
                           lastRestrictionStart.name);

                if (newRestrictionStart.position() < lastRestrictionStart.position() && newRestriction.isSlice())
                    throw invalidRequest("PRIMARY KEY column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)",
                                         restrictions.nextColumn(newRestrictionStart).name,
                                         newRestrictionStart.name);
            }
            else
            {
                restrictions.addRestriction(newRestriction);
            }

            return this;
        }

        public ClusteringColumnRestrictions build()
        {
            return new ClusteringColumnRestrictions(table.comparator, restrictions.build());
        }
    }
}

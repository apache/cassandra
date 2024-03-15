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

import javax.annotation.Nullable;

import com.google.common.collect.RangeSet;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.service.ClientState;

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
    private final ClusteringComparator comparator;

    /**
     * <code>true</code> if filtering is allowed for this restriction, <code>false</code> otherwise
     */
    private final boolean allowFiltering;

    public ClusteringColumnRestrictions(TableMetadata table, boolean allowFiltering)
    {
        this(table.comparator, RestrictionSet.empty(), allowFiltering);
    }

    private ClusteringColumnRestrictions(ClusteringComparator comparator,
                                         RestrictionSet restrictionSet,
                                         boolean allowFiltering)
    {
        super(restrictionSet);
        this.comparator = comparator;
        this.allowFiltering = allowFiltering;
    }

    public ClusteringColumnRestrictions mergeWith(Restriction restriction, @Nullable IndexRegistry indexRegistry) throws InvalidRequestException
    {
        SingleRestriction newRestriction = (SingleRestriction) restriction;
        RestrictionSet newRestrictionSet = restrictions.addRestriction(newRestriction);

        if (!isEmpty() && !allowFiltering && (indexRegistry == null || !newRestriction.hasSupportingIndex(indexRegistry)))
        {
            SingleRestriction lastRestriction = restrictions.lastRestriction();
            assert lastRestriction != null;

            ColumnMetadata lastRestrictionStart = lastRestriction.firstColumn();
            ColumnMetadata newRestrictionStart = restriction.firstColumn();

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

    public NavigableSet<Clustering<?>> valuesAsClustering(QueryOptions options, ClientState state) throws InvalidRequestException
    {
        MultiCBuilder builder = new MultiCBuilder(comparator);
        for (SingleRestriction r : restrictions)
        {
            List<ClusteringElements> values = r.values(options);
            builder.extend(values);

            // If values is greater than 1 we know that the restriction is an IN
            if (values.size() > 1 && Guardrails.inSelectCartesianProduct.enabled(state))
                Guardrails.inSelectCartesianProduct.guard(builder.buildSize(), "clustering key", false, state);

            if (builder.hasMissingElements())
                break;
        }
        return builder.build();
    }

    public Slices slices(QueryOptions options) throws InvalidRequestException
    {
        MultiCBuilder builder = new MultiCBuilder(comparator);
        int keyPosition = 0;

        for (SingleRestriction r : restrictions)
        {
            if (handleInFilter(r, keyPosition))
                break;

            if (r.isSlice())
            {
                RangeSet<ClusteringElements> rangeSet = r.restrict(ClusteringElements.all(), options);
                return builder.extend(rangeSet).buildSlices();
            }

            builder.extend(r.values(options));

            if (builder.hasMissingElements())
                break;

            keyPosition = r.lastColumn().position() + 1;
        }

        // Everything was an equal (or there was nothing)
        return builder.buildSlices();
    }

    /**
     * Checks if any of the underlying restriction is a slice restrictions.
     *
     * @return <code>true</code> if any of the underlying restriction is a slice restrictions,
     * <code>false</code> otherwise
     */
    public boolean hasSlice()
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
    public boolean needFiltering()
    {
        int position = 0;

        for (SingleRestriction restriction : restrictions)
        {
            if (handleInFilter(restriction, position))
                return true;

            if (!restriction.isSlice())
                position = restriction.lastColumn().position() + 1;
        }
        return false;
    }

    @Override
    public void addToRowFilter(RowFilter filter,
                               IndexRegistry indexRegistry,
                               QueryOptions options) throws InvalidRequestException
    {
        int position = 0;

        for (SingleRestriction restriction : restrictions)
        {
            // We ignore all the clustering columns that can be handled by slices.
            if (handleInFilter(restriction, position) || restriction.hasSupportingIndex(indexRegistry))
            {
                restriction.addToRowFilter(filter, indexRegistry, options);
                continue;
            }

            if (!restriction.isSlice())
                position = restriction.lastColumn().position() + 1;
        }
    }

    private boolean handleInFilter(SingleRestriction restriction, int index)
    {
        return restriction.needsFilteringOrIndexing() || index != restriction.firstColumn().position();
    }
}

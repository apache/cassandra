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

import javax.annotation.Nullable;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.service.ClientState;
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
        MultiCBuilder builder = MultiCBuilder.create(comparator, hasIN());
        for (SingleRestriction r : restrictions)
        {
            List<ValueList> values = r.values(options);
            builder.addAllElementsToAll(values);

            // If values is greater than 1 we know that the restriction is an IN
            if (values.size() > 1 && Guardrails.inSelectCartesianProduct.enabled(state))
                Guardrails.inSelectCartesianProduct.guard(builder.buildSize(), "clustering key", false, state);

            if (builder.hasMissingElements())
                break;
        }
        return builder.build();
    }

    public NavigableSet<ClusteringBound<?>> boundsAsClustering(Bound bound, QueryOptions options) throws InvalidRequestException
    {
        MultiCBuilder builder = MultiCBuilder.create(comparator, hasIN() || hasMultiColumnSlice());
        int keyPosition = 0;

        for (SingleRestriction r : restrictions)
        {
            if (handleInFilter(r, keyPosition))
                break;

            if (r.isSlice())
            {
                RangeSet<ValueList> rangeSet = TreeRangeSet.create();
                rangeSet.add(Range.all());
                rangeSet = r.restrict(rangeSet, options);
                
                if (rangeSet.isEmpty())
                    return BTreeSet.empty(comparator);

                Set<Range<ValueList>> ranges = rangeSet.asRanges();

                if (ranges.size() > 1)
                    throw new IllegalStateException("Not implemented!");

                appendBoundTo(builder, bound, r.columns(), ranges);

                Range<ValueList> range = ranges.iterator().next();
                boolean isStartInclusive = range.hasLowerBound() && range.lowerBoundType() == BoundType.CLOSED;
                boolean isEndInclusive = range.hasUpperBound() && range.upperBoundType() == BoundType.CLOSED;
                return builder.buildBoundForSlice(bound.isStart(),
                                                  bound.isStart() ? isStartInclusive : isEndInclusive,
                                                  bound.reverse().isStart() ? isStartInclusive : isEndInclusive,
                                                  r.columns());
            }

            builder.addAllElementsToAll(r.values(options));

            if (builder.hasMissingElements())
                return BTreeSet.empty(comparator);

            keyPosition = r.lastColumn().position() + 1;
        }

        // Everything was an equal (or there was nothing)
        return builder.buildBound(bound.isStart(), true);
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


    private void appendBoundTo(MultiCBuilder builder, Bound bound, List<ColumnMetadata> columns, Set<Range<ValueList>> ranges)
    {
        List<List<ByteBuffer>> toAdd = new ArrayList<>();

        for (Range<ValueList> range : ranges)
        {
            boolean reversed = columns.get(0).isReversedType();

            EnumMap<Bound, ValueList> componentBounds = new EnumMap<>(Bound.class);
            componentBounds.put(Bound.START, range.hasLowerBound() ? range.lowerEndpoint() : ValueList.of());
            componentBounds.put(Bound.END, range.hasUpperBound() ? range.upperEndpoint() : ValueList.of());

            List<ByteBuffer> values = new ArrayList<>();

            for (int i = 0, m = columns.size(); i < m; i++)
            {
                ColumnMetadata column = columns.get(i);
                Bound b = bound.reverseIfNeeded(column);

                // For mixed order columns, we need to create additional slices when 2 columns are in reverse order
                if (reversed != column.isReversedType())
                {
                    reversed = column.isReversedType();
                    // As we are switching direction we need to add the current composite
                    toAdd.add(values);

                    // The new bound side has no value for this component.  just stop
                    if (!hasComponent(b, i, componentBounds))
                        continue;

                    // The other side has still some components. We need to end the slice that we have just open.
                    if (hasComponent(b.reverse(), i, componentBounds))
                        toAdd.add(values);

                    // We need to rebuild where we are in this bound side
                    values = new ArrayList<>();

                    ValueList vals = componentBounds.get(b);

                    int n = Math.min(i, vals.size());
                    for (int j = 0; j < n; j++)
                    {
                        values.add(vals.get(j));
                    }
                }

                if (!hasComponent(b, i, componentBounds))
                    continue;

                values.add(componentBounds.get(b).get(i));
            }
            toAdd.add(values);
        }

        if (bound.isEnd())
            Collections.reverse(toAdd);

        builder.addAllElementsToAll(toAdd);
    }

    private boolean hasComponent(Bound b, int index, EnumMap<Bound, ValueList> componentBounds)
    {
        return componentBounds.get(b).size() > index;
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

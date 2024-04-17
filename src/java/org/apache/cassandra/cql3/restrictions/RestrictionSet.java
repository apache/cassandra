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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * Sets of column restrictions.
 *
 * <p>This class is immutable.</p>
 */
final class RestrictionSet implements Restrictions, Iterable<SingleRestriction>
{
    /**
     * The comparator used to sort the <code>Restriction</code>s.
     */
    private static final Comparator<ColumnMetadata> COLUMN_DEFINITION_COMPARATOR = new Comparator<ColumnMetadata>()
    {
        @Override
        public int compare(ColumnMetadata column, ColumnMetadata otherColumn)
        {
            int value = Integer.compare(column.position(), otherColumn.position());
            return value != 0 ? value : column.name.bytes.compareTo(otherColumn.name.bytes);
        }
    };

    private static final RestrictionSet EMPTY = new RestrictionSet(Collections.unmodifiableNavigableMap(new TreeMap<>(COLUMN_DEFINITION_COMPARATOR)),
                                                                   false, false, false,false);

    /**
     * The restrictions per column.
     */
    private final NavigableMap<ColumnMetadata, SingleRestriction> restrictions;

    private final boolean hasSlice;

    private final boolean hasIn;

    private final boolean hasAnn;

    private final boolean needsFilteringOrIndexing;

    /**
     * Returns an empty {@code RestrictionSet}.
     * @return an empty {@code RestrictionSet}.
     */
    public static RestrictionSet empty()
    {
        return EMPTY;
    }

    private RestrictionSet(NavigableMap<ColumnMetadata, SingleRestriction> restrictions,
                           boolean hasIn,
                           boolean hasSlice,
                           boolean hasAnn,
                           boolean needsFilteringOrIndexing)
    {
        this.restrictions = restrictions;
        this.hasIn = hasIn;
        this.hasSlice = hasSlice;
        this.hasAnn = hasAnn;
        this.needsFilteringOrIndexing = needsFilteringOrIndexing;
    }

    @Override
    public void addToRowFilter(RowFilter filter, IndexRegistry indexRegistry, QueryOptions options) throws InvalidRequestException
    {
        for (Restriction restriction : this)
            restriction.addToRowFilter(filter, indexRegistry, options);
    }

    @Override
    public boolean needsFilteringOrIndexing()
    {
        return needsFilteringOrIndexing;
    }

    public ColumnMetadata firstColumn()
    {
        return isEmpty() ? null : this.restrictions.firstKey();
    }

    @Override
    public ColumnMetadata lastColumn()
    {
        return isEmpty() ? null : this.restrictions.lastKey();
    }

    @Override
    public List<ColumnMetadata> columns()
    {
        return new ArrayList<>(restrictions.keySet());
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        for (Restriction restriction : this)
            restriction.addFunctionsTo(functions);
    }

    @Override
    public boolean isRestrictedByEquals(ColumnMetadata column)
    {
        SingleRestriction restriction = restrictions.get(column);
        return restriction != null && restriction.isColumnLevel() && restriction.isEQ();
    }

    @Override
    public boolean isRestrictedByEqualsOrIN(ColumnMetadata column)
    {
        SingleRestriction restriction = restrictions.get(column);
        return restriction != null && restriction.isColumnLevel() && (restriction.isEQ() || restriction.isIN());
    }

    @Override
    public int size()
    {
        return restrictions.size();
    }

    /**
     * Checks if one of the restrictions applies to a column of the specific kind.
     * @param kind the column kind
     * @return {@code true} if one of the restrictions applies to a column of the specific kind, {@code false} otherwise.
     */
    public boolean hasRestrictionFor(ColumnMetadata.Kind kind)
    {
        for (ColumnMetadata column : restrictions.keySet())
        {
            if (column.kind == kind)
                return true;
        }
        return false;
    }

    /**
     * Adds the specified restriction to this set of restrictions.
     *
     * @param restriction the restriction to add
     * @return the new set of restrictions
     */
    public RestrictionSet addRestriction(SingleRestriction restriction)
    {
        // RestrictionSet is immutable. Therefore, we need to clone the restrictions map.
        NavigableMap<ColumnMetadata, SingleRestriction> newRestricitons = new TreeMap<>(this.restrictions);

        boolean newHasIN = hasIn || restriction.isIN();
        boolean newHasSlice = hasSlice || restriction.isSlice();
        boolean newHasANN = hasAnn || restriction.isANN();
        boolean newNeedsFilteringOrIndexing = needsFilteringOrIndexing || restriction.needsFilteringOrIndexing();

        return new RestrictionSet(mergeRestrictions(newRestricitons, restriction),
                                  newHasIN,
                                  newHasSlice,
                                  newHasANN,
                                  newNeedsFilteringOrIndexing);
    }

    private NavigableMap<ColumnMetadata, SingleRestriction> mergeRestrictions(NavigableMap<ColumnMetadata,SingleRestriction> restrictions,
                                                                              SingleRestriction restriction)
    {
        Collection<ColumnMetadata> columns = restriction.columns();
        Set<SingleRestriction> existings = getRestrictions(columns);

        if (existings.isEmpty())
        {
            for (ColumnMetadata column : columns)
                restrictions.put(column, restriction);
        }
        else
        {
            for (SingleRestriction existing : existings)
            {
                SingleRestriction newRestriction = existing.mergeWith(restriction);

                for (ColumnMetadata column : newRestriction.columns())
                    restrictions.put(column, newRestriction);
            }
        }

        return restrictions;
    }


    /**
     * Returns all the restrictions applied to the specified columns.
     *
     * @param columns the column definitions
     * @return all the restrictions applied to the specified columns
     */
    private Set<SingleRestriction> getRestrictions(Collection<ColumnMetadata> columns)
    {
        Set<SingleRestriction> set = new HashSet<>();
        for (ColumnMetadata column : columns)
        {
            SingleRestriction existing = restrictions.get(column);
            if (existing != null)
                set.add(existing);
        }
        return set;
    }

    @Override
    public Index findSupportingIndex(Iterable<Index> indexes)
    {
        for (SingleRestriction restriction : restrictions.values())
        {
            Index index = restriction.findSupportingIndex(indexes);
            if (index != null)
                return index;
        }
        return null;
    }

    @Override
    public boolean needsFiltering(Index.Group indexGroup)
    {
        for (SingleRestriction restriction : this)
        {
            if (restriction.needsFiltering(indexGroup))
                return true;
        }
        return false;
    }

    @Override
    public Iterator<SingleRestriction> iterator()
    {
        // We need to eliminate duplicates in the case where we have multi-column restrictions.
        return new LinkedHashSet<>(restrictions.values()).iterator();
    }

    @Override
    public boolean hasIN()
    {
        return hasIn;
    }

    @Override
    public boolean hasSlice()
    {
        return hasSlice;
    }

    public boolean hasAnn()
    {
        return hasAnn;
    }

    /**
     * Returns the column after the specified one.
     *
     * @param columnDef the column for which the next one need to be found
     * @return the column after the specified one.
     */
    ColumnMetadata nextColumn(ColumnMetadata columnDef)
    {
        return restrictions.tailMap(columnDef, false).firstKey();
    }

    /**
     * Returns the last restriction.
     *
     * @return the last restriction.
     */
    SingleRestriction lastRestriction()
    {
        return restrictions.lastEntry().getValue();
    }
}

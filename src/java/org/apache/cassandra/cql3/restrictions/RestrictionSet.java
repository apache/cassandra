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

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

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

    private static final TreeMap<ColumnMetadata, SingleRestriction> EMPTY = new TreeMap<>(COLUMN_DEFINITION_COMPARATOR);

    /**
     * The restrictions per column.
     */
    private final TreeMap<ColumnMetadata, SingleRestriction> restrictions;

    /**
     * {@code true} if it contains multi-column restrictions, {@code false} otherwise.
     */
    private final boolean hasMultiColumnRestrictions;

    private final boolean hasIn;
    private final boolean hasContains;
    private final boolean hasSlice;
    private final boolean hasAnn;
    private final boolean hasOnlyEqualityRestrictions;

    public RestrictionSet()
    {
        this(EMPTY, false,
             false,
             false,
             false,
             false,
             true);
    }

    private RestrictionSet(TreeMap<ColumnMetadata, SingleRestriction> restrictions,
                           boolean hasMultiColumnRestrictions,
                           boolean hasIn,
                           boolean hasContains,
                           boolean hasSlice,
                           boolean hasAnn,
                           boolean hasOnlyEqualityRestrictions)
    {
        this.restrictions = restrictions;
        this.hasMultiColumnRestrictions = hasMultiColumnRestrictions;
        this.hasIn = hasIn;
        this.hasContains = hasContains;
        this.hasSlice = hasSlice;
        this.hasAnn = hasAnn;
        this.hasOnlyEqualityRestrictions = hasOnlyEqualityRestrictions;
    }

    @Override
    public void addToRowFilter(RowFilter filter, IndexRegistry indexRegistry, QueryOptions options) throws InvalidRequestException
    {
        for (Restriction restriction : restrictions.values())
            restriction.addToRowFilter(filter, indexRegistry, options);
    }

    @Override
    public boolean needsFiltering(Index.Group indexGroup)
    {
        for (SingleRestriction restriction : restrictions.values())
        {
            if (restriction.needsFiltering(indexGroup))
                return true;
        }
        return false;
    }

    @Override
    public List<ColumnMetadata> getColumnDefs()
    {
        return new ArrayList<>(restrictions.keySet());
    }

    /**
     * @return a direct reference to the key set from {@link #restrictions} with no defenseive copying
     */
    @Override
    public Collection<ColumnMetadata> getColumnDefinitions()
    {
        return restrictions.keySet();
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        for (Restriction restriction : this)
            restriction.addFunctionsTo(functions);
    }

    @Override
    public boolean isEmpty()
    {
        return restrictions.isEmpty();
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
        // RestrictionSet is immutable so we need to clone the restrictions map.
        TreeMap<ColumnMetadata, SingleRestriction> newRestrictions = new TreeMap<>(this.restrictions);

        boolean newHasIn = hasIn || restriction.isIN();
        boolean newHasContains = hasContains || restriction.isContains();
        boolean newHasSlice = hasSlice || restriction.isSlice();
        boolean newHasAnn = hasAnn || restriction.isANN();
        boolean newHasOnlyEqualityRestrictions = hasOnlyEqualityRestrictions && (restriction.isEQ() || restriction.isIN());

        return new RestrictionSet(mergeRestrictions(newRestrictions, restriction),
                                  hasMultiColumnRestrictions || restriction.isMultiColumn(),
                                  newHasIn,
                                  newHasContains,
                                  newHasSlice,
                                  newHasAnn,
                                  newHasOnlyEqualityRestrictions);
    }

    private TreeMap<ColumnMetadata, SingleRestriction> mergeRestrictions(TreeMap<ColumnMetadata, SingleRestriction> restrictions,
                                                                         SingleRestriction restriction)
    {
        Collection<ColumnMetadata> columnDefs = restriction.getColumnDefs();
        Set<SingleRestriction> existingRestrictions = getRestrictions(columnDefs);

        if (existingRestrictions.isEmpty())
        {
            for (ColumnMetadata columnDef : columnDefs)
                restrictions.put(columnDef, restriction);
        }
        else
        {
            for (SingleRestriction existing : existingRestrictions)
            {
                SingleRestriction newRestriction = mergeRestrictions(existing, restriction);

                for (ColumnMetadata columnDef : columnDefs)
                    restrictions.put(columnDef, newRestriction);
            }
        }

        return restrictions;
    }

    @Override
    public Set<Restriction> getRestrictions(ColumnMetadata columnDef)
    {
        Restriction existing = restrictions.get(columnDef);
        return existing == null ? Collections.emptySet() : Collections.singleton(existing);
    }

    /**
     * Returns all the restrictions applied to the specified columns.
     *
     * @param columnDefs the column definitions
     * @return all the restrictions applied to the specified columns
     */
    private Set<SingleRestriction> getRestrictions(Collection<ColumnMetadata> columnDefs)
    {
        Set<SingleRestriction> set = new HashSet<>();
        for (ColumnMetadata columnDef : columnDefs)
        {
            SingleRestriction existing = restrictions.get(columnDef);
            if (existing != null)
                set.add(existing);
        }
        return set;
    }

    @Override
    public boolean hasSupportingIndex(IndexRegistry indexRegistry)
    {
        for (Restriction restriction : restrictions.values())
        {
            if (restriction.hasSupportingIndex(indexRegistry))
                return true;
        }
        return false;
    }

    @Override
    public Index findSupportingIndex(IndexRegistry indexRegistry)
    {
        for (SingleRestriction restriction : restrictions.values())
        {
            Index index = restriction.findSupportingIndex(indexRegistry);
            if (index != null)
                return index;
        }
        return null;
    }

    @Override
    public Index findSupportingIndexFromQueryPlan(Index.QueryPlan indexQueryPlan)
    {
        for (SingleRestriction restriction : restrictions.values())
        {
            Index index = restriction.findSupportingIndexFromQueryPlan(indexQueryPlan);
            if (index != null)
                return index;
        }
        return null;
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

    @Override
    public ColumnMetadata getFirstColumn()
    {
        return isEmpty() ? null : this.restrictions.firstKey();
    }

    @Override
    public ColumnMetadata getLastColumn()
    {
        return isEmpty() ? null : this.restrictions.lastKey();
    }

    /**
     * Returns the last restriction.
     *
     * @return the last restriction.
     */
    SingleRestriction lastRestriction()
    {
        return isEmpty() ? null : this.restrictions.lastEntry().getValue();
    }

    /**
     * Merges the two specified restrictions.
     *
     * @param restriction the first restriction
     * @param otherRestriction the second restriction
     * @return the merged restriction
     * @throws InvalidRequestException if the two restrictions cannot be merged
     */
    private static SingleRestriction mergeRestrictions(SingleRestriction restriction,
                                                       SingleRestriction otherRestriction)
    {
        return restriction == null ? otherRestriction
                                   : restriction.mergeWith(otherRestriction);
    }

    @Override
    public Iterator<SingleRestriction> iterator()
    {
        Iterator<SingleRestriction> iterator = restrictions.values().iterator();
        return hasMultiColumnRestrictions ? new DistinctIterator<>(iterator) : iterator;
    }

    /**
     * Checks if any of the underlying restriction is an IN.
     * @return <code>true</code> if any of the underlying restriction is an IN, <code>false</code> otherwise
     */
    public final boolean hasIN()
    {
        return hasIn;
    }

    public boolean hasContains()
    {
        return hasContains;
    }

    public final boolean hasSlice()
    {
        return hasSlice;
    }

    public boolean hasAnn()
    {
        return hasAnn;
    }

    /**
     * Checks if all of the underlying restrictions are EQ or IN restrictions.
     *
     * @return <code>true</code> if all of the underlying restrictions are EQ or IN restrictions,
     * <code>false</code> otherwise
     */
    public final boolean hasOnlyEqualityRestrictions()
    {
        return hasOnlyEqualityRestrictions;
    }

    /**
     * {@code Iterator} decorator that removes duplicates in an ordered one.
     *
     * @param <E> the iterator element type.
     */
    private static final class DistinctIterator<E> extends AbstractIterator<E>
    {
        /**
         * The decorated iterator.
         */
        private final Iterator<E> iterator;

        /**
         * The previous element.
         */
        private E previous;

        public DistinctIterator(Iterator<E> iterator)
        {
            this.iterator = iterator;
        }

        protected E computeNext()
        {
            while(iterator.hasNext())
            {
                E next = iterator.next();
                if (!next.equals(previous))
                {
                    previous = next;
                    return next;
                }
            }
            return endOfData();
        }
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}

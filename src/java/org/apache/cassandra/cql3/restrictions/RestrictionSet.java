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

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction.ContainsRestriction;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.SecondaryIndexManager;

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
    private static final Comparator<ColumnDefinition> COLUMN_DEFINITION_COMPARATOR = new Comparator<ColumnDefinition>()
    {
        @Override
        public int compare(ColumnDefinition column, ColumnDefinition otherColumn)
        {
            int value = Integer.compare(column.position(), otherColumn.position());
            return value != 0 ? value : column.name.bytes.compareTo(otherColumn.name.bytes);
        }
    };

    /**
     * The restrictions per column.
     */
    protected final TreeMap<ColumnDefinition, SingleRestriction> restrictions;

    /**
     * {@code true} if it contains multi-column restrictions, {@code false} otherwise.
     */
    private final boolean hasMultiColumnRestrictions;

    public RestrictionSet()
    {
        this(new TreeMap<ColumnDefinition, SingleRestriction>(COLUMN_DEFINITION_COMPARATOR), false);
    }

    private RestrictionSet(TreeMap<ColumnDefinition, SingleRestriction> restrictions,
                           boolean hasMultiColumnRestrictions)
    {
        this.restrictions = restrictions;
        this.hasMultiColumnRestrictions = hasMultiColumnRestrictions;
    }

    @Override
    public void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) throws InvalidRequestException
    {
        for (Restriction restriction : restrictions.values())
            restriction.addRowFilterTo(filter, indexManager, options);
    }

    @Override
    public List<ColumnDefinition> getColumnDefs()
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
    public boolean hasRestrictionFor(ColumnDefinition.Kind kind)
    {
        for (ColumnDefinition column : restrictions.keySet())
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
        TreeMap<ColumnDefinition, SingleRestriction> newRestrictions = new TreeMap<>(this.restrictions);
        return new RestrictionSet(mergeRestrictions(newRestrictions, restriction), hasMultiColumnRestrictions || restriction.isMultiColumn());
    }

    private TreeMap<ColumnDefinition, SingleRestriction> mergeRestrictions(TreeMap<ColumnDefinition, SingleRestriction> restrictions,
                                                                           SingleRestriction restriction)
    {
        Collection<ColumnDefinition> columnDefs = restriction.getColumnDefs();
        Set<SingleRestriction> existingRestrictions = getRestrictions(columnDefs);

        if (existingRestrictions.isEmpty())
        {
            for (ColumnDefinition columnDef : columnDefs)
                restrictions.put(columnDef, restriction);
        }
        else
        {
            for (SingleRestriction existing : existingRestrictions)
            {
                SingleRestriction newRestriction = mergeRestrictions(existing, restriction);

                for (ColumnDefinition columnDef : columnDefs)
                    restrictions.put(columnDef, newRestriction);
            }
        }

        return restrictions;
    }

    @Override
    public Set<Restriction> getRestrictions(ColumnDefinition columnDef)
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
    private Set<SingleRestriction> getRestrictions(Collection<ColumnDefinition> columnDefs)
    {
        Set<SingleRestriction> set = new HashSet<>();
        for (ColumnDefinition columnDef : columnDefs)
        {
            SingleRestriction existing = restrictions.get(columnDef);
            if (existing != null)
                set.add(existing);
        }
        return set;
    }

    @Override
    public final boolean hasSupportingIndex(SecondaryIndexManager indexManager)
    {
        for (Restriction restriction : restrictions.values())
        {
            if (restriction.hasSupportingIndex(indexManager))
                return true;
        }
        return false;
    }

    /**
     * Returns the column after the specified one.
     *
     * @param columnDef the column for which the next one need to be found
     * @return the column after the specified one.
     */
    ColumnDefinition nextColumn(ColumnDefinition columnDef)
    {
        return restrictions.tailMap(columnDef, false).firstKey();
    }

    @Override
    public ColumnDefinition getFirstColumn()
    {
        return isEmpty() ? null : this.restrictions.firstKey();
    }

    @Override
    public ColumnDefinition getLastColumn()
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

    /**
     * Checks if the restrictions contains multiple contains, contains key, or map[key] = value.
     *
     * @return <code>true</code> if the restrictions contains multiple contains, contains key, or ,
     * map[key] = value; <code>false</code> otherwise
     */
    public final boolean hasMultipleContains()
    {
        int numberOfContains = 0;
        for (SingleRestriction restriction : restrictions.values())
        {
            if (restriction.isContains())
            {
                ContainsRestriction contains = (ContainsRestriction) restriction;
                numberOfContains += (contains.numberOfValues() + contains.numberOfKeys() + contains.numberOfEntries());
            }
        }
        return numberOfContains > 1;
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
        for (SingleRestriction restriction : this)
        {
            if (restriction.isIN())
                return true;
        }
        return false;
    }

    public boolean hasContains()
    {
        for (SingleRestriction restriction : this)
        {
            if (restriction.isContains())
                return true;
        }
        return false;
    }

    public final boolean hasSlice()
    {
        for (SingleRestriction restriction : this)
        {
            if (restriction.isSlice())
                return true;
        }
        return false;
    }

    /**
     * Checks if all of the underlying restrictions are EQ or IN restrictions.
     *
     * @return <code>true</code> if all of the underlying restrictions are EQ or IN restrictions,
     * <code>false</code> otherwise
     */
    public final boolean hasOnlyEqualityRestrictions()
    {
        for (SingleRestriction restriction : this)
        {
            if (!restriction.isEQ() && !restriction.isIN())
                return false;
        }
        return true;
    }

    /**
     * {@code Iterator} decorator that removes duplicates in an ordered one.
     *
     * @param iterator the decorated iterator
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
}

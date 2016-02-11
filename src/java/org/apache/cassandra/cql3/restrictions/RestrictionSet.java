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

import com.google.common.collect.Iterables;

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
 * <p>This class is immutable in order to be use within {@link PrimaryKeyRestrictionSet} which as
 * an implementation of {@link Restriction} need to be immutable.
 */
final class RestrictionSet implements Restrictions, Iterable<Restriction>
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
    protected final TreeMap<ColumnDefinition, Restriction> restrictions;

    public RestrictionSet()
    {
        this(new TreeMap<ColumnDefinition, Restriction>(COLUMN_DEFINITION_COMPARATOR));
    }

    private RestrictionSet(TreeMap<ColumnDefinition, Restriction> restrictions)
    {
        this.restrictions = restrictions;
    }

    @Override
    public final void addRowFilterTo(RowFilter filter, SecondaryIndexManager indexManager, QueryOptions options) throws InvalidRequestException
    {
        for (Restriction restriction : restrictions.values())
            restriction.addRowFilterTo(filter, indexManager, options);
    }

    @Override
    public final List<ColumnDefinition> getColumnDefs()
    {
        return new ArrayList<>(restrictions.keySet());
    }

    @Override
    public Iterable<Function> getFunctions()
    {
        com.google.common.base.Function<Restriction, Iterable<Function>> transform =
            new com.google.common.base.Function<Restriction, Iterable<Function>>()
        {
            public Iterable<Function> apply(Restriction restriction)
            {
                return restriction.getFunctions();
            }
        };

        return Iterables.concat(Iterables.transform(restrictions.values(), transform));
    }

    @Override
    public final boolean isEmpty()
    {
        return restrictions.isEmpty();
    }

    @Override
    public final int size()
    {
        return restrictions.size();
    }

    /**
     * Adds the specified restriction to this set of restrictions.
     *
     * @param restriction the restriction to add
     * @return the new set of restrictions
     * @throws InvalidRequestException if the new restriction cannot be added
     */
    public RestrictionSet addRestriction(Restriction restriction) throws InvalidRequestException
    {
        // RestrictionSet is immutable so we need to clone the restrictions map.
        TreeMap<ColumnDefinition, Restriction> newRestrictions = new TreeMap<>(this.restrictions);
        return new RestrictionSet(mergeRestrictions(newRestrictions, restriction));
    }

    private TreeMap<ColumnDefinition, Restriction> mergeRestrictions(TreeMap<ColumnDefinition, Restriction> restrictions,
                                                                     Restriction restriction)
                                                                     throws InvalidRequestException
    {
        Collection<ColumnDefinition> columnDefs = restriction.getColumnDefs();
        Set<Restriction> existingRestrictions = getRestrictions(columnDefs);

        if (existingRestrictions.isEmpty())
        {
            for (ColumnDefinition columnDef : columnDefs)
                restrictions.put(columnDef, restriction);
        }
        else
        {
            for (Restriction existing : existingRestrictions)
            {
                Restriction newRestriction = mergeRestrictions(existing, restriction);

                for (ColumnDefinition columnDef : columnDefs)
                    restrictions.put(columnDef, newRestriction);
            }
        }

        return restrictions;
    }

    /**
     * Returns all the restrictions applied to the specified columns.
     *
     * @param columnDefs the column definitions
     * @return all the restrictions applied to the specified columns
     */
    private Set<Restriction> getRestrictions(Collection<ColumnDefinition> columnDefs)
    {
        Set<Restriction> set = new HashSet<>();
        for (ColumnDefinition columnDef : columnDefs)
        {
            Restriction existing = restrictions.get(columnDef);
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

    /**
     * Returns the definition of the first column.
     *
     * @return the definition of the first column.
     */
    ColumnDefinition firstColumn()
    {
        return isEmpty() ? null : this.restrictions.firstKey();
    }

    /**
     * Returns the definition of the last column.
     *
     * @return the definition of the last column.
     */
    ColumnDefinition lastColumn()
    {
        return isEmpty() ? null : this.restrictions.lastKey();
    }

    /**
     * Returns the last restriction.
     *
     * @return the last restriction.
     */
    Restriction lastRestriction()
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
    private static Restriction mergeRestrictions(Restriction restriction,
                                                 Restriction otherRestriction) throws InvalidRequestException
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
        for (Restriction restriction : restrictions.values())
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
    public Iterator<Restriction> iterator()
    {
        return new LinkedHashSet<>(restrictions.values()).iterator();
    }
}

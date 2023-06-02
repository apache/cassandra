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
import java.util.stream.Collectors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction.ContainsRestriction;
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
public abstract class RestrictionSet implements Restrictions
{
    /**
     * The comparator used to sort the <code>Restriction</code>s.
     */
    private static final Comparator<ColumnMetadata> COLUMN_DEFINITION_COMPARATOR = Comparator.comparingInt(ColumnMetadata::position).thenComparing(column -> column.name.bytes);

    private static final class EmptyRestrictionSet extends RestrictionSet
    {
        private static final EmptyRestrictionSet INSTANCE = new EmptyRestrictionSet();

        private EmptyRestrictionSet()
        {
        }

        @Override
        public void addToRowFilter(RowFilter.Builder rowFilter, IndexRegistry indexRegistry, QueryOptions options) throws InvalidRequestException
        {
        }

        @Override
        public List<ColumnMetadata> getColumnDefs()
        {
            return Collections.EMPTY_LIST;
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
        }

        @Override
        public boolean isEmpty()
        {
            return true;
        }

        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public boolean hasRestrictionFor(ColumnMetadata.Kind kind)
        {
            return false;
        }

        @Override
        public Set<Restriction> getRestrictions(ColumnMetadata columnDef)
        {
            return Collections.emptySet();
        }

        @Override
        public boolean hasSupportingIndex(IndexRegistry indexRegistry)
        {
            return false;
        }

        @Override
        public boolean needsFiltering(Index.Group indexGroup)
        {
            return false;
        }

        @Override
        public ColumnMetadata getFirstColumn()
        {
            return null;
        }

        @Override
        public ColumnMetadata getLastColumn()
        {
            return null;
        }

        @Override
        public SingleRestriction lastRestriction()
        {
            return null;
        }

        @Override
        public boolean hasMultipleContains()
        {
            return false;
        }

        @Override
        public List<SingleRestriction> restrictions()
        {
            return Collections.EMPTY_LIST;
        }

        @Override
        public boolean hasMultiColumnSlice()
        {
            return false;
        }
    }

    private static final class DefaultRestrictionSet extends RestrictionSet
    {

        /**
         * The keys from the 'restrictions' parameter to the
         */
        private final List<ColumnMetadata> restrictionsKeys;
        /**
         * The values as returned from {@link #restrictions()}.
         */
        private final List<SingleRestriction> restrictionsValues;
        private final Multimap<ColumnMetadata, SingleRestriction> restrictionsMap;
        private final int hasBitmap;
        private final int restrictionForKindBitmap;
        private static final int maskHasContains = 1;
        private static final int maskHasSlice = 2;
        private static final int maskHasIN = 4;
        private static final int maskHasOnlyEqualityRestrictions = 8;
        private static final int maskHasMultiColumnSlice = 16;
        private static final int maskHasMultipleContains = 32;

        private DefaultRestrictionSet(Multimap<ColumnMetadata, SingleRestriction> restrictions,
                                      boolean hasMultiColumnRestrictions)
        {
            this.restrictionsKeys = new ArrayList<>(restrictions.keySet());
            restrictionsKeys.sort(COLUMN_DEFINITION_COMPARATOR);

            List<SingleRestriction> sortedRestrictions = new ArrayList<>();

            int numberOfContains = 0;
            int restrictionForBitmap = 0;
            int bitmap = maskHasOnlyEqualityRestrictions;

            SingleRestriction previous = null;
            for (int i = 0; i < restrictionsKeys.size(); i++)
            {
                ColumnMetadata col = restrictionsKeys.get(i);
                Collection<SingleRestriction> columnRestrictions = restrictions.get(col);

                for (SingleRestriction singleRestriction : columnRestrictions)
                {
                    if (singleRestriction.isContains())
                    {
                        bitmap |= maskHasContains;
                        ContainsRestriction contains = (ContainsRestriction) singleRestriction;
                        numberOfContains += (contains.numberOfValues() + contains.numberOfKeys() + contains.numberOfEntries());
                    }

                    if (hasMultiColumnRestrictions)
                    {
                        if (singleRestriction.equals(previous))
                            continue;
                        previous = singleRestriction;
                    }

                    restrictionForBitmap |= 1 << col.kind.ordinal();

                    sortedRestrictions.add(singleRestriction);

                    if (singleRestriction.isSlice())
                    {
                        bitmap |= maskHasSlice;
                        if (singleRestriction.isMultiColumn())
                            bitmap |= maskHasMultiColumnSlice;
                    }

                    if (singleRestriction.isIN())
                        bitmap |= maskHasIN;
                    else if (!singleRestriction.isEQ())
                        bitmap &= ~maskHasOnlyEqualityRestrictions;
                }
            }
            this.hasBitmap = bitmap | (numberOfContains > 1 ? maskHasMultipleContains : 0);
            this.restrictionForKindBitmap = restrictionForBitmap;

            this.restrictionsValues = Collections.unmodifiableList(sortedRestrictions);
            this.restrictionsMap = restrictions;
        }

        @Override
        public void addToRowFilter(RowFilter.Builder rowFilter,
                                   IndexRegistry indexRegistry,
                                   QueryOptions options) throws InvalidRequestException
        {
            for (SingleRestriction restriction : restrictionsMap.values())
                restriction.addToRowFilter(rowFilter, indexRegistry, options);
        }

        @Override
        public List<ColumnMetadata> getColumnDefs()
        {
            return restrictionsKeys;
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            for (int i = 0; i < restrictionsValues.size(); i++)
                restrictionsValues.get(i).addFunctionsTo(functions);
        }

        @Override
        public boolean isEmpty()
        {
            return false;
        }

        @Override
        public int size()
        {
            return restrictionsKeys.size();
        }

        @Override
        public boolean hasRestrictionFor(ColumnMetadata.Kind kind)
        {
            return 0 != (restrictionForKindBitmap & 1 << kind.ordinal());
        }

        @Override
        public Set<Restriction> getRestrictions(ColumnMetadata columnDef)
        {
            return restrictionsMap.get(columnDef).stream().map(r -> ((Restriction)r)).collect(Collectors.toSet());
        }

        @Override
        public boolean hasSupportingIndex(IndexRegistry indexRegistry)
        {
            for (SingleRestriction restriction : restrictionsMap.values())
                if (restriction.hasSupportingIndex(indexRegistry))
                    return true;
            return false;
        }

        @Override
        public boolean needsFiltering(Index.Group indexGroup)
        {
            for (SingleRestriction restriction : restrictionsMap.values())
                if (restriction.needsFiltering(indexGroup))
                    return true;

            return false;
        }

        @Override
        public ColumnMetadata getFirstColumn()
        {
            return this.restrictionsKeys.get(0);
        }

        @Override
        public ColumnMetadata getLastColumn()
        {
            return this.restrictionsKeys.get(this.restrictionsKeys.size() - 1);
        }

        @Override
        public SingleRestriction lastRestriction()
        {
            return this.restrictionsValues.get(this.restrictionsValues.size() - 1);
        }

        @Override
        public boolean hasMultipleContains()
        {
            return 0 != (hasBitmap & maskHasMultipleContains);
        }

        @Override
        public List<SingleRestriction> restrictions()
        {
            return restrictionsValues;
        }

        @Override
        public boolean hasIN()
        {
            return 0 != (hasBitmap & maskHasIN);
        }

        @Override
        public boolean hasContains()
        {
            return 0 != (hasBitmap & maskHasContains);
        }

        @Override
        public boolean hasSlice()
        {
            return 0 != (hasBitmap & maskHasSlice);
        }

        @Override
        public boolean hasMultiColumnSlice()
        {
            return 0 != (hasBitmap & maskHasMultiColumnSlice);
        }

        @Override
        public boolean hasOnlyEqualityRestrictions()
        {
            return 0 != (hasBitmap & maskHasOnlyEqualityRestrictions);
        }
    }

    /**
     * Checks if one of the restrictions applies to a column of the specific kind.
     * @param kind the column kind
     * @return {@code true} if one of the restrictions applies to a column of the specific kind, {@code false} otherwise.
     */
    public abstract boolean hasRestrictionFor(ColumnMetadata.Kind kind);

    /**
     * Returns the last restriction.
     */
    public abstract SingleRestriction lastRestriction();

    /**
     * Checks if the restrictions contains multiple contains, contains key, or map[key] = value.
     *
     * @return <code>true</code> if the restrictions contain multiple contains, contains key, or ,
     * map[key] = value; <code>false</code> otherwise
     */
    public abstract boolean hasMultipleContains();

    public abstract List<SingleRestriction> restrictions();

    /**
     * Checks if the restrictions contains multiple contains, contains key, or map[key] = value.
     *
     * @return <code>true</code> if the restrictions contains multiple contains, contains key, or ,
     * map[key] = value; <code>false</code> otherwise
     */
    public abstract boolean hasMultiColumnSlice();

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private final Multimap<ColumnMetadata, SingleRestriction> newRestrictions = ArrayListMultimap.create();
        private boolean multiColumn = false;

        private ColumnMetadata lastRestrictionColumn;
        private SingleRestriction lastRestriction;

        private Builder()
        {
        }

        public void addRestriction(SingleRestriction restriction, boolean isDisjunction)
        {
            List<ColumnMetadata> columnDefs = restriction.getColumnDefs();

            if (isDisjunction)
            {
                // If this restriction is part of a disjunction query then we don't want
                // to merge the restrictions (if that is possible), we just add the
                // restriction to the set of restrictions for the column.
                addRestrictionForColumns(columnDefs, restriction, false);
            }
            else
            {
                // If this restriction isn't part of a disjunction then we need to get
                // the set of existing restrictions for the column and merge them with the
                // new restriction
                Set<SingleRestriction> existingRestrictions = getRestrictions(newRestrictions, columnDefs);

                if (existingRestrictions.isEmpty())
                {
                    addRestrictionForColumns(columnDefs, restriction, false);
                }
                else
                {
                    for (SingleRestriction existing : existingRestrictions)
                    {
                        SingleRestriction newRestriction = existing.mergeWith(restriction);

                        addRestrictionForColumns(columnDefs, newRestriction, true);
                    }
                }
            }
        }

        private void addRestrictionForColumns(List<ColumnMetadata> columnDefs, SingleRestriction restriction, boolean replace)
        {
            for (int i = 0; i < columnDefs.size(); i++)
            {
                ColumnMetadata column = columnDefs.get(i);
                if (lastRestrictionColumn == null || COLUMN_DEFINITION_COMPARATOR.compare(lastRestrictionColumn, column) < 0)
                {
                    lastRestrictionColumn = column;
                    lastRestriction = restriction;
                }
                // If the restriction is a merger of new restriction and existing restrictions then
                // we need to remove the existing restrictions for the column before adding it
                if (replace)
                    newRestrictions.removeAll(column);
                newRestrictions.put(column, restriction);
            }

            multiColumn |= restriction.isMultiColumn();
        }

        private static Set<SingleRestriction> getRestrictions(Multimap<ColumnMetadata, SingleRestriction> restrictions,
                                                              List<ColumnMetadata> columnDefs)
        {
            Set<SingleRestriction> set = new HashSet<>();
            for (int i = 0; i < columnDefs.size(); i++)
            {
                Collection<SingleRestriction> existing = restrictions.get(columnDefs.get(i));
                if (!existing.isEmpty())
                    set.addAll(existing);
            }
            return set;
        }

        public RestrictionSet build()
        {
            return isEmpty() ? EmptyRestrictionSet.INSTANCE : new DefaultRestrictionSet(newRestrictions, multiColumn);
        }

        public boolean isEmpty()
        {
            return newRestrictions.isEmpty();
        }

        public SingleRestriction lastRestriction()
        {
            return lastRestriction;
        }

        public ColumnMetadata nextColumn(ColumnMetadata columnDef)
        {
            // This method is only invoked in the statement-preparation-phase to construct an error message.
            NavigableSet<ColumnMetadata> columns = new TreeSet<>(COLUMN_DEFINITION_COMPARATOR);
            columns.addAll(newRestrictions.keySet());
            return columns.tailSet(columnDef, false).first();
        }
    }
}

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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.RangeSet;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A {@code SingleRestriction} which is the result of merging multiple {@code SimpleRestriction}.
 */
public final class MergedRestriction implements SingleRestriction
{
    /**
     * The columns to which the restrictions apply.
     */
    private final List<ColumnMetadata> columns;

    /**
     * The restrictions composing this restriction
     */
    private final List<SimpleRestriction> restrictions;

    private final boolean isOnToken;

    private final boolean isSlice;

    private final boolean isMultiColumn;

    /**
     * The number of restrictions that require {@code CONTAINS}, {@code CONTAINS_KEY} and Map equality restrictions.
     */
    private final int containsCount;

    public MergedRestriction(SingleRestriction restriction,
                             SimpleRestriction other)
    {
        assert restriction.isOnToken() == other.isOnToken();

        this.columns = restriction.columns().size() < other.columns().size()
                     ? other.columns()
                     : restriction.columns();

        ImmutableList.Builder<SimpleRestriction> builder = ImmutableList.builder();
        int containsCount = 0;
        if (restriction instanceof MergedRestriction)
        {
            MergedRestriction composite = (MergedRestriction) restriction;

            for (SimpleRestriction r : composite.restrictions)
            {
                validate(r, other);
            }

            builder.addAll(composite.restrictions);
            containsCount = composite.containsCount;
        }
        else
        {
            SimpleRestriction r = (SimpleRestriction) restriction;
            validate(r, other);
            builder.add(r);
            if (isContains(r))
                containsCount++;
        }
        builder.add(other);
        if (isContains(restriction))
            containsCount++;

        this.restrictions = builder.build();
        this.isOnToken = restriction.isOnToken();
        this.isSlice = restriction.isSlice() && other.isSlice();
        this.isMultiColumn = restriction.isMultiColumn() || other.isMultiColumn();
        this.containsCount = containsCount;
    }

    @Override
    public boolean isOnToken()
    {
        return isOnToken;
    }

    @Override
    public boolean isMultiColumn()
    {
        return isMultiColumn;
    }

    private static void validate(SimpleRestriction restriction, SimpleRestriction other)
    {
        checkOperator(restriction);
        checkOperator(other);

        if (restriction.isSlice() && other.isSlice())
        {
            ColumnMetadata firstColumn = restriction.firstColumn();
            ColumnMetadata otherFirstColumn = other.firstColumn();
            if (!firstColumn.equals(otherFirstColumn))
            {
                ColumnMetadata column = firstColumn.position() > otherFirstColumn.position() ? firstColumn
                                                                                             : otherFirstColumn;

                throw invalidRequest("Column \"%s\" cannot be restricted by two inequalities not starting with the same column",
                                     column.name);
            }

            if ((restriction.operator() == Operator.GT || restriction.operator() == Operator.GTE) &&
                    (other.operator() == Operator.GT || other.operator() == Operator.GTE))
            {
                throw invalidRequest("More than one restriction was found for the start bound on %s",
                                     toCQLString(getColumnsInCommons(restriction, other)));
            }

            if ((restriction.operator() == Operator.LT || restriction.operator() == Operator.LTE) &&
                    (other.operator() == Operator.LT || other.operator() == Operator.LTE))
            {
                throw invalidRequest("More than one restriction was found for the end bound on %s",
                                     toCQLString(getColumnsInCommons(restriction, other)));
            }
        }
    }

    private static void checkOperator(SimpleRestriction restriction)
    {
        if (restriction.isColumnLevel() || restriction.isOnToken())
        {
            if (restriction.isEQ())
                throw invalidRequest("%s cannot be restricted by more than one relation if it includes an Equal",
                                      toCQLString(restriction.columns()));

            if (restriction.isIN())
                throw invalidRequest("%s cannot be restricted by more than one relation if it includes a IN",
                                     toCQLString(restriction.columns()));
        }
    }

    /**
     * Returns the columns that are specified within the 2 {@code Restrictions}.
     *
     * @param restriction the first restriction
     * @param other the other restriction
     * @return the columns that are specified within the 2 {@code Restrictions}.
     */
    private static Set<ColumnMetadata> getColumnsInCommons(Restriction restriction, Restriction other)
    {
        Set<ColumnMetadata> commons = new HashSet<>(restriction.columns());
        commons.retainAll(other.columns());
        return commons;
    }

    private static String toCQLString(Iterable<ColumnMetadata> columns)
    {
        StringBuilder builder = new StringBuilder();
        for (ColumnMetadata columnMetadata : columns)
        {
            if (builder.length() != 0)
                builder.append(" ,");
            builder.append(columnMetadata.name.toCQLString());
        }
        return builder.toString();
    }

    /**
     * Checks if the restriction operator is a CONTAINS, CONTAINS_KEY or is an equality on a map element.
     * @param restriction the restriction to check
     * @return {@code true} if the restriction operator is one of the contains operations, {@code false} otherwise.
     */
    private boolean isContains(SingleRestriction restriction)
    {
        return restriction instanceof SimpleRestriction && ((SimpleRestriction) restriction).isContains();
    }

    @Override
    public boolean isEQ() {
        return false; // For the moment we do not support merging EQ restriction with anything else.
    }

    @Override
    public boolean isIN()
    {
        return false; // For the moment we do not support merging IN restriction with anything else.
    }

    @Override
    public boolean isANN() {
        return false; // For the moment we do not support merging ANN restriction with anything else.
    }

    @Override
    public boolean isSlice()
    {
        return isSlice;
    }

    @Override
    public boolean isColumnLevel() {
        return false;
    }

    @Override
    public ColumnMetadata firstColumn()
    {
        return columns.get(0);
    }

    @Override
    public ColumnMetadata lastColumn()
    {
        return columns.get(columns.size() - 1);
    }

    @Override
    public List<ColumnMetadata> columns()
    {
        return columns;
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        for (int i = 0, m = restrictions.size(); i < m; i++)
        {
            restrictions.get(i).addFunctionsTo(functions);
        }
    }

    @Override
    public boolean needsFilteringOrIndexing()
    {
        for (int i = 0, m = restrictions.size(); i < m; i++)
        {
            if (restrictions.get(i).needsFilteringOrIndexing())
                return true;
        }
        return false;
    }

    @Override
    public boolean needsFiltering(Index.Group indexGroup)
    {
        // multiple contains might require filtering on some indexes, since that is equivalent to a disjunction (or)
        boolean hasMultipleContains = containsCount > 1;

        for (Index index : indexGroup.getIndexes())
        {
            if (isSupportedBy(index) && !(hasMultipleContains && index.filtersMultipleContains()))
                return false;
        }

        return true;
    }

    @Override
    public Index findSupportingIndex(Iterable<Index> indexes)
    {
        for (int i = 0, m = restrictions.size(); i < m; i++)
        {
            Index index = restrictions.get(i).findSupportingIndex(indexes);
            if (index != null)
                return index;
        }
        return null;
    }

    @Override
    public boolean isSupportedBy(Index index)
    {
        for (SingleRestriction restriction : restrictions)
        {
            if (restriction.isSupportedBy(index))
                return true;
        }
        return false;
    }

    @Override
    public List<ValueList> values(QueryOptions options)
    {
        List<ValueList> values = restrictions.get(0).values(options);
        for (int i = 1, m = restrictions.size(); i < m; i++)
        {
            values.retainAll(restrictions.get(i).values(options));
        }
        return values;
    }

    @Override
    public RangeSet<ValueList> restrict(RangeSet<ValueList> rangeSet, QueryOptions options)
    {
        for (int i = 0, m = restrictions.size(); i < m; i++)
        {
            restrictions.get(i).restrict(rangeSet, options);
        }
        return rangeSet;
    }

    @Override
    public void addToRowFilter(RowFilter filter, IndexRegistry indexRegistry, QueryOptions options)
    {
        for (int i = 0, m = restrictions.size(); i < m; i++)
        {
            restrictions.get(i).addToRowFilter(filter, indexRegistry, options);
        }
    }
}

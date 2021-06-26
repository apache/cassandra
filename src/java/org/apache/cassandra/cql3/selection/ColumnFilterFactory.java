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
package org.apache.cassandra.cql3.selection;

import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Factory for {@code ColumnFilter} instances.
 * <p>This class is used to abstract the fact that depending on the selection clause the {@code ColumnFilter} instances
 * can be computed at prepartion time (if all the requested columns are known) or must be computed at execution time.</p>
 */
abstract class ColumnFilterFactory
{
    /**
     * Returns the {@code ColumnFilter} instance corresponding to the specified selectors.
     * @param selectors the selectors for which the {@code ColumnFilter} must be created.
     * @return the {@code ColumnFilter} instance corresponding to the specified selectors
     */
    abstract ColumnFilter newInstance(List<Selector> selectors);

    public static ColumnFilterFactory wildcard(TableMetadata table)
    {
        return new PrecomputedColumnFilter(ColumnFilter.all(table));
    }

    public static ColumnFilterFactory fromColumns(TableMetadata table,
                                                  List<ColumnMetadata> selectedColumns,
                                                  Set<ColumnMetadata> orderingColumns,
                                                  Set<ColumnMetadata> nonPKRestrictedColumns,
                                                  boolean returnStaticContentOnPartitionWithNoRows)
    {
        ColumnFilter.Builder builder = ColumnFilter.allRegularColumnsBuilder(table, returnStaticContentOnPartitionWithNoRows);
        builder.addAll(selectedColumns);
        builder.addAll(orderingColumns);
        // we'll also need to fetch any column on which we have a restriction (so we can apply said restriction)
        builder.addAll(nonPKRestrictedColumns);
        return new PrecomputedColumnFilter(builder.build());
    }

    /**
     * Creates a new {@code ColumnFilterFactory} instance from the specified {@code SelectorFactories}.
     *
     * @param table the table metadata
     * @param factories the {@code SelectorFactories}
     * @param orderingColumns the columns used for ordering
     * @param nonPKRestrictedColumns the non primary key columns that have been restricted in the WHERE clause
     * @param returnStaticContentOnPartitionWithNoRows {@code true} if the query will return the static content when the
     * partition has no rows, {@code false} otherwise.
     * @return a new {@code ColumnFilterFactory} instance
     */
    public static ColumnFilterFactory fromSelectorFactories(TableMetadata table,
                                                            SelectorFactories factories,
                                                            Set<ColumnMetadata> orderingColumns,
                                                            Set<ColumnMetadata> nonPKRestrictedColumns,
                                                            boolean returnStaticContentOnPartitionWithNoRows)
    {
        if (factories.areAllFetchedColumnsKnown())
        {
            ColumnFilter.Builder builder = ColumnFilter.allRegularColumnsBuilder(table, returnStaticContentOnPartitionWithNoRows);
            factories.addFetchedColumns(builder);
            builder.addAll(orderingColumns);
            // we'll also need to fetch any column on which we have a restriction (so we can apply said restriction)
            builder.addAll(nonPKRestrictedColumns);
            return new PrecomputedColumnFilter(builder.build());
        }

        return new OnRequestColumnFilterFactory(table, nonPKRestrictedColumns, returnStaticContentOnPartitionWithNoRows);
    }

    /**
     * A factory that always return the same pre-computed {@code ColumnFilter}.
     */
    private static class PrecomputedColumnFilter extends ColumnFilterFactory
    {
        /**
         * The precomputed {@code ColumnFilter}
         */
        private final ColumnFilter columnFilter;

        public PrecomputedColumnFilter(ColumnFilter columnFilter)
        {
            this.columnFilter = columnFilter;
        }

        @Override
        public ColumnFilter newInstance(List<Selector> selectors)
        {
            return columnFilter;
        }
    }

    /**
     * A factory that will computed the {@code ColumnFilter} on request.
     */
    private static class OnRequestColumnFilterFactory extends ColumnFilterFactory
    {
        private final TableMetadata table;
        private final Set<ColumnMetadata> nonPKRestrictedColumns;
        private final boolean returnStaticContentOnPartitionWithNoRows;

        public OnRequestColumnFilterFactory(TableMetadata table,
                                            Set<ColumnMetadata> nonPKRestrictedColumns,
                                            boolean returnStaticContentOnPartitionWithNoRows)
        {
            this.table = table;
            this.nonPKRestrictedColumns = nonPKRestrictedColumns;
            this.returnStaticContentOnPartitionWithNoRows = returnStaticContentOnPartitionWithNoRows;
        }

        @Override
        public ColumnFilter newInstance(List<Selector> selectors)
        {
            ColumnFilter.Builder builder = ColumnFilter.allRegularColumnsBuilder(table, returnStaticContentOnPartitionWithNoRows);
            for (int i = 0, m = selectors.size(); i < m; i++)
                selectors.get(i).addFetchedColumns(builder);

            // we'll also need to fetch any column on which we have a restriction (so we can apply said restriction)
            builder.addAll(nonPKRestrictedColumns);
            return builder.build();
        }
    }
}

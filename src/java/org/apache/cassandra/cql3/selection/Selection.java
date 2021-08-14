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

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.selection.Selector.InputRow;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JsonUtils;

public abstract class Selection
{
    /**
     * A predicate that returns <code>true</code> for static columns.
     */
    private static final Predicate<ColumnMetadata> STATIC_COLUMN_FILTER = (column) -> column.isStatic();

    private final TableMetadata table;
    private final List<ColumnMetadata> columns;
    private final SelectionColumnMapping columnMapping;
    protected final ResultSet.ResultMetadata metadata;
    protected final ColumnFilterFactory columnFilterFactory;
    protected final boolean isJson;

    // Columns used to order the result set for JSON queries with post ordering.
    protected final List<ColumnMetadata> orderingColumns;

    protected Selection(TableMetadata table,
                        List<ColumnMetadata> selectedColumns,
                        Set<ColumnMetadata> orderingColumns,
                        SelectionColumnMapping columnMapping,
                        ColumnFilterFactory columnFilterFactory,
                        boolean isJson)
    {
        this.table = table;
        this.columns = selectedColumns;
        this.columnMapping = columnMapping;
        this.metadata = new ResultSet.ResultMetadata(columnMapping.getColumnSpecifications());
        this.columnFilterFactory = columnFilterFactory;
        this.isJson = isJson;

        // If we order post-query, the sorted column needs to be in the ResultSet for sorting,
        // even if we don't ultimately ship them to the client (CASSANDRA-4911).
        this.columns.addAll(orderingColumns);
        this.metadata.addNonSerializedColumns(orderingColumns);

        this.orderingColumns = orderingColumns.isEmpty() ? Collections.emptyList() : new ArrayList<>(orderingColumns);
    }

    // Overriden by SimpleSelection when appropriate.
    public boolean isWildcard()
    {
        return false;
    }

    /**
     * Checks if this selection contains static columns.
     * @return <code>true</code> if this selection contains static columns, <code>false</code> otherwise;
     */
    public boolean containsStaticColumns()
    {
        if (table.isStaticCompactTable() || !table.hasStaticColumns())
            return false;

        if (isWildcard())
            return true;

        return !Iterables.isEmpty(Iterables.filter(columns, STATIC_COLUMN_FILTER));
    }

    /**
     * Returns the corresponding column index used for post query ordering
     * @param c ordering column
     * @return
     */
    public Integer getOrderingIndex(ColumnMetadata c)
    {
        // If we order post-query in json, the first and only column that we ship to the client is the json column.
        // In that case, we should keep ordering columns around to perform the ordering, then these columns will
        // be placed after the json column. As a consequence of where the colums are placed, we should give the
        // ordering index a value based on their position in the json encoding and discard the original index.
        // (CASSANDRA-14286)
        if (isJson)
            return orderingColumns.indexOf(c) + 1;

        // If the column is masked it might appear twice, once masked in the selected column and once unmasked in
        // the ordering columns. For ordering we are interested in that second unmasked value.
        if (c.isMasked())
            return columns.lastIndexOf(c);

        return getResultSetIndex(c);
    }

    public ResultSet.ResultMetadata getResultMetadata()
    {
        if (!isJson)
            return metadata;

        ColumnSpecification firstColumn = metadata.names.get(0);
        ColumnSpecification jsonSpec = new ColumnSpecification(firstColumn.ksName, firstColumn.cfName, Json.JSON_COLUMN_ID, UTF8Type.instance);
        ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(Lists.newArrayList(jsonSpec));
        resultMetadata.addNonSerializedColumns(orderingColumns);
        return resultMetadata;
    }

    public static Selection wildcard(TableMetadata table, boolean isJson, boolean returnStaticContentOnPartitionWithNoRows)
    {
        List<ColumnMetadata> all = new ArrayList<>(table.columns().size());
        Iterators.addAll(all, table.allColumnsInSelectOrder());
        return new SimpleSelection(table, all, Collections.emptySet(), true, isJson, returnStaticContentOnPartitionWithNoRows);
    }

    public static Selection wildcardWithGroupByOrMaskedColumns(TableMetadata table,
                                                               VariableSpecifications boundNames,
                                                               Set<ColumnMetadata> orderingColumns,
                                                               boolean isJson,
                                                               boolean returnStaticContentOnPartitionWithNoRows)
    {
        return fromSelectors(table,
                             Lists.newArrayList(table.allColumnsInSelectOrder()),
                             boundNames,
                             orderingColumns,
                             Collections.emptySet(),
                             true,
                             isJson,
                             returnStaticContentOnPartitionWithNoRows);
    }

    public static Selection forColumns(TableMetadata table, List<ColumnMetadata> columns, boolean returnStaticContentOnPartitionWithNoRows)
    {
        return new SimpleSelection(table, columns, Collections.emptySet(), false, false, returnStaticContentOnPartitionWithNoRows);
    }

    public void addFunctionsTo(List<Function> functions)
    {
    }

    private static boolean processesSelection(List<Selectable> selectables)
    {
        for (Selectable selectable : selectables)
        {
            if (selectable.processesSelection())
                return true;
        }
        return false;
    }

    public static Selection fromSelectors(TableMetadata table,
                                          List<Selectable> selectables,
                                          VariableSpecifications boundNames,
                                          Set<ColumnMetadata> orderingColumns,
                                          Set<ColumnMetadata> nonPKRestrictedColumns,
                                          boolean hasGroupBy,
                                          boolean isJson,
                                          boolean returnStaticContentOnPartitionWithNoRows)
    {
        List<ColumnMetadata> selectedColumns = new ArrayList<>();

        SelectorFactories factories =
                SelectorFactories.createFactoriesAndCollectColumnDefinitions(selectables, null, table, selectedColumns, boundNames);
        SelectionColumnMapping mapping = collectColumnMappings(table, factories);

        Set<ColumnMetadata> filteredOrderingColumns = filterOrderingColumns(orderingColumns,
                                                                            selectedColumns,
                                                                            factories,
                                                                            isJson);

        return (processesSelection(selectables) || selectables.size() != selectedColumns.size() || hasGroupBy)
            ? new SelectionWithProcessing(table,
                                          selectedColumns,
                                          filteredOrderingColumns,
                                          nonPKRestrictedColumns,
                                          mapping,
                                          factories,
                                          isJson,
                                          returnStaticContentOnPartitionWithNoRows)
            : new SimpleSelection(table,
                                  selectedColumns,
                                  filteredOrderingColumns,
                                  nonPKRestrictedColumns,
                                  mapping,
                                  isJson,
                                  returnStaticContentOnPartitionWithNoRows);
    }

    /**
     * Removes the ordering columns that are already selected.
     *
     * @param orderingColumns the columns used to order the results
     * @param selectedColumns the selected columns
     * @param factories the factory used to create the selectors
     * @return the ordering columns that are not part of the selection
     */
    private static Set<ColumnMetadata> filterOrderingColumns(Set<ColumnMetadata> orderingColumns,
                                                             List<ColumnMetadata> selectedColumns,
                                                             SelectorFactories factories,
                                                             boolean isJson)
    {
        // CASSANDRA-14286
        if (isJson)
            return orderingColumns;
        Set<ColumnMetadata> filteredOrderingColumns = new LinkedHashSet<>(orderingColumns.size());
        for (ColumnMetadata orderingColumn : orderingColumns)
        {
            int index = selectedColumns.indexOf(orderingColumn);
            if (index >= 0 && factories.indexOfSimpleSelectorFactory(index) >= 0 && !orderingColumn.isMasked())
                continue;

            filteredOrderingColumns.add(orderingColumn);
        }
        return filteredOrderingColumns;
    }

    /**
     * Returns the index of the specified column within the resultset
     * @param c the column
     * @return the index of the specified column within the resultset or -1
     */
    public int getResultSetIndex(ColumnMetadata c)
    {
        return getColumnIndex(c);
    }

    /**
     * Returns the index of the specified column
     * @param c the column
     * @return the index of the specified column or -1
     */
    protected final int getColumnIndex(ColumnMetadata c)
    {
        return columns.indexOf(c);
    }

    private static SelectionColumnMapping collectColumnMappings(TableMetadata table,
                                                                SelectorFactories factories)
    {
        SelectionColumnMapping selectionColumns = SelectionColumnMapping.newMapping();
        for (Selector.Factory factory : factories)
        {
            ColumnSpecification colSpec = factory.getColumnSpecification(table);
            factory.addColumnMapping(selectionColumns, colSpec);
        }
        return selectionColumns;
    }

    public abstract Selectors newSelectors(QueryOptions options);

    /**
     * @return the list of CQL3 columns value this SelectionClause needs.
     */
    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }

    /**
     * @return the mappings between resultset columns and the underlying columns
     */
    public SelectionColumns getColumnMapping()
    {
        return columnMapping;
    }

    public abstract boolean isAggregate();

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("columns", columns)
                          .add("columnMapping", columnMapping)
                          .add("metadata", metadata)
                          .toString();
    }

    private static List<ByteBuffer> rowToJson(List<ByteBuffer> row,
                                              ProtocolVersion protocolVersion,
                                              ResultSet.ResultMetadata metadata,
                                              List<ColumnMetadata> orderingColumns)
    {
        ByteBuffer[] jsonRow = new ByteBuffer[orderingColumns.size() + 1];
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < metadata.names.size(); i++)
        {
            ColumnSpecification spec = metadata.names.get(i);
            ByteBuffer buffer = row.get(i);

            // If it is an ordering column we need to keep it in case we need it for post ordering
            int index = orderingColumns.indexOf(spec);
            if (index >= 0)
                jsonRow[index + 1] = buffer;

            // If the column is only used for ordering we can stop here.
            if (i >= metadata.getColumnCount())
                continue;

            if (i > 0)
                sb.append(", ");

            String columnName = spec.name.toString();
            if (!columnName.equals(columnName.toLowerCase(Locale.US)))
                columnName = "\"" + columnName + "\"";

            sb.append('"');
            sb.append(JsonUtils.quoteAsJsonString(columnName));
            sb.append("\": ");
            if (buffer == null)
                sb.append("null");
            else
                sb.append(spec.type.toJSONString(buffer, protocolVersion));
        }
        sb.append("}");

        jsonRow[0] = UTF8Type.instance.getSerializer().serialize(sb.toString());
        return Arrays.asList(jsonRow);
    }

    public static interface Selectors
    {
        /**
         * Returns the {@code ColumnFilter} corresponding to those selectors
         *
         * @return the {@code ColumnFilter} corresponding to those selectors
         */
        public ColumnFilter getColumnFilter();

        /**
         * Checks if this Selectors perform some processing
         * @return {@code true} if this Selectors perform some processing, {@code false} otherwise.
         */
        public boolean hasProcessing();

        /**
         * Checks if one of the selectors perform some aggregations.
         * @return {@code true} if one of the selectors perform some aggregations, {@code false} otherwise.
         */
        public boolean isAggregate();

        /**
         * Returns the number of fetched columns
         * @return the number of fetched columns
         */
        public int numberOfFetchedColumns();

        /**
         * Checks if one of the selectors collect TTLs.
         * @return {@code true} if one of the selectors collect TTLs, {@code false} otherwise.
         */
        public boolean collectTTLs();

        /**
         * Checks if one of the selectors collects write timestamps.
         * @return {@code true} if one of the selectors collects write timestamps, {@code false} otherwise.
         */
        public boolean collectWritetimes();

        /**
         * Adds the current row of the specified <code>ResultSetBuilder</code>.
         *
         * @param input the input row
         */
        public void addInputRow(InputRow input);

        public List<ByteBuffer> getOutputRow();

        public void reset();
    }

    // Special cased selection for when only columns are selected.
    private static class SimpleSelection extends Selection
    {
        private final boolean isWildcard;

        public SimpleSelection(TableMetadata table,
                               List<ColumnMetadata> selectedColumns,
                               Set<ColumnMetadata> orderingColumns,
                               boolean isWildcard,
                               boolean isJson,
                               boolean returnStaticContentOnPartitionWithNoRows)
        {
            this(table,
                 selectedColumns,
                 orderingColumns,
                 SelectionColumnMapping.simpleMapping(selectedColumns),
                 isWildcard ? ColumnFilterFactory.wildcard(table)
                            : ColumnFilterFactory.fromColumns(table, selectedColumns, orderingColumns, Collections.emptySet(), returnStaticContentOnPartitionWithNoRows),
                 isWildcard,
                 isJson);
        }

        public SimpleSelection(TableMetadata table,
                               List<ColumnMetadata> selectedColumns,
                               Set<ColumnMetadata> orderingColumns,
                               Set<ColumnMetadata> nonPKRestrictedColumns,
                               SelectionColumnMapping mapping,
                               boolean isJson,
                               boolean returnStaticContentOnPartitionWithNoRows)
        {
            this(table,
                 selectedColumns,
                 orderingColumns,
                 mapping,
                 ColumnFilterFactory.fromColumns(table, selectedColumns, orderingColumns, nonPKRestrictedColumns, returnStaticContentOnPartitionWithNoRows),
                 false,
                 isJson);
        }

        private SimpleSelection(TableMetadata table,
                                List<ColumnMetadata> selectedColumns,
                                Set<ColumnMetadata> orderingColumns,
                                SelectionColumnMapping mapping,
                                ColumnFilterFactory columnFilterFactory,
                                boolean isWildcard,
                                boolean isJson)
        {
            /*
             * In theory, even a simple selection could have multiple time the same column, so we
             * could filter those duplicate out of columns. But since we're very unlikely to
             * get much duplicate in practice, it's more efficient not to bother.
             */
            super(table, selectedColumns, orderingColumns, mapping, columnFilterFactory, isJson);
            this.isWildcard = isWildcard;
        }

        @Override
        public boolean isWildcard()
        {
            return isWildcard;
        }

        public boolean isAggregate()
        {
            return false;
        }

        public Selectors newSelectors(QueryOptions options)
        {
            return new Selectors()
            {
                private List<ByteBuffer> current;

                public void reset()
                {
                    current = null;
                }

                public List<ByteBuffer> getOutputRow()
                {
                    if (isJson)
                        return rowToJson(current, options.getProtocolVersion(), metadata, orderingColumns);
                    return current;
                }

                public void addInputRow(InputRow input)
                {
                    current = input.getValues();
                }

                public boolean isAggregate()
                {
                    return false;
                }

                public boolean hasProcessing()
                {
                    return false;
                }

                @Override
                public int numberOfFetchedColumns()
                {
                    return getColumns().size();
                }

                @Override
                public boolean collectTTLs()
                {
                    return false;
                }

                @Override
                public boolean collectWritetimes()
                {
                    return false;
                }

                @Override
                public ColumnFilter getColumnFilter()
                {
                    // In the case of simple selection we know that the ColumnFilter has already been computed and
                    // that by consequence the selectors argument has not impact on the output.
                    return columnFilterFactory.newInstance(null);
                }
            };
        }
    }

    private static class SelectionWithProcessing extends Selection
    {
        private final SelectorFactories factories;
        private final boolean collectWritetimes;
        private final boolean collectMaxWritetimes;
        private final boolean collectTTLs;

        public SelectionWithProcessing(TableMetadata table,
                                       List<ColumnMetadata> columns,
                                       Set<ColumnMetadata> orderingColumns,
                                       Set<ColumnMetadata> nonPKRestrictedColumns,
                                       SelectionColumnMapping metadata,
                                       SelectorFactories factories,
                                       boolean isJson,
                                       boolean returnStaticContentOnPartitionWithNoRows)
        {
            super(table,
                  columns,
                  orderingColumns,
                  metadata,
                  ColumnFilterFactory.fromSelectorFactories(table, factories, orderingColumns, nonPKRestrictedColumns, returnStaticContentOnPartitionWithNoRows),
                  isJson);

            this.factories = factories;
            this.collectWritetimes = factories.containsWritetimeSelectorFactory();
            this.collectMaxWritetimes = factories.containsMaxWritetimeSelectorFactory();
            this.collectTTLs = factories.containsTTLSelectorFactory();

            for (ColumnMetadata orderingColumn : orderingColumns)
            {
                factories.addSelectorForOrdering(orderingColumn, getColumnIndex(orderingColumn));
            }
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            factories.addFunctionsTo(functions);
        }

        @Override
        public int getResultSetIndex(ColumnMetadata c)
        {
            return factories.indexOfSimpleSelectorFactory(super.getResultSetIndex(c));
        }

        public boolean isAggregate()
        {
            return factories.doesAggregation();
        }

        public Selectors newSelectors(final QueryOptions options) throws InvalidRequestException
        {
            return new Selectors()
            {
                private final List<Selector> selectors = factories.newInstances(options);

                public void reset()
                {
                    for (Selector selector : selectors)
                        selector.reset();
                }

                public boolean isAggregate()
                {
                    return factories.doesAggregation();
                }

                public boolean hasProcessing()
                {
                    return true;
                }

                public List<ByteBuffer> getOutputRow()
                {
                    List<ByteBuffer> outputRow = new ArrayList<>(selectors.size());

                    for (Selector selector: selectors)
                        outputRow.add(selector.getOutput(options.getProtocolVersion()));

                    return isJson ? rowToJson(outputRow, options.getProtocolVersion(), metadata, orderingColumns) : outputRow;
                }

                public void addInputRow(InputRow input)
                {
                    for (Selector selector : selectors)
                        selector.addInput(input);
                }

                @Override
                public int numberOfFetchedColumns()
                {
                    return getColumns().size();
                }

                @Override
                public boolean collectTTLs()
                {
                    return collectTTLs;
                }

                @Override
                public boolean collectWritetimes()
                {
                    return collectWritetimes || collectMaxWritetimes;
                }

                @Override
                public ColumnFilter getColumnFilter()
                {
                    return columnFilterFactory.newInstance(selectors);
                }
            };
        }

    }
}

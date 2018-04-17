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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class Selection
{
    /**
     * A predicate that returns <code>true</code> for static columns.
     */
    private static final Predicate<ColumnDefinition> STATIC_COLUMN_FILTER = new Predicate<ColumnDefinition>()
    {
        public boolean apply(ColumnDefinition def)
        {
            return def.isStatic();
        }
    };

    private final CFMetaData cfm;
    private final List<ColumnDefinition> columns;
    private final SelectionColumnMapping columnMapping;
    private final ResultSet.ResultMetadata metadata;
    private final boolean collectTimestamps;
    private final boolean collectTTLs;
    // Columns used to order the result set for multi-partition queries
    private Map<ColumnDefinition, Integer> orderingIndex;

    protected Selection(CFMetaData cfm,
                        List<ColumnDefinition> columns,
                        SelectionColumnMapping columnMapping,
                        boolean collectTimestamps,
                        boolean collectTTLs)
    {
        this.cfm = cfm;
        this.columns = columns;
        this.columnMapping = columnMapping;
        this.metadata = new ResultSet.ResultMetadata(columnMapping.getColumnSpecifications());
        this.collectTimestamps = collectTimestamps;
        this.collectTTLs = collectTTLs;
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
        if (cfm.isStaticCompactTable() || !cfm.hasStaticColumns())
            return false;

        if (isWildcard())
            return true;

        return !Iterables.isEmpty(Iterables.filter(columns, STATIC_COLUMN_FILTER));
    }

    /**
     * Checks if this selection contains only static columns.
     * @return <code>true</code> if this selection contains only static columns, <code>false</code> otherwise;
     */
    public boolean containsOnlyStaticColumns()
    {
        if (!containsStaticColumns())
            return false;

        if (isWildcard())
            return false;

        for (ColumnDefinition def : getColumns())
        {
            if (!def.isPartitionKey() && !def.isStatic())
                return false;
        }

        return true;
    }

    /**
     * Checks if this selection contains a complex column.
     *
     * @return <code>true</code> if this selection contains a multicell collection or UDT, <code>false</code> otherwise.
     */
    public boolean containsAComplexColumn()
    {
        for (ColumnDefinition def : getColumns())
            if (def.isComplex())
                return true;

        return false;
    }

    public Map<ColumnDefinition, Integer> getOrderingIndex(boolean isJson)
    {
        if (!isJson)
            return orderingIndex;

        // If we order post-query in json, the first and only column that we ship to the client is the json column.
        // In that case, we should keep ordering columns around to perform the ordering, then these columns will
        // be placed after the json column. As a consequence of where the colums are placed, we should give the
        // ordering index a value based on their position in the json encoding and discard the original index.
        // (CASSANDRA-14286)
        int columnIndex = 1;
        Map<ColumnDefinition, Integer> jsonOrderingIndex = new LinkedHashMap<>(orderingIndex.size());
        for (ColumnDefinition column : orderingIndex.keySet())
            jsonOrderingIndex.put(column, columnIndex++);

        return jsonOrderingIndex;
    }

    public ResultSet.ResultMetadata getResultMetadata(boolean isJson)
    {
        if (!isJson)
            return metadata;

        ColumnSpecification firstColumn = metadata.names.get(0);
        ColumnSpecification jsonSpec = new ColumnSpecification(firstColumn.ksName, firstColumn.cfName, Json.JSON_COLUMN_ID, UTF8Type.instance);
        ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(Lists.newArrayList(jsonSpec));
        if (orderingIndex != null)
        {
            for (ColumnDefinition orderingColumn : orderingIndex.keySet())
                resultMetadata.addNonSerializedColumn(orderingColumn);
        }
        return resultMetadata;
    }

    public static Selection wildcard(CFMetaData cfm)
    {
        List<ColumnDefinition> all = new ArrayList<>(cfm.allColumns().size());
        Iterators.addAll(all, cfm.allColumnsInSelectOrder());
        return new SimpleSelection(cfm, all, true);
    }

    public static Selection wildcardWithGroupBy(CFMetaData cfm, VariableSpecifications boundNames)
    {
        List<RawSelector> rawSelectors = new ArrayList<>(cfm.allColumns().size());
        Iterator<ColumnDefinition> iter = cfm.allColumnsInSelectOrder();
        while (iter.hasNext())
        {
            ColumnDefinition.Raw raw = ColumnDefinition.Raw.forColumn(iter.next());
            rawSelectors.add(new RawSelector(raw, null));
        }
        return fromSelectors(cfm, rawSelectors, boundNames, true);
    }

    public static Selection forColumns(CFMetaData cfm, List<ColumnDefinition> columns)
    {
        return new SimpleSelection(cfm, columns, false);
    }

    public void addColumnForOrdering(ColumnDefinition c)
    {
        if (orderingIndex == null)
            orderingIndex = new LinkedHashMap<>();

        int index = getResultSetIndex(c);

        if (index < 0)
            index = addOrderingColumn(c);

        orderingIndex.put(c, index);
    }

    protected int addOrderingColumn(ColumnDefinition c)
    {
        columns.add(c);
        metadata.addNonSerializedColumn(c);
        return columns.size() - 1;
    }

    public void addFunctionsTo(List<Function> functions)
    {
    }

    private static boolean processesSelection(List<RawSelector> rawSelectors)
    {
        for (RawSelector rawSelector : rawSelectors)
        {
            if (rawSelector.processesSelection())
                return true;
        }
        return false;
    }

    public static Selection fromSelectors(CFMetaData cfm, List<RawSelector> rawSelectors, VariableSpecifications boundNames, boolean hasGroupBy)
    {
        List<ColumnDefinition> defs = new ArrayList<>();

        SelectorFactories factories =
                SelectorFactories.createFactoriesAndCollectColumnDefinitions(RawSelector.toSelectables(rawSelectors, cfm), null, cfm, defs, boundNames);
        SelectionColumnMapping mapping = collectColumnMappings(cfm, rawSelectors, factories);

        return (processesSelection(rawSelectors) || rawSelectors.size() != defs.size() || hasGroupBy)
               ? new SelectionWithProcessing(cfm, defs, mapping, factories)
               : new SimpleSelection(cfm, defs, mapping, false);
    }

    /**
     * Returns the index of the specified column within the resultset
     * @param c the column
     * @return the index of the specified column within the resultset or -1
     */
    public int getResultSetIndex(ColumnDefinition c)
    {
        return getColumnIndex(c);
    }

    /**
     * Returns the index of the specified column
     * @param c the column
     * @return the index of the specified column or -1
     */
    protected final int getColumnIndex(ColumnDefinition c)
    {
        for (int i = 0, m = columns.size(); i < m; i++)
            if (columns.get(i).name.equals(c.name))
                return i;
        return -1;
    }

    private static SelectionColumnMapping collectColumnMappings(CFMetaData cfm,
                                                                List<RawSelector> rawSelectors,
                                                                SelectorFactories factories)
    {
        SelectionColumnMapping selectionColumns = SelectionColumnMapping.newMapping();
        Iterator<RawSelector> iter = rawSelectors.iterator();
        for (Selector.Factory factory : factories)
        {
            ColumnSpecification colSpec = factory.getColumnSpecification(cfm);
            ColumnIdentifier alias = iter.next().alias;
            factory.addColumnMapping(selectionColumns,
                                     alias == null ? colSpec : colSpec.withAlias(alias));
        }
        return selectionColumns;
    }

    protected abstract Selectors newSelectors(QueryOptions options) throws InvalidRequestException;

    /**
     * @return the list of CQL3 columns value this SelectionClause needs.
     */
    public List<ColumnDefinition> getColumns()
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

    public ResultSetBuilder resultSetBuilder(QueryOptions options, boolean isJson)
    {
        return new ResultSetBuilder(options, isJson);
    }

    public ResultSetBuilder resultSetBuilder(QueryOptions options, boolean isJson, AggregationSpecification aggregationSpec)
    {
        return aggregationSpec == null ? new ResultSetBuilder(options, isJson)
                : new ResultSetBuilder(options, isJson, aggregationSpec.newGroupMaker());
    }

    public abstract boolean isAggregate();

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("columns", columns)
                          .add("columnMapping", columnMapping)
                          .add("metadata", metadata)
                          .add("collectTimestamps", collectTimestamps)
                          .add("collectTTLs", collectTTLs)
                          .toString();
    }

    public static List<ByteBuffer> rowToJson(List<ByteBuffer> row, ProtocolVersion protocolVersion, ResultSet.ResultMetadata metadata)
    {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < metadata.getColumnCount(); i++)
        {
            if (i > 0)
                sb.append(", ");

            ColumnSpecification spec = metadata.names.get(i);
            String columnName = spec.name.toString();
            if (!columnName.equals(columnName.toLowerCase(Locale.US)))
                columnName = "\"" + columnName + "\"";

            ByteBuffer buffer = row.get(i);
            sb.append('"');
            sb.append(Json.quoteAsJsonString(columnName));
            sb.append("\": ");
            if (buffer == null)
                sb.append("null");
            else if (!buffer.hasRemaining())
                sb.append("\"\"");
            else
                sb.append(spec.type.toJSONString(buffer, protocolVersion));
        }
        sb.append("}");
        List<ByteBuffer> jsonRow = new ArrayList<>();
        jsonRow.add(UTF8Type.instance.getSerializer().serialize(sb.toString()));
        return jsonRow;
    }

    public class ResultSetBuilder
    {
        private final ResultSet resultSet;
        private final ProtocolVersion protocolVersion;

        /**
         * As multiple thread can access a <code>Selection</code> instance each <code>ResultSetBuilder</code> will use
         * its own <code>Selectors</code> instance.
         */
        private final Selectors selectors;

        /**
         * The <code>GroupMaker</code> used to build the aggregates.
         */
        private final GroupMaker groupMaker;

        /*
         * We'll build CQL3 row one by one.
         * The currentRow is the values for the (CQL3) columns we've fetched.
         * We also collect timestamps and ttls for the case where the writetime and
         * ttl functions are used. Note that we might collect timestamp and/or ttls
         * we don't care about, but since the array below are allocated just once,
         * it doesn't matter performance wise.
         */
        List<ByteBuffer> current;
        final long[] timestamps;
        final int[] ttls;

        private final boolean isJson;

        private ResultSetBuilder(QueryOptions options, boolean isJson)
        {
            this(options, isJson, null);
        }

        private ResultSetBuilder(QueryOptions options, boolean isJson, GroupMaker groupMaker)
        {
            this.resultSet = new ResultSet(getResultMetadata(isJson).copy(), new ArrayList<List<ByteBuffer>>());
            this.protocolVersion = options.getProtocolVersion();
            this.selectors = newSelectors(options);
            this.groupMaker = groupMaker;
            this.timestamps = collectTimestamps ? new long[columns.size()] : null;
            this.ttls = collectTTLs ? new int[columns.size()] : null;
            this.isJson = isJson;

            // We use MIN_VALUE to indicate no timestamp and -1 for no ttl
            if (timestamps != null)
                Arrays.fill(timestamps, Long.MIN_VALUE);
            if (ttls != null)
                Arrays.fill(ttls, -1);
        }

        public void add(ByteBuffer v)
        {
            current.add(v);
        }

        public void add(Cell c, int nowInSec)
        {
            if (c == null)
            {
                current.add(null);
                return;
            }

            current.add(value(c));

            if (timestamps != null)
                timestamps[current.size() - 1] = c.timestamp();

            if (ttls != null)
                ttls[current.size() - 1] = remainingTTL(c, nowInSec);
        }

        private int remainingTTL(Cell c, int nowInSec)
        {
            if (!c.isExpiring())
                return -1;

            int remaining = c.localDeletionTime() - nowInSec;
            return remaining >= 0 ? remaining : -1;
        }

        private ByteBuffer value(Cell c)
        {
            return c.isCounterCell()
                 ? ByteBufferUtil.bytes(CounterContext.instance().total(c.value()))
                 : c.value();
        }

        /**
         * Notifies this <code>Builder</code> that a new row is being processed.
         *
         * @param partitionKey the partition key of the new row
         * @param clustering the clustering of the new row
         */
        public void newRow(DecoratedKey partitionKey, Clustering clustering)
        {
            // The groupMaker needs to be called for each row
            boolean isNewAggregate = groupMaker == null || groupMaker.isNewGroup(partitionKey, clustering);
            if (current != null)
            {
                selectors.addInputRow(protocolVersion, this);
                if (isNewAggregate)
                {
                    resultSet.addRow(getOutputRow());
                    selectors.reset();
                }
            }
            current = new ArrayList<>(columns.size());

            // Timestamps and TTLs are arrays per row, we must null them out between rows
            if (timestamps != null)
                Arrays.fill(timestamps, Long.MIN_VALUE);
            if (ttls != null)
                Arrays.fill(ttls, -1);
        }

        /**
         * Builds the <code>ResultSet</code>
         */
        public ResultSet build()
        {
            if (current != null)
            {
                selectors.addInputRow(protocolVersion, this);
                resultSet.addRow(getOutputRow());
                selectors.reset();
                current = null;
            }

            // For aggregates we need to return a row even it no records have been found
            if (resultSet.isEmpty() && groupMaker != null && groupMaker.returnAtLeastOneRow())
                resultSet.addRow(getOutputRow());
            return resultSet;
        }

        private List<ByteBuffer> getOutputRow()
        {
            List<ByteBuffer> outputRow = selectors.getOutputRow(protocolVersion);
            if (isJson)
            {
                // Keep all columns around for possible post-query ordering. (CASSANDRA-14286)
                List<ByteBuffer> jsonRow = rowToJson(outputRow, protocolVersion, metadata);

                // Keep ordering columns around for possible post-query ordering. (CASSANDRA-14286)
                if (orderingIndex != null)
                {
                    for (Integer orderingColumnIndex : orderingIndex.values())
                        jsonRow.add(outputRow.get(orderingColumnIndex));
                }
                outputRow = jsonRow;
            }
            return outputRow;
        }
    }

    private static interface Selectors
    {
        public boolean isAggregate();

        /**
         * Adds the current row of the specified <code>ResultSetBuilder</code>.
         *
         * @param protocolVersion
         * @param rs the <code>ResultSetBuilder</code>
         * @throws InvalidRequestException
         */
        public void addInputRow(ProtocolVersion protocolVersion, ResultSetBuilder rs) throws InvalidRequestException;

        public List<ByteBuffer> getOutputRow(ProtocolVersion protocolVersion) throws InvalidRequestException;

        public void reset();
    }

    // Special cased selection for when only columns are selected.
    private static class SimpleSelection extends Selection
    {
        private final boolean isWildcard;

        public SimpleSelection(CFMetaData cfm, List<ColumnDefinition> columns, boolean isWildcard)
        {
            this(cfm, columns, SelectionColumnMapping.simpleMapping(columns), isWildcard);
        }

        public SimpleSelection(CFMetaData cfm,
                               List<ColumnDefinition> columns,
                               SelectionColumnMapping metadata,
                               boolean isWildcard)
        {
            /*
             * In theory, even a simple selection could have multiple time the same column, so we
             * could filter those duplicate out of columns. But since we're very unlikely to
             * get much duplicate in practice, it's more efficient not to bother.
             */
            super(cfm, columns, metadata, false, false);
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

        protected Selectors newSelectors(QueryOptions options)
        {
            return new Selectors()
            {
                private List<ByteBuffer> current;

                public void reset()
                {
                    current = null;
                }

                public List<ByteBuffer> getOutputRow(ProtocolVersion protocolVersion)
                {
                    return current;
                }

                public void addInputRow(ProtocolVersion protocolVersion, ResultSetBuilder rs) throws InvalidRequestException
                {
                    current = rs.current;
                }

                public boolean isAggregate()
                {
                    return false;
                }
            };
        }
    }

    private static class SelectionWithProcessing extends Selection
    {
        private final SelectorFactories factories;

        public SelectionWithProcessing(CFMetaData cfm,
                                       List<ColumnDefinition> columns,
                                       SelectionColumnMapping metadata,
                                       SelectorFactories factories) throws InvalidRequestException
        {
            super(cfm,
                  columns,
                  metadata,
                  factories.containsWritetimeSelectorFactory(),
                  factories.containsTTLSelectorFactory());

            this.factories = factories;
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            factories.addFunctionsTo(functions);
        }

        @Override
        public int getResultSetIndex(ColumnDefinition c)
        {
            int index = getColumnIndex(c);

            if (index < 0)
                return -1;

            for (int i = 0, m = factories.size(); i < m; i++)
                if (factories.get(i).isSimpleSelectorFactory(index))
                    return i;

            return -1;
        }

        @Override
        protected int addOrderingColumn(ColumnDefinition c)
        {
            int index = super.addOrderingColumn(c);
            factories.addSelectorForOrdering(c, index);
            return factories.size() - 1;
        }

        public boolean isAggregate()
        {
            return factories.doesAggregation();
        }

        protected Selectors newSelectors(final QueryOptions options) throws InvalidRequestException
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

                public List<ByteBuffer> getOutputRow(ProtocolVersion protocolVersion) throws InvalidRequestException
                {
                    List<ByteBuffer> outputRow = new ArrayList<>(selectors.size());

                    for (Selector selector: selectors)
                        outputRow.add(selector.getOutput(protocolVersion));

                    return outputRow;
                }

                public void addInputRow(ProtocolVersion protocolVersion, ResultSetBuilder rs) throws InvalidRequestException
                {
                    for (Selector selector : selectors)
                        selector.addInput(protocolVersion, rs);
                }
            };
        }

    }
}

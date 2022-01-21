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

package org.apache.cassandra.db.virtual;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.metrics.CQLMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.QueryProcessor;

// Virtual table for CQL metrics
public class CQLMetricsTable extends AbstractVirtualTable
{
    public static final String TABLE_NAME = "cql_metrics";
    public static final String PREPARED_STATEMENTS_COUNT_COL = "prepared_statements_count";
    public static final String PREPARED_STATEMENTS_EVICTED_COL = "prepared_statements_evicted";
    public static final String PREPARED_STATEMENTS_EXECUTED_COL = "prepared_statements_executed";
    public static final String PREPARED_STATEMENTS_RATIO_COL = "prepared_statements_ratio";
    public static final String REGULAR_STATEMENTS_EXECUTED_COL = "regular_statements_executed";
    public static final String NAME_COL = "name";
    public static final String VALUE_ROW = "value";

    private final CQLMetrics cqlMetrics;

    // Default constructor references query processor metrics
    CQLMetricsTable(String keyspace)
    {
        this(keyspace, QueryProcessor.metrics);
    }

    // For dependency injection
    @VisibleForTesting
    CQLMetricsTable(String keyspace, CQLMetrics cqlMetrics)
    {
        // create virtual table with this name
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME_COL, UTF8Type.instance)
                           .addRegularColumn(PREPARED_STATEMENTS_COUNT_COL, DoubleType.instance)
                           .addRegularColumn(PREPARED_STATEMENTS_EVICTED_COL, DoubleType.instance)
                           .addRegularColumn(PREPARED_STATEMENTS_EXECUTED_COL, DoubleType.instance)
                           .addRegularColumn(PREPARED_STATEMENTS_RATIO_COL, DoubleType.instance)
                           .addRegularColumn(REGULAR_STATEMENTS_EXECUTED_COL, DoubleType.instance)
                           .build());
        this.cqlMetrics = cqlMetrics;
    }

    @Override
    public DataSet data()
    {
        // populate metrics in the virtual table
        SimpleDataSet result = new SimpleDataSet(metadata());
        addRow(result, VALUE_ROW, cqlMetrics);

        return result;
    }

    private void addRow(SimpleDataSet dataSet, String name, CQLMetrics cqlMetrics)
    {
        dataSet.row(name)
               .column(PREPARED_STATEMENTS_COUNT_COL, Double.valueOf(cqlMetrics.preparedStatementsCount.getValue()))
               .column(PREPARED_STATEMENTS_EVICTED_COL, Double.valueOf(cqlMetrics.preparedStatementsEvicted.getCount()))
               .column(PREPARED_STATEMENTS_EXECUTED_COL, Double.valueOf(cqlMetrics.preparedStatementsExecuted.getCount()))
               .column(PREPARED_STATEMENTS_RATIO_COL, cqlMetrics.preparedStatementsRatio.getValue())
               .column(REGULAR_STATEMENTS_EXECUTED_COL, Double.valueOf(cqlMetrics.regularStatementsExecuted.getCount()));
    }
}

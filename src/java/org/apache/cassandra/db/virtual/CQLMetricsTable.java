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


final class CQLMetricsTable extends AbstractVirtualTable
{
    public static final String TABLE_NAME = "cql_metrics";
    public static final String PREPARED_STATEMENTS_COUNT = "prepared_statements_count";
    public static final String PREPARED_STATEMENTS_EVICTED = "prepared_statements_evicted";
    public static final String PREPARED_STATEMENTS_EXECUTED = "prepared_statements_executed";
    public static final String PREPARED_STATEMENTS_RATIO = "prepared_statements_ratio";
    public static final String REGULAR_STATEMENTS_EXECUTED = "regular_statements_executed";
    public static final String NAME_COL = "name";
    public static final String VALUE_COL = "value";

    private final CQLMetrics cqlMetrics;

    CQLMetricsTable(String keyspace)
    {
        this(keyspace, QueryProcessor.metrics);
    }

    // For dependency injection
    @VisibleForTesting
    CQLMetricsTable(String keyspace, CQLMetrics cqlMetrics)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment("Metrics specific to CQL prepared statement caching")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME_COL, UTF8Type.instance)
                           .addRegularColumn(VALUE_COL, DoubleType.instance)
                           .build());
        this.cqlMetrics = cqlMetrics;
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        addRow(result, PREPARED_STATEMENTS_COUNT, cqlMetrics.preparedStatementsCount.getValue());
        addRow(result, PREPARED_STATEMENTS_EVICTED, cqlMetrics.preparedStatementsEvicted.getCount());
        addRow(result, PREPARED_STATEMENTS_EXECUTED, cqlMetrics.preparedStatementsExecuted.getCount());
        addRow(result, PREPARED_STATEMENTS_RATIO, cqlMetrics.preparedStatementsRatio.getValue());
        addRow(result, REGULAR_STATEMENTS_EXECUTED, cqlMetrics.regularStatementsExecuted.getCount());

        return result;
    }

    private void addRow(SimpleDataSet dataSet, String name, double value)
    {
        dataSet.row(name)
               .column(VALUE_COL, value);
    }
}

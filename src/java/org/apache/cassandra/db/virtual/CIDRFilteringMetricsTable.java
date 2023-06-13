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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Snapshot;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.metrics.CIDRAuthorizerMetrics;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.service.QueryState.forInternalCalls;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Virtual tables capturing metrics related to CIDR filtering
 */
public class CIDRFilteringMetricsTable implements CIDRFilteringMetricsTableMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=CIDRFilteringMetricsTable";

    private static final CIDRFilteringMetricsTable instance = new CIDRFilteringMetricsTable();

    CIDRFilteringMetricsTable()
    {
        // Register mbean for nodetool to access vtable entries
        if (!MBeanWrapper.instance.isRegistered(MBEAN_NAME))
            MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    public static Collection<VirtualTable> getAll(String keyspace)
    {
        return Arrays.asList(
        new CIDRFilteringMetricsCountsTable(keyspace),
        new CIDRFilteringMetricsLatenciesTable(keyspace)
        );
    }

    /**
     * Virtual table capturing counts i.e, non-latency metrics related to CIDR filtering
     */
    public static class CIDRFilteringMetricsCountsTable extends AbstractVirtualTable
    {
        public static final String TABLE_NAME = "cidr_filtering_metrics_counts";

        public static final String NAME_COL = "name";
        public static final String VALUE_COL = "value";
        public static final String CIDR_ACCESSES_ACCEPTED_COUNT_NAME_PREFIX = "Number of CIDR accesses accepted from CIDR group - ";
        public static final String CIDR_ACCESSES_REJECTED_COUNT_NAME_PREFIX = "Number of CIDR accesses rejected from CIDR group - ";
        public static final String CIDR_GROUPS_CACHE_RELOAD_COUNT_NAME = "CIDR groups cache reload count";

        @VisibleForTesting
        CIDRFilteringMetricsCountsTable(String keyspace)
        {
            super(TableMetadata.builder(keyspace, TABLE_NAME)
                               .comment("Count metrics specific to CIDR filtering")
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .partitioner(new LocalPartitioner(UTF8Type.instance))
                               .addPartitionKeyColumn(NAME_COL, UTF8Type.instance)
                               .addRegularColumn(VALUE_COL, LongType.instance)
                               .build());
        }

        private void addRow(SimpleDataSet dataSet, String name, long value)
        {
            dataSet.row(name)
                   .column(VALUE_COL, value);
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());

            CIDRAuthorizerMetrics cidrAuthorizerMetrics = DatabaseDescriptor.getCIDRAuthorizer().getCidrAuthorizerMetrics();

            for (Map.Entry<String, Counter> entry : cidrAuthorizerMetrics.acceptedCidrAccessCount.entrySet())
            {
                addRow(result, CIDR_ACCESSES_ACCEPTED_COUNT_NAME_PREFIX + entry.getKey(),
                       entry.getValue().getCount());
            }

            for (Map.Entry<String, Counter> entry : cidrAuthorizerMetrics.rejectedCidrAccessCount.entrySet())
            {
                addRow(result, CIDR_ACCESSES_REJECTED_COUNT_NAME_PREFIX + entry.getKey(),
                       entry.getValue().getCount());
            }

            addRow(result, CIDR_GROUPS_CACHE_RELOAD_COUNT_NAME, cidrAuthorizerMetrics.cacheReloadCount.getCount());

            return result;
        }
    }

    /**
     * Virtual table capturing latency metrics related to CIDR filtering
     */
    public static class CIDRFilteringMetricsLatenciesTable extends AbstractVirtualTable
    {
        public static final String TABLE_NAME = "cidr_filtering_metrics_latencies";

        public static final String NAME_COL = "name";
        public static final String P50_COL = "p50th";
        public static final String P95_COL = "p95th";
        public static final String P99_COL = "p99th";
        public static final String P999_COL = "p999th";
        public static final String MAX_COL = "max";

        public static final String CIDR_CHECKS_LATENCY_NAME = "CIDR checks latency (ns)";
        public static final String CIDR_GROUPS_CACHE_RELOAD_LATENCY_NAME = "CIDR groups cache reload latency (ns)";
        public static final String LOOKUP_CIDR_GROUPS_FOR_IP_LATENCY_NAME = "Lookup IP in CIDR groups cache latency (ns)";

        @VisibleForTesting
        CIDRFilteringMetricsLatenciesTable(String keyspace)
        {
            super(TableMetadata.builder(keyspace, TABLE_NAME)
                               .comment("Latency metrics specific to CIDR filtering")
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .partitioner(new LocalPartitioner(UTF8Type.instance))
                               .addPartitionKeyColumn(NAME_COL, UTF8Type.instance)
                               .addRegularColumn(P50_COL, DoubleType.instance)
                               .addRegularColumn(P95_COL, DoubleType.instance)
                               .addRegularColumn(P99_COL, DoubleType.instance)
                               .addRegularColumn(P999_COL, DoubleType.instance)
                               .addRegularColumn(MAX_COL, DoubleType.instance)
                               .build());
        }

        private void addRow(SimpleDataSet dataSet, String name, Snapshot snapshot)
        {
            dataSet.row(name)
                   .column(P50_COL, snapshot.getMedian())
                   .column(P95_COL, snapshot.get95thPercentile())
                   .column(P99_COL, snapshot.get99thPercentile())
                   .column(P999_COL, snapshot.get999thPercentile())
                   .column(MAX_COL, (double) snapshot.getMax());
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());

            CIDRAuthorizerMetrics cidrAuthorizerMetrics =
            DatabaseDescriptor.getCIDRAuthorizer().getCidrAuthorizerMetrics();

            addRow(result, CIDR_CHECKS_LATENCY_NAME, cidrAuthorizerMetrics.cidrChecksLatency.getSnapshot());
            addRow(result, CIDR_GROUPS_CACHE_RELOAD_LATENCY_NAME,
                   cidrAuthorizerMetrics.cacheReloadLatency.getSnapshot());
            addRow(result, LOOKUP_CIDR_GROUPS_FOR_IP_LATENCY_NAME,
                   cidrAuthorizerMetrics.lookupCidrGroupsForIpLatency.getSnapshot());

            return result;
        }
    }

    private UntypedResultSet retrieveRows(SelectStatement statement)
    {
        QueryOptions options = QueryOptions.forInternalCalls(CassandraAuthorizer.authReadConsistencyLevel(),
                                                             Collections.emptyList());

        ResultMessage.Rows rows = statement.execute(forInternalCalls(), options, nanoTime());
        return UntypedResultSet.create(rows.result);
    }

    public Map<String, Long> getCountsMetricsFromVtable()
    {
        String countsMetricsTableName = SchemaConstants.VIRTUAL_VIEWS + '.' +
                                        CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable.TABLE_NAME;

        SelectStatement getCountsMetricsStatement =
            (SelectStatement) QueryProcessor.getStatement(String.format("SELECT * FROM %s", countsMetricsTableName),
                                                          ClientState.forInternalCalls());

        Map<String, Long> metrics = new HashMap<>();

        UntypedResultSet result = retrieveRows(getCountsMetricsStatement);
        for (UntypedResultSet.Row row : result)
        {
            if (!row.has(CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable.NAME_COL) ||
                !row.has(CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable.VALUE_COL))
                throw new RuntimeException("Invalid row " + row + " in table: " + countsMetricsTableName);

            metrics.put(row.getString(CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable.NAME_COL),
                        row.getLong(CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable.VALUE_COL));
        }

        return metrics;
    }

    public Map<String, List<Double>> getLatenciesMetricsFromVtable()
    {
        String latenciesMetricsTableName = SchemaConstants.VIRTUAL_VIEWS + '.' +
                                           CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable.TABLE_NAME;

        SelectStatement getLatenciesMetricsStatement =
            (SelectStatement) QueryProcessor.getStatement(String.format("SELECT * FROM %s", latenciesMetricsTableName),
                                                          ClientState.forInternalCalls());

        Map<String, List<Double>> metrics = new HashMap<>();

        UntypedResultSet result = retrieveRows(getLatenciesMetricsStatement);
        for (UntypedResultSet.Row row : result)
        {
            if (!row.has(CIDRFilteringMetricsLatenciesTable.NAME_COL) ||
                !row.has(CIDRFilteringMetricsLatenciesTable.P50_COL))
                throw new RuntimeException("Invalid row " + row + " in table: " + latenciesMetricsTableName);

            metrics.put(row.getString(CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable.NAME_COL),
                        Arrays.asList(row.getDouble(CIDRFilteringMetricsLatenciesTable.P50_COL),
                                      row.getDouble(CIDRFilteringMetricsLatenciesTable.P95_COL),
                                      row.getDouble(CIDRFilteringMetricsLatenciesTable.P99_COL),
                                      row.getDouble(CIDRFilteringMetricsLatenciesTable.P999_COL),
                                      row.getDouble(CIDRFilteringMetricsLatenciesTable.MAX_COL)));
        }

        return metrics;
    }
}

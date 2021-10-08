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
package org.apache.cassandra.metrics;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Counter;
import com.codahale.metrics.DerivativeGauge;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AbstractTableOperation;
import org.apache.cassandra.db.compaction.CompactionAggregateStatistics;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionStrategyStatistics;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.schema.SchemaManager;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for the compaction executor. Note that several different operations execute on the compaction
 * executor, for example index or view building. These operations are abstracted by {@link AbstractTableOperation}
 * but previously we would refer to these operations as "compactions", so this incorrect name may still be
 * found in the metrics that are exported to the users.
 */
public class CompactionMetrics
{
    public static final MetricNameFactory factory = new DefaultNameFactory("Compaction");

    /** Estimated number of compactions remaining to perform */
    public final Gauge<Integer> pendingTasks;
    /** Estimated number of compactions remaining to perform, group by keyspace and then table name */
    public final Gauge<Map<String, Map<String, Integer>>> pendingTasksByTableName;

    /** Write amplification of compactions (bytes compacted / bytes flushed), group by keyspace and then table name */
    public final Gauge<Map<String, Map<String, Double>>> writeAmplificationByTableName;

    /** Number of completed operations since server [re]start */
    public final Gauge<Long> completedTasks;
    /** Total number of operations since server [re]start */
    public final Meter totalCompactionsCompleted;
    /** Total number of bytes processed by operations since server [re]start */
    public final Counter bytesCompacted;

    /**
     * The compaction strategy information for each table. Cached, because its computation might be fairly expensive.
     */
    public final CachedGauge<List<CompactionStrategyStatistics>> aggregateCompactions;

    /*
     * The compaction metrics below are derivatives of the complex compaction statistics metric aggregateCompactions.
     */

    /** Number of currently running compactions for all tables */
    public final DerivativeGauge<List<CompactionStrategyStatistics>, Integer> runningCompactions;
    /** Mean read throughput of currently running compactions in bytes per second */
    public final DerivativeGauge<List<CompactionStrategyStatistics>, Double> meanCompactionReadThroughput;
    /** Mean write throughput of currently running compactions in bytes per second */
    public final DerivativeGauge<List<CompactionStrategyStatistics>, Double> meanCompactionWriteThroughput;
    /** Total bytes to compact from currently running compactions */
    public final DerivativeGauge<List<CompactionStrategyStatistics>, Long> runningCompactionsTotalBytes;
    /** Remaining bytes to compact from currently running compactions */
    public final DerivativeGauge<List<CompactionStrategyStatistics>, Long> runningCompactionsRemainingBytes;

    /** Total number of compactions that have had sstables drop out of them */
    public final Counter compactionsReduced;

    /** Total number of sstables that have been dropped out */
    public final Counter sstablesDropppedFromCompactions;

    /** Total number of compactions which have outright failed due to lack of disk space */
    public final Counter compactionsAborted;

    /** Total number of deleted expired SSTables */
    public final Meter removedExpiredSSTables;
    /** Total number compactions that consisted of only expired SSTables */
    public final Meter deleteOnlyCompactions;

    public CompactionMetrics(final ThreadPoolExecutor... collectors)
    {
        pendingTasks = Metrics.register(factory.createMetricName("PendingTasks"), () -> {
            int n = 0;
            // add estimate number of compactions need to be done
            for (String keyspaceName : SchemaManager.instance.getKeyspaces())
            {
                for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
                    n += cfs.getCompactionStrategy().getEstimatedRemainingTasks();
            }
            // add number of currently running compactions
            return n + CompactionManager.instance.active.getTableOperations().size();
        });

        pendingTasksByTableName = Metrics.register(factory.createMetricName("PendingTasksByTableName"), () -> {
            Map<String, Map<String, Integer>> resultMap = new HashMap<>();
            // estimation of compactions need to be done
            for (String keyspaceName : SchemaManager.instance.getKeyspaces())
            {
                for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
                {
                    int taskNumber = cfs.getCompactionStrategy().getEstimatedRemainingTasks();
                    if (taskNumber > 0)
                    {
                        if (!resultMap.containsKey(keyspaceName))
                        {
                            resultMap.put(keyspaceName, new HashMap<>());
                        }
                        resultMap.get(keyspaceName).put(cfs.getTableName(), taskNumber);
                    }
                }
            }

            // currently running compactions
            // TODO DB-2701 - this includes all operations (previous behaviour), if we wanted only real
            // compactions we could remove this block of code and call getTotalCompactions() from the strategy managers
            for (TableOperation op : CompactionManager.instance.active.getTableOperations())
            {
                TableMetadata metaData = op.getProgress().metadata();
                if (metaData == null)
                {
                    continue;
                }
                if (!resultMap.containsKey(metaData.keyspace))
                {
                    resultMap.put(metaData.keyspace, new HashMap<>());
                }

                Map<String, Integer> tableNameToCountMap = resultMap.get(metaData.keyspace);
                if (tableNameToCountMap.containsKey(metaData.name))
                {
                    tableNameToCountMap.put(metaData.name,
                                            tableNameToCountMap.get(metaData.name) + 1);
                }
                else
                {
                    tableNameToCountMap.put(metaData.name, 1);
                }
            }
            return resultMap;
        });

        writeAmplificationByTableName = Metrics.register(factory.createMetricName("WriteAmplificationByTableName"), () -> {
            Map<String, Map<String, Double>> resultMap = new HashMap<>();

            for (String keyspaceName : SchemaManager.instance.getKeyspaces())
            {
                Map<String, Double> ksMap = new HashMap<>();
                resultMap.put(keyspaceName, ksMap);
                for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
                    ksMap.put(cfs.getTableName(), cfs.getWA());
            }

            return resultMap;
        });

        completedTasks = Metrics.register(factory.createMetricName("CompletedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                long completedTasks = 0;
                for (ThreadPoolExecutor collector : collectors)
                    completedTasks += collector.getCompletedTaskCount();
                return completedTasks;
            }
        });
        totalCompactionsCompleted = Metrics.meter(factory.createMetricName("TotalCompactionsCompleted"));
        bytesCompacted = Metrics.counter(factory.createMetricName("BytesCompacted"));

        // compaction failure metrics
        compactionsReduced = Metrics.counter(factory.createMetricName("CompactionsReduced"));
        sstablesDropppedFromCompactions = Metrics.counter(factory.createMetricName("SSTablesDroppedFromCompaction"));
        compactionsAborted = Metrics.counter(factory.createMetricName("CompactionsAborted"));

        removedExpiredSSTables = Metrics.meter(factory.createMetricName("ExpiredSSTablesDropped"));
        deleteOnlyCompactions = Metrics.meter(factory.createMetricName("DeleteOnlyCompactions"));

        aggregateCompactions = Metrics.register(factory.createMetricName("AggregateCompactions"),
                                                // TODO 50 ms is 100x less than the default report interval of our distributed test harness (Fallout) at
                                                //  the moment of writing this. This implies that even a bigger timeout might be OK.
                                                new CachedGauge<List<CompactionStrategyStatistics>>(50, TimeUnit.MILLISECONDS)
                                                {
                                                    @Override
                                                    protected List<CompactionStrategyStatistics> loadValue()
                                                    {
                                                        List<CompactionStrategyStatistics> ret = new ArrayList<>();
                                                        for (String keyspaceName : SchemaManager.instance.getKeyspaces())
                                                        {
                                                            // Scan all the compactions strategies of all tables and find those that have compactions in progress.
                                                            for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
                                                                // For those return the statistics.
                                                                ret.addAll(cfs.getCompactionStrategy().getStatistics());
                                                        }

                                                        return ret;
                                                    }
                                                });

        runningCompactions = Metrics.register(factory.createMetricName("RunningCompactions"),
                                              new DerivativeGauge<List<CompactionStrategyStatistics>, Integer>(aggregateCompactions)
        {
            @Override
            protected Integer transform(List<CompactionStrategyStatistics> value)
            {
                return deriveSafeAggregateStatisticsStream(value)
                       .mapToInt(CompactionAggregateStatistics::numCompactionsInProgress)
                       .sum();
            }
        });
        meanCompactionReadThroughput = Metrics.register(factory.createMetricName("MeanCompactionReadThroughput"),
                                                        new DerivativeGauge<List<CompactionStrategyStatistics>, Double>(aggregateCompactions)
        {
            @Override
            protected Double transform(List<CompactionStrategyStatistics> value)
            {
                return deriveSafeAggregateStatisticsStream(value)
                       // Don't take into account aggregates for which there are no running compactions
                       .filter(s -> s.numCompactionsInProgress() > 0)
                       .mapToDouble(CompactionAggregateStatistics::readThroughput)
                       .average()
                       .orElse(0.0);
            }
        });
        meanCompactionWriteThroughput = Metrics.register(factory.createMetricName("MeanCompactionWriteThroughput"),
                                                         new DerivativeGauge<List<CompactionStrategyStatistics>, Double>(aggregateCompactions)
        {
            @Override
            protected Double transform(List<CompactionStrategyStatistics> value)
            {
                return deriveSafeAggregateStatisticsStream(value)
                       // Don't take into account aggregates for which there are no running compactions
                       .filter(s -> s.numCompactionsInProgress() > 0)
                       .mapToDouble(CompactionAggregateStatistics::writeThroughput)
                       .average()
                       .orElse(0.0);
            }
        });
        runningCompactionsTotalBytes = Metrics.register(factory.createMetricName("RunningCompactionsTotalBytes"),
                                                        new DerivativeGauge<List<CompactionStrategyStatistics>, Long>(aggregateCompactions)
        {
            @Override
            protected Long transform(List<CompactionStrategyStatistics> value)
            {
                return deriveSafeAggregateStatisticsStream(value)
                       .mapToLong(CompactionAggregateStatistics::tot)
                       .sum();
            }
        });
        runningCompactionsRemainingBytes = Metrics.register(factory.createMetricName("RunningCompactionsRemainingBytes"),
                                                            new DerivativeGauge<List<CompactionStrategyStatistics>, Long>(aggregateCompactions)
        {
            @Override
            protected Long transform(List<CompactionStrategyStatistics> value)
            {
                return deriveSafeAggregateStatisticsStream(value)
                       .mapToLong(s -> s.tot() - s.read())
                       .sum();
            }
        });
    }

    /**
     * Needed because deriving from a CachedGauge might hit a NullPointerException until we move to a version of
     * dropwizard's metrics-core where https://github.com/dropwizard/metrics/pull/711 /
     * https://github.com/dropwizard/metrics/pull/1566 are fixed (currently targeting metrics-core 4.1.7).
     *
     * @param aggregateCompactions The cached compaction strategy statistics to derive from.
     *
     * @return A stream (potentially empty) of the aggregate statistics corresponding to the given strategy statistics.
     */
    private static Stream<CompactionAggregateStatistics> deriveSafeAggregateStatisticsStream(List<CompactionStrategyStatistics> aggregateCompactions)
    {
        if (aggregateCompactions == null)
            return Stream.empty();
        return aggregateCompactions.stream().flatMap(s -> s.aggregates().stream());
    }
}

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

package org.apache.cassandra.service;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.utils.FBUtilities;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryAnalyticsService
{
    @VisibleForTesting
    protected QueryAnalyticsConfig config;
    public static final QueryAnalyticsService instance = new QueryAnalyticsService();
    private String hostName;
    private String DC;
    private static final Logger logger = LoggerFactory.getLogger(QueryAnalyticsService.class);
    @VisibleForTesting
    protected  QueryAnalyticsDataProducer dataProducer;

    public static void setup() throws IOException
    {
        instance.config = DatabaseDescriptor.getQueryAnalyticsConfig();
        try {
            if (instance.config.getProducer() != null && instance.config.getProducer().class_name != null) {
                instance.dataProducer = createDataProducer(instance.config.getProducer().class_name);
            } else {
                instance.dataProducer = null;
            }
        } catch (Exception e) {
            logger.warn("Failed to setup QueryAnalyticsDataProducer: {}", e.getMessage());
            instance.dataProducer = null;
        }
        instance.hostName = FBUtilities.getLocalAddressAndPort().getHostName().split("\\.")[0];

        if (instance.hostName != null)
        {
            instance.DC = instance.hostName.split("-")[0];
        }
    }

    @VisibleForTesting
    protected QueryAnalyticsService()
    {
    }

    public void processLatencyMetric(String metricName, String latency, SinglePartitionReadCommand command)
    {
        if (config == null || !config.isQueryAnalyticsEnabled())
        {
            return;
        }

        if (command == null || command.metadata() == null || command.partitionKey() == null)
        {
            return;
        }

        //For backwards compatibility, add the metric name as a property. This will be removed when we switch to v2 schema.
        Map<String, Object> properties = new HashMap<>();
        properties.put("metric_name", metricName);

        processLatencyMetric(latency, String.valueOf(command.partitionKey()), command.metadata().keyspace, command.metadata().name, properties);
    }

    private void processLatencyMetric(String latency, String partitionKey, String keyspace, String tableName)
    {
        processLatencyMetric(latency, partitionKey, keyspace, tableName, null);
    }

    private void processLatencyMetric(String latency, String partitionKey, String keyspace, String tableName, Map<String, Object> properties)
    {
        try
        {
            Long nanoTimeMetric = FBUtilities.timestampMicros();
            QueryAnalyticsDatapoint datapoint = createDatapoint(
            tableName, nanoTimeMetric, keyspace, partitionKey, latency, DC, hostName, DatabaseDescriptor.getClusterName(), properties);

            if (dataProducer != null) {
                dataProducer.produceDatapoint(datapoint);
            }
        }
        catch (Exception e)
        {
            logger.error("Error processing latency metrics, error: ", e);
        }
    }

    @VisibleForTesting
    protected static QueryAnalyticsDataProducer createDataProducer(String clazz) {
        if (clazz == null || clazz.trim().isEmpty()) {
            logger.warn("No producer class specified - query analytics disabled");
            return null;
        }

        try {
            Class<?> clazzObj = Class.forName(clazz);
            if (!QueryAnalyticsDataProducer.class.isAssignableFrom(clazzObj)) {
                logger.error("{} does not implement QueryAnalyticsDataProducer", clazz);
                return null;
            }

            Map<String, String> options = new HashMap<>();

            if (instance.config.getProducer() != null) {
                options.putAll(instance.config.getProducer().parameters);
            }

            if (!options.containsKey("enabled")) {
                options.put("enabled", instance.config.isQueryAnalyticsEnabled().toString());
            }
            if (!options.containsKey("logs_enabled")) {
                options.put("logs_enabled", instance.config.getLogsEnabled().toString());
            }

            logger.info("Initializing QueryAnalyticsDataProducer {} with options: {}", clazz, options);

            java.lang.reflect.Constructor<?> constructor = clazzObj.getConstructor(Map.class);
            QueryAnalyticsDataProducer producer = (QueryAnalyticsDataProducer) constructor.newInstance(options);

            return producer;
        } catch (ClassNotFoundException e) {
            logger.info("{} not available - query analytics disabled", clazz);
            return null;
        } catch (Exception e) {
            logger.error("Failed to create {}: {}", clazz, e.getMessage());
            return null;
        }
    }

    @VisibleForTesting
    protected static QueryAnalyticsDatapoint createDatapoint(String tableName, Long timestamp, String keyspace, String partition, String latency, String DC, String hostName, String clusterName)
    {
        return createDatapoint(tableName, timestamp, keyspace, partition, latency, DC, hostName, clusterName, null);
    }

    @VisibleForTesting
    protected static QueryAnalyticsDatapoint createDatapoint(String tableName, Long timestamp, String keyspace, String partition, String latency, String DC, String hostName, String clusterName, Map<String, Object> properties)
    {
        // Convert the latency string to Long
        Long latencyValue = null;
        try {
            latencyValue = Long.valueOf(latency);
        } catch (NumberFormatException e) {
            latencyValue = 0L;
        }

        QueryAnalyticsDatapoint.Builder builder = QueryAnalyticsDatapoint.builder()
            .instance(clusterName)  // instance = cluster for now
            .cluster("TODO")        // cluster - not currently ingested
            .host(hostName)
            .keyspace(keyspace)
            .table(tableName)
            .partition(partition)
            .timestamp(timestamp)
            .latency(latencyValue);

        if (DC != null) {
            builder.DC(DC);
        }

        if (properties != null) {
            builder.properties(properties);
        }

        return builder.build();
    }
}

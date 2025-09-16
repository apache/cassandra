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
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

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
    private static final Logger logger = LoggerFactory.getLogger(QueryAnalyticsService.class);
    @VisibleForTesting
    protected  QueryAnalyticsDataProducer dataProducer;
    
    @VisibleForTesting
    private String hostName;
    @VisibleForTesting
    private String DC;

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

    public void processLatencyMetric(long latency, SinglePartitionReadCommand command)
    {
        if (config == null || !config.isQueryAnalyticsEnabled())
        {
            return;
        }

        if (command == null || command.metadata() == null || command.partitionKey() == null)
        {
            return;
        }

        processLatencyMetric(latency, command.partitionKey().toCQLString(command.metadata()), command.metadata().keyspace, command.metadata().name, null);
    }

    private void processLatencyMetric(long latency, String partitionKey, String keyspace, String tableName)
    {
        processLatencyMetric(latency, partitionKey, keyspace, tableName, null);
    }

    private void processLatencyMetric(long latency, String partitionKey, String keyspace, String tableName, Map<String, Object> properties)
    {
        try
        {
            Long timestamp = currentTimeMillis();
            QueryAnalyticsDatapoint datapoint = createDatapoint(
            tableName, timestamp, keyspace, partitionKey, latency, DC, hostName, properties);

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
    protected static QueryAnalyticsDatapoint createDatapoint(String tableName, Long timestamp, String keyspace, String partition, long latency, String DC, String hostName)
    {
        return createDatapoint(tableName, timestamp, keyspace, partition, latency, DC, hostName, null);
    }

    @VisibleForTesting
    protected static QueryAnalyticsDatapoint createDatapoint(String tableName, Long timestamp, String keyspace, String partition, long latency, String DC, String hostName, Map<String, Object> properties)
    {
        QueryAnalyticsDatapoint.Builder builder = QueryAnalyticsDatapoint.builder()
            .host(hostName)
            .keyspace(keyspace)
            .table(tableName)
            .partition(partition)
            .timestamp(timestamp)
            .latency(latency);

        if (DC != null) {
            builder.DC(DC);
        }

        if (properties != null) {
            builder.properties(properties);
        }

        return builder.build();
    }
}

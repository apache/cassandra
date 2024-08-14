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

import com.uber.stream.java.kafka.rest.client.KafkaRestClientException;
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

    public static void setup() throws IOException, KafkaRestClientException
    {
        instance.config = DatabaseDescriptor.getQueryAnalyticsConfig();
        KafkaHandler.setup();
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

        processLatencyMetric(metricName, latency, String.valueOf(command.partitionKey()), command.metadata().keyspace, command.metadata().name);
    }

    private void processLatencyMetric(String metricName, String latency, String partitionKey, String keyspace, String tableName)
    {
        try
        {
            Long nanoTimeMetric = FBUtilities.timestampMicros();
            Map<String, Object> dataMap = createDataMap(
            tableName, nanoTimeMetric, keyspace, metricName, partitionKey, latency, DC, hostName, DatabaseDescriptor.getClusterName());

            KafkaHandler.instance.sendToKafka(dataMap);
        }
        catch (Exception e)
        {
            logger.error("Error processing latency metrics, error: ", e);
        }
    }

    @VisibleForTesting
    protected static Map<String, Object> createDataMap(String tableName, Long nanoTimeMetric, String keyspace, String metricName, String token, String value, String DC, String hostName, String clusterName)
    {
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("table", tableName);
        dataMap.put("nano_time", nanoTimeMetric);
        dataMap.put("keyspace", keyspace);
        dataMap.put("name", metricName);
        dataMap.put("value", value);
        dataMap.put("token", token);
        dataMap.put("dc", DC);
        dataMap.put("host", hostName);
        dataMap.put("cluster", clusterName); // TODO: change field to instance
        return dataMap;
    }
}


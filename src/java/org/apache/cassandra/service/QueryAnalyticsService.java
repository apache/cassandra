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
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.MonotonicClock;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryAnalyticsService implements QueryAnalyticsServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=QueryAnalyticsService";
    
    public static final QueryAnalyticsService instance = new QueryAnalyticsService();
    private static final Logger logger = LoggerFactory.getLogger(QueryAnalyticsService.class);
    @VisibleForTesting
    protected static QueryAnalyticsDataProducer dataProducer;
    
    static
    {
        MBeanWrapper.instance.registerMBean(instance, MBEAN_NAME);
    }
    
    public QueryAnalyticsConfig getQueryAnalyticsConfig()
    {
        return DatabaseDescriptor.getQueryAnalyticsConfig();
    }
    
    public static void setup()
    {
        QueryAnalyticsConfig currentConfig = DatabaseDescriptor.getQueryAnalyticsConfig();
        
        // Initialize producer if enabled and configured
        if (currentConfig != null && currentConfig.isQueryAnalyticsEnabled()) {
            if (currentConfig.getProducer() != null && currentConfig.getProducer().class_name != null) {
                try {
                    dataProducer = createDataProducer(currentConfig.getProducer().class_name);
                    logger.info("QueryAnalytics setup complete - producer initialized");
                } catch (Exception e) {
                    logger.warn("Failed to initialize QueryAnalytics producer: {}", e.getMessage());
                }
            } else {
                logger.warn("QueryAnalytics setup complete - no producer configured, metrics will not be sent");
            }
        } else {
            logger.info("QueryAnalytics setup complete - disabled");
        }
    }

    @VisibleForTesting
    protected QueryAnalyticsService()
    {
    }

    /**
     * Check if this metric should be sampled (internal use only)
     * @return true if the metric should be processed, false if it should be sampled out
     */
    private boolean shouldSample()
    {
        QueryAnalyticsConfig config = getQueryAnalyticsConfig();
        double samplingRatio = config.getSamplingRatio();
        return ThreadLocalRandom.current().nextDouble() < samplingRatio;
    }

    /**
     * Main method to process query latency metrics
     */
    public void processLatencyMetric(long latency, SinglePartitionReadCommand command, ReadResponse response)
    {
        if (command == null || command.metadata() == null || command.partitionKey() == null)
        {
            return;
        }
        
        if (!isQueryAnalyticsEnabled())
        {
            logger.debug("QueryAnalytics disabled - skipping metric processing");
            return;
        }

        if (!shouldSample())
        {
            logger.trace("QueryAnalytics metric sampled out");
            return;
        }

        try
        {
            // Calculate payload sizes with fallback
            long requestPayloadSize = 0;
            long responsePayloadSize = 0;
            
            try {
                requestPayloadSize = ReadCommand.serializer.serializedSize(command, MessagingService.current_version);
                responsePayloadSize = ReadResponse.serializer.serializedSize(response, MessagingService.current_version);
            } catch (Exception e) {
                logger.warn("Could not serialize request/response for payload size calculation: {}", e.getMessage());
            }
            
            String partitionKey = command.partitionKey().toCQLString(command.metadata());
            String keyspace = command.metadata().keyspace;
            String tableName = command.metadata().name;
            Long timestamp = currentTimeMillis();
            
            // Add payload sizes to properties
            Map<String, Object> properties = new HashMap<>();
            properties.put("request_payload_size", requestPayloadSize);
            properties.put("response_payload_size", responsePayloadSize);
            
            QueryAnalyticsDatapoint datapoint = createDatapoint(
                tableName, timestamp, keyspace, partitionKey, latency, properties);
            
            if (dataProducer != null)
            {
                dataProducer.produceDatapoint(datapoint);
                logger.debug("QueryAnalytics datapoint sent: keyspace={}, table={}, latency={}, partition={}, req_size={}, resp_size={}", 
                               keyspace, tableName, latency, partitionKey, requestPayloadSize, responsePayloadSize);
            }
            else
            {
                logger.debug("QueryAnalytics datapoint not sent: no producer configured");
            }
        }
        catch (Exception e)
        {
            logger.error("Error processing latency metrics, error: ", e);
        }
    }

    
    //MBean Interface Implementations
    @Override
    public void setQueryAnalyticsEnabled(boolean enabled) {
        DatabaseDescriptor.getQueryAnalyticsConfig().setEnabled(enabled);
        
        // Initialize producer if enabling
        QueryAnalyticsConfig config = DatabaseDescriptor.getQueryAnalyticsConfig();
        if (enabled && dataProducer == null && config.getProducer() != null) {
            dataProducer = createDataProducer(config.getProducer().class_name);
            logger.info("QueryAnalytics producer initialized");
        } else if (enabled && config.getProducer() == null) {
            logger.warn("QueryAnalytics enabled but no producer configured - metrics will not be sent");
        }
        
        logger.info("QueryAnalytics enabled set to: {}", enabled);
    }

    @Override
    public boolean isQueryAnalyticsEnabled() {
        QueryAnalyticsConfig config = getQueryAnalyticsConfig();
        return config != null && config.isQueryAnalyticsEnabled();
    }

    @Override
    public void setQueryAnalyticsSamplingRatio(double samplingRatio) {
        DatabaseDescriptor.getQueryAnalyticsConfig().setSamplingRatio(samplingRatio);
        logger.info("QueryAnalytics sampling ratio set to: {}", samplingRatio);
    }

    @Override
    public double getQueryAnalyticsSamplingRatio() {
        QueryAnalyticsConfig config = getQueryAnalyticsConfig();
        return config != null ? config.getSamplingRatio() : QueryAnalyticsConfig.DEFAULT_SAMPLING_RATIO;
    }

    @Override
    public String getQueryAnalyticsConfiguration() {
        QueryAnalyticsConfig config = getQueryAnalyticsConfig();
        StringBuilder sb = new StringBuilder();
        sb.append("Query Analytics Configuration:\n");
        sb.append("  enabled: ").append(config.isQueryAnalyticsEnabled()).append("\n");
        sb.append("  sampling_ratio: ").append(config.getSamplingRatio()).append("\n");
        
        if (config.getProducer() != null) {
            sb.append("  producer:\n");
            sb.append("    class_name: ").append(config.getProducer().class_name).append("\n");
            if (config.getProducer().parameters != null && !config.getProducer().parameters.isEmpty()) {
                sb.append("    parameters:\n");
                for (Map.Entry<String, String> entry : config.getProducer().parameters.entrySet()) {
                    sb.append("      ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
                }
            }
        } else {
            sb.append("  producer: none\n");
        }
        
        return sb.toString().trim();
    }

    //Helper Methods for Producer and Datapoint creation
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
                throw new RuntimeException("QueryAnalytics producer class " + clazz + " does not implement QueryAnalyticsDataProducer");
            }

            Map<String, String> options = new HashMap<>();

            // Get current configuration dynamically
            QueryAnalyticsConfig currentConfig = instance.getQueryAnalyticsConfig();
            if (currentConfig != null && currentConfig.getProducer() != null) {
                options.putAll(currentConfig.getProducer().parameters);
            }

            if (!options.containsKey("enabled")) {
                options.put("enabled", currentConfig != null ? currentConfig.isQueryAnalyticsEnabled().toString() : "false");
            }

            logger.info("Initializing QueryAnalyticsDataProducer {} with options: {}", clazz, options);

            java.lang.reflect.Constructor<?> constructor = clazzObj.getConstructor(Map.class);
            QueryAnalyticsDataProducer producer = (QueryAnalyticsDataProducer) constructor.newInstance(options);

            return producer;
        } catch (ClassNotFoundException e) {
            logger.error("QueryAnalytics producer class not found: {}", clazz);
            throw new RuntimeException("QueryAnalytics producer class not found: " + clazz, e);
        } catch (Exception e) {
            logger.error("Failed to create QueryAnalytics producer {}: {}", clazz, e.getMessage());
            throw new RuntimeException("Failed to create QueryAnalytics producer " + clazz + ": " + e.getMessage(), e);
        }
    }

    @VisibleForTesting
    protected static QueryAnalyticsDatapoint createDatapoint(String tableName, Long timestamp, String keyspace, String partition, long latency)
    {
        return createDatapoint(tableName, timestamp, keyspace, partition, latency, null);
    }

    @VisibleForTesting
    protected static QueryAnalyticsDatapoint createDatapoint(String tableName, Long timestamp, String keyspace, String partition, long latency, Map<String, Object> properties)
    {
        QueryAnalyticsDatapoint.Builder builder = QueryAnalyticsDatapoint.builder()
            .keyspace(keyspace)
            .table(tableName)
            .partition(partition)
            .timestamp(timestamp)
            .latency(latency);

        if (properties != null) {
            builder.properties(properties);
        }

        return builder.build();
    }
}

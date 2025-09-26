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
 * distributed under this work is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * Object for query analytics datapoints.
 * Uses the new schema structure but can output to both v1 and v2 schemas.
 */
public class QueryAnalyticsDatapoint implements Serializable {

    private final String instance;
    private final String cluster; // TODO: This field is not currently ingested
    private final String keyspace;
    private final String table;
    private final String partition;
    private final Long timestamp;
    private final Long latency;

    private final Map<String, Object> properties;

    private QueryAnalyticsDatapoint(Builder builder)
    {
        this.instance = builder.instance;
        this.cluster = builder.cluster;
        this.keyspace = builder.keyspace;
        this.table = builder.table;
        this.partition = builder.partition;
        this.timestamp = builder.timestamp;
        this.latency = builder.latency;
        this.properties = builder.properties;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(QueryAnalyticsDatapoint datapoint)
    {
        return new Builder().instance(datapoint.instance)
                           .cluster(datapoint.cluster)
                           .keyspace(datapoint.keyspace)
                           .table(datapoint.table)
                           .partition(datapoint.partition)
                           .timestamp(datapoint.timestamp)
                           .latency(datapoint.latency)
                           .properties(datapoint.properties);
    }

    public Builder unbuild()
    {
        return builder(this);
    }

    public String getInstance() {
        return instance;
    }

    public String getCluster() {
        return cluster;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getTable() {
        return table;
    }

    public String getPartition() {
        return partition;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Long getLatency() {
        return latency;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public Object getProperty(String key) {
        return properties.get(key);
    }

    public <T> T getProperty(String key, Class<T> type) {
        Object value = properties.get(key);
        return type.isInstance(value) ? type.cast(value) : null;
    }

    public boolean hasProperty(String key) {
        return properties.containsKey(key);
    }

    //Will be used when logs are enabled
    @Override
    public String toString() {
        return "QueryAnalyticsDatapoint{" +
               "instance='" + instance + '\'' +
               ", cluster='" + cluster + '\'' +
               ", keyspace='" + keyspace + '\'' +
               ", table='" + table + '\'' +
               ", partition='" + partition + '\'' +
               ", timestamp=" + timestamp +
               ", latency=" + latency +
               ", properties=" + properties +
               '}';
    }

    public static final class Builder
    {
        private String instance;
        private String cluster;
        private String keyspace;
        private String table;
        private String partition;
        private Long timestamp;
        private Long latency;
        private Map<String, Object> properties = ImmutableMap.of();

        public Builder()
        {
        }

        public QueryAnalyticsDatapoint build()
        {
            return new QueryAnalyticsDatapoint(this);
        }

        public Builder instance(String val)
        {
            instance = val;
            return this;
        }

        public Builder cluster(String val)
        {
            cluster = val;
            return this;
        }

        public Builder keyspace(String val)
        {
            keyspace = val;
            return this;
        }

        public Builder table(String val)
        {
            table = val;
            return this;
        }

        public Builder partition(String val)
        {
            partition = val;
            return this;
        }

        public Builder timestamp(Long val)
        {
            timestamp = val;
            return this;
        }

        public Builder latency(Long val)
        {
            latency = val;
            return this;
        }

        public Builder properties(Map<String, Object> val)
        {
            if (val == null) {
                properties = ImmutableMap.of();
            } else {
                Map<String, Object> filtered = new HashMap<>();
                for (Map.Entry<String, Object> entry : val.entrySet()) {
                    if (entry.getKey() != null && entry.getValue() != null) {
                        filtered.put(entry.getKey(), entry.getValue());
                    }
                }
                properties = ImmutableMap.copyOf(filtered);
            }
            return this;
        }
    }
}

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

public class QueryAnalyticsConfig
{
    // enable/disable Query Analytics globally, overrides all other settings
    public final Boolean enabled;

    // name of Kafka Topic where data will be sent
    public final String kafka_topic;

    // enable/disable query anaylytic data from appearing in logs
    public final Boolean logs_enabled;

    public QueryAnalyticsConfig()
    {
        this(false, "hp-cstar-qan", false);
    }

    public QueryAnalyticsConfig(Boolean enabled, String kafkaTopic, Boolean logsEnabled)
    {
        this.enabled = enabled;
        this.kafka_topic = kafkaTopic;
        this.logs_enabled = logsEnabled;
    }

    public Boolean isQueryAnalyticsEnabled()
    {
        return enabled;
    }

    public String getKafkaTopic()
    {
        return kafka_topic;
    }

    public Boolean getLogsEnabled()
    {
        return logs_enabled;
    }
}

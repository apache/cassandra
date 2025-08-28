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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.ParameterizedClass;

public class QueryAnalyticsConfig
{
    // enable/disable Query Analytics globally, overrides all other settings
    public final Boolean enabled;

    // enable/disable query analytic data from appearing in logs
    public Boolean logs_enabled;

    // producer configuration  
    public ParameterizedClass producer;

    public QueryAnalyticsConfig()
    {
        this(false, false, new ParameterizedClass("", Collections.emptyMap()));
    }

    public QueryAnalyticsConfig(Boolean enabled, Boolean logsEnabled, ParameterizedClass producer)
    {
        this.enabled = enabled;
        this.logs_enabled = logsEnabled;
        this.producer = producer;
    }

    public Boolean isQueryAnalyticsEnabled()
    {
        return enabled;
    }

    public Boolean getLogsEnabled()
    {
        return logs_enabled;
    }

    public ParameterizedClass getProducer()
    {
        return producer;
    }
}

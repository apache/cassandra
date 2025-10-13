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
    // Default sampling ratio of 1%
    public static final double DEFAULT_SAMPLING_RATIO = 0.01;
    
    // enable/disable Query Analytics globally, overrides all other settings
    public volatile Boolean enabled;

    // producer configuration  
    public volatile ParameterizedClass producer;
    
    // sampling ratio: 0.0 to 1.0 (0% to 100% of queries)
    public volatile Double sampling_ratio;

    public QueryAnalyticsConfig()
    {
        this(false, new ParameterizedClass("", Collections.emptyMap()), DEFAULT_SAMPLING_RATIO);
    }

    public QueryAnalyticsConfig(Boolean enabled, ParameterizedClass producer)
    {
        this(enabled, producer, DEFAULT_SAMPLING_RATIO);
    }

    public QueryAnalyticsConfig(Boolean enabled, ParameterizedClass producer, Double samplingRatio)
    {
        this.enabled = enabled;
        this.producer = producer;
        this.sampling_ratio = samplingRatio != null ? samplingRatio : DEFAULT_SAMPLING_RATIO;
    }

    public Boolean isQueryAnalyticsEnabled()
    {
        if (enabled == null) {
            return false;
        }
        return enabled;
    }

    public void setEnabled(Boolean enabled)
    {
        this.enabled = enabled;
    }

    public ParameterizedClass getProducer()
    {
        return producer;
    }

    public void setProducer(ParameterizedClass producer)
    {
        this.producer = producer;
    }

    public Double getSamplingRatio()
    {
        return sampling_ratio != null ? sampling_ratio : DEFAULT_SAMPLING_RATIO;
    }

    public void setSamplingRatio(Double samplingRatio)
    {
        if (samplingRatio != null && (samplingRatio < 0.0 || samplingRatio > 1.0))
        {
            throw new IllegalArgumentException("Sampling ratio must be between 0.0 and 1.0, got: " + samplingRatio);
        }
        this.sampling_ratio = samplingRatio != null ? samplingRatio : DEFAULT_SAMPLING_RATIO;
    }
}

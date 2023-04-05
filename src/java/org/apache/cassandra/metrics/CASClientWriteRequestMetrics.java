/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for tracking information about CAS write requests.
 *
 */
public class CASClientWriteRequestMetrics extends CASClientRequestMetrics
{
    /**
     * Metric for tracking the mutation sizes in bytes.
     */
    public final Histogram mutationSize;

    public final Counter conditionNotMet;

    public CASClientWriteRequestMetrics(String scope)
    {
        super(scope);
        mutationSize = Metrics.histogram(factory.createMetricName("MutationSizeHistogram"), false);
        // scope for this metric was changed in 4.0; adding backward compatibility
        conditionNotMet = Metrics.counter(factory.createMetricName("ConditionNotMet"),
                                          DefaultNameFactory.createMetricName("ClientRequest", "ConditionNotMet", "CASRead"));
    }

    public void release()
    {
        super.release();
        Metrics.remove(factory.createMetricName("ConditionNotMet"),
                       DefaultNameFactory.createMetricName("ClientRequest", "ConditionNotMet", "CASRead"));
        Metrics.remove(factory.createMetricName("MutationSizeHistogram"));
    }
}

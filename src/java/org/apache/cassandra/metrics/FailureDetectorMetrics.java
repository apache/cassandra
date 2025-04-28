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

import com.codahale.metrics.Counter;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class FailureDetectorMetrics
{
    public static final String TYPE_NAME = "FailureDetector";
    private static final MetricNameFactory factory = new DefaultNameFactory(TYPE_NAME);

    public static final Counter interpret = Metrics.counter(factory.createMetricName("Interpret"));
    public static final Counter report = Metrics.counter(factory.createMetricName("Report"));
    public static final Counter remove = Metrics.counter(factory.createMetricName("Remove"));
    public static final Counter convict = Metrics.counter(factory.createMetricName("Convict"));

}

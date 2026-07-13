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

public class ReadResponseMetrics
{
    public static final String TYPE_NAME = "ReadResponse";
    private static final MetricNameFactory factory = new DefaultNameFactory(TYPE_NAME);

    /**
     * Incremented when a local single-partition read response hit the per-request in-memory row-count limit
     */
    public static final Counter inMemoryRowLimitHits = Metrics.counter(factory.createMetricName("InMemoryRowLimitHits"));

    /**
     * Incremented when a local single-partition read response hit the per-request in-memory heap-size limit.
     * The row-count limit is checked first, so this
     * counts only responses that passed the row-count limit (or have it disabled) but crossed the size limit.
     */
    public static final Counter inMemorySizeLimitHits = Metrics.counter(factory.createMetricName("InMemorySizeLimitHits"));

    public static void init() {}
}

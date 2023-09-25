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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.transport.CQLMessageHandler;
import org.apache.cassandra.transport.ServerError;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class TransportMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("TransportMessage");

    // Metrics for all coded exceptions collected in QuerryMessage.java
    public static final Counter cdcWriteFailureExceptionCount =
    Metrics.counter(factory.createMetricName("CdcWriteFailureExceptionCount"));
    public static final Counter writeTimeoutExceptionCount =
    Metrics.counter(factory.createMetricName("WriteTimeoutExceptionCount"));
    public static final Counter casWriteUnknownExceptionCount =
    Metrics.counter(factory.createMetricName("CasWriteUnknownExceptionCount"));
    public static final Counter functionFailureExceptionCount =
    Metrics.counter(factory.createMetricName("FunctionFailureExceptionCount"));
    public static final Counter isBootstrappingExceptionCount =
    Metrics.counter(factory.createMetricName("IsBootstrappingExceptionCount"));
    public static final Counter overloadedExceptionCount =
    Metrics.counter(factory.createMetricName("OverloadedExceptionCount"));
    public static final Counter readFailureExceptionCount =
    Metrics.counter(factory.createMetricName("ReadFailureExceptionCount"));
    public static final Counter readTimeoutExceptionCount =
    Metrics.counter(factory.createMetricName("ReadTimeoutExceptionCount"));
    public static final Counter truncateErrorExceptionCount =
    Metrics.counter(factory.createMetricName("TruncateErrorExceptionCount"));
    public static final Counter unavailableExceptionCount =
    Metrics.counter(factory.createMetricName("UnavailableExceptionCount"));
    public static final Counter writeFailureExceptionCount =
    Metrics.counter(factory.createMetricName("WriteFailureExceptionCount"));
    public static final Counter serverErrorExceptionCount =
    Metrics.counter(factory.createMetricName("ServerErrorExceptionCount"));
    public static final Counter otherExceptionCount = Metrics.counter(factory.createMetricName("OtherExceptionCount"));
}

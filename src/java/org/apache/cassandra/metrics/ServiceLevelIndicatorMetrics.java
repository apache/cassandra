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

import com.codahale.metrics.Meter;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class ServiceLevelIndicatorMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("ServiceLevelIndicator");

    // Metrics for all coded exceptions collected in QuerryMessage.java
    public static final Meter cdcWriteFailureExceptionMetrics =
    Metrics.meter(factory.createMetricName("CdcWriteFailureExceptionMetrics"));
    public static final Meter writeTimeoutExceptionMetrics =
    Metrics.meter(factory.createMetricName("WriteTimeoutExceptionMetrics"));
    public static final Meter casWriteUnknownExceptionMetrics =
    Metrics.meter(factory.createMetricName("CasWriteUnknownExceptionMetrics"));
    public static final Meter functionFailureExceptionMetrics =
    Metrics.meter(factory.createMetricName("FunctionFailureExceptionMetrics"));
    public static final Meter isBootstrappingExceptionMetrics =
    Metrics.meter(factory.createMetricName("IsBootstrappingExceptionMetrics"));
    public static final Meter overloadedExceptionMetrics =
    Metrics.meter(factory.createMetricName("OverloadedExceptionMetrics"));
    public static final Meter readFailureExceptionMetrics =
    Metrics.meter(factory.createMetricName("ReadFailureExceptionMetrics"));
    public static final Meter readTimeoutExceptionMetrics =
    Metrics.meter(factory.createMetricName("ReadTimeoutExceptionMetrics"));
    public static final Meter truncateErrorExceptionMetrics =
    Metrics.meter(factory.createMetricName("TruncateErrorExceptionMetrics"));
    public static final Meter unavailableExceptionMetrics =
    Metrics.meter(factory.createMetricName("UnavailableExceptionMetrics"));
    public static final Meter writeFailureExceptionMetrics =
    Metrics.meter(factory.createMetricName("WriteFailureExceptionMetrics"));
    public static final Meter serverErrorExceptionMetrics =
    Metrics.meter(factory.createMetricName("ServerErrorExceptionMetrics"));
    public static final Meter otherExceptionMetrics = Metrics.meter(factory.createMetricName("OtherExceptionMetrics"));
}

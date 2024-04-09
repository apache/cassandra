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

import com.codahale.metrics.Histogram;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Captures metrics related to Mutual TLS certificate expiration and certificate age for client
 * and internode connections
 */
public class MutualTlsMetrics
{
    public static final String TYPE_NAME = "MutualTls";
    private static final MetricNameFactory factory = new DefaultNameFactory(TYPE_NAME);

    public final static MutualTlsMetrics instance = new MutualTlsMetrics();

    /**
     * Histogram of expiration days for client certificates
     */
    public final Histogram clientCertificateExpirationDays;

    /**
     * Histogram of expiration days for internode certificates
     */
    public final Histogram internodeCertificateExpirationDays;

    public MutualTlsMetrics()
    {
        clientCertificateExpirationDays = Metrics.histogram(factory.createMetricName("ClientCertificateExpirationDays"), true);
        internodeCertificateExpirationDays = Metrics.histogram(factory.createMetricName("InternodeCertificateExpirationDays"), true);
    }
}

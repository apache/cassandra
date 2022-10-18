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


import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_CLIENT_REQUEST_METRICS_PROVIDER_PROPERTY;

/**
 * Provides access to the {@link ClientRequestsMetrics} instance used by this node
 * and provides per-tenant metrics in CNDB.
 */
public interface ClientRequestsMetricsProvider
{
    ClientRequestsMetricsProvider instance = CUSTOM_CLIENT_REQUEST_METRICS_PROVIDER_PROPERTY.getString() == null ?
                                             new DefaultClientRequestsMetricsProvider() :
                                             make(CUSTOM_CLIENT_REQUEST_METRICS_PROVIDER_PROPERTY.getString());

    ClientRequestsMetrics metrics(String keyspace);

    static ClientRequestsMetricsProvider make(String customImpl)
    {
        try
        {
            return (ClientRequestsMetricsProvider) Class.forName(customImpl).newInstance();
        }
        catch (Throwable ex)
        {
            throw new IllegalStateException("Unknown client request metrics provider: " + customImpl);
        }
    }

    class DefaultClientRequestsMetricsProvider implements ClientRequestsMetricsProvider
    {
        private final ClientRequestsMetrics metrics = new ClientRequestsMetrics("");

        @Override
        public ClientRequestsMetrics metrics(String keyspace)
        {
            return metrics;
        }
    }
}

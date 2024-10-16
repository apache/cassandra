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

public class ClientSessionMetrics
{
    public Meter sessions;

    public ClientSessionMetrics(String serviceName, String tenancy, String tier) {
        MetricNameFactory factory = new ClientSessionsMetricsFactory(serviceName, tenancy, tier);
        sessions = Metrics.meter(factory.createMetricName("ClientSessions"));
    }

    class ClientSessionsMetricsFactory implements MetricNameFactory
    {
        private static final String TYPE = "ClientSessions";
        private String serviceName;
        private String tenancy;
        private String tier;

        protected ClientSessionsMetricsFactory(String serviceName, String tenancy, String tier) {
            this.serviceName = serviceName;
            this.tenancy = tenancy;
            this.tier = tier;
        }

        @Override
        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            String groupName = ClientMetrics.class.getPackage().getName();

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=").append(TYPE);
            mbeanName.append(",name=").append(metricName);
            mbeanName.append(",service=").append(serviceName);
            mbeanName.append(",tenancy=").append(tenancy);
            mbeanName.append(",tier=").append(tier);

            StringBuilder scope = new StringBuilder();
            scope.append("service=").append(serviceName);
            scope.append(",tenancy=").append(tenancy);
            scope.append(",tier=").append(tier);

            return new CassandraMetricsRegistry.MetricName(groupName, TYPE, metricName, scope.toString(), mbeanName.toString());
        }
    }
}

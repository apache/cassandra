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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClientMetricsManager
{
    private static final Map<String, ClientSessionMetrics> sessionMetrics = new ConcurrentHashMap<>();
    private static final Map<String, ClientQueryMetrics> queryMetrics = new ConcurrentHashMap<>();
    private ClientMetricsManager()
    {
    }

    public static ClientSessionMetrics getSessionMetrics(String clientService, String tenancy, String tier, String driverName, String isAuthenticated)
    {
        String key = String.join(",", clientService, tenancy, tier, driverName, isAuthenticated);
        return sessionMetrics.computeIfAbsent(key, k -> new ClientSessionMetrics(clientService, tenancy, tier, driverName, isAuthenticated));
    }

    public static ClientQueryMetrics getQueryMetrics(String clientService, String tenancy)
    {
        String key = String.join(",", clientService, tenancy);
        return queryMetrics.computeIfAbsent(key, k -> new ClientQueryMetrics(clientService, tenancy));
    }
}

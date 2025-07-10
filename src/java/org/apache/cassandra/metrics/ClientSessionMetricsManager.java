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

public class ClientSessionMetricsManager extends AbstractMetricsManager<String, ClientSessionMetrics>
{
    private final static ClientSessionMetricsManager instance = new ClientSessionMetricsManager();

    @Override
    protected ClientSessionMetrics createMetric(String key) throws IllegalArgumentException
    {
        String[] parts = key.split(",", -1);

        if (parts.length != 7) {
            throw new IllegalArgumentException("Invalid key for ClientSessionMetrics: expected 7 parts but got " + parts.length);
        }

        String clientService = parts[0];
        String tenancy = parts[1];
        String tier = parts[2];
        String driverName = parts[3];
        String enforcementLevelString = parts[4];
        String isDriverSupportedString = parts[5];
        String isAuthenticated = parts[6];

        return new ClientSessionMetrics(clientService, tenancy, tier, driverName,
                                        enforcementLevelString, isDriverSupportedString, isAuthenticated);
    }

    @Override
    protected String buildKey(Object... objects) throws IllegalArgumentException
    {
        if (objects.length != 7)
            throw new IllegalArgumentException("Expected 7 arguments: clientService, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated");

        return String.join(",", (String) objects[0], (String) objects[1], (String) objects[2],
                           (String) objects[3], (String) objects[4], (String) objects[5], (String) objects[6]);
    }

    public static ClientSessionMetrics getSessionMetrics(String clientService,
                                                         String tenancy,
                                                         String tier,
                                                         String driverName,
                                                         String enforcementLevelString,
                                                         String isDriverSupportedString,
                                                         String isAuthenticated)
    {
        return instance.getMetricsSync(clientService, tenancy, tier, driverName,
                                       enforcementLevelString, isDriverSupportedString, isAuthenticated);
    }
}

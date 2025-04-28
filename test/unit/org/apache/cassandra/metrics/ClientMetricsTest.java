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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClientMetricsTest
{
    @Test
    public void testClientSessionMetrics() {
        String serviceName = "service";
        String tenancy = "staging";
        String tier = "4";
        String driverName = "Java";
        String enforcementLevelString = "none";
        String isDriverSupportedString = "true";
        String isAuthenticated = "true";
        long beforeCount = ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.getCount();
        ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.mark();
        long curCount = ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.getCount();
        assertEquals(beforeCount+1, curCount);
    }

    @Test
    public void testClientSessionMetricsWithSoftEnforcement() {
        String serviceName = "service";
        String tenancy = "staging";
        String tier = "4";
        String driverName = "Java";
        String enforcementLevelString = "soft";
        String isDriverSupportedString = "true";
        String isAuthenticated = "true";

        long beforeCount = ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.getCount();
        ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.mark();
        long curCount = ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.getCount();

        assertEquals(beforeCount+1, curCount);
    }

    @Test
    public void testClientSessionMetricsWithNoneEnforcement() {
        String serviceName = "service";
        String tenancy = "staging";
        String tier = "4";
        String driverName = "Java";
        String enforcementLevelString = "none";
        String isDriverSupportedString = "true";
        String isAuthenticated = "true";

        long beforeCount = ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.getCount();
        ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.mark();
        long curCount = ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.getCount();

        assertEquals(beforeCount+1, curCount);
    }

    @Test
    public void testClientSessionMetricsWithDriverNotAllowed() {
        String serviceName = "service";
        String tenancy = "staging";
        String tier = "4";
        String driverName = "Java";
        String enforcementLevelString = "soft";
        String isDriverSupportedString = "false";
        String isAuthenticated = "true";

        long beforeCount = ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.getCount();
        ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.mark();
        long curCount = ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.getCount();

        assertEquals(beforeCount+1, curCount);
    }

    @Test
    public void testClientSessionMetricsWithHardEnforcementAndDriverAllowed() {
        String serviceName = "service";
        String tenancy = "staging";
        String tier = "4";
        String driverName = "Java";
        String enforcementLevelString = "hard";
        String isDriverSupportedString = "true";
        String isAuthenticated = "true";

        long beforeCount = ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.getCount();
        ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.mark();
        long curCount = ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.getCount();

        assertEquals(beforeCount+1, curCount);
    }

    @Test
    public void testClientSessionMetricsWithHardEnforcementAndDriverNotAllowed() {
        String serviceName = "service";
        String tenancy = "staging";
        String tier = "4";
        String driverName = "Java";
        String enforcementLevelString = "hard";
        String isDriverSupportedString = "false";
        String isAuthenticated = "true";

        long beforeCount = ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.getCount();
        ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.mark();
        long curCount = ClientMetricsManager.getSessionMetrics(serviceName, tenancy, tier, driverName, enforcementLevelString, isDriverSupportedString, isAuthenticated).sessions.getCount();

        assertEquals(beforeCount+1, curCount);
    }

    @Test
    public void testClientQueryMetrics() {
        String serviceName = "service";
        String tenancy = "staging";
        long beforeCount = ClientMetricsManager.getQueryMetrics(serviceName, tenancy).query.getCount();
        ClientMetricsManager.getQueryMetrics(serviceName, tenancy).query.inc();
        long curCount = ClientMetricsManager.getQueryMetrics(serviceName, tenancy).query.getCount();
        assertEquals(beforeCount+1, curCount);
    }
}

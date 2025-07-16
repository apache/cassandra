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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.management.MalformedObjectNameException;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.Config.ClientLibsEnforcementLevel;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ClientSessionMetricsTest
{
    private static final String SERVICE = "service";
    private static final String TENANCY = "staging";
    private static final String TIER = "4";
    private static final String DRIVER_NAME = "Java";
    private static final String IS_AUTHENTICATED = "true";

    private final String enforcementLevel;
    private final String isDriverSupported;

    public ClientSessionMetricsTest(String enforcementLevel, String isDriverSupported) {
        this.enforcementLevel = enforcementLevel;
        this.isDriverSupported = isDriverSupported;
    }

    @Parameterized.Parameters(name = "{index}: enforcement={0}, supported={1}, async={2}")
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();
        for (ClientLibsEnforcementLevel level : ClientLibsEnforcementLevel.values())
            for (boolean isSupported : new boolean[]{true, false})
                params.add(new Object[]{level.name().toLowerCase(), String.valueOf(isSupported)});
        return params;
    }

    @After
    public void cleanup() throws InterruptedException, MalformedObjectNameException
    {
        ClientSessionMetricsManager.release(
            SERVICE, TENANCY, TIER, DRIVER_NAME, enforcementLevel, isDriverSupported, IS_AUTHENTICATED);
    }

    @Test
    public void testClientSessionMetrics() throws InterruptedException
    {
        long beforeCount = ClientSessionMetricsManager
                           .getSessionMetrics(SERVICE, TENANCY, TIER, DRIVER_NAME, enforcementLevel, isDriverSupported, IS_AUTHENTICATED)
                           .sessions.getCount();
        ClientSessionMetricsManager.markSession(SERVICE, TENANCY, TIER, DRIVER_NAME, enforcementLevel, isDriverSupported, IS_AUTHENTICATED).await();
        long afterCount = ClientSessionMetricsManager
                          .getSessionMetrics(SERVICE, TENANCY, TIER, DRIVER_NAME, enforcementLevel, isDriverSupported, IS_AUTHENTICATED)
                          .sessions.getCount();
        assertEquals(beforeCount + 1, afterCount);
    }
}

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

package org.apache.cassandra.profiling;

import java.time.Duration;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for ContinuousProfilerConfig configuration class.
 * Tests verify default values and all getter methods work correctly.
 */
public class ContinuousProfilerConfigTest
{
    @Test
    public void testConfigDefaultsAndGetters()
    {
        ContinuousProfilerConfig config = new ContinuousProfilerConfig();

        // Test ContinuousProfilerConfig getters
        assertFalse("isEnabled() should return false", config.isEnabled());
        assertEquals("getEnablementCheckInterval() should return Duration.ofMillis(600000)",
                     Duration.ofMillis(600000), config.getEnablementCheckInterval());
        assertTrue("getProfiles() should be empty by default", config.getProfiles().isEmpty());

        // Test PyroscopeProxyConfig getters
        ContinuousProfilerConfig.PyroscopeProxyConfig proxy = config.getPyroscopeProxy();
        assertNotNull("getPyroscopeProxy() should not return null", proxy);
        assertEquals("getUploadUrl() should return correct default",
                     "http://127.0.0.1:5436/continuous-profiler/uploadv1", proxy.getUploadUrl());
        assertEquals("getEnablementUrl() should return correct default",
                     "http://127.0.0.1:5436/continuous-profiler/enablementv1", proxy.getEnablementUrl());
        assertEquals("getTimeout() should return Duration.ofMillis(30000)",
                     Duration.ofMillis(30000), proxy.getTimeout());
        assertEquals("getServiceName() should return correct default",
                     "serverless-pyroscope-proxy-handler", proxy.getServiceName());

        // Test ProfileConfig getters
        ContinuousProfilerConfig.ProfileConfig profile = new ContinuousProfilerConfig.ProfileConfig();
        assertEquals("getEnablementFraction() should return 0.0",
                     0.0, profile.getEnablementFraction(), 0.0001);
        assertEquals("getProfileInterval() should return Duration.ofMillis(60000)",
                     Duration.ofMillis(60000), profile.getProfileInterval());
        assertEquals("getCollectionDuration() should return Duration.ofMillis(60000)",
                     Duration.ofMillis(60000), profile.getCollectionDuration());

        // Test getters with modified values
        config.profiles.put("cpu", profile);
        assertEquals("getProfileConfig() should return the profile",
                     profile, config.getProfileConfig("cpu"));
        proxy.service_name = "custom-service";
        assertEquals("getServiceName() should return updated value",
                     "custom-service", proxy.getServiceName());
    }
}

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
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for continuous profiler integration, read from cassandra.yaml under the 'continuous_profiler_config' section.
 * Configures profiling behavior including Pyroscope proxy endpoints, enablement check intervals, and
 * profile settings (enablement fractions, collection intervals, and durations) per profile type.
 * <p>
 * Note: Duration fields are stored as milliseconds (long) for YAML compatibility and exposed via helper methods as Duration objects.
 */
public final class ContinuousProfilerConfig {

    /**
     * Enable/disable the continuous profiler
     */
    public boolean enabled = false;

    /** [profile_interval, enablement_check_interval, collection_duration, timeout]
     * Interval for checking profiler enablement status (in milliseconds)
     * Default: 10 minutes
     */
    public long enablement_check_interval = 600000;

    /**
     * Pyroscope proxy configuration
     */
    public PyroscopeProxyConfig pyroscope_proxy = new PyroscopeProxyConfig();

    /**
     * Profile configurations by profile type (e.g., cpu, alloc)
     */
    public Map<String, ProfileConfig> profiles = new HashMap<>();

    public boolean isEnabled() {
        return enabled;
    }

    public PyroscopeProxyConfig getPyroscopeProxy() {
        return pyroscope_proxy;
    }

    public Map<String, ProfileConfig> getProfiles() {
        return profiles;
    }

    /**
     * Get the profile configuration for a given profile type.
     *
     * @param profileType The profile type (e.g., "cpu", "heap")
     * @return The profile configuration, or null if not found
     */
    public ProfileConfig getProfileConfig(String profileType) {
        return profiles.get(profileType);
    }

    /**
     * Get the enablement check interval as Duration.
     *
     * @return The enablement check interval
     */
    public Duration getEnablementCheckInterval() {
        return Duration.ofMillis(enablement_check_interval);
    }

    public static class PyroscopeProxyConfig {

        /**
         * URL for uploading profiling data
         */
        public String upload_url = "http://127.0.0.1:5436/continuous-profiler/uploadv1";

        /**
         * URL for checking enablement status
         */
        public String enablement_url = "http://127.0.0.1:5436/continuous-profiler/enablementv1";

        /**
         * Timeout for proxy HTTP requests (in milliseconds)
         * Default: 30 seconds
         */
        public long timeout = 30000;

        /**
         * Service name for the proxy handler
         */
        public String service_name = "serverless-pyroscope-proxy-handler";

        public String getUploadUrl() {
            return upload_url;
        }

        public String getEnablementUrl() {
            return enablement_url;
        }

        public String getServiceName() {
            return service_name;
        }

        /**
         * Get the timeout as Duration.
         *
         * @return The timeout duration
         */
        public Duration getTimeout() {
            return Duration.ofMillis(timeout);
        }

    }

    public static class ProfileConfig {

        /**
         * Fraction of instances that should enable profiling (0.0 to 1.0)
         */
        public double enablement_fraction = 0.0;

        /**
         * Interval between profile collections (in milliseconds)
         * Default: 60 seconds
         */
        public long profile_interval = 60000;

        /**
         * Duration of each profile collection (in milliseconds)
         * Default: 60 seconds
         */
        public long collection_duration = 60000;

        public double getEnablementFraction() {
            return enablement_fraction;
        }

        /**
         * Get the profile interval as Duration.
         *
         * @return The profile interval
         */
        public Duration getProfileInterval() {
            return Duration.ofMillis(profile_interval);
        }

        /**
         * Get the collection duration as Duration.
         *
         * @return The collection duration
         */
        public Duration getCollectionDuration() {
            return Duration.ofMillis(collection_duration);
        }

    }

}

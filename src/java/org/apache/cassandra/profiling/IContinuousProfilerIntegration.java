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

/**
 * Interface for continuous profiler integration that can be consumed from external packages.
 * Implementations should handle lifecycle management including initialization and shutdown of profiling components.
 */
public interface IContinuousProfilerIntegration
{
    /**
     * Initialize the continuous profiler.
     * Configuration is loaded from cassandra.yaml and passed to the implementation.
     * Safe to call multiple times - implementations should only initialize once.
     * Will not throw exceptions - failures are logged and profiler is disabled.
     *
     * @param config The continuous profiler configuration from cassandra.yaml
     */
    void initialize(ContinuousProfilerConfig config);

    /**
     * Stop the continuous profiler and clean up resources.
     * Safe to call multiple times - implementations should handle redundant shutdown calls gracefully.
     * Will not throw exceptions - failures are logged.
     */
    void shutdown();
}


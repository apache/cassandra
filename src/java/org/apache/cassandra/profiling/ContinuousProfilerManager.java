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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.FBUtilities;

/**
 * Manager for continuous profiler integration lifecycle.
 * <p>
 * This class handles loading and initializing continuous profiler implementations from an external package
 * using reflection to avoid creating a hard dependency.
 */
public class ContinuousProfilerManager
{
    private static final Logger logger = LoggerFactory.getLogger(ContinuousProfilerManager.class);
    private static final String DEFAULT_PROFILER_CLASS = "com.uber.cassandra.profiling.ContinuousProfilerIntegration";

    private static ContinuousProfilerManager instance = null;

    private final IContinuousProfilerIntegration profiler;

    ContinuousProfilerManager(IContinuousProfilerIntegration profiler)
    {
        this.profiler = profiler;
    }

    /**
     * Attempts to load and instantiate the continuous profiler.
     * Returns null if the profiler class is not available.
     */
    private static ContinuousProfilerManager load()
    {
        try
        {
            IContinuousProfilerIntegration instance = FBUtilities.instanceOrConstruct(DEFAULT_PROFILER_CLASS, "continuous profiling integration");
            logger.info("Loaded continuous profiler from {}", DEFAULT_PROFILER_CLASS);
            return new ContinuousProfilerManager(instance);
        }
        catch (Throwable e)
        {
            logger.warn("Failed to load continuous profiler from {}", DEFAULT_PROFILER_CLASS, e);
            return null;
        }
    }

    /**
     * Initializes the continuous profiler.
     * Loads the profiler implementation and stores the instance for later shutdown.
     * Safe to use for daemon startup - all exceptions are logged, not thrown.
     * Synchronized to ensure thread-safe instance management.
     *
     * @param config The continuous profiler configuration from cassandra.yaml
     */
    public static synchronized void initialize(ContinuousProfilerConfig config)
    {
        ContinuousProfilerManager manager = load();
        if (manager != null)
        {
            manager.initializeProfiler(config);
            instance = manager;
        }
    }

    /**
     * Safely shuts down the continuous profiler if it was loaded.
     * Uses the stored instance from initialize().
     * Safe to call even if profiler was not loaded - no exceptions thrown.
     * Synchronized to ensure thread-safe instance management.
     */
    public static synchronized void shutdown()
    {
        if (instance != null)
        {
            instance.shutdownInstance();
            instance = null;
        }
    }

    /**
     * Package-private getter for testing purposes only.
     * Allows tests to verify internal state without exposing it publicly.
     */
    static ContinuousProfilerManager getInstance()
    {
        return instance;
    }

    /**
     * Initializes the profiler with the provided configuration.
     * Logs a warning if initialization fails but does not throw an exception.
     *
     * @param config The continuous profiler configuration
     */
    void initializeProfiler(ContinuousProfilerConfig config)
    {
        if (profiler == null)
            return;

        try
        {
            profiler.initialize(config);
            logger.info("Continuous profiler initialized successfully");
        }
        catch (Throwable e)
        {
            logger.warn("Failed to initialize continuous profiler", e);
        }
    }

    /**
     * Shuts down the profiler.
     * Logs a warning if shutdown fails but does not throw an exception.
     */
    void shutdownInstance()
    {
        if (profiler == null)
            return;

        try
        {
            profiler.shutdown();
            logger.info("Continuous profiler shutdown successfully");
        }
        catch (Throwable e)
        {
            logger.warn("Failed to shutdown continuous profiler", e);
        }
    }
}


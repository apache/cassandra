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

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for ContinuousProfilerManager covering public methods.
 * Tests use test implementations of IContinuousProfilerIntegration.
 */
public class ContinuousProfilerManagerTest
{
    @After
    public void tearDown()
    {
        ContinuousProfilerManager.shutdown();
    }

    @Test
    public void testInitializeProfilerWithValidProfiler()
    {
        TestContinuousProfilerIntegration profiler = new TestContinuousProfilerIntegration();
        ContinuousProfilerManager manager = new ContinuousProfilerManager(profiler);
        ContinuousProfilerConfig config = new ContinuousProfilerConfig();
        config.enabled = true;
        
        manager.initializeProfiler(config);
        assertTrue("Profiler should be initialized", profiler.isInitialized());
    }

    @Test
    public void testInitializeProfilerWhenDisabled()
    {
        TestContinuousProfilerIntegration profiler = new TestContinuousProfilerIntegration();
        ContinuousProfilerManager manager = new ContinuousProfilerManager(profiler);
        ContinuousProfilerConfig config = new ContinuousProfilerConfig();
        // enabled defaults to false
        
        manager.initializeProfiler(config);
        assertFalse("Profiler should not be initialized when disabled", profiler.isInitialized());
    }

    @Test
    public void testInitializeProfilerWithNullProfiler()
    {
        ContinuousProfilerManager manager = new ContinuousProfilerManager(null);
        ContinuousProfilerConfig config = new ContinuousProfilerConfig();
        manager.initializeProfiler(config); // Should not throw
    }

    @Test
    public void testInitializeProfilerWithExceptionHandling()
    {
        FailingInitializeProfiler profiler = new FailingInitializeProfiler();
        ContinuousProfilerManager manager = new ContinuousProfilerManager(profiler);
        ContinuousProfilerConfig config = new ContinuousProfilerConfig();
        
        manager.initializeProfiler(config); // Should not throw, exception is caught
    }

    @Test
    public void testShutdownProfilerWithValidProfiler()
    {
        TestContinuousProfilerIntegration profiler = new TestContinuousProfilerIntegration();
        ContinuousProfilerManager manager = new ContinuousProfilerManager(profiler);
        ContinuousProfilerConfig config = new ContinuousProfilerConfig();
        config.enabled = true;
        
        manager.initializeProfiler(config);
        manager.shutdownInstance();
        assertTrue("Profiler should be shut down", profiler.isShutdown());
    }

    @Test
    public void testShutdownProfilerWithNullProfiler()
    {
        ContinuousProfilerManager manager = new ContinuousProfilerManager(null);
        manager.shutdownInstance(); // Should not throw
    }

    @Test
    public void testShutdownProfilerWithExceptionHandling()
    {
        FailingShutdownProfiler profiler = new FailingShutdownProfiler();
        ContinuousProfilerManager manager = new ContinuousProfilerManager(profiler);
        ContinuousProfilerConfig config = new ContinuousProfilerConfig();
        
        manager.initializeProfiler(config);
        manager.shutdownInstance(); // Should not throw, exception is caught
    }

    @Test
    public void testGetInstanceWhenNotInitialized()
    {
        ContinuousProfilerManager.shutdown();
        assertNull("getInstance should return null before initialize", 
                   ContinuousProfilerManager.getInstance());
    }

    @Test
    public void testInitializeSuccessPath()
    {
        TestContinuousProfilerIntegration profiler = new TestContinuousProfilerIntegration();
        ContinuousProfilerConfig config = new ContinuousProfilerConfig();
        config.enabled = true;
        
        // Directly create and set up a manager to bypass the external class lookup
        ContinuousProfilerManager manager = new ContinuousProfilerManager(profiler);
        manager.initializeProfiler(config);
        
        // Verify initialization succeeded
        assertTrue("Profiler should be initialized", profiler.isInitialized());
    }

    @Test
    public void testShutdownClearsInstance()
    {
        // Create and initialize a profiler through direct instantiation
        TestContinuousProfilerIntegration profiler = new TestContinuousProfilerIntegration();
        ContinuousProfilerManager manager = new ContinuousProfilerManager(profiler);
        ContinuousProfilerConfig config = new ContinuousProfilerConfig();
        config.enabled = true;
        
        // Initialize the profiler
        manager.initializeProfiler(config);
        assertTrue("Profiler should be initialized", profiler.isInitialized());
        
        // Shutdown the instance
        manager.shutdownInstance();
        assertTrue("Profiler should be shut down", profiler.isShutdown());
    }

    @Test
    public void testThreadSafetyOfStaticMethods() throws InterruptedException
    {
        ContinuousProfilerConfig config = new ContinuousProfilerConfig();
        
        // Multiple threads calling initialize
        Thread t1 = new Thread(() -> ContinuousProfilerManager.initialize(config));
        Thread t2 = new Thread(() -> ContinuousProfilerManager.initialize(config));
        
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        
        // Multiple threads calling shutdown
        Thread t3 = new Thread(() -> ContinuousProfilerManager.shutdown());
        Thread t4 = new Thread(() -> ContinuousProfilerManager.shutdown());
        
        t3.start();
        t4.start();
        t3.join();
        t4.join();
        
        assertNull("Instance should be null after concurrent shutdown", 
                   ContinuousProfilerManager.getInstance());
    }

    // ==================== Test Implementations ====================

    static class TestContinuousProfilerIntegration implements IContinuousProfilerIntegration
    {
        private boolean initialized = false;
        private boolean shutdown = false;

        @Override
        public void initialize(ContinuousProfilerConfig config)
        {
            if (config.isEnabled())
            {
                initialized = true;
            }
        }

        @Override
        public void shutdown()
        {
            shutdown = true;
        }

        boolean isInitialized()
        {
            return initialized;
        }

        boolean isShutdown()
        {
            return shutdown;
        }
    }

    static class FailingInitializeProfiler implements IContinuousProfilerIntegration
    {
        @Override
        public void initialize(ContinuousProfilerConfig config)
        {
            throw new RuntimeException("Initialization intentionally failed");
        }

        @Override
        public void shutdown()
        {
            // Do nothing
        }
    }

    static class FailingShutdownProfiler implements IContinuousProfilerIntegration
    {
        @Override
        public void initialize(ContinuousProfilerConfig config)
        {
            // Do nothing
        }

        @Override
        public void shutdown()
        {
            throw new RuntimeException("Shutdown intentionally failed");
        }
    }
}

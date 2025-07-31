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

package org.apache.cassandra.tools;

import java.io.File;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.tools.profiler.AsyncProfilerService;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_ADVANCED_MODE;

public class AsyncProfilerServiceTest
{

    private static AsyncProfilerService profiler;
    private static String testOutputFile = "/tmp/test-profile.html";

    @BeforeClass
    public static void setUpClass() {
        profiler = new AsyncProfilerService();

        if (!profiler.isAvailable()) {
            fail("AsyncProfilerService could not initialize (native lib not found or invalid).");
        }
    }

    @Before
    public void setUp(){
        System.setProperty(ASYNC_PROFILER_ENABLED.getKey(), "true");
    }

    @After
    public void tearDown()
    {
        try {
            profiler.stop(testOutputFile);
            File outputFile = new File(testOutputFile);
            if (outputFile.exists()) {
                outputFile.delete();
            }
        } catch (Exception e){
            // The only meaningful exception that can surface here is if profiler.start
            // was not called prior to profiler.stop, we can safely ignore this.
        }
    }

    @Test
    public void testStartAndStopProfiling() {


        try {
            profiler.start("cpu", "flamegraph");
            Thread.sleep(5000);
            profiler.stop(testOutputFile);
        } catch (Exception e) {
            fail("Profiling failed: " + e.getMessage());
        }

        File file = new File(testOutputFile);
        assertTrue("Output profile file should exist", file.exists());
        assertTrue("Output profile file should not be empty", file.length() > 0);
    }

    @Test
    public void testInvalidEventThrowsException() {
        try {
            profiler.start("not_a_real_event", "flamegraph");
            fail("Expected RuntimeException due to invalid event");
        } catch (RuntimeException e) {
            assertNotNull(e.getMessage());
            assertTrue("Invalid event should not start profiler", e.getMessage().contains("Event must be one or a combination of"));
        }
    }

    @Test
    public void testInvalidFormatThrowsException() {
        try {
            profiler.start("cpu", "not_a_real_format");
            fail("Expected RuntimeException due to invalid format");
        } catch (RuntimeException e) {
            assertNotNull(e.getMessage());
            assertTrue("Invalid format should not start profiler", e.getMessage().contains("Format must be one or a combination of"));
        }
    }

    @Test
    public void testInvalidOutputFileNameThrowsException() {
        try {
            profiler.start("cpu", "flamegraph");
            Thread.sleep(5000);
            profiler.stop("| grep test");
            fail("Expected RuntimeException due to invalid output file name");
        } catch (Exception e) {
            assertNotNull(e.getMessage());
            assertTrue("Invalid output file name", e.getMessage().contains("Output file name must not contain any invalid characters"));
        }
    }

    @Test
    public void testMultipleStartCallsThrowsException()
    {
        try
        {
            profiler.start("cpu", "flamegraph");
            profiler.start("cpu", "flamegraph");
            fail("Expected IllegalStateException due to multiple start calls");
        }
        catch (IllegalStateException e)
        {
            assertNotNull(e.getMessage());
            assertTrue("Process should not start another profiling job", e.getMessage().contains("Profiler already started"));
        }
    }

    @Test
    public void testProfilerDisabledThrowsException()
    {
        try
        {
            System.setProperty(ASYNC_PROFILER_ENABLED.getKey(), "false");
            profiler.execute(String.format("start,event=cpu"));
            fail("Expected IllegalStateException due to disabled profiler");
        }
        catch (IllegalStateException e)
        {
            assertNotNull(e.getMessage());
            assertTrue("ASYNC_PROFILER_ENABLED is false", e.getMessage().contains("async-profiler is not enabled."));
        }
    }

    @Test
    public void testAdvancedModeDisabledThrowsException()
    {
        try
        {
            System.setProperty(ASYNC_PROFILER_ADVANCED_MODE.getKey(), "false");
            profiler.execute(String.format("start,event=cpu"));
            fail("Expected IllegalStateException due to disabled advanced mode");
        }
        catch (IllegalStateException e)
        {
            assertNotNull(e.getMessage());
            assertTrue("ASYNC_PROFILER_ADVANCED_MODE is false", e.getMessage().contains("ASYNC_PROFILER_ADVANCED_MODE must be set to true to execute raw commands."));
        }
    }

    @Test
    public void testAdvancedModeEnabledSuccess()
    {
        try {
            System.setProperty(ASYNC_PROFILER_ADVANCED_MODE.getKey(), "true");
            profiler.execute(String.format("start,event=cpu"));
            Thread.sleep(5000);
            profiler.execute(String.format("stop,file=%s", testOutputFile));
        } catch (Exception e) {
            fail("Profiling failed: " + e.getMessage());
        }

        File file = new File(testOutputFile);
        assertTrue("Output profile file for advanced mode should exist", file.exists());
        assertTrue("Output profile file for advanced mode should not be empty", file.length() > 0);
    }
}

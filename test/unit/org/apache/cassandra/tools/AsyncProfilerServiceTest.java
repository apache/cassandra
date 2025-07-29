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

import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.tools.profiler.AsyncProfilerService;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AsyncProfilerServiceTest
{

    private static AsyncProfilerService profiler;

    @BeforeClass
    public static void setUp() {
        profiler = new AsyncProfilerService();

        if (!profiler.isAvailable()) {
            fail("AsyncProfilerService could not initialize (native lib not found or invalid).");
        }
    }

    @After
    public void tearDown()
    {
        try {
            profiler.stop("/tmp/test-profile.html");
            File outputFile = new File("/tmp/test-profile.html");
            if (outputFile.exists()) {
                outputFile.delete();
            }
        } catch (Exception e){}
    }

    @Test
    public void testStartAndStopProfiling() {
        String outputFile = "/tmp/test-profile.html";

        try {
            profiler.start("cpu", "flamegraph");
            Thread.sleep(5000);
            profiler.stop(outputFile);
        } catch (Exception e) {
            fail("Profiling failed: " + e.getMessage());
        }

        File file = new File(outputFile);
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


}

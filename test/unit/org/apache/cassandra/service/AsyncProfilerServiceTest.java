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

package org.apache.cassandra.service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static java.lang.String.format;
import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_UNSAFE_MODE;
import static org.apache.cassandra.service.AsyncProfilerService.ASYNC_PROFILER_START_DURATION_PARAM;
import static org.apache.cassandra.service.AsyncProfilerService.ASYNC_PROFILER_START_EVENTS_PARAM;
import static org.apache.cassandra.service.AsyncProfilerService.ASYNC_PROFILER_START_OUTPUT_FILE_NAME_PARAM;
import static org.apache.cassandra.service.AsyncProfilerService.ASYNC_PROFILER_START_OUTPUT_FORMAT_PARAM;
import static org.apache.cassandra.service.AsyncProfilerService.ASYNC_PROFILER_STOP_OUTPUT_FILE_NAME_PARAM;
import static org.apache.cassandra.service.AsyncProfilerService.AsyncProfilerEvent.cpu;
import static org.apache.cassandra.service.AsyncProfilerService.AsyncProfilerFormat.flamegraph;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AsyncProfilerServiceTest
{
    private static final String testOutputPath = FileUtils.getTempDir().path();

    private AsyncProfilerService profiler;
    private File testOutputFile;

    /**
     * Test-friendly kernel params check that returns valid values without reading from /proc
     */
    private static class TestAsyncProfilerKernelParamsCheck extends StartupChecks.AsyncProfilerKernelParamsCheck
    {
        @Override
        protected int readPerfEventParanoid()
        {
            return 1; // Valid value (must be <= 1)
        }

        @Override
        protected int readKptrRestrict()
        {
            return 0; // Valid value (must be == 0)
        }
    }

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        ASYNC_PROFILER_ENABLED.setBoolean(true);
        testOutputFile = new File(testOutputPath, UUID.randomUUID().toString());
    }

    @After
    public void tearDown()
    {
        try
        {
            Map<String, String> stopParameters = Map.of(ASYNC_PROFILER_STOP_OUTPUT_FILE_NAME_PARAM, testOutputFile.absolutePath());
            profiler.stop(stopParameters);
            testOutputFile.deleteIfExists();
        }
        catch (Exception e)
        {
            // The only meaningful exception that can surface here is if profiler.start
            // was not called prior to profiler.stop, we can safely ignore this.
        }

        profiler = null;
    }

    private AsyncProfilerService getProfiler()
    {
        AsyncProfilerService.reset();
        return AsyncProfilerService.instance(testOutputPath, false, new TestAsyncProfilerKernelParamsCheck());
    }

    @Test
    public void testStartAndStopProfiling() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(ASYNC_PROFILER_UNSAFE_MODE, false))
        {
            Map<String, String> startParameters = Map.of(ASYNC_PROFILER_START_EVENTS_PARAM, cpu.name(),
                                                         ASYNC_PROFILER_START_OUTPUT_FORMAT_PARAM, flamegraph.name(),
                                                         ASYNC_PROFILER_START_DURATION_PARAM, "10s",
                                                         ASYNC_PROFILER_START_OUTPUT_FILE_NAME_PARAM, testOutputFile.name());

            Map<String, String> stopParameters = Map.of(ASYNC_PROFILER_STOP_OUTPUT_FILE_NAME_PARAM, testOutputFile.name());
            AsyncProfilerService profiler = getProfiler();
            profiler.start(startParameters);
            Thread.sleep(5000);
            profiler.stop(stopParameters);

            assertTrue("Output profile file should exist", testOutputFile.exists());
            assertTrue("Output profile file should not be empty", testOutputFile.length() > 0);

            List<String> list = profiler.list();
            assertFalse(list.isEmpty());

            Optional<String> resultFile = list.stream().filter(f -> f.equals(testOutputFile.name())).findFirst();
            assertTrue(resultFile.isPresent());
            byte[] content = profiler.fetch(resultFile.get());
            assertNotNull(content);

            assertEquals("Profiler is not active\n", profiler.status());
        }
    }

    @Test
    public void testInvalidParametersThrowsException()
    {
        try (WithProperties properties = new WithProperties().set(ASYNC_PROFILER_UNSAFE_MODE, false))
        {
            Map<String, String> startParameters = Map.of();
            assertThatThrownBy(() -> getProfiler().start(startParameters))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Wrong parameters passed to start async profiler method. Passed parameters " +
                                  "should be:");
        }
    }

    @Test
    public void testInvalidEventThrowsException()
    {
        try (WithProperties properties = new WithProperties().set(ASYNC_PROFILER_UNSAFE_MODE, false))
        {
            Map<String, String> startParameters = Map.of(ASYNC_PROFILER_START_EVENTS_PARAM, "not_a_real_event",
                                                         ASYNC_PROFILER_START_OUTPUT_FORMAT_PARAM, "flamegraph",
                                                         ASYNC_PROFILER_START_DURATION_PARAM, "60s",
                                                         ASYNC_PROFILER_START_OUTPUT_FILE_NAME_PARAM, testOutputFile.name());

            assertThatThrownBy(() -> getProfiler().start(startParameters))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Event must be one or a combination of [cpu, alloc, lock, wall, nativemem, cache_misses]");
        }
    }

    @Test
    public void testInvalidDurationThrowsException()
    {
        try (WithProperties properties = new WithProperties().set(ASYNC_PROFILER_UNSAFE_MODE, false))
        {
            Map<String, String> startParameters = Map.of(ASYNC_PROFILER_START_EVENTS_PARAM, cpu.name(),
                                                         ASYNC_PROFILER_START_OUTPUT_FORMAT_PARAM, flamegraph.name(),
                                                         ASYNC_PROFILER_START_DURATION_PARAM, "13h",
                                                         ASYNC_PROFILER_START_OUTPUT_FILE_NAME_PARAM, testOutputFile.name());
            assertThatThrownBy(() -> getProfiler().start(startParameters))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Max profiling duration is 43200 seconds. If you need longer profiling, use execute command instead");
        }
    }

    @Test
    public void testInvalidFormatThrowsException()
    {
        try (WithProperties properties = new WithProperties().set(ASYNC_PROFILER_UNSAFE_MODE, false))
        {
            Map<String, String> startParameters = Map.of(ASYNC_PROFILER_START_EVENTS_PARAM, cpu.name(),
                                                         ASYNC_PROFILER_START_OUTPUT_FORMAT_PARAM, "not_a_real_format",
                                                         ASYNC_PROFILER_START_DURATION_PARAM, "60s",
                                                         ASYNC_PROFILER_START_OUTPUT_FILE_NAME_PARAM, testOutputFile.name());
            assertThatThrownBy(() -> getProfiler().start(startParameters))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Format must be one of [flat, traces, collapsed, flamegraph, tree, jfr]");
        }
    }

    @Test
    public void testInvalidOutputFileNameThrowsException()
    {
        try (WithProperties properties = new WithProperties().set(ASYNC_PROFILER_UNSAFE_MODE, false))
        {
            Map<String, String> startParameters = Map.of(ASYNC_PROFILER_START_EVENTS_PARAM, cpu.name(),
                                                         ASYNC_PROFILER_START_OUTPUT_FORMAT_PARAM, flamegraph.name(),
                                                         ASYNC_PROFILER_START_DURATION_PARAM, "60s",
                                                         ASYNC_PROFILER_START_OUTPUT_FILE_NAME_PARAM, "| grep test");
            assertThatThrownBy(() -> getProfiler().start(startParameters))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Output file name must match pattern ^[a-zA-Z0-9-]*\\.?[a-zA-Z0-9-]*$");
        }
    }

    @Test
    public void testInvalidTimeoutThrowsException()
    {
        try (WithProperties properties = new WithProperties().set(ASYNC_PROFILER_UNSAFE_MODE, false))
        {
            Map<String, String> startParameters = Map.of(ASYNC_PROFILER_START_EVENTS_PARAM, cpu.name(),
                                                         ASYNC_PROFILER_START_OUTPUT_FORMAT_PARAM, flamegraph.name(),
                                                         ASYNC_PROFILER_START_DURATION_PARAM, "10abc",
                                                         ASYNC_PROFILER_START_OUTPUT_FILE_NAME_PARAM, "abc");
            assertThatThrownBy(() -> {
                getProfiler().start(startParameters);
            })
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid duration: 10abc Accepted units:[SECONDS, MINUTES, HOURS, DAYS] where case matters and only non-negative values.");
        }
    }

    @Test
    public void testSecondStartNotExecuted()
    {
        try (WithProperties properties = new WithProperties().set(ASYNC_PROFILER_UNSAFE_MODE, false))
        {
            Map<String, String> startParameters = Map.of(ASYNC_PROFILER_START_EVENTS_PARAM, cpu.name(),
                                                         ASYNC_PROFILER_START_OUTPUT_FORMAT_PARAM, flamegraph.name(),
                                                         ASYNC_PROFILER_START_DURATION_PARAM, "60s",
                                                         ASYNC_PROFILER_START_OUTPUT_FILE_NAME_PARAM, testOutputFile.name());
            Map<String, String> stopParameters = Map.of(ASYNC_PROFILER_STOP_OUTPUT_FILE_NAME_PARAM, testOutputFile.name());

            AsyncProfilerService profiler = getProfiler();
            assertTrue(profiler.start(startParameters));
            assertFalse(profiler.start(startParameters));
            profiler.stop(stopParameters);
        }
    }

    @Test
    public void testProfilerDisabledThrowsException()
    {
        try (WithProperties properties = new WithProperties().set(ASYNC_PROFILER_UNSAFE_MODE, true).set(ASYNC_PROFILER_ENABLED, false))
        {
            assertThatThrownBy(() -> {
                AsyncProfilerService profiler = getProfiler();
                profiler.status();
            }).hasMessageContaining("Async Profiler is not enabled. Enable it by setting cassandra.async_profiler.enabled property to true.");
        }
    }

    @Test
    public void testAdvancedModeEnabledSuccess() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(ASYNC_PROFILER_UNSAFE_MODE, true))
        {
            AsyncProfilerService profiler = getProfiler();

            profiler.execute("start,event=" + cpu.name() + ",file=" + testOutputFile.absolutePath());
            Thread.sleep(5000);
            profiler.execute(format("stop,file=%s", testOutputFile));

            assertTrue("Output profile file for unsafe mode should exist", testOutputFile.exists());
            assertTrue("Output profile file for unsafe mode should not be empty", testOutputFile.length() > 0);
        }
    }

    @Test
    public void testUnsafeExecute()
    {
        try (WithProperties properties = new WithProperties().set(ASYNC_PROFILER_UNSAFE_MODE, true))
        {
            getProfiler().execute("foo");
        }
    }

    @Test
    public void testFetchIllegalFile()
    {
        assertThatThrownBy(() -> getProfiler().fetch("../abc"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Illegal file to fetch: ../abc");

        assertThatThrownBy(() -> getProfiler().fetch("/etc/abc"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Illegal file to fetch: /etc/abc");
    }

    @Test
    public void testSafeExecute()
    {
        try (WithProperties properties = new WithProperties().set(ASYNC_PROFILER_UNSAFE_MODE, false))
        {
            assertThatThrownBy(() -> getProfiler().execute("foo"))
            .isInstanceOf(SecurityException.class)
            .hasMessageContaining("The arbitrary command execution is not permitted with org.apache.cassandra.profiler:type=AsyncProfiler " +
                                  "MBean. If unsafe command execution is required, start Cassandra with " + ASYNC_PROFILER_UNSAFE_MODE.getKey() + ' ' +
                                  "property set to true. Rejected command: foo");
        }
    }
}

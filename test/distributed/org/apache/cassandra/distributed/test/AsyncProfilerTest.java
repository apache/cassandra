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

package org.apache.cassandra.distributed.test;

import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.UUID;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.AsyncProfilerService;
import org.apache.cassandra.service.StartupChecks;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.ASYNC_PROFILER_ENABLED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AsyncProfilerTest extends TestBaseImpl
{
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    private Cluster cluster;

    /**
     * Test-friendly kernel params check that returns valid values without reading from /proc
     */
    public static class TestAsyncProfilerKernelParamsCheck extends StartupChecks.AsyncProfilerKernelParamsCheck
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

    @Test
    public void testNodetoolCommands() throws Throwable
    {
        File newTmpDir = new File(tmpDir.newFolder());

        try (WithProperties withProperties = new WithProperties()
                                             .set(ASYNC_PROFILER_ENABLED, true);
             Cluster cluster = init(builder().withNodes(1).withConfig(c -> c.with(Feature.JMX)).start()))
        {
            this.cluster = cluster;

            // start / stop / list
            assertTrue(status().contains("Profiler is not active"));
            startAndAssert();
            Uninterruptibles.sleepUninterruptibly(10, SECONDS);
            stop();

            // start / stop with file name
            startAndAssert();
            Uninterruptibles.sleepUninterruptibly(10, SECONDS);
            String fileName = UUID.randomUUID().toString();
            stop(fileName);
            assertTrue(list().contains(fileName));

            // fetch
            File destination = new File(newTmpDir, UUID.randomUUID().toString());
            fetch(fileName, destination.absolutePath());
            assertTrue(destination.length() != 0);

            // list
            assertFalse(list().isEmpty());
            assertTrue(list().contains(fileName));

            // purge
            purge();
            assertTrue(list().isEmpty());

            // double start
            startAndAssert();
            NodeToolResult secondStart = start();
            secondStart.asserts().failure();
            assertTrue(secondStart.getStderr().contains("Profiler has already started"));
        }
    }

    @Test
    public void testListPurgeFetchWorksWithDisabledProfiler() throws Throwable
    {
        File newTmpDir = new File(tmpDir.newFolder());
        try (WithProperties withProperties = new WithProperties().set(ASYNC_PROFILER_ENABLED, false);
             Cluster cluster = init(builder().withNodes(1).withConfig(c -> c.with(Feature.JMX)).start()))
        {
            this.cluster = cluster;

            String fileNameToWriteTo = UUID.randomUUID().toString();

            FileUtils.write(new File(newTmpDir, fileNameToWriteTo), List.of("hello world"), StandardOpenOption.CREATE_NEW);

            // Initialize AsyncProfilerService instance in the cluster node context with the test directory
            String tmpDirPath = newTmpDir.absolutePath();
            cluster.get(1).runOnInstance(() -> {
                AsyncProfilerService.instance(tmpDirPath, true, new TestAsyncProfilerKernelParamsCheck());
            });

            // fetch
            String destinationFileName = UUID.randomUUID().toString();
            File destination = new File(newTmpDir, destinationFileName);
            fetch(fileNameToWriteTo, destination.absolutePath());
            assertTrue(destination.length() != 0);

            // list
            assertFalse(list().isEmpty());
            assertTrue(list().contains(fileNameToWriteTo));

            // purge
            purge();
            assertTrue(list().isEmpty());
        }
    }

    private String list()
    {
        NodeToolResult result = cluster.get(1).nodetoolResult("profile", "list");
        result.asserts().success();
        return result.getStdout();
    }

    private void stop()
    {
        NodeToolResult result = cluster.get(1).nodetoolResult("profile", "stop");
        result.asserts().success();
    }

    private void stop(String file)
    {
        NodeToolResult result = cluster.get(1).nodetoolResult("profile", "stop", "-o", file);
        result.asserts().success();
    }

    private NodeToolResult startAndAssert()
    {
        NodeToolResult result = cluster.get(1).nodetoolResult("profile", "start", "-e", "cpu");
        result.asserts().success();
        return result;
    }

    private NodeToolResult start()
    {
        return cluster.get(1).nodetoolResult("profile", "start", "-e", "cpu");
    }

    private String status()
    {
        NodeToolResult result = cluster.get(1).nodetoolResult("profile", "status");
        result.asserts().success();
        return result.getStdout();
    }

    private void purge()
    {
        NodeToolResult result = cluster.get(1).nodetoolResult("profile", "purge");
        result.asserts().success();
    }

    private void fetch(String what, String where)
    {
        NodeToolResult result = cluster.get(1).nodetoolResult("profile", "fetch", what, where);
        result.asserts().success();
    }
}

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

import java.io.IOException;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;

import static org.junit.Assert.assertEquals;

public class NodeToolTest extends TestBaseImpl
{
    private static Cluster CLUSTER;
    private static IInvokableInstance NODE;

    @BeforeClass
    public static void before() throws IOException
    {
        CLUSTER = init(Cluster.build().withNodes(1).start());
        NODE = CLUSTER.get(1);
    }

    @AfterClass
    public static void after()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void testCommands()
    {
        assertEquals(0, NODE.nodetool("help"));
        assertEquals(0, NODE.nodetool("flush"));
        assertEquals(1, NODE.nodetool("not_a_legal_command"));
    }

    @Test
    public void testCaptureConsoleOutput()
    {
        NodeToolResult ringResult = NODE.nodetoolResult("ring");
        ringResult.asserts().stdoutContains("Datacenter: datacenter0");
        ringResult.asserts().stdoutContains("127.0.0.1       rack0       Up     Normal");
        assertEquals("Non-empty error output", "", ringResult.getStderr());
    }

    @Test
    public void testNodetoolSystemExit()
    {
        // Verify currently calls System.exit, this test uses that knowlege to test System.exit behavior in jvm-dtest
        NODE.nodetoolResult("verify", "--check-tokens", "--force")
            .asserts()
            .failure()
            .stdoutContains("Token verification requires --extended-verify");
    }

    @Test
    public void testSetGetTimeout()
    {
        Consumer<String> test = timeout ->
        {
            if (timeout != null)
                NODE.nodetool("settimeout", "internodestreaminguser", timeout);
            timeout = NODE.callOnInstance(() -> String.valueOf(DatabaseDescriptor.getInternodeStreamingTcpUserTimeoutInMS()));
            NODE.nodetoolResult("gettimeout", "internodestreaminguser")
                .asserts()
                .success()
                .stdoutContains("Current timeout for type internodestreaminguser: " + timeout + " ms");
        };

        test.accept(null); // test the default value
        test.accept("1000"); // 1 second
        test.accept("36000000"); // 10 minutes
    }

    @Test
    public void testSetTimeoutInvalidInput()
    {
        NODE.nodetoolResult("settimeout", "internodestreaminguser", "-1")
            .asserts()
            .failure()
            .stdoutContains("timeout must be non-negative");
    }

    @Test
    public void testSetCacheCapacityWhenDisabled() throws Throwable
    {
        try (ICluster cluster = init(builder().withNodes(1).withConfig(c->c.set("row_cache_size", "0MiB")).start()))
        {
            NodeToolResult ringResult = cluster.get(1).nodetoolResult("setcachecapacity", "1", "1", "1");
            ringResult.asserts().stderrContains("is not permitted as this cache is disabled");
        }
    }

    @Test
    public void testInfoOutput() throws Throwable
    {
        try (ICluster<?> cluster = init(builder().withNodes(1).start()))
        {
            NodeToolResult ringResult = cluster.get(1).nodetoolResult("info");
            ringResult.asserts().stdoutContains("ID");
            ringResult.asserts().stdoutContains("Gossip active");
            ringResult.asserts().stdoutContains("Native Transport active");
            ringResult.asserts().stdoutContains("Load");
            ringResult.asserts().stdoutContains("Uncompressed load");
            ringResult.asserts().stdoutContains("Generation");
            ringResult.asserts().stdoutContains("Uptime");
            ringResult.asserts().stdoutContains("Heap Memory");
        }
    }

    @Test
    public void testVersionIncludesGitSHAWhenVerbose() throws Throwable
    {
        NODE.nodetoolResult("version")
            .asserts()
            .success()
            .stdoutNotContains("GitSHA:");

        NODE.nodetoolResult("version", "--verbose")
            .asserts()
            .success()
            .stdoutContains("GitSHA:");
    }
}

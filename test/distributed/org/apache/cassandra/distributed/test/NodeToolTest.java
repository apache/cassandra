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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.NodeToolResult;

import static org.junit.Assert.assertEquals;

public class NodeToolTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void before() throws IOException
    {
        CLUSTER = init(Cluster.build().withNodes(1).start());
    }

    @AfterClass
    public static void after()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void testCommands() throws Throwable
    {
        assertEquals(0, CLUSTER.get(1).nodetool("help"));
        assertEquals(0, CLUSTER.get(1).nodetool("flush"));
        assertEquals(1, CLUSTER.get(1).nodetool("not_a_legal_command"));
    }

    @Test
    public void testCaptureConsoleOutput() throws Throwable
    {
        NodeToolResult ringResult = CLUSTER.get(1).nodetoolResult("ring");
        ringResult.asserts().stdoutContains("Datacenter: datacenter0");
        ringResult.asserts().stdoutContains("127.0.0.1  rack0       Up     Normal");
        assertEquals("Non-empty error output", "", ringResult.getStderr());
    }

    @Test
    public void testNodetoolSystemExit()
    {
        // Verify currently calls System.exit, this test uses that knowlege to test System.exit behavior in jvm-dtest
        CLUSTER.get(1).nodetoolResult("verify", "--check-tokens")
               .asserts()
               .failure()
               .stdoutContains("Token verification requires --extended-verify");
    }
}

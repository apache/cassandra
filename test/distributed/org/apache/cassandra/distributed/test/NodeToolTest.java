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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.NodeToolResult;

import static org.junit.Assert.assertEquals;

public class NodeToolTest extends TestBaseImpl
{
    @Test
    public void testCommands() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(1)))
        {
            assertEquals(0, cluster.get(1).nodetool("help"));
            assertEquals(0, cluster.get(1).nodetool("flush"));
            assertEquals(1, cluster.get(1).nodetool("not_a_legal_command"));
        }
    }

    @Test
    public void testCaptureConsoleOutput() throws Throwable
    {
        try (ICluster cluster = init(builder().withNodes(1).start()))
        {
            NodeToolResult ringResult = cluster.get(1).nodetoolResult("ring");
            ringResult.asserts().stdoutContains("Datacenter: datacenter0");
            ringResult.asserts().stdoutContains("127.0.0.1  rack0       Up     Normal");
            assertEquals("Non-empty error output", "", ringResult.getStderr());
        }
    }

    @Test
    public void testSetCacheCapacityWhenDisabled() throws Throwable
    {
        try (ICluster cluster = init(builder().withNodes(1).withConfig(c->c.set("row_cache_size_in_mb", "0")).start()))
        {
            NodeToolResult ringResult = cluster.get(1).nodetoolResult("setcachecapacity", "1", "1", "1");
            ringResult.asserts().stderrContains("is not permitted as this cache is disabled");
        }
    }
}

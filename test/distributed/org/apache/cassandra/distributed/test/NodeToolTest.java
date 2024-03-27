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

import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;

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
        try (Cluster cluster = init(builder().withNodes(1).start()))
        {
            NodeToolResult ringResult = cluster.get(1).nodetoolResult("ring");
            ringResult.asserts().stdoutContains("Datacenter: datacenter0");
            ringResult.asserts().stdoutContains("127.0.0.1  rack0       Up     Normal");
            assertEquals("Non-empty error output", "", ringResult.getStderr());
        }
    }

    @Test
    public void drainShutsDown() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(1)))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl(pk int PRIMARY KEY)"));

            // make sure read/write works
            IInvokableInstance node = cluster.get(1);
            node.executeInternal(withKeyspace("INSERT INTO %s.tbl (pk) VALUES (?)"), 0);
            SimpleQueryResult qr = node.executeInternalWithResult(withKeyspace("SELECT * FROM %s.tbl"));
            Assert.assertArrayEquals(new Integer[]{ 0 }, Iterators.toArray(qr.map(r -> r.getInteger("pk")), Integer.class));

            // drain will cause writes to fail, but reads will still work
            node.nodetoolResult("drain").asserts().success();

            // verify reads
            qr = node.executeInternalWithResult(withKeyspace("SELECT * FROM %s.tbl"));
            Assert.assertArrayEquals(new Integer[]{ 0 }, Iterators.toArray(qr.map(r -> r.getInteger("pk")), Integer.class));
            // verify writes; must use coordinator, else the cluster hangs waiting for a CL segment that won't come
            try
            {
                cluster.coordinator(1).executeWithResult(withKeyspace("INSERT INTO %s.tbl (pk) VALUES (?)"), ConsistencyLevel.ONE, 1);
            }
            catch (Exception e)
            {
                Throwable cause = Throwables.getRootCause(e);
                // test runs in app classloader, but the exception will exist in the instance classloader; validate via name
                if (!"org.apache.cassandra.exceptions.WriteTimeoutException".equals(cause.getClass().getCanonicalName()))
                    throw e;
            }
        }
    }
}

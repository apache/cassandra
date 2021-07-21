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

package org.apache.cassandra.tools.nodetool;

import java.io.IOException;
import java.lang.InterruptedException;
import java.util.Map;
import javax.management.openmbean.TabularData;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(OrderedJUnit4ClassRunner.class)
public class TTLSnapshotTest extends CQLTester
{
    private static NodeProbe probe;

    @BeforeClass
    public static void setup() throws Exception
    {
        StorageService.instance.initServer();
        startJMXServer();
        probe = new NodeProbe(jmxHost, jmxPort);
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        probe.close();
    }

    @Test
    public void testTTLSnapshot_InvalidArgument()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("snapshot","--ttl","30s");

        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("ttl for snapshot must be at least 1 minute");
    
        tool = ToolRunner.invokeNodetool("snapshot","--ttl","invalid-ttl");
        assertThat(tool.getExitCode()).isEqualTo(1);
    }

    @Test
    public void testTTLSnapshot_SimpleCleanup() throws InterruptedException
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("snapshot","--ttl","1m", "-t", "some-name");

        tool.assertOnCleanExit();
        Map<String, TabularData> snapshots_before = probe.getSnapshotDetails();
        assertThat(snapshots_before).containsKey("some-name");

        Thread.sleep(50000);

        Map<String, TabularData> snapshots_after = probe.getSnapshotDetails();
        assertThat(snapshots_after).containsKey("some-name");

        Thread.sleep(50000);

        snapshots_after = probe.getSnapshotDetails();
        assertThat(snapshots_after).doesNotContainKey("some-name");
    }
}

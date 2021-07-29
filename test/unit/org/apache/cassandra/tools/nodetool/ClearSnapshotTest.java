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
import java.util.Map;
import javax.management.openmbean.TabularData;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class ClearSnapshotTest extends CQLTester
{
    private static NodeProbe probe;

    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
        probe = new NodeProbe(jmxHost, jmxPort);
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        probe.close();
    }

    @Test
    public void testClearSnapshot_NoArgs()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("clearsnapshot");
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertThat(tool.getCleanedStderr()).contains("Specify snapshot name or --all");
        
        tool = ToolRunner.invokeNodetool("clearsnapshot", "--all");
        tool.assertOnCleanExit();
    }

    @Test
    public void testClearSnapshot_AllAndName()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("clearsnapshot", "-t", "some-name", "--all");
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertThat(tool.getCleanedStderr()).contains("Specify only one of snapshot name or --all");
    }

    @Test
    public void testClearSnapshot_RemoveByName()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("snapshot","-t","some-name");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();
        
        Map<String, TabularData> snapshots_before = probe.getSnapshotDetails();
        assertThat(snapshots_before).containsKey("some-name");

        tool = ToolRunner.invokeNodetool("clearsnapshot","-t","some-name");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();
        
        Map<String, TabularData> snapshots_after = probe.getSnapshotDetails();
        assertThat(snapshots_after).doesNotContainKey("some-name");
    }

    @Test
    public void testClearSnapshot_RemoveMultiple()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("snapshot","-t","some-name");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();

        tool = ToolRunner.invokeNodetool("snapshot","-t","some-other-name");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();

        Map<String, TabularData> snapshots_before = probe.getSnapshotDetails();
        assertThat(snapshots_before).hasSize(2);

        tool = ToolRunner.invokeNodetool("clearsnapshot","--all");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isNotEmpty();
        
        Map<String, TabularData> snapshots_after = probe.getSnapshotDetails();
        assertThat(snapshots_after).isEmpty();
    }
    
}

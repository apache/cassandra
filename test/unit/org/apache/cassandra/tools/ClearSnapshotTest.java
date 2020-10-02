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

import java.io.IOException;
import java.util.Map;

import javax.management.openmbean.TabularData;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
        ToolResult tool = ToolRunner.invokeNodetool("clearsnapshot");
        assertEquals(2, tool.getExitCode());
        assertTrue("Tool stderr: " +  tool.getCleanedStderr(), tool.getCleanedStderr().contains("Specify snapshot name or --all"));
        
        tool = ToolRunner.invokeNodetool("clearsnapshot", "--all");
        tool.assertOnCleanExit();
    }

    @Test
    public void testClearSnapshot_AllAndName()
    {
        ToolResult tool = ToolRunner.invokeNodetool("clearsnapshot", "-t", "some-name", "--all");
        assertEquals(2, tool.getExitCode());
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Specify only one of snapshot name or --all"));
    }

    @Test
    public void testClearSnapshot_RemoveByName()
    {
        ToolResult tool = ToolRunner.invokeNodetool("snapshot","-t","some-name");
        tool.assertOnCleanExit();
        assertTrue(!tool.getStdout().isEmpty());
        
        Map<String, TabularData> snapshots_before = probe.getSnapshotDetails();
        Assert.assertTrue(snapshots_before.containsKey("some-name"));
        
        tool = ToolRunner.invokeNodetool("clearsnapshot","-t","some-name");
        tool.assertOnCleanExit();
        assertTrue(!tool.getStdout().isEmpty());
        
        Map<String, TabularData> snapshots_after = probe.getSnapshotDetails();
        Assert.assertFalse(snapshots_after.containsKey("some-name"));
    }

    @Test
    public void testClearSnapshot_RemoveMultiple()
    {
        ToolResult tool = ToolRunner.invokeNodetool("snapshot","-t","some-name");
        tool.assertOnCleanExit();
        assertTrue(!tool.getStdout().isEmpty());

        tool = ToolRunner.invokeNodetool("snapshot","-t","some-other-name");
        tool.assertOnCleanExit();
            assertTrue(!tool.getStdout().isEmpty());
        
        Map<String, TabularData> snapshots_before = probe.getSnapshotDetails();
        Assert.assertTrue(snapshots_before.size() == 2);

        tool = ToolRunner.invokeNodetool("clearsnapshot","--all");
        tool.assertOnCleanExit();
        assertTrue(!tool.getStdout().isEmpty());
        
        Map<String, TabularData> snapshots_after = probe.getSnapshotDetails();
        Assert.assertTrue(snapshots_after.size() == 0);
    }
    
}
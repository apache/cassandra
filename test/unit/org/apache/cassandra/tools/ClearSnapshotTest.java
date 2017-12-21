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

import org.apache.commons.lang3.ArrayUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.service.EmbeddedCassandraService;

public class ClearSnapshotTest extends ToolsTester
{
    private static EmbeddedCassandraService cassandra;
    private static String initialJmxPortValue;
    private static NodeProbe probe;
    private static final int JMX_PORT = 7188;

    @BeforeClass
    public static void setup() throws IOException
    {
        // Set system property to enable JMX port on localhost for embedded server
        initialJmxPortValue = System.getProperty("cassandra.jmx.local.port");
        System.setProperty("cassandra.jmx.local.port", String.valueOf(JMX_PORT));

        SchemaLoader.prepareServer();
        cassandra = new EmbeddedCassandraService();
        cassandra.start();

        probe = new NodeProbe("127.0.0.1", JMX_PORT);
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        cassandra.stop();
        if (initialJmxPortValue != null)
        {
            System.setProperty("cassandra.jmx.local.port", initialJmxPortValue);
        }

        probe.close();
    }

    private String[] constructParamaterArray(final String command, final String... commandParams)
    {
        String[] baseCommandLine = {"-p", String.valueOf(JMX_PORT), command};
        return ArrayUtils.addAll(baseCommandLine, commandParams);
    }

    @Test
    public void testClearSnapshot_NoArgs() throws IOException
    {
        runTool(2, "org.apache.cassandra.tools.NodeTool",
                constructParamaterArray("clearsnapshot"));
    }

    @Test
    public void testClearSnapshot_AllAndName() throws IOException
    {
        runTool(2, "org.apache.cassandra.tools.NodeTool",
                constructParamaterArray("clearsnapshot", "-t", "some-name", "--all"));
    }

    @Test
    public void testClearSnapshot_RemoveByName() throws IOException
    {
         runTool(0,"org.apache.cassandra.tools.NodeTool",
                 constructParamaterArray("snapshot","-t","some-name"));

         Map<String, TabularData> snapshots_before = probe.getSnapshotDetails();
         Assert.assertTrue(snapshots_before.containsKey("some-name"));

         runTool(0,"org.apache.cassandra.tools.NodeTool",
                 constructParamaterArray("clearsnapshot","-t","some-name"));
         Map<String, TabularData> snapshots_after = probe.getSnapshotDetails();
         Assert.assertFalse(snapshots_after.containsKey("some-name"));
    }

    @Test
    public void testClearSnapshot_RemoveMultiple() throws IOException
    {
        runTool(0,"org.apache.cassandra.tools.NodeTool",
                constructParamaterArray("snapshot","-t","some-name"));
        runTool(0,"org.apache.cassandra.tools.NodeTool",
                constructParamaterArray("snapshot","-t","some-other-name"));

        Map<String, TabularData> snapshots_before = probe.getSnapshotDetails();
        Assert.assertTrue(snapshots_before.size() == 2);

        runTool(0,"org.apache.cassandra.tools.NodeTool",
                constructParamaterArray("clearsnapshot","--all"));
        Map<String, TabularData> snapshots_after = probe.getSnapshotDetails();
        Assert.assertTrue(snapshots_after.size() == 0);
    }
    
}
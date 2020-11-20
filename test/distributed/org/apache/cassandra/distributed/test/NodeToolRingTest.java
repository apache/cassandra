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
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NodeToolRingTest extends TestBaseImpl
{
    private static ICluster cluster;

    @Before
    public void setupEnv() throws IOException
    {
        if (cluster == null)
            cluster = init(builder().withNodes(1).start());
    }

    @AfterClass
    public static void teardownEnv() throws Exception
    {
        cluster.close();
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeNodetoolJvmDtest(cluster.get(1), "--wrongarg", "ring");
        Assertions.assertThat(tool.getStdout()).containsIgnoringCase("nodetool help");
        assertEquals(1, tool.getExitCode());
        assertTrue(tool.getCleanedStderr().isEmpty());
    }

    @Test
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary

        ToolResult tool = ToolRunner.invokeNodetoolJvmDtest(cluster.get(1), "help", "ring");
        
        String help = "NAME\n" + "        nodetool ring - Print information about the token ring\n"
                      + "\n"
                      + "SYNOPSIS\n"
                      + "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n"
                      + "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n"
                      + "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n"
                      + "                [(-u <username> | --username <username>)] ring [(-r | --resolve-ip)]\n"
                      + "                [--] [<keyspace>]\n"
                      + "\n"
                      + "OPTIONS\n"
                      + "        -h <host>, --host <host>\n"
                      + "            Node hostname or ip address\n"
                      + "\n"
                      + "        -p <port>, --port <port>\n"
                      + "            Remote jmx agent port number\n"
                      + "\n"
                      + "        -pp, --print-port\n"
                      + "            Operate in 4.0 mode with hosts disambiguated by port number\n"
                      + "\n"
                      + "        -pw <password>, --password <password>\n"
                      + "            Remote jmx agent password\n"
                      + "\n"
                      + "        -pwf <passwordFilePath>, --password-file <passwordFilePath>\n"
                      + "            Path to the JMX password file\n"
                      + "\n"
                      + "        -r, --resolve-ip\n"
                      + "            Show node domain names instead of IPs\n"
                      + "\n"
                      + "        -u <username>, --username <username>\n"
                      + "            Remote jmx agent username\n"
                      + "\n"
                      + "        --\n"
                      + "            This option can be used to separate command-line options from the\n"
                      + "            list of argument, (useful when arguments might be mistaken for\n"
                      + "            command-line options\n"
                      + "\n"
                      + "        <keyspace>\n"
                      + "            Specify a keyspace for accurate ownership information (topology\n"
                      + "            awareness)\n"
                      + "\n"
                      + "\n";
        Assertions.assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testRing()
    {
        ToolResult tool = ToolRunner.invokeNodetoolJvmDtest(cluster.get(1), "ring");
        
        Assertions.assertThat(tool.getStdout())
                  .contains("Datacenter: datacenter0")
                  .contains("Address    Rack        Status State   Load            Owns                Token")
                  .contains("127.0.0.1  rack0       Up     Normal")
                  .contains("100.00%             9223372036854775807");
        assertEquals(0, tool.getExitCode());
        assertTrue(tool.getCleanedStderr().isEmpty());
    }

    @Test
    public void testRingPrintPort()
    {
        Arrays.asList("-pp", "--print-port").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeNodetoolJvmDtest(cluster.get(1), arg, "ring");
            Assertions.assertThat(tool.getStdout())
                      .contains("Datacenter: datacenter0")
                      .contains("Address         Rack        Status State   Load            Owns                Token")
                      .contains("Unknown")
                      .contains("100.00%             9223372036854775807");
            assertEquals(0, tool.getExitCode());
            assertTrue(tool.getCleanedStderr().isEmpty());
        });
    }

    @Test
    public void testRingResolve()
    {
        Arrays.asList("-r", "--resolve-ip").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeNodetoolJvmDtest(cluster.get(1), "ring", arg);
            Assertions.assertThat(tool.getStdout())
                      .contains("Datacenter: datacenter0")
                      .contains("Address    Rack        Status State   Load            Owns                Token")
                      .contains("localhost")
                      .contains("rack0       Up     Normal")
                      .contains("100.00%             9223372036854775807");
            assertEquals(0, tool.getExitCode());
            assertTrue(tool.getCleanedStderr().isEmpty());
        });
    }

    @Test
    public void testRingKeyspace()
    {
        // Bad KS
        ToolResult tool = ToolRunner.invokeNodetoolJvmDtest(cluster.get(1), "ring", "mockks");
        Assertions.assertThat(tool.getStdout()).contains("The keyspace mockks, does not exist");
        assertEquals(0, tool.getExitCode());
        assertTrue(tool.getCleanedStderr().isEmpty());

        // Good KS
        tool = ToolRunner.invokeNodetoolJvmDtest(cluster.get(1), "ring", "system_schema");
        Assertions.assertThat(tool.getStdout()).contains("Datacenter: datacenter0");
        assertEquals(0, tool.getExitCode());
        assertTrue(tool.getCleanedStderr().isEmpty());
    }
}

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
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Session;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NodeToolEnableDisableBinaryTest extends TestBaseImpl
{
    private static ICluster cluster;

    @Before
    public void setupEnv() throws IOException
    {
        if (cluster == null)
            cluster = init(builder().withNodes(1)
                                    .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                    .start());
    }

    @AfterClass
    public static void teardownEnv() throws Exception
    {
        cluster.close();
    }

    @Test
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary

        ToolResult tool = ToolRunner.invokeNodetoolJvmDtest(cluster.get(1), "help", "disablebinary");
        String help =   "NAME\n" + 
                        "        nodetool disablebinary - Disable native transport (binary protocol)\n" + 
                        "\n" + 
                        "SYNOPSIS\n" + 
                        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" + 
                        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" + 
                        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" + 
                        "                [(-u <username> | --username <username>)] disablebinary\n" + 
                        "\n" + 
                        "OPTIONS\n" + 
                        "        -h <host>, --host <host>\n" + 
                        "            Node hostname or ip address\n" + 
                        "\n" + 
                        "        -p <port>, --port <port>\n" + 
                        "            Remote jmx agent port number\n" + 
                        "\n" + 
                        "        -pp, --print-port\n" + 
                        "            Operate in 4.0 mode with hosts disambiguated by port number\n" + 
                        "\n" + 
                        "        -pw <password>, --password <password>\n" + 
                        "            Remote jmx agent password\n" + 
                        "\n" + 
                        "        -pwf <passwordFilePath>, --password-file <passwordFilePath>\n" + 
                        "            Path to the JMX password file\n" + 
                        "\n" + 
                        "        -u <username>, --username <username>\n" + 
                        "            Remote jmx agent username\n" + 
                        "\n" + 
                        "\n";
        Assertions.assertThat(tool.getStdout()).isEqualTo(help);

        tool = ToolRunner.invokeNodetoolJvmDtest(cluster.get(1), "help", "enablebinary");
        help =  "NAME\n" + 
                "        nodetool enablebinary - Reenable native transport (binary protocol)\n" + 
                "\n" + 
                "SYNOPSIS\n" + 
                "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" + 
                "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" + 
                "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" + 
                "                [(-u <username> | --username <username>)] enablebinary\n" + 
                "\n" + 
                "OPTIONS\n" + 
                "        -h <host>, --host <host>\n" + 
                "            Node hostname or ip address\n" + 
                "\n" + 
                "        -p <port>, --port <port>\n" + 
                "            Remote jmx agent port number\n" + 
                "\n" + 
                "        -pp, --print-port\n" + 
                "            Operate in 4.0 mode with hosts disambiguated by port number\n" + 
                "\n" + 
                "        -pw <password>, --password <password>\n" + 
                "            Remote jmx agent password\n" + 
                "\n" + 
                "        -pwf <passwordFilePath>, --password-file <passwordFilePath>\n" + 
                "            Path to the JMX password file\n" + 
                "\n" + 
                "        -u <username>, --username <username>\n" + 
                "            Remote jmx agent username\n" + 
                "\n" + 
                "\n";
        Assertions.assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testEnableDisableBinary() throws Throwable
    {
        // We can connect
        assertTrue(canConnect());

        // We can't connect after disabling
        ToolResult tool = ToolRunner.invokeNodetoolJvmDtest(cluster.get(1), "disablebinary");
        Assertions.assertThat(tool.getStdout()).containsIgnoringCase("Stop listening for CQL clients");
        assertTrue(tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
        assertFalse(canConnect());

        // We can connect after re-enabling
        tool = ToolRunner.invokeNodetoolJvmDtest(cluster.get(1), "enablebinary");
        Assertions.assertThat(tool.getStdout()).containsIgnoringCase("Starting listening for CQL clients");
        assertTrue(tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
        assertTrue(canConnect());
    }

    private boolean canConnect()
    {
        boolean canConnect = false;
        try(com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder()
                                                                                 .addContactPoint("127.0.0.1")
                                                                                 .build();
            Session s = c.connect("system_schema"))
        {
            s.execute("SELECT * FROM system_schema.aggregates");
            canConnect = true;
        }
        catch(Exception e)
        {
            canConnect = false;
        }

        return canConnect;
    }
}

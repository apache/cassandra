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

import org.apache.commons.lang3.StringUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.net.Verb.ECHO_REQ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NodeToolGossipInfoTest extends CQLTester
{
    private static NodeProbe probe;

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
        probe = new NodeProbe(jmxHost, jmxPort);
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        probe.close();
    }

    @Test
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolResult tool = ToolRunner.invokeNodetool("help", "gossipinfo");
        String help =   "NAME\n" + 
                        "        nodetool gossipinfo - Shows the gossip information for the cluster\n" + 
                        "\n" + 
                        "SYNOPSIS\n" + 
                        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" + 
                        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" + 
                        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" + 
                        "                [(-u <username> | --username <username>)] gossipinfo\n" + 
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
    public void testGossipInfo() throws Throwable
    {
        ToolResult tool = ToolRunner.invokeNodetool("gossipinfo");
        Assertions.assertThat(tool.getStdout()).contains("/127.0.0.1");
        Assertions.assertThat(tool.getStdout()).containsPattern("heartbeat:[0-9]+");
        Assertions.assertThat(tool.getStdout()).containsPattern("STATUS:[0-9]+:NORMAL,.+");
        Assertions.assertThat(tool.getStdout()).containsPattern("SCHEMA:.+");
        Assertions.assertThat(tool.getStdout()).containsPattern("DC:[0-9]+:datacenter1");
        Assertions.assertThat(tool.getStdout()).containsPattern("RACK:[0-9]+:rack1");
        Assertions.assertThat(tool.getStdout()).containsPattern("RELEASE_VERSION:.+");
        Assertions.assertThat(tool.getStdout()).containsPattern("RPC_ADDRESS:[0-9]+:127.0.0.1");
        Assertions.assertThat(tool.getStdout()).containsPattern("NET_VERSION:[0-9]+:.+");
        Assertions.assertThat(tool.getStdout()).containsPattern("HOST_ID:[0-9]+:.+");
        Assertions.assertThat(tool.getStdout()).containsPattern("NATIVE_ADDRESS_AND_PORT:[0-9]+:127.0.0.1:[0-9]+");
        Assertions.assertThat(tool.getStdout()).containsPattern("STATUS_WITH_PORT:[0-9]+:NORMAL,.+");
        Assertions.assertThat(tool.getStdout()).containsPattern("TOKENS:[0-9]+:<hidden>");
        assertTrue(tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());

        // Make sure heartbeats are detected
        Message<NoPayload> echoMessageOut = Message.out(ECHO_REQ, NoPayload.noPayload);
        MessagingService.instance().send(echoMessageOut, FBUtilities.getBroadcastAddressAndPort());

        String origHeartbeatCount = StringUtils.substringBetween(tool.getStdout(), "heartbeat:", "\n");
        tool = ToolRunner.invokeNodetool("gossipinfo");
        assertTrue(tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
        String newHeartbeatCount = StringUtils.substringBetween(tool.getStdout(), "heartbeat:", "\n");
        assertTrue(Integer.parseInt(origHeartbeatCount) <= Integer.parseInt(newHeartbeatCount));
    }
}

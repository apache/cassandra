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

import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.net.Verb.ECHO_REQ;
import static org.assertj.core.api.Assertions.assertThat;

public class GossipInfoTest extends CQLTester
{
    private static String token;

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
        token = StorageService.instance.getTokens().get(0);
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "gossipinfo");
        tool.assertOnCleanExit();

        String help =   "NAME\n" +
                "        nodetool gossipinfo - Shows the gossip information for the cluster\n" +
                "\n" +
                "SYNOPSIS\n" +
                "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                "                [(-u <username> | --username <username>)] gossipinfo\n" +
                "                [(-r | --resolve-ip)]\n" +
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
                "        -r, --resolve-ip\n" +
                "            Show node domain names instead of IPs\n" +
                "\n" +
                "        -u <username>, --username <username>\n" +
                "            Remote jmx agent username\n" +
                "\n" +
                "\n";
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testGossipInfo()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("gossipinfo");
        tool.assertOnCleanExit();
        String stdout = tool.getStdout();
        Assertions.assertThat(stdout).contains("/127.0.0.1");
        Assertions.assertThat(stdout).containsPattern("\\s+generation:[0-9]+");
        Assertions.assertThat(stdout).containsPattern("heartbeat:[0-9]+");
        Assertions.assertThat(stdout).containsPattern("STATUS:[0-9]+:NORMAL," + token);
        Assertions.assertThat(stdout).containsPattern("SCHEMA:.+");
        Assertions.assertThat(stdout).containsPattern("DC:[0-9]+:datacenter1");
        Assertions.assertThat(stdout).containsPattern("RACK:[0-9]+:rack1");
        Assertions.assertThat(stdout).containsPattern("RELEASE_VERSION:.+");
        Assertions.assertThat(stdout).containsPattern("RPC_ADDRESS:[0-9]+:127.0.0.1");
        Assertions.assertThat(stdout).containsPattern("NET_VERSION:[0-9]+:.+");
        Assertions.assertThat(stdout).containsPattern("HOST_ID:[0-9]+:.+");
        Assertions.assertThat(stdout).containsPattern("NATIVE_ADDRESS_AND_PORT:[0-9]+:127.0.0.1:[0-9]+");
        Assertions.assertThat(stdout).containsPattern("SSTABLE_VERSIONS:[0-9]+:");
        Assertions.assertThat(stdout).containsPattern("STATUS_WITH_PORT:[0-9]+:NORMAL,.+");
        Assertions.assertThat(stdout).containsPattern("TOKENS:[0-9]+:<hidden>");

        // Make sure heartbeats are detected
        Message<NoPayload> echoMessageOut = Message.out(ECHO_REQ, NoPayload.noPayload);
        MessagingService.instance().send(echoMessageOut, FBUtilities.getBroadcastAddressAndPort());

        String origHeartbeatCount = StringUtils.substringBetween(stdout, "heartbeat:", "\n");
        tool = ToolRunner.invokeNodetool("gossipinfo");
        tool.assertOnCleanExit();
        String newHeartbeatCount = StringUtils.substringBetween(stdout, "heartbeat:", "\n");
        assertThat(Integer.parseInt(origHeartbeatCount)).isLessThanOrEqualTo(Integer.parseInt(newHeartbeatCount));
    }

    @Test
    public void testGossipInfoWithPortPrint()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("-pp", "gossipinfo");
        tool.assertOnCleanExit();
        String stdout = tool.getStdout();
        Assertions.assertThat(stdout).containsPattern("/127.0.0.1\\:[0-9]+\\s+generation");
    }

    @Test
    public void testGossipInfoWithResolveIp()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("gossipinfo", "--resolve-ip");
        tool.assertOnCleanExit();
        String stdout = tool.getStdout();
        Assertions.assertThat(stdout).containsPattern("^localhost\\s+generation");
    }

    @Test
    public void testGossipInfoWithPortPrintAndResolveIp()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("-pp", "gossipinfo", "--resolve-ip");
        tool.assertOnCleanExit();
        String stdout = tool.getStdout();
        Assertions.assertThat(stdout).containsPattern("^localhost\\:[0-9]+\\s+generation");
    }
}

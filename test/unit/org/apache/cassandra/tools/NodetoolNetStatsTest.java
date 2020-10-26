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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;

import static org.apache.cassandra.net.Verb.ECHO_REQ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class NodetoolNetStatsTest extends CQLTester
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
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolResult tool = ToolRunner.invokeNodetool("help", "netstats");
        String help =   "NAME\n" + 
                        "        nodetool netstats - Print network information on provided host\n" + 
                        "        (connecting node by default)\n" + 
                        "\n" + 
                        "SYNOPSIS\n" + 
                        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" + 
                        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" + 
                        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" + 
                        "                [(-u <username> | --username <username>)] netstats\n" + 
                        "                [(-H | --human-readable)]\n" + 
                        "\n" + 
                        "OPTIONS\n" + 
                        "        -h <host>, --host <host>\n" + 
                        "            Node hostname or ip address\n" + 
                        "\n" + 
                        "        -H, --human-readable\n" + 
                        "            Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB\n" + 
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
    public void testNetStats() throws Throwable
    {
        Message<NoPayload> echoMessageOut = Message.out(ECHO_REQ, NoPayload.noPayload);
        MessagingService.instance().send(echoMessageOut, FBUtilities.getBroadcastAddressAndPort());
        
        ToolResult tool = ToolRunner.invokeNodetool("netstats");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("Gossip messages                 n/a         0              2         0"));
        assertTrue(tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
    }
}

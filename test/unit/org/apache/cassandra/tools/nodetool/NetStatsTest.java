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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamSession.State;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.Verb.ECHO_REQ;
import static org.assertj.core.api.Assertions.assertThat;

public class NetStatsTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "netstats");
        tool.assertOnCleanExit();

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
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testNetStats()
    {
        Message<NoPayload> echoMessageOut = Message.out(ECHO_REQ, NoPayload.noPayload);
        MessagingService.instance().send(echoMessageOut, FBUtilities.getBroadcastAddressAndPort());

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("netstats");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Gossip messages                 n/a         0              2         0");
    }

    @Test
    public void testHumanReadable() throws IOException
    {
        List<StreamSummary> streamSummaries = Collections.singletonList(new StreamSummary(TableId.generate(), 1, 1024));
        SessionInfo info = new SessionInfo(InetAddressAndPort.getLocalHost(),
                                           1,
                                           InetAddressAndPort.getLocalHost(),
                                           streamSummaries,
                                           streamSummaries,
                                           State.COMPLETE,
                                           null);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); PrintStream out = new PrintStream(baos))
        {
            NetStats nstats = new NetStats();

            nstats.printReceivingSummaries(out, info, false);
            String stdout = getSummariesStdout(baos, out);
            assertThat(stdout).doesNotContain("Kib");

            baos.reset();
            nstats.printSendingSummaries(out, info, false);
            stdout = getSummariesStdout(baos, out);
            assertThat(stdout).doesNotContain("Kib");

            baos.reset();
            nstats.printReceivingSummaries(out, info, true);
            stdout = getSummariesStdout(baos, out);
            assertThat(stdout).contains("KiB");

            baos.reset();
            nstats.printSendingSummaries(out, info, true);
            stdout = getSummariesStdout(baos, out);
            assertThat(stdout).contains("KiB");
        }
    }

    private String getSummariesStdout(ByteArrayOutputStream baos, PrintStream ps) throws IOException
    {
        baos.flush();
        ps.flush();
        return baos.toString(StandardCharsets.UTF_8.toString());
    }
}

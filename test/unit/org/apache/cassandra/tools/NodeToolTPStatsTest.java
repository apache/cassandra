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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;
import org.yaml.snakeyaml.Yaml;

import static org.apache.cassandra.net.Verb.ECHO_REQ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class NodeToolTPStatsTest extends CQLTester
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
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolResult tool = ToolRunner.invokeNodetool("help", "tpstats");
        String help =   "NAME\n" + 
                        "        nodetool tpstats - Print usage statistics of thread pools\n" + 
                        "\n" + 
                        "SYNOPSIS\n" + 
                        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" + 
                        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" + 
                        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" + 
                        "                [(-u <username> | --username <username>)] tpstats\n" + 
                        "                [(-F <format> | --format <format>)]\n" + 
                        "\n" + 
                        "OPTIONS\n" + 
                        "        -F <format>, --format <format>\n" + 
                        "            Output format (json, yaml)\n" + 
                        "\n" + 
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
    public void testTPStats() throws Throwable
    {
        ToolResult tool = ToolRunner.invokeNodetool("tpstats");
        Assertions.assertThat(tool.getStdout()).containsPattern("Pool Name \\s* Active Pending Completed Blocked All time blocked");
        Assertions.assertThat(tool.getStdout()).containsIgnoringCase("Latencies waiting in queue (micros) per dropped message types");
        assertTrue(tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());

        // Does inserting data alter tpstats?
        String nonZeroedThreadsRegExp = "((?m)\\D.*[1-9].*)";
        ArrayList<String> origStats = getAllGroupMatches(nonZeroedThreadsRegExp, tool.getStdout());
        Collections.sort(origStats);

        createTable("CREATE TABLE %s (pk int, c int, PRIMARY KEY(pk))");
        execute("INSERT INTO %s (pk, c) VALUES (?, ?)", 1, 1);
        tool = ToolRunner.invokeNodetool("tpstats");
        assertTrue(tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
        ArrayList<String> newStats = getAllGroupMatches(nonZeroedThreadsRegExp, tool.getStdout());
        Collections.sort(newStats);

        assertNotEquals(origStats, newStats);

        // Does sending a message alter Gossip & ECHO stats?
        String origGossip = getAllGroupMatches("((?m)GossipStage.*)", tool.getStdout()).get(0);
        Assertions.assertThat(tool.getStdout()).doesNotContainPattern("ECHO_REQ\\D.*[1-9].*");
        Assertions.assertThat(tool.getStdout()).doesNotContainPattern("ECHO_RSP\\D.*[1-9].*");

        Message<NoPayload> echoMessageOut = Message.out(ECHO_REQ, NoPayload.noPayload);
        MessagingService.instance().send(echoMessageOut, FBUtilities.getBroadcastAddressAndPort());

        tool = ToolRunner.invokeNodetool("tpstats");
        assertTrue(tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
        String newGossip = getAllGroupMatches("((?m)GossipStage.*)", tool.getStdout()).get(0);

        assertNotEquals(origGossip, newGossip);
        Assertions.assertThat(tool.getStdout()).containsPattern("ECHO_REQ\\D.*[1-9].*");
        Assertions.assertThat(tool.getStdout()).containsPattern("ECHO_RSP\\D.*[1-9].*");
    }

    @Test
    public void testFormatArg()
    {
        Arrays.asList(Pair.of("-F", "json"), Pair.of("--format", "json")).forEach(arg -> {
            ToolResult tool = ToolRunner.invokeNodetool("tpstats", arg.getLeft(), arg.getRight());
            String json = tool.getStdout();
            assertThat(isJSONString(json)).isTrue();
            assertThat(json).containsPattern("\"WaitLatencies\"\\s*:\\s*\\{\\s*\"");
            assertTrue(tool.getCleanedStderr().isEmpty());
            assertEquals(0, tool.getExitCode());
        });

        Arrays.asList( Pair.of("-F", "yaml"), Pair.of("--format", "yaml")).forEach(arg -> {
            ToolResult tool = ToolRunner.invokeNodetool("tpstats", arg.getLeft(), arg.getRight());
            String yaml = tool.getStdout();
            assertThat(isYAMLString(yaml)).isTrue();
            assertThat(yaml).containsPattern("WaitLatencies:\\s*[A-Z|_]+:\\s+-\\s");
            assertTrue(tool.getCleanedStderr().isEmpty());
            assertEquals(0, tool.getExitCode());
        });
    }

    public static boolean isJSONString(String str)
    {
        try
        {
            ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(str);
            return true;
        }
        catch(IOException e)
        {
            return false;
        }
    }

    public static boolean isYAMLString(String str)
    {
        try
        {
            Yaml yaml = new Yaml();
            yaml.load(str);
            return true;
        }
        catch(Exception e)
        {
            return false;
        }
    }

    private ArrayList<String> getAllGroupMatches(String regExp, String in)
    {
        Pattern pattern = Pattern.compile(regExp);
        Matcher m = pattern.matcher(in);

        ArrayList<String> matches = new ArrayList<>();
        while (m.find())
            matches.add(m.group(1));

        return matches;
    }
}

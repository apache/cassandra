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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;

public class RingTest extends CQLTester
{
    private static String token;

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
        token = StorageService.instance.getTokens().get(0);
    }

    /**
     * Validate output, making sure the table mappings work with various host-modifying arguments in use.
     */
    @Test
    public void testRingOutput()
    {
        final HostStatWithPort host = new HostStatWithPort(null, FBUtilities.getBroadcastAddressAndPort(),
                                                           false, null);
        validateRingOutput(host.ipOrDns(false), "ring");
        Arrays.asList("-pp", "--print-port").forEach(arg -> validateRingOutput(host.ipOrDns(true), "-pp", "ring"));

        final HostStatWithPort hostResolved = new HostStatWithPort(null, FBUtilities.getBroadcastAddressAndPort(),
                                                                   true, null);
        Arrays.asList("-r", "--resolve-ip").forEach(arg ->
                validateRingOutput(hostResolved.ipOrDns(false), "ring", "-r"));
        validateRingOutput(hostResolved.ipOrDns(true), "-pp", "ring", "-r");
    }

    @SuppressWarnings("DynamicRegexReplaceableByCompiledPattern")
    private void validateRingOutput(String hostForm, String... args)
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(args);
        tool.assertOnCleanExit();
        /*
         Datacenter: datacenter1
         ==========
         Address         Rack        Status State   Load            Owns                Token

         127.0.0.1       rack1       Up     Normal  45.71 KiB       100.00%             4652409154190094022

         */
        String[] lines = tool.getStdout().split("\\R");
        assertThat(lines[1].trim()).endsWith(SimpleSnitch.DATA_CENTER_NAME);
        assertThat(lines[3]).containsPattern("Address *Rack *Status *State *Load *Owns *Token *");
        String hostRing = lines[lines.length-4].trim(); // this command has a couple extra newlines and an empty error message at the end. Not messing with it.
        assertThat(hostRing).startsWith(hostForm);
        assertThat(hostRing).contains(SimpleSnitch.RACK_NAME);
        assertThat(hostRing).contains("Up");
        assertThat(hostRing).contains("Normal");
        assertThat(hostRing).containsPattern("\\d+\\.?\\d+ KiB");
        assertThat(hostRing).containsPattern("\\d+\\.\\d+%");
        assertThat(hostRing).endsWith(token);
        assertThat(hostRing).doesNotContain("?");
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("--wrongarg", "ring");
        tool.assertCleanStdErr();
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("nodetool help");
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "ring");
        tool.assertOnCleanExit();

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
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testRingKeyspace()
    {
        // Bad KS
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("ring", "mockks");
        Assert.assertEquals(1, tool.getExitCode());
        assertThat(tool.getStdout()).contains("The keyspace mockks, does not exist");

        // Good KS
        tool = ToolRunner.invokeNodetool("ring", "system_schema");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Datacenter: datacenter1");
    }
}

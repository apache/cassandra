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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RingTest extends CQLTester
{
    private static String token;

    @BeforeClass
    public static void setup() throws Exception
    {
        StorageService.instance.initServer();
        token = StorageService.instance.getTokens().get(0);
        startJMXServer();
    }

    /**
     * Validate output, making sure the table mappings work with various host-modifying arguments in use.
     */
    @Test
    public void testRingOutput()
    {
        final HostStatWithPort host = new HostStatWithPort(null, FBUtilities.getBroadcastAddressAndPort(),
                                                           false, null);
        validateRingOutput(host.ipOrDns(false),
                            "ring");
        Arrays.asList("-pp", "--print-port").forEach(arg -> {
            validateRingOutput(host.ipOrDns(true),
                               "-pp", "ring");
        });

        final HostStatWithPort hostResolved = new HostStatWithPort(null, FBUtilities.getBroadcastAddressAndPort(),
                                                                   true, null);
        Arrays.asList("-r", "--resolve-ip").forEach(arg -> {
            validateRingOutput(hostResolved.ipOrDns(false),
                               "ring", "-r");
        });
        validateRingOutput(hostResolved.ipOrDns(true),
                            "-pp", "ring", "-r");
    }

    private void validateRingOutput(String hostForm, String... args)
    {
        ToolRunner.ToolResult nodetool = ToolRunner.invokeNodetool(args);
        logger.info(nodetool.getStdout());
        nodetool.assertOnCleanExit();
        /*
         Datacenter: datacenter1
         ==========
         Address         Rack        Status State   Load            Owns                Token

         127.0.0.1       rack1       Up     Normal  45.71 KiB       100.00%             4652409154190094022

         */
        String[] lines = nodetool.getStdout().split("\\R");
        assertThat(lines[1].trim(), endsWith(SimpleSnitch.DATA_CENTER_NAME));
        assertThat(lines[3], matchesPattern("Address *Rack *Status *State *Load *Owns *Token *"));
        String hostRing = lines[lines.length-4].trim(); // this command has a couple extra newlines and an empty error message at the end. Not messing with it.
        assertThat(hostRing, startsWith(hostForm));
        assertThat(hostRing, containsString(SimpleSnitch.RACK_NAME));
        assertThat(hostRing, containsString("Up"));
        assertThat(hostRing, containsString("Normal"));
        assertThat(hostRing, matchesPattern(".*\\d+\\.\\d+ KiB.*"));
        assertThat(hostRing, matchesPattern(".*\\d+\\.\\d+%.*"));
        assertThat(hostRing, endsWith(token));
        assertThat(hostRing, not(containsString("?")));
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("--wrongarg", "ring");
        Assertions.assertThat(tool.getStdout()).containsIgnoringCase("nodetool help");
        assertEquals(1, tool.getExitCode());
        assertTrue(tool.getCleanedStderr().isEmpty());
    }

    @Test
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "ring");

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
    public void testRingKeyspace()
    {
        // Bad KS
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("ring", "mockks");
        Assertions.assertThat(tool.getStdout()).contains("The keyspace mockks, does not exist");
        assertEquals(0, tool.getExitCode());
        assertTrue(tool.getCleanedStderr().isEmpty());

        // Good KS
        tool = ToolRunner.invokeNodetool("ring", "system_schema");
        Assertions.assertThat(tool.getStdout()).contains("Datacenter: datacenter1");
        assertEquals(0, tool.getExitCode());
        assertTrue(tool.getCleanedStderr().isEmpty());
    }
}

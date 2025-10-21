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
import org.apache.cassandra.locator.SimpleLocationProvider;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @see Ring
 */
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
        Arrays.asList("-pp", "--print-port").forEach(arg -> validateRingOutput(host.ipOrDns(true), arg, "ring"));

        final HostStatWithPort hostResolved = new HostStatWithPort(null, FBUtilities.getBroadcastAddressAndPort(),
                                                                   true, null);
        Arrays.asList("-r", "--resolve-ip").forEach(arg ->
                validateRingOutput(hostResolved.ipOrDns(false), "ring", arg));
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
        assertThat(lines[1].trim()).endsWith(SimpleLocationProvider.LOCATION.datacenter);
        assertThat(lines[3]).containsPattern("Address *Rack *Status *State *Load *Owns *Token *");
        String hostRing = lines[lines.length-4].trim(); // this command has a couple extra newlines and an empty error message at the end. Not messing with it.
        assertThat(hostRing).startsWith(hostForm);
        assertThat(hostRing).contains(SimpleLocationProvider.LOCATION.rack);
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

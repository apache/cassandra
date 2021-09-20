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

import java.util.regex.Pattern;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;

public class StatusTest extends CQLTester
{
    private static final Pattern PATTERN = Pattern.compile("\\R");
    private static String localHostId;
    private static String token;

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
        localHostId = StorageService.instance.getLocalHostId();
        token = StorageService.instance.getTokens().get(0);
    }

    /**
     * Validate output, making sure the table mappings work with various host-modifying arguments in use.
     */
    @Test
    public void testStatusOutput()
    {
        HostStatWithPort host = new HostStatWithPort(null, FBUtilities.getBroadcastAddressAndPort(), false, null);
        validateStatusOutput(host.ipOrDns(false),
                            "status");
        validateStatusOutput(host.ipOrDns(true),
                            "-pp", "status");
        host = new HostStatWithPort(null, FBUtilities.getBroadcastAddressAndPort(), true, null);
        validateStatusOutput(host.ipOrDns(false),
                            "status", "-r");
        validateStatusOutput(host.ipOrDns(true),
                            "-pp", "status", "-r");
    }

    /**
     * Validate output, making sure even when bootstrapping any available info is displayed (c16412)
     */
    @Test
    public void testOutputWhileBootstrapping()
    {
        // Deleting these tables will simulate we're bootstrapping
        schemaChange("DROP KEYSPACE " + SchemaConstants.TRACE_KEYSPACE_NAME);
        schemaChange("DROP KEYSPACE " + CQLTester.KEYSPACE);
        schemaChange("DROP KEYSPACE " + CQLTester.KEYSPACE_PER_TEST);

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("status");
        tool.assertOnCleanExit();
        String[] lines = PATTERN.split(tool.getStdout());

        String hostStatus = lines[lines.length-3].trim();
        assertThat(hostStatus).startsWith("UN");
        assertThat(hostStatus).contains(FBUtilities.getJustLocalAddress().getHostAddress());
        assertThat(hostStatus).containsPattern("\\d+\\.?\\d+ KiB");
        assertThat(hostStatus).contains(localHostId);
        assertThat(hostStatus).contains(token);
        assertThat(hostStatus).endsWith(SimpleSnitch.RACK_NAME);

        String bootstrappingWarn = lines[lines.length-1].trim();
        assertThat(bootstrappingWarn)
                .contains("probably still bootstrapping. Effective ownership information is meaningless.");
    }

    private void validateStatusOutput(String hostForm, String... args)
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(args);
        tool.assertOnCleanExit();
        /*
         Datacenter: datacenter1
         =======================
         Status=Up/Down
         |/ State=Normal/Leaving/Joining/Moving
         --  Address    Load       Owns (effective)  Host ID                               Token                Rack
         UN  localhost  45.71 KiB  100.0%            0b1b5e91-ad3b-444e-9c24-50578486978a  1849950853373272258  rack1
         */
        String[] lines = PATTERN.split(tool.getStdout());
        assertThat(lines[0].trim()).endsWith(SimpleSnitch.DATA_CENTER_NAME);
        String hostStatus = lines[lines.length-1].trim();
        assertThat(hostStatus).startsWith("UN");
        assertThat(hostStatus).contains(hostForm);
        assertThat(hostStatus).containsPattern("\\d+\\.?\\d+ KiB");
        assertThat(hostStatus).containsPattern("\\d+\\.\\d+%");
        assertThat(hostStatus).contains(localHostId);
        assertThat(hostStatus).contains(token);
        assertThat(hostStatus).endsWith(SimpleSnitch.RACK_NAME);
        assertThat(hostStatus).doesNotContain("?");
    }
}

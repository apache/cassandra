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
import java.util.regex.Pattern;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.locator.SimpleLocationProvider;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.CMSOperations;
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
        CMSOperations.initJmx();

        localHostId = StorageService.instance.getLocalHostId();
        token = StorageService.instance.getTokens().get(0);
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "status");
        tool.assertOnCleanExit();

        String help = "NAME\n" +
                      "        nodetool status - Print cluster information (state, load, IDs, ...)\n" +
                      "\n" +
                      "SYNOPSIS\n" +
                      "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                      "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                      "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                      "                [(-u <username> | --username <username>)] status [(-c | --cms)]\n" +
                      "                [(-o <sort_order> | --order <sort_order>)] [(-r | --resolve-ip)]\n" +
                      "                [(-s <sort> | --sort <sort>)] [--] [<keyspace>]\n" +
                      "\n" +
                      "OPTIONS\n" +
                      "        -c, --cms\n" +
                      "            Show a node whether is a cms node\n" +
                      "\n" +
                      "        -h <host>, --host <host>\n" +
                      "            Node hostname or ip address\n" +
                      "\n" +
                      "        -o <sort_order>, --order <sort_order>\n" +
                      "            Sorting order: 'asc' for ascending, 'desc' for descending.\n" +
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
                      "        -s <sort>, --sort <sort>\n" +
                      "            Sort by one of 'ip', 'host', 'load', 'owns', 'id', 'rack', 'state'\n" +
                      "            or 'token'. Default ordering is ascending for 'ip', 'host', 'id',\n" +
                      "            'token', 'rack' and descending for 'load', 'owns', 'state'. Sorting\n" +
                      "            by token is possible only when cluster does not use vnodes. When\n" +
                      "            using vnodes, default sorting is by id otherwise by token.\n" +
                      "\n" +
                      "        -u <username>, --username <username>\n" +
                      "            Remote jmx agent username\n" +
                      "\n" +
                      "        --\n" +
                      "            This option can be used to separate command-line options from the\n" +
                      "            list of argument, (useful when arguments might be mistaken for\n" +
                      "            command-line options\n" +
                      "\n" +
                      "        [<keyspace>]\n" +
                      "            The keyspace name\n"
                      + "\n"
                      + "\n";
        assertThat(tool.getStdout()).isEqualTo(help);
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
        assertThat(hostStatus).endsWith(SimpleLocationProvider.LOCATION.rack);

        String bootstrappingWarn = lines[lines.length-1].trim();
        assertThat(bootstrappingWarn)
                .contains("probably still bootstrapping. Effective ownership information is meaningless.");
    }

    private void validateStatusOutputBasic(String hostForm, String expectedEndsWith, String... args)
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
        assertThat(lines[0].trim()).endsWith(SimpleLocationProvider.LOCATION.datacenter);
        String hostStatus = lines[lines.length-1].trim();
        assertThat(hostStatus).startsWith("UN");
        assertThat(hostStatus).contains(hostForm);
        assertThat(hostStatus).containsPattern("\\d+\\.?\\d+ KiB");
        assertThat(hostStatus).containsPattern("\\d+\\.\\d+%");
        assertThat(hostStatus).contains(localHostId);
        assertThat(hostStatus).contains(token);
        assertThat(hostStatus).endsWith(expectedEndsWith);
        assertThat(hostStatus).doesNotContain("?");
    }

    private void validateStatusOutput(String hostForm, String... args)
    {
        validateStatusOutputBasic(hostForm, SimpleLocationProvider.LOCATION.rack, args);
    }

    @Test
    public void testStatusOutputWithCmsOption()
    {
        HostStatWithPort host = new HostStatWithPort(null, FBUtilities.getBroadcastAddressAndPort(), false, null);
        Arrays.asList("-c", "--cms").forEach(arg -> validateStatusOutputBasic(host.ipOrDns(false),
                                                                            Boolean.TRUE.toString(), "status", arg));
    }
}

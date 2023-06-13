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

import java.net.InetSocketAddress;
import java.util.Collections;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.cql3.CIDR;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.virtual.CIDRFilteringMetricsTable;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CIDRFilteringStatsTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.setUpClass();
        CQLTester.requireAuthentication();
        AuthCacheService.initializeAndRegisterCaches();

        String KS_NAME = SchemaConstants.VIRTUAL_VIEWS;
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME,
                                                                      CIDRFilteringMetricsTable.getAll(KS_NAME)));

        startJMXServer();
    }

    @Before
    public void before()
    {
        AuthTestUtils.createUsersWithCidrAccess(Collections.singletonMap("user1",
                                                                         Collections.singletonList("cidrGroup1")));

        AuthTestUtils.insertCidrsMappings(Collections.singletonMap("cidrGroup1",
                                                                   Collections.singletonList(
                                                                   CIDR.getInstance("10.20.30.5/24"))));
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "cidrfilteringstats");
        tool.assertOnCleanExit();

        String help = "NAME\n" +
                      "        nodetool cidrfilteringstats - Print statistics on CIDR filtering\n" +
                      "\n" +
                      "SYNOPSIS\n" +
                      "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                      "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                      "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                      "                [(-u <username> | --username <username>)] cidrfilteringstats\n" +
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
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testCIDRFilteringStats()
    {
        AuthenticatedUser user = new AuthenticatedUser("user1");
        assertTrue(user.hasAccessFromIp(new InetSocketAddress("10.20.30.5", 0)));
        assertFalse(user.hasAccessFromIp(new InetSocketAddress("11.20.30.5", 0)));

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("cidrfilteringstats");
        tool.assertOnCleanExit();

        String output = tool.getStdout();

        assertThat(output).contains(CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable
                                    .CIDR_ACCESSES_ACCEPTED_COUNT_NAME_PREFIX);
        assertThat(output).contains(CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable
                                    .CIDR_ACCESSES_REJECTED_COUNT_NAME_PREFIX);
        assertThat(output).contains(CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable
                                    .CIDR_GROUPS_CACHE_RELOAD_COUNT_NAME);

        assertThat(output).contains(CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable
                                    .CIDR_CHECKS_LATENCY_NAME);
        assertThat(output).contains(CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable
                                    .CIDR_GROUPS_CACHE_RELOAD_LATENCY_NAME);
        assertThat(output).contains(CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable
                                    .LOOKUP_CIDR_GROUPS_FOR_IP_LATENCY_NAME);
    }
}

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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static java.lang.String.format;
import static org.apache.cassandra.auth.AuthKeyspace.CIDR_GROUPS;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_A;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_B;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public class ReloadCIDRGroupsCacheTest extends CQLTester
{
    static InetSocketAddress ipAddr;

    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.setUpClass();
        CQLTester.requireAuthentication();

        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_A, AuthTestUtils.getLoginRoleOptions());
        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_B, AuthTestUtils.getLoginRoleOptions());
        AuthCacheService.initializeAndRegisterCaches();

        startJMXServer();

        ipAddr = new InetSocketAddress("127.0.0.0", 0);
    }

    @Before
    public void before() throws Throwable
    {
        execute(format("insert into %s.%s (cidr_group, cidrs) values('test1', { ('10.11.12.0', 24) })",
                       AUTH_KEYSPACE_NAME, CIDR_GROUPS));
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "reloadcidrgroupscache");
        tool.assertOnCleanExit();

        String help = "NAME\n" +
                      "        nodetool reloadcidrgroupscache - Reload CIDR groups cache with latest\n" +
                      "        entries in cidr_groups table, when CIDR authorizer is enabled\n" +
                      "\n" +
                      "SYNOPSIS\n" +
                      "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                      "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                      "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                      "                [(-u <username> | --username <username>)] reloadcidrgroupscache\n" +
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

    private long getCidrGroupsReadCount()
    {
        return DatabaseDescriptor.getCIDRAuthorizer().getCidrAuthorizerMetrics().cacheReloadCount.getCount();
    }

    @Test
    public void testReloadCidrGroupsCache()
    {
        AuthenticatedUser role = new AuthenticatedUser(ROLE_A.getRoleName());

        long originalReadsCount = getCidrGroupsReadCount();

        // ensure cidr groups cache not reloaded
        role.hasAccessFromIp(ipAddr);
        assertThat(originalReadsCount).isEqualTo(getCidrGroupsReadCount());

        // invalidate all, which reloads cidr groups cache
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("reloadcidrgroupscache");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Reloaded CIDR groups cache");

        // ensure cidr groups cache reloaded
        role.hasAccessFromIp(ipAddr);
        assertThat(originalReadsCount).isLessThan(getCidrGroupsReadCount());
    }
}

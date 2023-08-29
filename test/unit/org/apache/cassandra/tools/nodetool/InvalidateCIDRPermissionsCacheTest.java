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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IRoleManager;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.auth.AuthTestUtils.ROLE_A;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_B;
import static org.apache.cassandra.auth.AuthTestUtils.getCidrPermissionsReadCount;
import static org.assertj.core.api.Assertions.assertThat;

public class InvalidateCIDRPermissionsCacheTest extends CQLTester
{
    static InetSocketAddress ipAddr;

    @BeforeClass
    public static void setup() throws Exception
    {
        DatabaseDescriptor.setRolesValidity(Integer.MAX_VALUE-1);
        CQLTester.setUpClass();
        CQLTester.requireAuthentication();

        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_A, AuthTestUtils.getLoginRoleOptions());
        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_B, AuthTestUtils.getLoginRoleOptions());
        AuthCacheService.initializeAndRegisterCaches();

        startJMXServer();

        ipAddr = new InetSocketAddress("127.0.0.0", 0);
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "invalidatecidrpermissionscache");
        tool.assertOnCleanExit();

        String help = "NAME\n" +
                      "        nodetool invalidatecidrpermissionscache - Invalidate the cidr\n" +
                      "        permissions cache\n" +
                      "\n" +
                      "SYNOPSIS\n" +
                      "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                      "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                      "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                      "                [(-u <username> | --username <username>)] invalidatecidrpermissionscache\n" +
                      "                [--] [<role>...]\n" +
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
                      "        --\n" +
                      "            This option can be used to separate command-line options from the\n" +
                      "            list of argument, (useful when arguments might be mistaken for\n" +
                      "            command-line options\n" +
                      "\n" +
                      "        [<role>...]\n" +
                      "            List of roles to invalidate. By default, all roles\n" +
                      "\n" +
                      "\n";
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testInvalidateSingleCidrPermission()
    {
        AuthenticatedUser role = new AuthenticatedUser(ROLE_A.getRoleName());

        // cache cidr permission
        role.hasAccessFromIp(ipAddr);
        long originalReadsCount = getCidrPermissionsReadCount();

        // ensure cidr permission is cached
        role.hasAccessFromIp(ipAddr);
        assertThat(originalReadsCount).isEqualTo(getCidrPermissionsReadCount());

        // invalidate cidr permission
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatecidrpermissionscache", ROLE_A.getRoleName());
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Invalidated the role role_a from CIDR permissions cache");

        // ensure cidr permission is reloaded
        assertThat(role.hasAccessFromIp(new InetSocketAddress("127.0.0.0", 0))).isTrue();
    }

    @Test
    public void testInvalidateAllCidrPermissions()
    {
        AuthenticatedUser roleA = new AuthenticatedUser(ROLE_A.getRoleName());
        AuthenticatedUser roleB = new AuthenticatedUser(ROLE_B.getRoleName());

        // cache cidr permissions
        roleA.hasAccessFromIp(ipAddr);
        roleB.hasAccessFromIp(ipAddr);
        long originalReadsCount = getCidrPermissionsReadCount();

        // enure cidr permissions are cached
        assertThat(roleA.hasAccessFromIp(ipAddr)).isTrue();
        assertThat(roleB.hasAccessFromIp(ipAddr)).isTrue();
        assertThat(originalReadsCount).isEqualTo(getCidrPermissionsReadCount());

        // invalidate both cidr permissions
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatecidrpermissionscache");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Invalidated CIDR permissions cache");

        // ensure cidr permission for roleA is reloaded
        assertThat(roleA.hasAccessFromIp(ipAddr)).isTrue();
        long readsCountAfterFirstReLoad = getCidrPermissionsReadCount();
        assertThat(originalReadsCount).isLessThan(readsCountAfterFirstReLoad);

        // ensure cidr permission for roleB is reloaded
        assertThat(roleB.hasAccessFromIp(ipAddr)).isTrue();
        long readsCountAfterSecondReLoad = getCidrPermissionsReadCount();
        assertThat(readsCountAfterFirstReLoad).isLessThan(readsCountAfterSecondReLoad);
    }

    @Test
    public void testInvalidateNonExistingRole()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatecidrpermissionscache", "role1");
        assertThat(tool.getStdout()).contains("Not found role role1 in CIDR permissions cache, nothing to invalidate");
    }
}

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

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.auth.AuthTestUtils.ROLE_A;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_B;
import static org.apache.cassandra.auth.AuthTestUtils.getRolesReadCount;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class InvalidateRolesCacheTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.prepareServer();
        AuthTestUtils.LocalCassandraRoleManager roleManager = new AuthTestUtils.LocalCassandraRoleManager();
        SchemaLoader.setupAuth(roleManager,
                new AuthTestUtils.LocalPasswordAuthenticator(),
                new AuthTestUtils.LocalCassandraAuthorizer(),
                new AuthTestUtils.LocalCassandraNetworkAuthorizer());

        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_A, AuthTestUtils.getLoginRoleOprions());
        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_B, AuthTestUtils.getLoginRoleOprions());

        startJMXServer();
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "invalidaterolescache");
        tool.assertOnCleanExit();

        String help =   "NAME\n" +
                        "        nodetool invalidaterolescache - Invalidate the roles cache\n" +
                        "\n" +
                        "SYNOPSIS\n" +
                        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                        "                [(-u <username> | --username <username>)] invalidaterolescache [--]\n" +
                        "                [<role>...]\n" +
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
        assertThat(tool.getStdout(), equalTo(help));
    }

    @Test
    public void testInvalidateSingleRole()
    {
        AuthenticatedUser role = new AuthenticatedUser(ROLE_A.getRoleName());

        // cache role
        role.canLogin();
        long originalReadsCount = getRolesReadCount();

        // enure role is cached
        assertTrue(role.canLogin());
        assertThat(originalReadsCount, equalTo(getRolesReadCount()));

        // invalidate role
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidaterolescache", ROLE_A.getRoleName());
        tool.assertOnCleanExit();
        assertThat(tool.getStdout(), emptyString());

        // ensure role is reloaded
        assertTrue(role.canLogin());
        assertThat(originalReadsCount, lessThan(getRolesReadCount()));
    }

    @Test
    public void testInvalidateAllRoles()
    {
        AuthenticatedUser roleA = new AuthenticatedUser(ROLE_A.getRoleName());
        AuthenticatedUser roleB = new AuthenticatedUser(ROLE_B.getRoleName());

        // cache roles
        roleA.canLogin();
        roleB.canLogin();
        long originalReadsCount = getRolesReadCount();

        // enure roles are cached
        assertTrue(roleA.canLogin());
        assertTrue(roleB.canLogin());
        assertThat(originalReadsCount, equalTo(getRolesReadCount()));

        // invalidate both roles
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidaterolescache");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout(), emptyString());

        // ensure role for roleA is reloaded
        assertTrue(roleA.canLogin());
        long readsCountAfterFirstReLoad = getRolesReadCount();
        assertThat(originalReadsCount, lessThan(readsCountAfterFirstReLoad));

        // ensure role for roleB is reloaded
        assertTrue(roleB.canLogin());
        long readsCountAfterSecondReLoad = getRolesReadCount();
        assertThat(readsCountAfterFirstReLoad, lessThan(readsCountAfterSecondReLoad));
    }
}

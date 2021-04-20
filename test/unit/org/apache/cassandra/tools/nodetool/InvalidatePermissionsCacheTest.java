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

import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.auth.AuthTestUtils.ROLE_A;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_B;
import static org.apache.cassandra.auth.AuthTestUtils.getRolePermissionsReadCount;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

@RunWith(OrderedJUnit4ClassRunner.class)
public class InvalidatePermissionsCacheTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.prepareServer();
        AuthTestUtils.LocalCassandraRoleManager roleManager = new AuthTestUtils.LocalCassandraRoleManager();
        AuthTestUtils.LocalCassandraAuthorizer authorizer = new AuthTestUtils.LocalCassandraAuthorizer();
        SchemaLoader.setupAuth(roleManager,
                new AuthTestUtils.LocalPasswordAuthenticator(),
                authorizer,
                new AuthTestUtils.LocalCassandraNetworkAuthorizer());

        DataResource rootDataResource = DataResource.root();
        Set<Permission> dataPermissions = rootDataResource.applicablePermissions();

        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_A, AuthTestUtils.getLoginRoleOprions());
        authorizer.grant(AuthenticatedUser.SYSTEM_USER, dataPermissions, rootDataResource, ROLE_A);

        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_B, AuthTestUtils.getLoginRoleOprions());
        authorizer.grant(AuthenticatedUser.SYSTEM_USER, dataPermissions, rootDataResource, ROLE_B);

        startJMXServer();
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "invalidatepermissionscache");
        tool.assertOnCleanExit();

        String help =   "NAME\n" +
                        "        nodetool invalidatepermissionscache - Invalidate the permissions cache\n" +
                        "\n" +
                        "SYNOPSIS\n" +
                        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                        "                [(-u <username> | --username <username>)] invalidatepermissionscache\n" +
                        "                [--] [<user> <resource>...]\n" +
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
                        "        [<user> <resource>...]\n" +
                        "            The user followed by one or many resources. By default, all users\n" +
                        "\n" +
                        "\n";
        assertThat(tool.getStdout(), equalTo(help));
    }

    @Test
    public void testInvalidateSinglePermission()
    {
        DataResource rootDataResource = DataResource.root();
        Set<Permission> dataPermissions = rootDataResource.applicablePermissions();

        AuthenticatedUser role = new AuthenticatedUser(ROLE_A.getRoleName());

        // cache permission
        role.getPermissions(rootDataResource);
        long originalReadsCount = getRolePermissionsReadCount();

        // enure permission is cached
        assertThat(role.getPermissions(rootDataResource), equalTo(dataPermissions));
        assertThat(originalReadsCount, equalTo(getRolePermissionsReadCount()));

        // invalidate permission
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatepermissionscache",
                ROLE_A.getRoleName(), "data");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout(), emptyString());

        // ensure permission is reloaded
        assertThat(role.getPermissions(rootDataResource), equalTo(dataPermissions));
        assertThat(originalReadsCount, lessThan(getRolePermissionsReadCount()));
    }

    @Test
    public void testInvalidateAllPermissions()
    {
        DataResource rootDataResource = DataResource.root();
        Set<Permission> dataPermissions = rootDataResource.applicablePermissions();

        AuthenticatedUser roleA = new AuthenticatedUser(ROLE_A.getRoleName());
        AuthenticatedUser roleB = new AuthenticatedUser(ROLE_B.getRoleName());

        // cache permissions
        roleA.getPermissions(rootDataResource);
        roleB.getPermissions(rootDataResource);
        long originalReadsCount = getRolePermissionsReadCount();

        // enure permissions are cached
        assertThat(roleA.getPermissions(rootDataResource), equalTo(dataPermissions));
        assertThat(roleB.getPermissions(rootDataResource), equalTo(dataPermissions));
        assertThat(originalReadsCount, equalTo(getRolePermissionsReadCount()));

        // invalidate both permissions
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatepermissionscache");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout(), emptyString());

        // ensure permission for roleA is reloaded
        assertThat(roleA.getPermissions(rootDataResource), equalTo(dataPermissions));
        long readsCountAfterFirstReLoad = getRolePermissionsReadCount();
        assertThat(originalReadsCount, lessThan(readsCountAfterFirstReLoad));

        // ensure permission for roleB is reloaded
        assertThat(roleB.getPermissions(rootDataResource), equalTo(dataPermissions));
        long readsCountAfterSecondReLoad = getRolePermissionsReadCount();
        assertThat(readsCountAfterFirstReLoad, lessThan(readsCountAfterSecondReLoad));
    }
}

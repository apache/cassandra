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
import javax.security.auth.Subject;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraPrincipal;
import org.apache.cassandra.auth.JMXResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.jmx.AuthorizationProxy;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.auth.AuthTestUtils.ROLE_A;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_B;
import static org.apache.cassandra.auth.AuthTestUtils.getRolePermissionsReadCount;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class InvalidateJmxPermissionsCacheTest extends CQLTester
{
    private static final AuthorizationProxy authorizationProxy = new NoAuthSetupAuthorizationProxy();

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

        JMXResource rootJmxResource = JMXResource.root();
        Set<Permission> jmxPermissions = rootJmxResource.applicablePermissions();

        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_A, AuthTestUtils.getLoginRoleOprions());
        authorizer.grant(AuthenticatedUser.SYSTEM_USER, jmxPermissions, rootJmxResource, ROLE_A);

        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_B, AuthTestUtils.getLoginRoleOprions());
        authorizer.grant(AuthenticatedUser.SYSTEM_USER, jmxPermissions, rootJmxResource, ROLE_B);

        startJMXServer();
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "invalidatejmxpermissionscache");
        tool.assertOnCleanExit();

        String help =   "NAME\n" +
                        "        nodetool invalidatejmxpermissionscache - Invalidate the JMX permissions\n" +
                        "        cache\n" +
                        "\n" +
                        "SYNOPSIS\n" +
                        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                        "                [(-u <username> | --username <username>)] invalidatejmxpermissionscache\n" +
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
        assertThat(tool.getStdout(), equalTo(help));
    }

    @Test
    public void testInvalidateSingleJMXPermission()
    {
        Subject userSubject = subject(ROLE_A.getRoleName());

        // cache role permission
        authorizationProxy.authorize(userSubject, "queryNames", null);
        long originalReadsCount = getRolePermissionsReadCount();

        // enure role permission is cached
        assertTrue(authorizationProxy.authorize(userSubject, "queryNames", null));
        assertThat(originalReadsCount, equalTo(getRolePermissionsReadCount()));

        // invalidate role permission
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatejmxpermissionscache", ROLE_A.getRoleName());
        tool.assertOnCleanExit();
        assertThat(tool.getStdout(), emptyString());

        // ensure role permission is reloaded
        assertTrue(authorizationProxy.authorize(userSubject, "queryNames", null));
        assertThat(originalReadsCount, lessThan(getRolePermissionsReadCount()));
    }

    @Test
    public void testInvalidateAllJMXPermissions()
    {
        Subject roleASubject = subject(ROLE_A.getRoleName());
        Subject roleBSubject = subject(ROLE_B.getRoleName());

        // cache role permissions
        authorizationProxy.authorize(roleASubject, "queryNames", null);
        authorizationProxy.authorize(roleBSubject, "queryNames", null);
        long originalReadsCount = getRolePermissionsReadCount();

        // enure role permissions are cached
        assertTrue(authorizationProxy.authorize(roleASubject, "queryNames", null));
        assertTrue(authorizationProxy.authorize(roleBSubject, "queryNames", null));
        assertThat(originalReadsCount, equalTo(getRolePermissionsReadCount()));

        // invalidate both role permissions
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatejmxpermissionscache");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout(), emptyString());

        // ensure role permission for roleA is reloaded
        assertTrue(authorizationProxy.authorize(roleASubject, "queryNames", null));
        long readsCountAfterFirstReLoad = getRolePermissionsReadCount();
        assertThat(originalReadsCount, lessThan(readsCountAfterFirstReLoad));

        // ensure role permission for roleB is reloaded
        assertTrue(authorizationProxy.authorize(roleBSubject, "queryNames", null));
        long readsCountAfterSecondReLoad = getRolePermissionsReadCount();
        assertThat(readsCountAfterFirstReLoad, lessThan(readsCountAfterSecondReLoad));
    }

    private static Subject subject(String roleName)
    {
        Subject subject = new Subject();
        subject.getPrincipals().add(new CassandraPrincipal(roleName));
        return subject;
    }

    private static class NoAuthSetupAuthorizationProxy extends AuthorizationProxy
    {
        public NoAuthSetupAuthorizationProxy()
        {
            super();
            this.isAuthSetupComplete = () -> true;
        }
    }
}

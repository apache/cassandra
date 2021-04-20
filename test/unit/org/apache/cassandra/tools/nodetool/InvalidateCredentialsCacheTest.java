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

import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.PlainTextAuthProvider;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.auth.AuthTestUtils.ROLE_A;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_B;
import static org.apache.cassandra.auth.AuthTestUtils.getRolesReadCount;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

@RunWith(OrderedJUnit4ClassRunner.class)
public class InvalidateCredentialsCacheTest extends CQLTester
{
    private static IAuthenticator.SaslNegotiator roleANegotiator;
    private static IAuthenticator.SaslNegotiator roleBNegotiator;

    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.prepareServer();
        AuthTestUtils.LocalCassandraRoleManager roleManager = new AuthTestUtils.LocalCassandraRoleManager();
        PasswordAuthenticator authenticator = new AuthTestUtils.LocalPasswordAuthenticator();
        SchemaLoader.setupAuth(roleManager,
                authenticator,
                new AuthTestUtils.LocalCassandraAuthorizer(),
                new AuthTestUtils.LocalCassandraNetworkAuthorizer());

        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_A, AuthTestUtils.getLoginRoleOprions());
        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_B, AuthTestUtils.getLoginRoleOprions());

        roleANegotiator = authenticator.newSaslNegotiator(null);
        roleANegotiator.evaluateResponse(new PlainTextAuthProvider(ROLE_A.getRoleName(), "ignored")
                .newAuthenticator((EndPoint) null, null)
                .initialResponse());
        roleBNegotiator = authenticator.newSaslNegotiator(null);
        roleBNegotiator.evaluateResponse(new PlainTextAuthProvider(ROLE_B.getRoleName(), "ignored")
                .newAuthenticator((EndPoint) null, null)
                .initialResponse());

        startJMXServer();
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "invalidatecredentialscache");
        tool.assertOnCleanExit();

        String help =   "NAME\n" +
                        "        nodetool invalidatecredentialscache - Invalidate the credentials cache\n" +
                        "\n" +
                        "SYNOPSIS\n" +
                        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                        "                [(-u <username> | --username <username>)] invalidatecredentialscache\n" +
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
    public void testInvalidateSingleCredential()
    {
        // cache credential
        roleANegotiator.getAuthenticatedUser();
        long originalReadsCount = getRolesReadCount();

        // enure credential is cached
        assertThat(roleANegotiator.getAuthenticatedUser(), equalTo(new AuthenticatedUser(ROLE_A.getRoleName())));
        assertThat(originalReadsCount, equalTo(getRolesReadCount()));

        // invalidate credential
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatecredentialscache", ROLE_A.getRoleName());
        tool.assertOnCleanExit();
        assertThat(tool.getStdout(), emptyString());

        // ensure credential is reloaded
        assertThat(roleANegotiator.getAuthenticatedUser(), equalTo(new AuthenticatedUser(ROLE_A.getRoleName())));
        assertThat(originalReadsCount, lessThan(getRolesReadCount()));
    }

    @Test
    public void testInvalidateAllCredentials()
    {
        // cache credentials
        roleANegotiator.getAuthenticatedUser();
        roleBNegotiator.getAuthenticatedUser();
        long originalReadsCount = getRolesReadCount();

        // enure credentials are cached
        assertThat(roleANegotiator.getAuthenticatedUser(), equalTo(new AuthenticatedUser(ROLE_A.getRoleName())));
        assertThat(roleBNegotiator.getAuthenticatedUser(), equalTo(new AuthenticatedUser(ROLE_B.getRoleName())));
        assertThat(originalReadsCount, equalTo(getRolesReadCount()));

        // invalidate both credentials
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatecredentialscache");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout(), emptyString());

        // ensure credential for roleA is reloaded
        assertThat(roleANegotiator.getAuthenticatedUser(), equalTo(new AuthenticatedUser(ROLE_A.getRoleName())));
        long readsCountAfterFirstReLoad = getRolesReadCount();
        assertThat(originalReadsCount, lessThan(readsCountAfterFirstReLoad));

        // ensure credential for roleB is reloaded
        assertThat(roleBNegotiator.getAuthenticatedUser(), equalTo(new AuthenticatedUser(ROLE_B.getRoleName())));
        long readsCountAfterSecondReLoad = getRolesReadCount();
        assertThat(readsCountAfterFirstReLoad, lessThan(readsCountAfterSecondReLoad));
    }
}

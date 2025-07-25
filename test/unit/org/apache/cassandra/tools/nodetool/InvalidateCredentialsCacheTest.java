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

import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.PlainTextAuthProvider;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.auth.AuthTestUtils.ROLE_A;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_B;
import static org.apache.cassandra.auth.AuthTestUtils.getRolesReadCount;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @see InvalidateCredentialsCache
 */
public class InvalidateCredentialsCacheTest extends CQLTester
{
    private static IAuthenticator.SaslNegotiator roleANegotiator;
    private static IAuthenticator.SaslNegotiator roleBNegotiator;

    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.requireAuthentication();
        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_A, AuthTestUtils.getLoginRoleOptions());
        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_B, AuthTestUtils.getLoginRoleOptions());
        PasswordAuthenticator passwordAuthenticator = (PasswordAuthenticator) DatabaseDescriptor.getAuthenticator();
        roleANegotiator = passwordAuthenticator.newSaslNegotiator(null);
        roleANegotiator.evaluateResponse(new PlainTextAuthProvider(ROLE_A.getRoleName(), "ignored")
                .newAuthenticator((EndPoint) null, null)
                .initialResponse());
        roleBNegotiator = passwordAuthenticator.newSaslNegotiator(null);
        roleBNegotiator.evaluateResponse(new PlainTextAuthProvider(ROLE_B.getRoleName(), "ignored")
                .newAuthenticator((EndPoint) null, null)
                .initialResponse());
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testInvalidateSingleCredential()
    {
        // cache credential
        roleANegotiator.getAuthenticatedUser();
        long originalReadsCount = getRolesReadCount();

        // enure credential is cached
        assertThat(roleANegotiator.getAuthenticatedUser()).isEqualTo(new AuthenticatedUser(ROLE_A.getRoleName()));
        assertThat(originalReadsCount).isEqualTo(getRolesReadCount());

        // invalidate credential
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatecredentialscache", ROLE_A.getRoleName());
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        // ensure credential is reloaded
        assertThat(roleANegotiator.getAuthenticatedUser()).isEqualTo(new AuthenticatedUser(ROLE_A.getRoleName()));
        assertThat(originalReadsCount).isLessThan(getRolesReadCount());
    }

    @Test
    public void testInvalidateAllCredentials()
    {
        // cache credentials
        roleANegotiator.getAuthenticatedUser();
        roleBNegotiator.getAuthenticatedUser();
        long originalReadsCount = getRolesReadCount();

        // enure credentials are cached
        assertThat(roleANegotiator.getAuthenticatedUser()).isEqualTo(new AuthenticatedUser(ROLE_A.getRoleName()));
        assertThat(roleBNegotiator.getAuthenticatedUser()).isEqualTo(new AuthenticatedUser(ROLE_B.getRoleName()));
        assertThat(originalReadsCount).isEqualTo(getRolesReadCount());

        // invalidate both credentials
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatecredentialscache");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        // ensure credential for roleA is reloaded
        assertThat(roleANegotiator.getAuthenticatedUser()).isEqualTo(new AuthenticatedUser(ROLE_A.getRoleName()));
        long readsCountAfterFirstReLoad = getRolesReadCount();
        assertThat(originalReadsCount).isLessThan(readsCountAfterFirstReLoad);

        // ensure credential for roleB is reloaded
        assertThat(roleBNegotiator.getAuthenticatedUser()).isEqualTo(new AuthenticatedUser(ROLE_B.getRoleName()));
        long readsCountAfterSecondReLoad = getRolesReadCount();
        assertThat(readsCountAfterFirstReLoad).isLessThan(readsCountAfterSecondReLoad);
    }
}

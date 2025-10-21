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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraPrincipal;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.auth.JMXResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.jmx.AuthorizationProxy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.auth.AuthTestUtils.ROLE_A;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_B;
import static org.apache.cassandra.auth.AuthTestUtils.getRolePermissionsReadCount;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @see InvalidateJmxPermissionsCache
 */
public class InvalidateJmxPermissionsCacheTest extends CQLTester
{
    private static final AuthorizationProxy authorizationProxy = new AuthTestUtils.NoAuthSetupAuthorizationProxy();

    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.requireAuthentication();
        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_A, AuthTestUtils.getLoginRoleOptions());
        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_B, AuthTestUtils.getLoginRoleOptions());
        AuthCacheService.initializeAndRegisterCaches();
        requireNetwork();
        startJMXServer();
    }

    @Before
    public void grantInitialPermissions()
    {
        // Because we reset the CMS in CQLTester::afterTest, the per-test keyspaces created in CQLTester::beforeTest
        // get dropped and re-created for every individual test. This means we need to recreate the perms here (we
        // could markCMS() after granting the first time and add a flag to avoid re-granting but this is simpler).
        JMXResource rootJmxResource = JMXResource.root();
        Set<Permission> jmxPermissions = rootJmxResource.applicablePermissions();
        IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
        authorizer.grant(AuthenticatedUser.SYSTEM_USER, jmxPermissions, rootJmxResource, ROLE_A);
        authorizer.grant(AuthenticatedUser.SYSTEM_USER, jmxPermissions, rootJmxResource, ROLE_B);
    }

    @Test
    public void testInvalidateSingleJMXPermission()
    {
        Subject userSubject = subject(ROLE_A.getRoleName());

        // cache role permission
        authorizationProxy.authorize(userSubject, "queryNames", null);
        long originalReadsCount = getRolePermissionsReadCount();

        // enure role permission is cached
        assertThat(authorizationProxy.authorize(userSubject, "queryNames", null)).isTrue();
        assertThat(originalReadsCount).isEqualTo(getRolePermissionsReadCount());

        // invalidate role permission
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatejmxpermissionscache", ROLE_A.getRoleName());
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        // ensure role permission is reloaded
        assertThat(authorizationProxy.authorize(userSubject, "queryNames", null)).isTrue();
        assertThat(originalReadsCount).isLessThan(getRolePermissionsReadCount());
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
        assertThat(authorizationProxy.authorize(roleASubject, "queryNames", null)).isTrue();
        assertThat(authorizationProxy.authorize(roleBSubject, "queryNames", null)).isTrue();
        assertThat(originalReadsCount).isEqualTo(getRolePermissionsReadCount());

        // invalidate both role permissions
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatejmxpermissionscache");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        // ensure role permission for roleA is reloaded
        assertThat(authorizationProxy.authorize(roleASubject, "queryNames", null)).isTrue();
        long readsCountAfterFirstReLoad = getRolePermissionsReadCount();
        assertThat(originalReadsCount).isLessThan(readsCountAfterFirstReLoad);

        // ensure role permission for roleB is reloaded
        assertThat(authorizationProxy.authorize(roleBSubject, "queryNames", null)).isTrue();
        long readsCountAfterSecondReLoad = getRolePermissionsReadCount();
        assertThat(readsCountAfterFirstReLoad).isLessThan(readsCountAfterSecondReLoad);
    }

    private static Subject subject(String roleName)
    {
        Subject subject = new Subject();
        subject.getPrincipals().add(new CassandraPrincipal(roleName));
        return subject;
    }

}

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

import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.auth.AuthTestUtils.ROLE_A;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_B;
import static org.apache.cassandra.auth.AuthTestUtils.getNetworkPermissionsReadCount;
import static org.assertj.core.api.Assertions.assertThat;

public class InvalidateNetworkPermissionsCacheTest extends CQLTester
{
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

    @Test
    public void testInvalidateSingleNetworkPermission()
    {
        AuthenticatedUser role = new AuthenticatedUser(ROLE_A.getRoleName());

        // cache network permission
        role.hasLocalAccess();
        long originalReadsCount = getNetworkPermissionsReadCount();

        // enure network permission is cached
        assertThat(role.hasLocalAccess()).isTrue();
        assertThat(originalReadsCount).isEqualTo(getNetworkPermissionsReadCount());

        // invalidate network permission
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatenetworkpermissionscache", ROLE_A.getRoleName());
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        // ensure network permission is reloaded
        assertThat(role.hasLocalAccess()).isTrue();
        assertThat(originalReadsCount).isLessThan(getNetworkPermissionsReadCount());
    }

    @Test
    public void testInvalidateAllNetworkPermissions()
    {
        AuthenticatedUser roleA = new AuthenticatedUser(ROLE_A.getRoleName());
        AuthenticatedUser roleB = new AuthenticatedUser(ROLE_B.getRoleName());

        // cache network permissions
        roleA.hasLocalAccess();
        roleB.hasLocalAccess();
        long originalReadsCount = getNetworkPermissionsReadCount();

        // enure network permissions are cached
        assertThat(roleA.hasLocalAccess()).isTrue();
        assertThat(roleB.hasLocalAccess()).isTrue();
        assertThat(originalReadsCount).isEqualTo(getNetworkPermissionsReadCount());

        // invalidate both network permissions
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatenetworkpermissionscache");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        // ensure network permission for roleA is reloaded
        assertThat(roleA.hasLocalAccess()).isTrue();
        long readsCountAfterFirstReLoad = getNetworkPermissionsReadCount();
        assertThat(originalReadsCount).isLessThan(readsCountAfterFirstReLoad);

        // ensure network permission for roleB is reloaded
        assertThat(roleB.hasLocalAccess()).isTrue();
        long readsCountAfterSecondReLoad = getNetworkPermissionsReadCount();
        assertThat(readsCountAfterFirstReLoad).isLessThan(readsCountAfterSecondReLoad);
    }
}

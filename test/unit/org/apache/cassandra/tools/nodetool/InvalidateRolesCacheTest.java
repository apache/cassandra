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
import static org.apache.cassandra.auth.AuthTestUtils.getRolesReadCount;
import static org.assertj.core.api.Assertions.assertThat;

public class InvalidateRolesCacheTest extends CQLTester
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
    public void testInvalidateSingleRole()
    {
        AuthenticatedUser role = new AuthenticatedUser(ROLE_A.getRoleName());

        // cache role
        role.canLogin();
        long originalReadsCount = getRolesReadCount();

        // enure role is cached
        assertThat(role.canLogin()).isTrue();
        assertThat(originalReadsCount).isEqualTo(getRolesReadCount());

        // invalidate role
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidaterolescache", ROLE_A.getRoleName());
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        // ensure role is reloaded
        assertThat(role.canLogin()).isTrue();
        assertThat(originalReadsCount).isLessThan(getRolesReadCount());
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
        assertThat(roleA.canLogin()).isTrue();
        assertThat(roleB.canLogin()).isTrue();
        assertThat(originalReadsCount).isEqualTo(getRolesReadCount());

        // invalidate both roles
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidaterolescache");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        // ensure role for roleA is reloaded
        assertThat(roleA.canLogin()).isTrue();
        long readsCountAfterFirstReLoad = getRolesReadCount();
        assertThat(originalReadsCount).isLessThan(readsCountAfterFirstReLoad);

        // ensure role for roleB is reloaded
        assertThat(roleB.canLogin()).isTrue();
        long readsCountAfterSecondReLoad = getRolesReadCount();
        assertThat(readsCountAfterFirstReLoad).isLessThan(readsCountAfterSecondReLoad);
    }
}

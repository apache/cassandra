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

package org.apache.cassandra.auth;

import java.util.Set;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.auth.RoleTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CassandraRoleManagerTest
{

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        // create the system_auth keyspace so the IRoleManager can function as normal
        SchemaLoader.createKeyspace(SchemaConstants.AUTH_KEYSPACE_NAME,
                                    KeyspaceParams.simple(1),
                                    Iterables.toArray(AuthKeyspace.metadata().tables, TableMetadata.class));
    }

    @Test
    public void getGrantedRolesImplMinimizesReads()
    {
        // IRoleManager::getRoleDetails was not in the initial API, so a default impl
        // was added which uses the existing methods on IRoleManager as primitives to
        // construct the Role objects. While this will work for any IRoleManager impl
        // it is inefficient, so CassandraRoleManager has its own implementation which
        // collects all of the necessary info with a single query for each granted role.
        // This just tests that that is the case, i.e. we perform 1 read per role in the
        // transitive set of granted roles
        IRoleManager roleManager = new LocalCassandraRoleManager();
        roleManager.setup();
        for (RoleResource r : ALL_ROLES)
            roleManager.createRole(AuthenticatedUser.ANONYMOUS_USER, r, new RoleOptions());

        // simple role with no grants
        fetchRolesAndCheckReadCount(roleManager, ROLE_A);
        // single level of grants
        grantRolesTo(roleManager, ROLE_A, ROLE_B, ROLE_C);
        fetchRolesAndCheckReadCount(roleManager, ROLE_A);

        // multi level role hierarchy
        grantRolesTo(roleManager, ROLE_B, ROLE_B_1, ROLE_B_2, ROLE_B_3);
        grantRolesTo(roleManager, ROLE_C, ROLE_C_1, ROLE_C_2, ROLE_C_3);
        fetchRolesAndCheckReadCount(roleManager, ROLE_A);

        // Check that when granted roles appear multiple times in parallel levels of the hierarchy, we don't
        // do redundant reads. E.g. here role_b_1, role_b_2 and role_b_3 are granted to both role_b and role_c
        // but we only want to actually read them once
        grantRolesTo(roleManager, ROLE_C, ROLE_B_1, ROLE_B_2, ROLE_B_3);
        fetchRolesAndCheckReadCount(roleManager, ROLE_A);
    }

    private void fetchRolesAndCheckReadCount(IRoleManager roleManager, RoleResource primaryRole)
    {
        long before = getReadCount();
        Set<Role> granted = roleManager.getRoleDetails(primaryRole);
        long after = getReadCount();
        assertEquals(granted.size(), after - before);
    }

    @Test
    public void testPasswordUpdateRateLimiting() throws Exception
    {
        try
        {
            CassandraRoleManager.updatePasswordUpdateMinInterval(100);

            IRoleManager roleManager = new LocalCassandraRoleManager();
            roleManager.setup();

            RoleResource testRole = RoleResource.role("test_password_role");
            RoleOptions options = getLoginRoleOptions("initial_password");
            roleManager.createRole(AuthenticatedUser.ANONYMOUS_USER, testRole, options);

            // Wait for the rate limit interval to pass
            Thread.sleep(150);

            // First password change should succeed
            RoleOptions newOptions1 = getLoginRoleOptions("new_password_1");
            roleManager.alterRole(AuthenticatedUser.ANONYMOUS_USER, testRole, newOptions1);

            // Immediate second password change should fail with OverloadedException
            try
            {
                RoleOptions newOptions2 = getLoginRoleOptions("new_password_2");
                roleManager.alterRole(AuthenticatedUser.ANONYMOUS_USER, testRole, newOptions2);
                fail("Expected OverloadedException due to password update rate limiting");
            }
            catch (OverloadedException e)
            {
                assertEquals("Password for role test_password_role can only be changed every 100ms.", e.getMessage());
            }

            // Wait for the rate limit interval to pass
            Thread.sleep(150);

            // After waiting, password change should succeed again
            RoleOptions newOptions3 = getLoginRoleOptions("new_password_3");
            roleManager.alterRole(AuthenticatedUser.ANONYMOUS_USER, testRole, newOptions3);

            roleManager.dropRole(AuthenticatedUser.ANONYMOUS_USER, testRole);
        }
        finally
        {
            CassandraRoleManager.updatePasswordUpdateMinInterval(CassandraRelevantProperties.ROLE_PASSWORD_UPDATE_MIN_INTERVAL_MS.getInt());
        }
    }

    @Test
    public void testPasswordUpdateRateLimitingDisabled() throws Exception
    {
        try
        {
            CassandraRoleManager.updatePasswordUpdateMinInterval(0);

            IRoleManager roleManager = new LocalCassandraRoleManager();
            roleManager.setup();

            RoleResource testRole = RoleResource.role("test_no_limit_role");
            RoleOptions options = getLoginRoleOptions("initial_password");
            roleManager.createRole(AuthenticatedUser.ANONYMOUS_USER, testRole, options);

            // Multiple rapid password changes should all succeed when rate limiting is disabled
            for (int i = 0; i < 5; i++)
                roleManager.alterRole(AuthenticatedUser.ANONYMOUS_USER, testRole, getLoginRoleOptions("password_" + i));

            roleManager.dropRole(AuthenticatedUser.ANONYMOUS_USER, testRole);
        }
        finally
        {
            CassandraRoleManager.updatePasswordUpdateMinInterval(CassandraRelevantProperties.ROLE_PASSWORD_UPDATE_MIN_INTERVAL_MS.getInt());
        }
    }

    @Test
    public void testPasswordUpdateRateLimitingPerRole() throws Exception
    {
        try
        {
            CassandraRoleManager.updatePasswordUpdateMinInterval(100);

            IRoleManager roleManager = new LocalCassandraRoleManager();
            roleManager.setup();

            RoleResource role1 = RoleResource.role("test_role_1");
            RoleResource role2 = RoleResource.role("test_role_2");

            RoleOptions options1 = getLoginRoleOptions("password1");
            roleManager.createRole(AuthenticatedUser.ANONYMOUS_USER, role1, options1);

            RoleOptions options2 = getLoginRoleOptions("password2");
            roleManager.createRole(AuthenticatedUser.ANONYMOUS_USER, role2, options2);

            // Wait for the rate limit interval to pass since creation
            Thread.sleep(150);

            RoleOptions newOptions1 = getLoginRoleOptions("new_password1");
            roleManager.alterRole(AuthenticatedUser.ANONYMOUS_USER, role1, newOptions1);

            try
            {
                RoleOptions newOptions1Again = getLoginRoleOptions("another_password1");
                roleManager.alterRole(AuthenticatedUser.ANONYMOUS_USER, role1, newOptions1Again);
                fail("Expected OverloadedException for test_role_1");
            }
            catch (OverloadedException e)
            {
                assertEquals("Password for role test_role_1 can only be changed every 100ms.", e.getMessage());
            }

            RoleOptions newOptions2 = getLoginRoleOptions("new_password2");
            roleManager.alterRole(AuthenticatedUser.ANONYMOUS_USER, role2, newOptions2);

            try
            {
                RoleOptions newOptions2Again = getLoginRoleOptions("another_password2");
                roleManager.alterRole(AuthenticatedUser.ANONYMOUS_USER, role2, newOptions2Again);
                fail("Expected OverloadedException for test_role_2");
            }
            catch (OverloadedException e)
            {
                assertEquals("Password for role test_role_2 can only be changed every 100ms.", e.getMessage());
            }

            roleManager.dropRole(AuthenticatedUser.ANONYMOUS_USER, role1);
            roleManager.dropRole(AuthenticatedUser.ANONYMOUS_USER, role2);
        }
        finally
        {
            CassandraRoleManager.updatePasswordUpdateMinInterval(CassandraRelevantProperties.ROLE_PASSWORD_UPDATE_MIN_INTERVAL_MS.getInt());
        }
    }

    public static RoleOptions getLoginRoleOptions(String password)
    {
        RoleOptions roleOptions = new RoleOptions();
        roleOptions.setOption(IRoleManager.Option.SUPERUSER, false);
        roleOptions.setOption(IRoleManager.Option.LOGIN, true);
        roleOptions.setOption(IRoleManager.Option.PASSWORD, password);
        return roleOptions;
    }
}

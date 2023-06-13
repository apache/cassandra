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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;

import static org.apache.cassandra.auth.AuthTestUtils.ALL_ROLES;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_A;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_B;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_C;
import static org.apache.cassandra.auth.AuthTestUtils.getRolesReadCount;
import static org.apache.cassandra.auth.AuthTestUtils.grantRolesTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RolesTest
{

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        IRoleManager roleManager = new AuthTestUtils.LocalCassandraRoleManager();
        SchemaLoader.setupAuth(roleManager,
                               new AuthTestUtils.LocalPasswordAuthenticator(),
                               new AuthTestUtils.LocalCassandraAuthorizer(),
                               new AuthTestUtils.LocalCassandraNetworkAuthorizer(),
                               new AuthTestUtils.LocalCassandraCIDRAuthorizer());

        for (RoleResource role : ALL_ROLES)
            roleManager.createRole(AuthenticatedUser.ANONYMOUS_USER, role, new RoleOptions());
        grantRolesTo(roleManager, ROLE_A, ROLE_B, ROLE_C);

        roleManager.setup();
        AuthCacheService.initializeAndRegisterCaches();
    }

    @Test
    public void superuserStatusIsCached()
    {
        boolean hasSuper = Roles.hasSuperuserStatus(ROLE_A);
        long count = getRolesReadCount();

        assertEquals(hasSuper, Roles.hasSuperuserStatus(ROLE_A));
        assertEquals(count, getRolesReadCount());
    }

    @Test
    public void loginPrivilegeIsCached()
    {
        boolean canLogin = Roles.canLogin(ROLE_A);
        long count = getRolesReadCount();

        assertEquals(canLogin, Roles.canLogin(ROLE_A));
        assertEquals(count, getRolesReadCount());
    }

    @Test
    public void grantedRoleDetailsAreCached()
    {
        Iterable<Role> granted = Roles.getRoleDetails(ROLE_A);
        long count = getRolesReadCount();

        assertTrue(Iterables.elementsEqual(granted, Roles.getRoleDetails(ROLE_A)));
        assertEquals(count, getRolesReadCount());
    }

    @Test
    public void grantedRoleResourcesAreCached()
    {
        Set<RoleResource> granted = Roles.getRoles(ROLE_A);
        long count = getRolesReadCount();

        assertEquals(granted, Roles.getRoles(ROLE_A));
        assertEquals(count, getRolesReadCount());
    }

    @Test
    public void confirmSuperUserConsistency()
    {
        // Confirm special treatment of superuser
        ConsistencyLevel readLevel = CassandraRoleManager.consistencyForRoleRead(CassandraRoleManager.DEFAULT_SUPERUSER_NAME);
        Assert.assertEquals(CassandraRoleManager.DEFAULT_SUPERUSER_CONSISTENCY_LEVEL, readLevel);

        ConsistencyLevel writeLevel = CassandraRoleManager.consistencyForRoleWrite(CassandraRoleManager.DEFAULT_SUPERUSER_NAME);
        Assert.assertEquals(CassandraRoleManager.DEFAULT_SUPERUSER_CONSISTENCY_LEVEL, writeLevel);

        // Confirm standard config-based treatment of non
        ConsistencyLevel nonPrivReadLevel = CassandraRoleManager.consistencyForRoleRead("non-privilaged");
        Assert.assertEquals(nonPrivReadLevel, DatabaseDescriptor.getAuthReadConsistencyLevel());

        ConsistencyLevel nonPrivWriteLevel = CassandraRoleManager.consistencyForRoleWrite("non-privilaged");
        Assert.assertEquals(nonPrivWriteLevel, DatabaseDescriptor.getAuthWriteConsistencyLevel());
    }
}

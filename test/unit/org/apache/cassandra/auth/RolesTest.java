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
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.auth.RoleTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RolesTest
{

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        // create the system_auth keyspace so the IRoleManager can function as normal
        SchemaLoader.createKeyspace(SchemaConstants.AUTH_KEYSPACE_NAME,
                                    KeyspaceParams.simple(1),
                                    Iterables.toArray(AuthKeyspace.metadata().tables, TableMetadata.class));

        IRoleManager roleManager = new LocalCassandraRoleManager();
        roleManager.setup();
        Roles.initRolesCache(roleManager, () -> true);
        for (RoleResource role : ALL_ROLES)
            roleManager.createRole(AuthenticatedUser.ANONYMOUS_USER, role, new RoleOptions());
        grantRolesTo(roleManager, ROLE_A, ROLE_B, ROLE_C);
    }

    @Test
    public void superuserStatusIsCached()
    {
        boolean hasSuper = Roles.hasSuperuserStatus(ROLE_A);
        long count = getReadCount();

        assertEquals(hasSuper, Roles.hasSuperuserStatus(ROLE_A));
        assertEquals(count, getReadCount());
    }

    @Test
    public void loginPrivilegeIsCached()
    {
        boolean canLogin = Roles.canLogin(ROLE_A);
        long count = getReadCount();

        assertEquals(canLogin, Roles.canLogin(ROLE_A));
        assertEquals(count, getReadCount());
    }

    @Test
    public void grantedRoleDetailsAreCached()
    {
        Iterable<Role> granted = Roles.getRoleDetails(ROLE_A);
        long count = getReadCount();

        assertTrue(Iterables.elementsEqual(granted, Roles.getRoleDetails(ROLE_A)));
        assertEquals(count, getReadCount());
    }

    @Test
    public void grantedRoleResourcesAreCached()
    {
        Set<RoleResource> granted = Roles.getRoles(ROLE_A);
        long count = getReadCount();

        assertEquals(granted, Roles.getRoles(ROLE_A));
        assertEquals(count, getReadCount());
    }
}

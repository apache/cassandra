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
        // was added which uses the existing methods on IRoleManager as primitive to
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
        // do redundant reads. E.g. here role_b_1, role_b_2 and role_b3 are granted to both role_b and role_c
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
}

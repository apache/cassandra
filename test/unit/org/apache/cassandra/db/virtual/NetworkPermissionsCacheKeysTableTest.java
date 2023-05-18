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

package org.apache.cassandra.db.virtual;

import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.DCPermissions;
import org.apache.cassandra.auth.INetworkAuthorizer;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.apache.cassandra.auth.AuthTestUtils.ROLE_A;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_B;

public class NetworkPermissionsCacheKeysTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    @SuppressWarnings("FieldCanBeLocal")
    private NetworkPermissionsCacheKeysTable table;

    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();

        // high value is used for convenient debugging
        DatabaseDescriptor.setPermissionsValidity(20_000);

        CQLTester.setUpClass();
        CQLTester.requireAuthentication();

        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_A, AuthTestUtils.getLoginRoleOptions());
        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_B, AuthTestUtils.getLoginRoleOptions());

        INetworkAuthorizer networkAuthorizer = DatabaseDescriptor.getNetworkAuthorizer();
        networkAuthorizer.setRoleDatacenters(ROLE_A, DCPermissions.all());
        networkAuthorizer.setRoleDatacenters(ROLE_B, DCPermissions.subset(DATA_CENTER, DATA_CENTER_REMOTE));
    }

    @Before
    public void config()
    {
        table = new NetworkPermissionsCacheKeysTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));

        // ensure nothing keeps cached between tests
        AuthenticatedUser.networkPermissionsCache.invalidate();
        disablePreparedReuseForTest();
    }

    @AfterClass
    public static void tearDownClass()
    {
        DatabaseDescriptor.setPermissionsValidity(DatabaseDescriptor.getRawConfig().permissions_validity.toMilliseconds());
    }

    @Test
    public void testSelectAllWhenPermissionsAreNotCached() throws Throwable
    {
        assertEmpty(execute("SELECT * FROM vts.network_permissions_cache_keys"));
    }

    @Test
    public void testSelectAllWhenPermissionsAreCached() throws Throwable
    {
        cachePermissions(ROLE_A);
        cachePermissions(ROLE_B);

        assertRows(execute("SELECT * FROM vts.network_permissions_cache_keys"),
                row("role_a"),
                row("role_b"));
    }

    @Test
    public void testSelectPartitionWhenPermissionsAreNotCached() throws Throwable
    {
        assertEmpty(execute("SELECT * FROM vts.network_permissions_cache_keys WHERE role='role_a'"));
    }

    @Test
    public void testSelectPartitionWhenPermissionsAreCached() throws Throwable
    {
        cachePermissions(ROLE_A);
        cachePermissions(ROLE_B);

        assertRows(execute("SELECT * FROM vts.network_permissions_cache_keys WHERE role='role_a'"),
                row("role_a"));
    }

    @Test
    public void testDeletePartition() throws Throwable
    {
        cachePermissions(ROLE_A);
        cachePermissions(ROLE_B);

        execute("DELETE FROM vts.network_permissions_cache_keys WHERE role='role_a'");

        assertRows(execute("SELECT * FROM vts.network_permissions_cache_keys"),
                row("role_b"));
    }

    @Test
    public void testDeletePartitionWithInvalidValues() throws Throwable
    {
        cachePermissions(ROLE_A);

        execute("DELETE FROM vts.network_permissions_cache_keys WHERE role='invalid_role'");

        assertRows(execute("SELECT * FROM vts.network_permissions_cache_keys WHERE role='role_a'"),
                row("role_a"));
    }

    @Test
    public void testTruncateTable() throws Throwable
    {
        cachePermissions(ROLE_A);
        cachePermissions(ROLE_B);

        execute("TRUNCATE vts.network_permissions_cache_keys");

        assertEmpty(execute("SELECT * FROM vts.network_permissions_cache_keys"));
    }

    @Test
    public void testUnsupportedOperations() throws Throwable
    {
        // range tombstone is not supported, however, this table has no clustering columns, so it is not covered by tests

        // column deletion is not supported, however, this table has no regular columns, so it is not covered by tests

        // insert is not supported
        assertInvalidMessage("Column modification is not supported by table vts.network_permissions_cache_keys",
                "INSERT INTO vts.network_permissions_cache_keys (role) VALUES ('role_e')");

        // update is not supported, however, this table has no regular columns, so it is not covered by tests
    }

    private void cachePermissions(RoleResource roleResource)
    {
        AuthenticatedUser.networkPermissionsCache.get(roleResource);
    }
}

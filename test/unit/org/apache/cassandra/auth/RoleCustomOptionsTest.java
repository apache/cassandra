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

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.apache.cassandra.auth.AuthKeyspace.ROLES;
import static org.apache.cassandra.auth.AuthKeyspace.ROLE_OPTIONS;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public class RoleCustomOptionsTest extends CQLTester
{
    private static final String TEST_ROLE = "testrole";
    private static IRoleManager roleManager;

    @BeforeClass
    public static void setupClass()
    {
        ServerTestUtils.daemonInitialization();

        DatabaseDescriptor.setPermissionsValidity(0);
        DatabaseDescriptor.setRolesValidity(0);
        DatabaseDescriptor.setCredentialsValidity(0);

        CQLTester.setUpClass();
        requireAuthentication();
        requireNetwork();

        roleManager = new AuthTestUtils.LocalCassandraRoleManager();
        roleManager.setup();
    }

    @Before
    @After
    public void cleanupRoles()
    {
        useSuperUser();
        executeNet(String.format("DELETE FROM %s.%s WHERE role = '%s'", AUTH_KEYSPACE_NAME, ROLES, TEST_ROLE));
        executeNet(String.format("DELETE FROM %s.%s WHERE role = '%s'", AUTH_KEYSPACE_NAME, ROLE_OPTIONS, TEST_ROLE));
    }

    @Test
    public void createRoleWithCustomOptions()
    {
        executeNet(String.format("CREATE ROLE %s WITH OPTIONS = { 'option1': 'value1', 'option2': 0 }", TEST_ROLE));

        RoleResource testRole = RoleResource.role(TEST_ROLE);
        assertThat(roleManager.isExistingRole(testRole)).isTrue();

        Map<String, String> customOptions = roleManager.getCustomOptions(testRole);

        Map<String, String> expectedOptions = Map.of("option1", "value1", "option2", "0");
        assertThat(customOptions).containsExactlyInAnyOrderEntriesOf(expectedOptions);
    }

    @Test
    public void dropRoleWithCustomOptions()
    {
        executeNet(String.format("CREATE ROLE %s WITH OPTIONS = { 'option1': 'value1', 'option2': 0 }", TEST_ROLE));
        executeNet("DROP ROLE testrole");

        assertThat(roleManager.isExistingRole(RoleResource.role(TEST_ROLE))).isFalse();

        assertNoRoleOptionsEntryFor(TEST_ROLE);
    }

    @Test
    public void alterRoleReplacesCustomOptions()
    {
        executeNet(String.format("CREATE ROLE %s WITH OPTIONS = { 'option1': 'value1' }", TEST_ROLE));
        executeNet(String.format("ALTER ROLE %s WITH OPTIONS = { 'option2': 'value2' }", TEST_ROLE));

        Map<String, String> customOptions = roleManager.getCustomOptions(RoleResource.role(TEST_ROLE));

        assertThat(customOptions).containsExactlyEntriesOf(Map.of("option2", "value2"));
    }

    @Test
    public void alterRoleDeletesCustomOptions()
    {
        executeNet(String.format("CREATE ROLE %s WITH OPTIONS = { 'option1': 'value1' }", TEST_ROLE));
        executeNet(String.format("ALTER ROLE %s WITH OPTIONS = {}", TEST_ROLE));

        Map<String, String> customOptions = roleManager.getCustomOptions(RoleResource.role(TEST_ROLE));

        assertThat(customOptions).isEmpty();
        assertNoRoleOptionsEntryFor(TEST_ROLE);
    }

    @Test
    public void alterRoleInsertsCustomOptions()
    {
        executeNet(String.format("CREATE ROLE %s WITH OPTIONS = {}", TEST_ROLE));
        assertNoRoleOptionsEntryFor(TEST_ROLE);

        executeNet(String.format("ALTER ROLE %s WITH OPTIONS = { 'option1': 'value1' }", TEST_ROLE));

        Map<String, String> customOptions = roleManager.getCustomOptions(RoleResource.role(TEST_ROLE));

        assertThat(customOptions).containsExactlyEntriesOf(Map.of("option1", "value1"));
    }

    @Test
    public void alterRoleWithoutModifyingCustomOptions()
    {
        executeNet(String.format("CREATE ROLE %s WITH OPTIONS = { 'option1': 'value1' }", TEST_ROLE));
        executeNet(String.format("ALTER ROLE %s WITH LOGIN = true", TEST_ROLE));

        Map<String, String> customOptions = roleManager.getCustomOptions(RoleResource.role(TEST_ROLE));

        assertThat(customOptions).containsExactlyEntriesOf(Map.of("option1", "value1"));
    }

    private void assertNoRoleOptionsEntryFor(String roleName)
    {
        String query = String.format("SELECT * FROM %s.%s WHERE role = '%s'", AUTH_KEYSPACE_NAME, ROLE_OPTIONS, roleName);
        ResultSet rows = executeNet(query);
        assertThat(rows).isEmpty();
    }
}

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

package org.apache.cassandra.cql3.statements;

import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import org.apache.cassandra.auth.Roles;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.mockito.MockedStatic;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mockStatic;

public class ListSuperUsersStatementTest extends CQLTester
{
    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.setPermissionsValidity(0);
        DatabaseDescriptor.setRolesValidity(0);
        DatabaseDescriptor.setCredentialsValidity(0);

        requireAuthentication();
        requireNetwork();
    }

    @Test
    public void testAcquiredSuperUsers() throws InterruptedException
    {
        useSuperUser();
        assertRowsNet(executeNet("list superusers"), row("cassandra"));

        executeNet("create role role1 with login=true and password='role1'");
        executeNet("create role role11 with login=true and password='role11'");
        executeNet("create role role2 with login=true and password='role2'");
        assertRowsNet(executeNet("list superusers"), row("cassandra"));

        executeNet("grant cassandra to role1");
        executeNet("grant role1 to role11");
        Roles.cache.invalidate();
        assertRowsNet(executeNet("list superusers"), row("cassandra"), row("role1"), row("role11"));

        useUser("role1", "role1");
        assertRowsNet(executeNet("list superusers"), row("cassandra"), row("role1"), row("role11"));

        useUser("role11", "role11");
        assertRowsNet(executeNet("list superusers"), row("cassandra"), row("role1"), row("role11"));
    }

    @Test
    public void testNoRoles()
    {
        try (MockedStatic<Roles> roles = mockStatic(Roles.class))
        {
            roles.when(Roles::getAllRoles).thenReturn(Collections.emptySet());
            ClientState state = ClientState.forInternalCalls("system_auth");
            ListSuperUsersStatement listSuperUsersStatement = new ListSuperUsersStatement();
            ResultMessage result = listSuperUsersStatement.execute(state);
            assertEquals("EMPTY RESULT", result.toString());
        }
    }

    @Test
    public void testGetAllRolesReturnsNull()
    {
        try (MockedStatic<Roles> roles = mockStatic(Roles.class))
        {
            roles.when(Roles::getAllRoles).thenReturn(null);
            ClientState state = ClientState.forInternalCalls("system_auth");
            ListSuperUsersStatement listSuperUsersStatement = new ListSuperUsersStatement();
            ResultMessage result = listSuperUsersStatement.execute(state);
            assertEquals("EMPTY RESULT", result.toString());
        }
    }

    @Test
    public void testNonSuperUserDescribePermission()
    {
        useSuperUser();
        executeNet("create role nonsuper with login=true and password='nonsuper'");
        Roles.cache.invalidate();

        useUser("nonsuper", "nonsuper");
        assertThatThrownBy(() -> executeNet("list superusers"))
        .isInstanceOf(UnauthorizedException.class)
        .hasMessage("You are not authorized to view superuser details");

        useSuperUser();
        executeNet("GRANT DESCRIBE ON ALL ROLES to nonsuper");
        Roles.cache.invalidate();

        useUser("nonsuper", "nonsuper");
        ResultSet result = executeNet("list superusers");
        // verify list command returned non-empty results
        assertTrue(result.iterator().hasNext());
    }

    @Test
    public void testListSuperUserStatementToString()
    {
        ListSuperUsersStatement listSuperUsersStatement = new ListSuperUsersStatement();
        assertEquals("ListSuperUsersStatement[bindVariables=<null>]", listSuperUsersStatement.toString());
    }
}

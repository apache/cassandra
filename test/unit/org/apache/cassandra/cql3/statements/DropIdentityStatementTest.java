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

import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;

import static org.apache.cassandra.auth.AuthKeyspace.IDENTITY_TO_ROLES;
import static org.apache.cassandra.cql3.statements.AddIdentityStatementTest.defineSchema;
import static org.apache.cassandra.cql3.statements.AddIdentityStatementTest.getClientState;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DropIdentityStatementTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private static final String  IDENTITY = "spiffe://testdomain.com/testIdentifier/testValue";
    private static final String DROP_QUERY = String.format("DROP IDENTITY '%s'", IDENTITY);

    @BeforeClass
    public static void beforeClasss() throws ConfigurationException
    {
        defineSchema();
    }

    @Before
    public void clear()
    {
        Keyspace.open(AUTH_KEYSPACE_NAME).getColumnFamilyStore(IDENTITY_TO_ROLES).truncateBlocking();
    }


    @Test
    public void testDropIdentityStatementParsing()
    {
        CQLStatement.Raw statement = QueryProcessor.parseStatement(DROP_QUERY);
        assertTrue(statement instanceof DropIdentityStatement);
        DropIdentityStatement dropIdentityStatement = (DropIdentityStatement) statement;
        assertEquals(IDENTITY, dropIdentityStatement.identity);
        assertFalse(dropIdentityStatement.ifExists);
    }

    @Test
    public void testDroppingValidIdentity()
    {
        DatabaseDescriptor.getRoleManager().addIdentity(IDENTITY, "cassandra");
        QueryProcessor.process(DROP_QUERY, ConsistencyLevel.QUORUM, getClientState(), 10L);
        assertFalse(DatabaseDescriptor.getRoleManager().isExistingIdentity(IDENTITY));
    }

    @Test
    public void testAnonymousUser()
    {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage("You have not logged in");
        QueryProcessor.executeInternal(DROP_QUERY);
    }

    @Test
    public void testUsersWithoutPrevilegesCannotDropIdentities()
    {
        // Added user to roles table
        AuthenticatedUser authenticatedUser = new AuthenticatedUser("readwrite_user");
        DatabaseDescriptor.getRoleManager().createRole(authenticatedUser, RoleResource.role("readwrite_user"), AuthTestUtils.getLoginRoleOptions());
        ClientState state = ClientState.forInternalCalls();
        state.login(authenticatedUser);

        String query = String.format("DROP IDENTITY '%s';", IDENTITY);
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage("User readwrite_user does not have sufficient privileges to perform the requested operation");
        QueryProcessor.process(query, ConsistencyLevel.QUORUM, new QueryState(state), 10L);
    }

    @Test
    public void dropRoleSholdRemoveAllAssociatedIdentities()
    {
        // Create a new user and associate identities to the user
        AuthenticatedUser authenticatedUser = new AuthenticatedUser("readwrite_user");
        DatabaseDescriptor.getRoleManager().createRole(authenticatedUser, RoleResource.role("readwrite_user"), AuthTestUtils.getLoginRoleOptions());
        DatabaseDescriptor.getRoleManager().addIdentity(IDENTITY, "readwrite_user");
        String identity1 = "spiffe://testdomain.com/testIdentifier/testValue1";
        DatabaseDescriptor.getRoleManager().addIdentity(identity1, "readwrite_user");

        // Dropping role should remove identities associated with it
        String query = "DROP ROLE readwrite_user";
        QueryProcessor.process(query,  ConsistencyLevel.QUORUM, getClientState(), 10L);

        Map<String, String > m = DatabaseDescriptor.getRoleManager().authorizedIdentities();
        assertFalse(m.containsKey(IDENTITY));
        assertFalse(m.containsKey(identity1));
    }

    @Test
    public void ifExistsTest()
    {
        // Assert that identity is not present in the table
        assertNull(DatabaseDescriptor.getRoleManager().roleForIdentity(IDENTITY));

        String dropQueryWithIfExists = String.format("DROP IDENTITY IF EXISTS '%s'", IDENTITY);

        // Identity in the table & IF EXISTS in query should succeed
        DatabaseDescriptor.getRoleManager().addIdentity(IDENTITY, "cassandra");
        QueryProcessor.process(dropQueryWithIfExists, ConsistencyLevel.QUORUM, getClientState(), 10L);

        // Identity not in the table & IF EXISTS in query should succeed
        QueryProcessor.process(dropQueryWithIfExists, ConsistencyLevel.QUORUM, getClientState(), 10L);

        String dropQueryWithOutIfExists = String.format("DROP IDENTITY '%s'", IDENTITY);
        // Identity in the table & no IF EXISTS in query should succeed
        DatabaseDescriptor.getRoleManager().addIdentity(IDENTITY, "cassandra");
        QueryProcessor.process(dropQueryWithOutIfExists, ConsistencyLevel.QUORUM, getClientState(), 10L);

        // Identity not in the table & no IF EXISTS in query should fail
        expectedException.expect(InvalidRequestException.class);
        expectedException.expectMessage(String.format("identity '%s' doesn't exist", IDENTITY));
        QueryProcessor.process(dropQueryWithOutIfExists, ConsistencyLevel.QUORUM, getClientState(), 10L);
    }
}

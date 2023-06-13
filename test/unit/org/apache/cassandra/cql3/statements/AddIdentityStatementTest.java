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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraRoleManager;
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
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AddIdentityStatementTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private static final String USER_ROLE = "cassandra";
    private static final String IDENTITY = "spiffe://testdomain.com/testIdentifier/testValue";
    private static final String ADD_QUERY = String.format("ADD IDENTITY '%s' TO ROLE '%s';", IDENTITY, USER_ROLE);
    private static void setupPrivilegedUser()
    {
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (role, is_superuser, can_login, salted_hash) "
                                                     + "VALUES ('%s', true, true, '%s')",
                                                     AUTH_KEYSPACE_NAME,
                                                     AuthKeyspace.ROLES,
                                                     CassandraRoleManager.DEFAULT_SUPERUSER_NAME,
                                                     "xxx"));
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.setupAuth(new AuthTestUtils.LocalCassandraRoleManager(),
                               new AuthTestUtils.LocalPasswordAuthenticator(),
                               new AuthTestUtils.LocalCassandraAuthorizer(),
                               new AuthTestUtils.LocalCassandraNetworkAuthorizer(),
                               new AuthTestUtils.LocalCassandraCIDRAuthorizer());
        AuthCacheService.initializeAndRegisterCaches();
        setupPrivilegedUser();
    }

    @Before
    public void clear()
    {
        Keyspace.open(AUTH_KEYSPACE_NAME).getColumnFamilyStore(IDENTITY_TO_ROLES).truncateBlocking();
    }

    @Test
    public void testAddIdentityStatementParsing()
    {
        CQLStatement.Raw statement = QueryProcessor.parseStatement(ADD_QUERY);
        assertTrue(statement instanceof AddIdentityStatement);
        AddIdentityStatement addIdentityStatement =  (AddIdentityStatement)statement;
        assertEquals(IDENTITY, addIdentityStatement.identity);
        assertEquals(USER_ROLE, addIdentityStatement.role);
    }

    @Test
    public void testAddingValidIdentity()
    {
        QueryProcessor.process(ADD_QUERY, ConsistencyLevel.QUORUM, getClientState(), 10L);
        assertEquals(USER_ROLE, DatabaseDescriptor.getRoleManager().roleForIdentity(IDENTITY));
    }

    @Test
    public void testAddingExistingIdentity()
    {
        QueryProcessor.process(ADD_QUERY, ConsistencyLevel.QUORUM, getClientState(), 10L);
        expectedException.expect(InvalidRequestException.class);
        expectedException.expectMessage(IDENTITY +" already exists");
        QueryProcessor.process(ADD_QUERY, ConsistencyLevel.QUORUM, getClientState(), 10L);
    }

    @Test
    public void testAddIdentityOnlyWhenNotPresent()
    {
        DatabaseDescriptor.getRoleManager().addIdentity(IDENTITY, USER_ROLE);
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Identity is already associated with another role, cannot associate it with role read_write_user");
        DatabaseDescriptor.getRoleManager().addIdentity(IDENTITY, "read_write_user");
    }

    @Test
    public void testAnonymousUser()
    {
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage("You have not logged in");
        QueryProcessor.executeInternal(ADD_QUERY);
    }

    @Test
    public void testAddingNonExistentRole()
    {
        String query = String.format("ADD IDENTITY '%s' TO ROLE 'non-existing-role';", IDENTITY);
        expectedException.expect(InvalidRequestException.class);
        expectedException.expectMessage("Can not add identity for non-existent role 'non-existing-role'");
        QueryProcessor.process(query, ConsistencyLevel.QUORUM, getClientState(), 10L);
    }

    @Test
    public void testUsersWithNoPrevilegesCannotAddIdentitiess()
    {
        // Added user to roles table
        AuthenticatedUser authenticatedUser = new AuthenticatedUser("readwrite_user");
        DatabaseDescriptor.getRoleManager().createRole(authenticatedUser, RoleResource.role("readwrite_user"), AuthTestUtils.getLoginRoleOptions());
        ClientState state = ClientState.forInternalCalls();
        state.login(authenticatedUser);

        String query = String.format("ADD IDENTITY '%s' TO ROLE 'readwrite_user';", IDENTITY);
        expectedException.expect(UnauthorizedException.class);
        expectedException.expectMessage("User readwrite_user does not have sufficient privileges to perform the requested operation");
        QueryProcessor.process(query, ConsistencyLevel.QUORUM, new QueryState(state), 10L);
    }


    @Test
    public void creatingRoleWithIdentitiesAlreadyExisting()
    {
        DatabaseDescriptor.getRoleManager().addIdentity(IDENTITY, "readwrite_user");
        AuthenticatedUser authenticatedUser = new AuthenticatedUser("cassandra");
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Cannot create a role 'readwrite_user' when identities already exists for it");
        DatabaseDescriptor.getRoleManager().createRole(authenticatedUser, RoleResource.role("readwrite_user"), AuthTestUtils.getLoginRoleOptions());
    }

    @Test
    public void ifNotExistsTest()
    {
        // Assert that identity is not present in the table
        assertNull(DatabaseDescriptor.getRoleManager().roleForIdentity(IDENTITY));

        String addQueryWithIfNotExists = String.format("ADD IDENTITY IF NOT EXISTS '%s' TO ROLE '%s';", IDENTITY, USER_ROLE);

        // Identity not in the table & add identity query with IF NOT EXISTS should succeed
        QueryProcessor.process(addQueryWithIfNotExists, ConsistencyLevel.QUORUM, getClientState(), 10L);
        assertEquals(USER_ROLE, DatabaseDescriptor.getRoleManager().roleForIdentity(IDENTITY));

        // Identity in the table & add identity query with IF NOT EXISTS should succeed
        QueryProcessor.process(addQueryWithIfNotExists, ConsistencyLevel.QUORUM, getClientState(), 10L);
        assertEquals(USER_ROLE, DatabaseDescriptor.getRoleManager().roleForIdentity(IDENTITY));

        clear();
        String addQueryWithOutIfNotExists = String.format("ADD IDENTITY '%s' TO ROLE '%s';", IDENTITY, USER_ROLE);
        // Identity not in the table & add identity query without IF NOT EXISTS should succeed
        QueryProcessor.process(addQueryWithOutIfNotExists, ConsistencyLevel.QUORUM, getClientState(), 10L);
        assertEquals(USER_ROLE, DatabaseDescriptor.getRoleManager().roleForIdentity(IDENTITY));

        // Identity in the table & add identity query without IF NOT EXISTS should fail
        expectedException.expect(InvalidRequestException.class);
        expectedException.expectMessage(IDENTITY + " already exists");
        QueryProcessor.process(addQueryWithOutIfNotExists, ConsistencyLevel.QUORUM, getClientState(), 10L);
    }

    static QueryState getClientState()
    {
        ClientState state = ClientState.forInternalCalls();
        state.login(new AuthenticatedUser(CassandraRoleManager.DEFAULT_SUPERUSER_NAME));
        return new QueryState(state);
    }
}

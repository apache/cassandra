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
import com.google.common.collect.Sets;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.AlterRoleStatement;
import org.apache.cassandra.cql3.statements.AuthenticationStatement;
import org.apache.cassandra.cql3.statements.CreateRoleStatement;
import org.apache.cassandra.cql3.statements.DropRoleStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.ClientState;

import static org.apache.cassandra.auth.AuthKeyspace.NETWORK_PERMISSIONS;
import static org.apache.cassandra.auth.AuthTestUtils.getRolesReadCount;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;

public class CassandraNetworkAuthorizerTest
{
    private static void setupSuperUser()
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
                               new AuthTestUtils.LocalCassandraNetworkAuthorizer());
        AuthCacheService.initializeAndRegisterCaches();
        setupSuperUser();
    }

    @Before
    public void clear()
    {
        Keyspace.open(AUTH_KEYSPACE_NAME).getColumnFamilyStore(NETWORK_PERMISSIONS).truncateBlocking();
    }

    private static void assertNoDcPermRow(String username)
    {
        String query = String.format("SELECT dcs FROM %s.%s WHERE role = '%s'",
                                     AUTH_KEYSPACE_NAME,
                                     NETWORK_PERMISSIONS,
                                     RoleResource.role(username).getName());
        UntypedResultSet results = QueryProcessor.executeInternal(query);
        Assert.assertTrue(results.isEmpty());
    }

    private static void assertDcPermRow(String username, String... dcs)
    {
        Set<String> expected = Sets.newHashSet(dcs);
        String query = String.format("SELECT dcs FROM %s.%s WHERE role = '%s'",
                                     AUTH_KEYSPACE_NAME,
                                     NETWORK_PERMISSIONS,
                                     RoleResource.role(username).getName());
        UntypedResultSet results = QueryProcessor.executeInternal(query);
        UntypedResultSet.Row row = Iterables.getOnlyElement(results);
        Set<String> actual = row.getFrozenSet("dcs", UTF8Type.instance);
        Assert.assertEquals(expected, actual);
    }

    private static String createName()
    {
        return RandomStringUtils.randomAlphabetic(8).toLowerCase();
    }

    private static ClientState getClientState()
    {
        ClientState state = ClientState.forInternalCalls();
        state.login(new AuthenticatedUser(CassandraRoleManager.DEFAULT_SUPERUSER_NAME));
        return state;
    }

    private static void auth(String query, Object... args)
    {
        CQLStatement statement = QueryProcessor.parseStatement(String.format(query, args)).prepare(ClientState.forInternalCalls());
        assert statement instanceof CreateRoleStatement
               || statement instanceof AlterRoleStatement
               || statement instanceof DropRoleStatement;
        AuthenticationStatement authStmt = (AuthenticationStatement) statement;

        // invalidate roles cache so that any changes to the underlying roles are picked up
        Roles.cache.invalidate();
        authStmt.execute(getClientState());
    }

    private static DCPermissions dcPerms(String username)
    {
        AuthenticatedUser user = new AuthenticatedUser(username);
        return DatabaseDescriptor.getNetworkAuthorizer().authorize(user.getPrimaryRole());
    }

    @Test
    public void create()
    {
        String username = createName();

        // user should implicitly have access to all datacenters
        assertNoDcPermRow(username);
        auth("CREATE ROLE %s WITH password = 'password' AND LOGIN = true AND ACCESS TO DATACENTERS {'dc1', 'dc2'}", username);
        Assert.assertEquals(DCPermissions.subset("dc1", "dc2"), dcPerms(username));
        assertDcPermRow(username, "dc1", "dc2");
    }

    @Test
    public void alter()
    {

        String username = createName();

        assertNoDcPermRow(username);
        // user should implicitly have access to all datacenters
        auth("CREATE ROLE %s WITH password = 'password' AND LOGIN = true", username);
        Assert.assertEquals(DCPermissions.all(), dcPerms(username));
        assertDcPermRow(username);

        // unless explicitly restricted
        auth("ALTER ROLE %s WITH ACCESS TO DATACENTERS {'dc1', 'dc2'}", username);
        Assert.assertEquals(DCPermissions.subset("dc1", "dc2"), dcPerms(username));
        assertDcPermRow(username, "dc1", "dc2");

        auth("ALTER ROLE %s WITH ACCESS TO DATACENTERS {'dc1'}", username);
        Assert.assertEquals(DCPermissions.subset("dc1"), dcPerms(username));
        assertDcPermRow(username, "dc1");

        auth("ALTER ROLE %s WITH ACCESS TO ALL DATACENTERS", username);
        Assert.assertEquals(DCPermissions.all(), dcPerms(username));
        assertDcPermRow(username);
    }

    @Test
    public void drop()
    {
        String username = createName();

        assertNoDcPermRow(username);
        // user should implicitly have access to all datacenters
        auth("CREATE ROLE %s WITH password = 'password' AND LOGIN = true AND ACCESS TO DATACENTERS {'dc1'}", username);
        assertDcPermRow(username, "dc1");

        auth("DROP ROLE %s", username);
        assertNoDcPermRow(username);
    }

    @Test
    public void superUser()
    {
        String username = createName();
        auth("CREATE ROLE %s WITH password = 'password' AND LOGIN = true AND ACCESS TO DATACENTERS {'dc1'}", username);
        Assert.assertEquals(DCPermissions.subset("dc1"), dcPerms(username));
        assertDcPermRow(username, "dc1");

        // clear the roles cache to lose the (non-)superuser status for the user
        Roles.cache.invalidate();
        auth("ALTER ROLE %s WITH superuser = true", username);
        Assert.assertEquals(DCPermissions.all(), dcPerms(username));
    }

    @Test
    public void cantLogin()
    {
        String username = createName();
        auth("CREATE ROLE %s", username);
        Assert.assertEquals(DCPermissions.none(), dcPerms(username));
    }

    @Test
    public void getLoginPrivilegeFromRolesCache()
    {
        String username = createName();
        auth("CREATE ROLE %s", username);
        long readCount = getRolesReadCount();
        dcPerms(username);
        Assert.assertEquals(++readCount, getRolesReadCount());
        dcPerms(username);
        Assert.assertEquals(readCount, getRolesReadCount());
    }
}

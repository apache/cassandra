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

import java.net.InetSocketAddress;
import java.util.Collections;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.ClientState;

import static org.apache.cassandra.auth.AuthKeyspace.CIDR_GROUPS;
import static org.apache.cassandra.auth.AuthKeyspace.CIDR_PERMISSIONS;
import static org.apache.cassandra.auth.AuthTestUtils.auth;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;

/**
 * Tests to verify AllowAllCIDRAuthorizer allows access from any CIDR
 */
public class AllowAllCIDRAuthorizerTest extends CQLTester
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
                               new AuthTestUtils.LocalCassandraNetworkAuthorizer(),
                               new AuthTestUtils.LocalAllowAllCIDRAuthorizer());
        AuthCacheService.initializeAndRegisterCaches();
        setupSuperUser();
    }

    @Before
    public void before()
    {
        Keyspace.open(AUTH_KEYSPACE_NAME).getColumnFamilyStore(CIDR_GROUPS).truncateBlocking();
        Keyspace.open(AUTH_KEYSPACE_NAME).getColumnFamilyStore(CIDR_PERMISSIONS).truncateBlocking();
    }

    private void testAccessSucceeds(String userName, String ip)
    {
        InetSocketAddress ipAddr = new InetSocketAddress(ip, 0);

        AuthenticatedUser user = new AuthenticatedUser(userName);
        Assert.assertTrue(user.hasAccessFromIp(ipAddr));

        ClientState clientState = ClientState.forExternalCalls(ipAddr);
        clientState.login(user);
        clientState.validateLogin(); // expect no exception
    }

    @Test
    public void testSuperUser()
    {
        testAccessSucceeds(CassandraRoleManager.DEFAULT_SUPERUSER_NAME, "10.20.30.5");
    }

    @Test
    public void testUserWithoutCidrRestrictions()
    {
        String roleName = "role1";
        auth("CREATE ROLE %s WITH password = 'password' AND LOGIN = true ",
             roleName);
        testAccessSucceeds(roleName, "127.0.0.1");
    }

    @Test
    public void testUserWithRestrictingCidrGroups()
    {
        String roleName = "role1";
        auth("CREATE ROLE %s WITH password = 'password' AND LOGIN = true AND ACCESS FROM CIDRS { 'cidrGroup1' }",
             roleName);
        testAccessSucceeds(roleName, "12.0.0.1");
    }

    @Test
    public void testUserAllowedforAllCidrs()
    {
        String roleName = "role1";
        auth("CREATE ROLE %s WITH password = 'password' AND LOGIN = true AND ACCESS FROM ALL CIDRS",
             roleName);
        testAccessSucceeds(roleName, "17.0.0.1");
    }

    private UntypedResultSet getCidrGroups(String role)
    {
        String query = String.format("SELECT %s FROM %s.%s WHERE role = '%s'",
                                     "cidr_groups",
                                     AUTH_KEYSPACE_NAME,
                                     CIDR_PERMISSIONS,
                                     RoleResource.role(role).getRoleName());
        return QueryProcessor.executeInternal(query);
    }

    // Create role command with access from cidrs clause should work even when CIDR authorizer is disabled
    @Test
    public void testCreateAndDropRoleWithCidrsClause() throws Throwable
    {
        String role = "role1";

        assertEmpty(getCidrGroups(role));

        AuthTestUtils.createUsersWithCidrAccess(Collections.singletonMap(role,
                                                                         Collections.singletonList("cidrGroup1")));
        UntypedResultSet results = getCidrGroups(role);
        Assert.assertEquals(Sets.newHashSet("cidrGroup1"),
                            Iterables.getOnlyElement(results).getFrozenSet("cidr_groups", UTF8Type.instance));

        AuthTestUtils.auth("DROP ROLE %s", role);
        assertEmpty(getCidrGroups(role));
    }

    // Alter role command with access from cidrs clause should work even when CIDR authorizer is disabled
    @Test
    public void testAlterAndDropRoleWithCidrsClause() throws Throwable
    {
        String role = "role1";

        assertEmpty(getCidrGroups(role));

        auth("CREATE ROLE %s WITH password = 'password' AND LOGIN = true ", role);
        auth("ALTER ROLE %s WITH ACCESS FROM CIDRS {'%s'}", role, "cidrGroup1");

        UntypedResultSet results = getCidrGroups(role);
        Assert.assertEquals(Sets.newHashSet("cidrGroup1"),
                            Iterables.getOnlyElement(results).getFrozenSet("cidr_groups", UTF8Type.instance));

        AuthTestUtils.auth("DROP ROLE %s", role);
        assertEmpty(getCidrGroups(role));
    }
}

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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.CIDR;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;

import static java.lang.String.format;
import static org.apache.cassandra.auth.AuthKeyspace.CIDR_GROUPS;
import static org.apache.cassandra.auth.AuthKeyspace.CIDR_PERMISSIONS;
import static org.apache.cassandra.auth.AuthTestUtils.auth;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class CassandraCIDRAuthorizerEnforceModeTest extends CQLTester
{
    private static final AuthTestUtils.LocalCassandraCIDRAuthorizer cidrAuthorizer = new AuthTestUtils.LocalCassandraCIDRAuthorizer();

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
                               cidrAuthorizer);
        AuthCacheService.initializeAndRegisterCaches();
        setupSuperUser();
    }

    @Before
    public void clear()
    {
        Keyspace.open(AUTH_KEYSPACE_NAME).getColumnFamilyStore(CIDR_GROUPS).truncateBlocking();
        Keyspace.open(AUTH_KEYSPACE_NAME).getColumnFamilyStore(CIDR_PERMISSIONS).truncateBlocking();
        cidrAuthorizer.getCidrPermissionsCache().invalidate();
    }

    private void testValidCidrAccess(String userName, String ip)
    {
        InetSocketAddress ipAddr = new InetSocketAddress(ip, 0);

        AuthenticatedUser user = new AuthenticatedUser(userName);
        Assert.assertTrue(user.hasAccessFromIp(ipAddr));

        ClientState clientState = ClientState.forExternalCalls(ipAddr);
        clientState.login(user);
        clientState.validateLogin(); // expect no exception
    }

    private void testInvalidCidrAccess(String userName, String ip)
    {
        InetSocketAddress ipAddr = new InetSocketAddress(ip, 0);

        AuthenticatedUser user = new AuthenticatedUser(userName);
        Assert.assertFalse(user.hasAccessFromIp(ipAddr));
    }

    private void testInvalidCidrLogin(String userName, String ip)
    {
        ClientState clientState = ClientState.forExternalCalls(new InetSocketAddress(ip, 0));
        clientState.login(new AuthenticatedUser(userName));

        assertThatThrownBy(clientState::validateLogin).isInstanceOf(UnauthorizedException.class).hasMessageContaining(
            format("You do not have access from this IP %s", ip));
    }

    @Test
    public void testCidrAccesses()
    {
        Map<String, List<String>> usersList = new HashMap<String, List<String>>()
        {{
            put("user1", Collections.singletonList("cidrGroup1"));
            put("user2", Arrays.asList("cidrGroup2", "cidrGroup3"));
        }};

        Map<String, List<CIDR>> cidrsMapping = new HashMap<String, List<CIDR>>()
        {{
            put("cidrGroup1", Collections.singletonList(CIDR.getInstance("10.20.30.5/24")));
            put("cidrGroup2", Arrays.asList(CIDR.getInstance("1111:2222:3333:4444:5555:6666:7777:8888/106"),
                                            CIDR.getInstance("2001:3002::/16")));
            put("cidrGroup3", Arrays.asList(CIDR.getInstance("40.50.60.7/32"), CIDR.getInstance("50.60.70.80/10"),
                                            CIDR.getInstance("60.70.80.90/22")));
        }};

        AuthTestUtils.createUsersWithCidrAccess(usersList);
        AuthTestUtils.insertCidrsMappings(cidrsMapping);

        for (Map.Entry<String, List<String>> user : usersList.entrySet())
        {
            for (String cidrGroup : user.getValue())
            {
                List<CIDR> cidrsofCidrGroup = cidrsMapping.get(cidrGroup);
                for (CIDR cidr : cidrsofCidrGroup)
                {
                    testValidCidrAccess(user.getKey(), cidr.getStartIpAddress().getHostAddress());
                }
            }
        }

        for (CIDR cidr : cidrsMapping.get("cidrGroup2"))
        {
            testInvalidCidrAccess("user1", cidr.getStartIpAddress().getHostAddress());
        }

        for (CIDR cidr : cidrsMapping.get("cidrGroup3"))
        {
            testInvalidCidrAccess("user1", cidr.getStartIpAddress().getHostAddress());
        }

        for (CIDR cidr : cidrsMapping.get("cidrGroup1"))
        {
            testInvalidCidrAccess("user2", cidr.getStartIpAddress().getHostAddress());
        }

        testInvalidCidrLogin("user1", "20.30.40.6");
    }

    @Test
    public void testNonexistingCidrLogin()
    {
        AuthTestUtils.createUsersWithCidrAccess(Collections.singletonMap("user11",
                                                                         Collections.singletonList("cidrGroup11")));
        AuthTestUtils.insertCidrsMappings(Collections.singletonMap("cidrGroup11",
                                                                   Collections.singletonList(
                                                                   CIDR.getInstance("200.30.40.60/24"))));

        testInvalidCidrLogin("user11", "250.30.40.60");
    }

    @Test
    public void testSuperUserAccess()
    {
        testValidCidrAccess(CassandraRoleManager.DEFAULT_SUPERUSER_NAME, "255.255.255.255");
    }

    @Test
    public void testUserWithoutCidrRestrictions()
    {
        String roleName = "role1";
        AuthTestUtils.auth("CREATE ROLE %s WITH password = 'password' AND LOGIN = true ",
             roleName);
        testValidCidrAccess(roleName, "127.0.0.1");
    }

    @Test
    public void testUserAllowedforAllCidrs()
    {
        String roleName = "role1";
        AuthTestUtils.auth("CREATE ROLE %s WITH password = 'password' AND LOGIN = true AND ACCESS FROM ALL CIDRS",
             roleName);
        testValidCidrAccess(roleName, "17.0.0.1");
    }

    @Test
    public void testUserWithoutLogin()
    {
        String roleName = "role1";
        AuthTestUtils.auth("CREATE ROLE %s WITH password = 'password'", roleName);
        testInvalidCidrAccess(roleName, "17.0.0.1");
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

    @Test
    public void testInvalidCidrs()
    {
        Map<String, List<String>> usersList = new HashMap<String, List<String>>()
        {{
            put("user1", Collections.singletonList("cidrGroup1"));
        }};

        AuthTestUtils.createUsersWithCidrAccess(usersList);

        QueryProcessor.executeInternal(format(
            "insert into %s.%s(cidr_group, cidrs) values('cidrGroup1', { ('10.20.30.5', 33), ('11.20.30.6', 16) } );",
            AUTH_KEYSPACE_NAME, CIDR_GROUPS));
        DatabaseDescriptor.getCIDRAuthorizer().loadCidrGroupsCache(); // update cache with CIDRs inserted above

        testInvalidCidrAccess("user1", "10.20.30.5");
        testValidCidrAccess("user1", "11.20.30.60");
    }

    @Test
    public void testCidrChecksForSuperUsers()
    {
        testValidCidrAccess(CassandraRoleManager.DEFAULT_SUPERUSER_NAME, "10.20.30.5");
        testValidCidrAccess(CassandraRoleManager.DEFAULT_SUPERUSER_NAME, "200.30.40.60");

        Config conf = DatabaseDescriptor.getRawConfig();
        conf.cidr_authorizer = new ParameterizedClass(CassandraCIDRAuthorizer.class.getName(), new HashMap<>());
        conf.cidr_authorizer.parameters.put("cidr_checks_for_superusers", String.valueOf(true));

        AuthTestUtils.insertCidrsMappings(Collections.singletonMap("cidrGroup1",
                                                                   Collections.singletonList(
                                                                   CIDR.getInstance("200.30.40.60/24"))));
        AuthTestUtils.auth("alter role %s with access from cidrs {'cidrGroup1'}",
                           CassandraRoleManager.DEFAULT_SUPERUSER_NAME);
        cidrAuthorizer.getCidrPermissionsCache().invalidate();

        testInvalidCidrAccess(CassandraRoleManager.DEFAULT_SUPERUSER_NAME, "10.20.30.5");
        testValidCidrAccess(CassandraRoleManager.DEFAULT_SUPERUSER_NAME, "200.30.40.60");
    }
}

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
import java.util.HashMap;

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
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.ClientState;

import static org.apache.cassandra.auth.AuthKeyspace.CIDR_GROUPS;
import static org.apache.cassandra.auth.AuthKeyspace.CIDR_PERMISSIONS;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;

public class CassandraCIDRAuthorizerMonitorModeTest extends CQLTester
{
    private static final AuthTestUtils.LocalCassandraCIDRAuthorizer cidrAuthorizer =
    new AuthTestUtils.LocalCassandraCIDRAuthorizer(ICIDRAuthorizer.CIDRAuthorizerMode.MONITOR);
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
    public void before()
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

    @Test
    public void testCidrAccesses()
    {
        AuthTestUtils.createUsersWithCidrAccess(Collections.singletonMap("user1",
                                                                         Collections.singletonList("cidrGroup1")));
        AuthTestUtils.insertCidrsMappings(Collections.singletonMap("cidrGroup1",
                                                                   Collections.singletonList(
                                                                   CIDR.getInstance("10.20.30.5/24"))));

        testValidCidrAccess("user1", "10.20.30.5");
        testValidCidrAccess("user1", "20.20.30.5");
    }

    @Test
    public void testNonexistingCidrLogin()
    {
        AuthTestUtils.createUsersWithCidrAccess(Collections.singletonMap("user11",
                                                                         Collections.singletonList("cidrGroup11")));
        AuthTestUtils.insertCidrsMappings(Collections.singletonMap("cidrGroup11",
                                                                   Collections.singletonList(
                                                                   CIDR.getInstance("200.30.40.60/24"))));

        testValidCidrAccess("user11", "250.30.40.60");
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

        testValidCidrAccess(CassandraRoleManager.DEFAULT_SUPERUSER_NAME, "10.20.30.5");
        testValidCidrAccess(CassandraRoleManager.DEFAULT_SUPERUSER_NAME, "200.30.40.60");
    }
}

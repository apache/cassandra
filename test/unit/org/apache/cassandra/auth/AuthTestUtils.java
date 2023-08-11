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

import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.auth.jmx.AuthorizationProxy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CIDR;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.AlterRoleStatement;
import org.apache.cassandra.cql3.statements.AuthenticationStatement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.CreateRoleStatement;
import org.apache.cassandra.cql3.statements.DropRoleStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.ResultMessage;

import static java.lang.String.format;
import static org.apache.cassandra.auth.AuthKeyspace.CIDR_GROUPS;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;
import static org.junit.Assert.assertNotNull;


public class AuthTestUtils
{

    public static final RoleResource ROLE_A = RoleResource.role("role_a");
    public static final RoleResource ROLE_B = RoleResource.role("role_b");
    public static final RoleResource ROLE_B_1 = RoleResource.role("role_b_1");
    public static final RoleResource ROLE_B_2 = RoleResource.role("role_b_2");
    public static final RoleResource ROLE_B_3 = RoleResource.role("role_b_3");
    public static final RoleResource ROLE_C = RoleResource.role("role_c");
    public static final RoleResource ROLE_C_1 = RoleResource.role("role_c_1");
    public static final RoleResource ROLE_C_2 = RoleResource.role("role_c_2");
    public static final RoleResource ROLE_C_3 = RoleResource.role("role_c_3");
    public static final RoleResource[] ALL_ROLES  = new RoleResource[] {ROLE_A,
                                                                        ROLE_B, ROLE_B_1, ROLE_B_2, ROLE_B_3,
                                                                        ROLE_C, ROLE_C_1, ROLE_C_2, ROLE_C_3};
    /**
     * This just extends the internal IRoleManager implementation to ensure that
     * all access to underlying tables is made via
     * QueryProcessor.executeOnceInternal/CQLStatement.executeInternal and not
     * StorageProxy so that it can be used in unit tests.
     */
    public static class LocalCassandraRoleManager extends CassandraRoleManager
    {
        @Override
        ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
        {
            return statement.executeLocally(QueryState.forInternalCalls(), options);
        }

        @Override
        UntypedResultSet process(String query, ConsistencyLevel consistencyLevel)
        {
            return QueryProcessor.executeInternal(query);
        }

        @Override
        UntypedResultSet process(String query, ConsistencyLevel consistencyLevel, ByteBuffer... values)
        {
            return QueryProcessor.executeInternal(query, (Object[]) values);
        }

        @Override
        protected void scheduleSetupTask(final Callable<Void> setupTask)
        {
            // skip data migration or setting up default role for tests
        }
    }

    public static class LocalCassandraAuthorizer extends CassandraAuthorizer
    {
        @Override
        ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
        {
            return statement.executeLocally(QueryState.forInternalCalls(), options);
        }

        @Override
        UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
        {
            return QueryProcessor.executeInternal(query);
        }

        @Override
        void processBatch(BatchStatement statement)
        {
            statement.executeLocally(QueryState.forInternalCalls(), QueryOptions.DEFAULT);
        }
    }

    public static class LocalCassandraNetworkAuthorizer extends CassandraNetworkAuthorizer
    {
        @Override
        ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
        {
            return statement.executeLocally(QueryState.forInternalCalls(), options);
        }

        @Override
        UntypedResultSet process(String query, ConsistencyLevel cl)
        {
            return QueryProcessor.executeInternal(query);
        }
    }

    public static class LocalCIDRGroupsMappingManager extends CIDRGroupsMappingManager
    {
        @Override
        ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
        {
            return statement.executeLocally(QueryState.forInternalCalls(), options);
        }

        @Override
        UntypedResultSet process(String query, ConsistencyLevel cl)
        {
            return QueryProcessor.executeInternal(query);
        }

        public static String getCidrTuplesSet(List<CIDR> cidrs)
        {
            return getCidrTuplesSetString(cidrs);
        }
    }

    public static class LocalCIDRPermissionsManager extends CIDRPermissionsManager
    {
        @Override
        public ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
        {
            return statement.executeLocally(QueryState.forInternalCalls(), options);
        }

        @Override
        public UntypedResultSet process(String query, ConsistencyLevel cl)
        {
            return QueryProcessor.executeInternal(query);
        }
    }

    public static class LocalCassandraCIDRAuthorizer extends CassandraCIDRAuthorizer
    {
        CIDRAuthorizerMode cidrAuthorizerMode;

        public LocalCassandraCIDRAuthorizer()
        {
            cidrAuthorizerMode = CIDRAuthorizerMode.ENFORCE;
        }

        public LocalCassandraCIDRAuthorizer(CIDRAuthorizerMode mode)
        {
            cidrAuthorizerMode = mode;
        }

        @Override
        protected void createManagers()
        {
            cidrPermissionsManager = new LocalCIDRPermissionsManager();
            cidrGroupsMappingManager = new LocalCIDRGroupsMappingManager();
        }

        @Override
        protected boolean isMonitorMode()
        {
            return cidrAuthorizerMode == CIDRAuthorizerMode.MONITOR;
        }

        CIDRPermissionsCache getCidrPermissionsCache()
        {
            return cidrPermissionsCache;
        }
    }

    public static class LocalAllowAllCIDRAuthorizer extends AllowAllCIDRAuthorizer
    {
        @Override
        protected void createManagers()
        {
            cidrPermissionsManager = new LocalCIDRPermissionsManager();
            cidrGroupsMappingManager = new LocalCIDRGroupsMappingManager();
        }
    }

    public static class LocalPasswordAuthenticator extends PasswordAuthenticator
    {
        @Override
        ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
        {
            return statement.executeLocally(QueryState.forInternalCalls(), options);
        }

        @Override
        UntypedResultSet process(String query, ConsistencyLevel cl)
        {
            return QueryProcessor.executeInternal(query);
        }
    }

    public static class NoAuthSetupAuthorizationProxy extends AuthorizationProxy
    {
        public NoAuthSetupAuthorizationProxy()
        {
            super();
            this.isAuthSetupComplete = () -> true;
        }
    }

    public static void grantRolesTo(IRoleManager roleManager, RoleResource grantee, RoleResource...granted)
    {
        for(RoleResource toGrant : granted)
            roleManager.grantRole(AuthenticatedUser.ANONYMOUS_USER, toGrant, grantee);
    }

    public static long getNetworkPermissionsReadCount()
    {
        ColumnFamilyStore networkPemissionsTable =
                Keyspace.open(SchemaConstants.AUTH_KEYSPACE_NAME).getColumnFamilyStore(AuthKeyspace.NETWORK_PERMISSIONS);
        return networkPemissionsTable.metric.readLatency.latency.getCount();
    }

    public static long getCidrPermissionsReadCount()
    {
        ColumnFamilyStore cidrPemissionsTable =
        Keyspace.open(SchemaConstants.AUTH_KEYSPACE_NAME).getColumnFamilyStore(AuthKeyspace.CIDR_PERMISSIONS);
        return cidrPemissionsTable.metric.readLatency.latency.getCount();
    }

    public static long getRolePermissionsReadCount()
    {
        ColumnFamilyStore rolesPemissionsTable =
                Keyspace.open(SchemaConstants.AUTH_KEYSPACE_NAME).getColumnFamilyStore(AuthKeyspace.ROLE_PERMISSIONS);
        return rolesPemissionsTable.metric.readLatency.latency.getCount();
    }

    public static long getRolesReadCount()
    {
        ColumnFamilyStore rolesTable = Keyspace.open(SchemaConstants.AUTH_KEYSPACE_NAME).getColumnFamilyStore(AuthKeyspace.ROLES);
        return rolesTable.metric.readLatency.latency.getCount();
    }

    public static RoleOptions getLoginRoleOptions()
    {
        RoleOptions roleOptions = new RoleOptions();
        roleOptions.setOption(IRoleManager.Option.SUPERUSER, false);
        roleOptions.setOption(IRoleManager.Option.LOGIN, true);
        roleOptions.setOption(IRoleManager.Option.PASSWORD, "ignored");
        return roleOptions;
    }

    private static ClientState getClientState()
    {
        ClientState state = ClientState.forInternalCalls();
        state.login(new AuthenticatedUser(CassandraRoleManager.DEFAULT_SUPERUSER_NAME));
        return state;
    }

    public static AuthenticationStatement authWithoutInvalidate(String query, Object... args)
    {
        CQLStatement statement = QueryProcessor.parseStatement(String.format(query, args)).prepare(ClientState.forInternalCalls());
        assert statement instanceof CreateRoleStatement
               || statement instanceof AlterRoleStatement
               || statement instanceof DropRoleStatement;
        AuthenticationStatement authStmt = (AuthenticationStatement) statement;

        authStmt.execute(getClientState());

        return authStmt;
    }

    public static AuthenticationStatement auth(String query, Object... args)
    {
        AuthenticationStatement authStmt = authWithoutInvalidate(query, args);

        // invalidate roles cache so that any changes to the underlying roles are picked up
        Roles.cache.invalidate();

        return authStmt;
    }

    public static void createUsersWithCidrAccess(Map<String, List<String>> userToCidrPermsMapping)
    {
        for (Map.Entry<String, List<String>> userMapping : userToCidrPermsMapping.entrySet())
        {
            authWithoutInvalidate(
                "CREATE ROLE %s WITH password = 'password' AND LOGIN = true AND ACCESS FROM CIDRS {'%s'}",
                 userMapping.getKey(), String.join("', '", userMapping.getValue()));
        }
        Roles.cache.invalidate();
    }

    public static void insertCidrsMappings(Map<String, List<CIDR>> cidrsMapping)
    {
        for (Map.Entry<String, List<CIDR>> cidrMapping : cidrsMapping.entrySet())
        {
            QueryProcessor.executeInternal(format("insert into %s.%s(cidr_group, cidrs) values('%s', %s );",
                                                  AUTH_KEYSPACE_NAME, CIDR_GROUPS, cidrMapping.getKey(),
                                                  AuthTestUtils.LocalCIDRGroupsMappingManager.getCidrTuplesSet(
                                                  cidrMapping.getValue())));
        }
        DatabaseDescriptor.getCIDRAuthorizer().loadCidrGroupsCache(); // update cache with CIDRs inserted above
    }

    // mTLS authenticators related utility methods
    public static InetAddress getMockInetAddress() throws UnknownHostException
    {
        return InetAddress.getByName("127.0.0.1");
    }

    public static Certificate[] loadCertificateChain(final String path) throws CertificateException
    {
        InputStream inputStream = MutualTlsAuthenticator.class.getClassLoader().getResourceAsStream(path);
        assertNotNull(inputStream);
        Collection<? extends Certificate> c = CertificateFactory.getInstance("X.509").generateCertificates(inputStream);
        X509Certificate[] certs = new X509Certificate[c.size()];
        for (int i = 0; i < certs.length; i++)
        {
            certs[i] = (X509Certificate) c.toArray()[i];
        }
        return certs;
    }

    public static void initializeIdentityRolesTable(final String identity) throws IOException, TimeoutException
    {
        StorageService.instance.truncate(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.IDENTITY_TO_ROLES);
        String insertQuery = "Insert into %s.%s (identity, role) values ('%s', 'readonly_user');";
        QueryProcessor.process(String.format(insertQuery, SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.IDENTITY_TO_ROLES, identity), ConsistencyLevel.ONE);
    }
}

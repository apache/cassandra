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

import java.util.concurrent.Callable;

import org.apache.cassandra.auth.jmx.AuthorizationProxy;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;


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
}

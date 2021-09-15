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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.service.QueryState.forInternalCalls;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class CassandraNetworkAuthorizer implements INetworkAuthorizer
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraNetworkAuthorizer.class);
    private SelectStatement authorizeUserStatement = null;

    public void setup()
    {
        String query = String.format("SELECT dcs FROM %s.%s WHERE role = ?",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.NETWORK_PERMISSIONS);
        authorizeUserStatement = (SelectStatement) QueryProcessor.getStatement(query, ClientState.forInternalCalls());
    }

    @VisibleForTesting
    ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
    {
        return statement.execute(forInternalCalls(), options, nanoTime());
    }

    /**
     * This is exposed so we can override the consistency level for tests that are single node
     */
    @VisibleForTesting
    UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        return QueryProcessor.process(query, cl);
    }

    private Set<String> getAuthorizedDcs(String name)
    {
        QueryOptions options = QueryOptions.forInternalCalls(CassandraAuthorizer.authReadConsistencyLevel(),
                                                             Lists.newArrayList(ByteBufferUtil.bytes(name)));

        ResultMessage.Rows rows = select(authorizeUserStatement, options);
        UntypedResultSet result = UntypedResultSet.create(rows.result);
        Set<String> dcs = null;
        if (!result.isEmpty() && result.one().has("dcs"))
        {
            dcs = result.one().getFrozenSet("dcs", UTF8Type.instance);
        }
        return dcs;
    }

    public DCPermissions authorize(RoleResource role)
    {
        if (!Roles.canLogin(role))
        {
            return DCPermissions.none();
        }
        if (Roles.hasSuperuserStatus(role))
        {
            return DCPermissions.all();
        }

        Set<String> dcs = getAuthorizedDcs(role.getName());

        if (dcs == null || dcs.isEmpty())
        {
            return DCPermissions.all();
        }
        else
        {
            return DCPermissions.subset(dcs);
        }
    }

    private static String getSetString(DCPermissions permissions)
    {
        if (permissions.restrictsAccess())
        {
            StringBuilder builder = new StringBuilder();
            builder.append('{');
            boolean first = true;
            for (String dc: permissions.allowedDCs())
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    builder.append(", ");
                }
                builder.append('\'');
                builder.append(dc);
                builder.append('\'');
            }
            builder.append('}');
            return builder.toString();
        }
        else
        {
            return "{}";
        }
    }

    public void setRoleDatacenters(RoleResource role, DCPermissions permissions)
    {
        String query = String.format("UPDATE %s.%s SET dcs = %s WHERE role = '%s'",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.NETWORK_PERMISSIONS,
                                     getSetString(permissions),
                                     role.getName());

        process(query, CassandraAuthorizer.authWriteConsistencyLevel());
    }

    public void drop(RoleResource role)
    {
        String query = String.format("DELETE FROM %s.%s WHERE role = '%s'",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.NETWORK_PERMISSIONS,
                                     role.getName());

        process(query, CassandraAuthorizer.authWriteConsistencyLevel());
    }

    public void validateConfiguration() throws ConfigurationException
    {
        // noop
    }

    @Override
    public Supplier<Map<RoleResource, DCPermissions>> bulkLoader()
    {
        return () -> {
            logger.info("Pre-warming datacenter permissions cache from network_permissions table");
            Map<RoleResource, DCPermissions> entries = new HashMap<>();
            UntypedResultSet rows = process(String.format("SELECT role, dcs FROM %s.%s",
                                                          SchemaConstants.AUTH_KEYSPACE_NAME,
                                                          AuthKeyspace.NETWORK_PERMISSIONS),
                                            CassandraAuthorizer.authReadConsistencyLevel());

            for (UntypedResultSet.Row row : rows)
            {
                RoleResource role = RoleResource.role(row.getString("role"));
                DCPermissions.Builder builder = new DCPermissions.Builder();
                Set<String> dcs = row.getFrozenSet("dcs", UTF8Type.instance);
                for (String dc : dcs)
                    builder.add(dc);
                entries.put(role, builder.build());
            }

            return entries;
        };
    }
}

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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.reads.range.RangeCommands;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.service.QueryState.forInternalCalls;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Provides functionality to list/update/drop CIDR permissions of a role
 * Backend to build Role to CIDR permissions cache
 */
public class CIDRPermissionsManager implements CIDRPermissionsManagerMBean, AuthCache.BulkLoader<RoleResource, CIDRPermissions>
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=CIDRPermissionsManager";

    private static final Logger logger = LoggerFactory.getLogger(CIDRPermissionsManager.class);
    private SelectStatement getCidrPermissionsOfUserStatement = null;

    public void setup()
    {
        if (!MBeanWrapper.instance.isRegistered(MBEAN_NAME))
            MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);

        String getCidrPermissionsOfUserQuery = String.format("SELECT %s FROM %s.%s WHERE %s = ?",
                                                             AuthKeyspace.CIDR_PERMISSIONS_TBL_CIDR_GROUPS_COL_NAME,
                                                             SchemaConstants.AUTH_KEYSPACE_NAME,
                                                             AuthKeyspace.CIDR_PERMISSIONS,
                                                             AuthKeyspace.CIDR_PERMISSIONS_TBL_ROLE_COL_NAME);
        getCidrPermissionsOfUserStatement = (SelectStatement) QueryProcessor.getStatement(getCidrPermissionsOfUserQuery,
                                                                                          ClientState.forInternalCalls());
    }

    @VisibleForTesting
    ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
    {
        return statement.execute(forInternalCalls(), options, nanoTime());
    }

    @VisibleForTesting
    UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        return QueryProcessor.process(query, cl);
    }

    private Set<String> getAuthorizedCIDRGroups(String name)
    {
        QueryOptions options = QueryOptions.forInternalCalls(CassandraAuthorizer.authReadConsistencyLevel(),
                                                             Lists.newArrayList(ByteBufferUtil.bytes(name)));

        ResultMessage.Rows rows = select(getCidrPermissionsOfUserStatement, options);
        UntypedResultSet result = UntypedResultSet.create(rows.result);
        if (!result.isEmpty() && result.one().has(AuthKeyspace.CIDR_PERMISSIONS_TBL_CIDR_GROUPS_COL_NAME))
        {
            return result.one().getFrozenSet(AuthKeyspace.CIDR_PERMISSIONS_TBL_CIDR_GROUPS_COL_NAME, UTF8Type.instance);
        }

        return Collections.emptySet();
    }

    private static String getCidrPermissionsSetString(CIDRPermissions permissions)
    {
        String inner = "";

        if (permissions.restrictsAccess())
            inner = permissions.allowedCIDRGroups().stream().map(s -> '\'' + s + '\'')
                               .collect(Collectors.joining(", "));

        return '{' + inner + '}';
    }

    /**
     * Get CIDR permissions of a role
     * @param role role for which to get CIDR permissions
     * @return returns CIDR permissions of a role
     */
    public CIDRPermissions getCidrPermissionsForRole(RoleResource role)
    {
        if (!Roles.canLogin(role))
        {
            return CIDRPermissions.none();
        }
        if (Roles.hasSuperuserStatus(role) && !DatabaseDescriptor.getCidrChecksForSuperusers())
        {
            return CIDRPermissions.all();
        }

        Set<String> cidrGroups = getAuthorizedCIDRGroups(role.getRoleName());
        // User don't have CIDR permissions explicitly enabled, i.e, allow from all
        if (cidrGroups == null || cidrGroups.isEmpty())
        {
            // No explicit CIDR groups set for the role, allow from all
            return CIDRPermissions.all();
        }

        return CIDRPermissions.subset(cidrGroups);
    }

    /**
     * Set CIDR permissions for a given role
     * @param role role for which to set CIDR permissions
     * @param cidrPermissions CIR permissions to set for the role
     */
    public void setCidrGroupsForRole(RoleResource role, CIDRPermissions cidrPermissions)
    {
        String query = String.format("UPDATE %s.%s SET %s = %s WHERE %s = '%s'",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.CIDR_PERMISSIONS,
                                     AuthKeyspace.CIDR_PERMISSIONS_TBL_CIDR_GROUPS_COL_NAME,
                                     getCidrPermissionsSetString(cidrPermissions),
                                     AuthKeyspace.CIDR_PERMISSIONS_TBL_ROLE_COL_NAME,
                                     role.getRoleName());

        process(query, CassandraAuthorizer.authWriteConsistencyLevel());
    }

    /**
     * Drop CIDR permissions of a role, i.e, delete corresponding row from the table
     * @param role for which to drop cidr permissions
     */
    public void drop(RoleResource role)
    {
        String query = String.format("DELETE FROM %s.%s WHERE role = '%s'",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.CIDR_PERMISSIONS,
                                     role.getRoleName());

        process(query, CassandraAuthorizer.authWriteConsistencyLevel());
    }

    /**
     * Function to bulk load role to cidr permissions cache
     * This gets called when cache warmup is enabled in the config
     * @return returns nothing
     */
    @Override
    public Supplier<Map<RoleResource, CIDRPermissions>> bulkLoader()
    {
        return () -> {
            if (!RangeCommands.sufficientLiveNodesForSelectStar(AuthKeyspace.metadata().tables.getNullable(AuthKeyspace.CIDR_PERMISSIONS),
                                                                AuthProperties.instance.getReadConsistencyLevel()))
            {
                // Prevent running the query we know will fail so as not to increment unavailable stats for a performance
                // optimization
                throw new RuntimeException("insufficient live nodes for " +
                                           AuthProperties.instance.getReadConsistencyLevel() +
                                           "pre-warm query again system_auth." + AuthKeyspace.CIDR_PERMISSIONS);
            }

            logger.info("Pre-warming CIDR permissions cache from cidr_permissions table");
            Map<RoleResource, CIDRPermissions> entries = new HashMap<>();
            UntypedResultSet rows = process(String.format("SELECT %s, %s FROM %s.%s",
                                                          AuthKeyspace.CIDR_PERMISSIONS_TBL_ROLE_COL_NAME,
                                                          AuthKeyspace.CIDR_PERMISSIONS_TBL_CIDR_GROUPS_COL_NAME,
                                                          SchemaConstants.AUTH_KEYSPACE_NAME,
                                                          AuthKeyspace.CIDR_PERMISSIONS),
                                            CassandraAuthorizer.authReadConsistencyLevel());

            for (UntypedResultSet.Row row : rows)
            {
                RoleResource role = RoleResource.role(row.getString(AuthKeyspace.CIDR_PERMISSIONS_TBL_ROLE_COL_NAME));
                CIDRPermissions.Builder builder = new CIDRPermissions.Builder();
                Set<String> cidrGroups = row.getFrozenSet(AuthKeyspace.CIDR_PERMISSIONS_TBL_CIDR_GROUPS_COL_NAME,
                                                          UTF8Type.instance);
                for (String cidrGroup : cidrGroups)
                    builder.add(cidrGroup);
                entries.put(role, builder.build());
            }

            return entries;
        };
    }

    public boolean invalidateCidrPermissionsCache(String roleName)
    {
        return DatabaseDescriptor.getCIDRAuthorizer().invalidateCidrPermissionsCache(roleName);
    }
}

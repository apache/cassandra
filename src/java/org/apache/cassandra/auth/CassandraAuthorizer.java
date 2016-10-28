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

import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.serializers.SetSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.service.ClientState;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * CassandraAuthorizer is an IAuthorizer implementation that keeps
 * user permissions internally in C* using the system_auth.role_permissions
 * table.
 */
public class CassandraAuthorizer implements IAuthorizer
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraAuthorizer.class);

    private static final String ROLE = "role";
    private static final String RESOURCE = "resource";
    private static final String PERMISSIONS = "permissions";

    // used during upgrades to perform authz on mixed clusters
    public static final String USERNAME = "username";
    public static final String USER_PERMISSIONS = "permissions";

    private SelectStatement authorizeRoleStatement;
    private SelectStatement legacyAuthorizeRoleStatement;

    public CassandraAuthorizer()
    {
    }

    // Returns every permission on the resource granted to the user either directly
    // or indirectly via roles granted to the user.
    public Set<Permission> authorize(AuthenticatedUser user, IResource resource)
    {
        if (user.isSuper())
            return resource.applicablePermissions();

        Set<Permission> permissions = EnumSet.noneOf(Permission.class);
        try
        {
            for (RoleResource role: user.getRoles())
                addPermissionsForRole(permissions, resource, role);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            logger.warn("CassandraAuthorizer failed to authorize {} for {}", user, resource);
            throw new RuntimeException(e);
        }

        return permissions;
    }

    public void grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource grantee)
    throws RequestValidationException, RequestExecutionException
    {
        modifyRolePermissions(permissions, resource, grantee, "+");
        addLookupEntry(resource, grantee);
    }

    public void revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource revokee)
    throws RequestValidationException, RequestExecutionException
    {
        modifyRolePermissions(permissions, resource, revokee, "-");
        removeLookupEntry(resource, revokee);
    }

    // Called when deleting a role with DROP ROLE query.
    // Internal hook, so no permission checks are needed here.
    // Executes a logged batch removing the granted premissions
    // for the role as well as the entries from the reverse index
    // table
    public void revokeAllFrom(RoleResource revokee)
    {
        try
        {
            UntypedResultSet rows = process(String.format("SELECT resource FROM %s.%s WHERE role = '%s'",
                                                          SchemaConstants.AUTH_KEYSPACE_NAME,
                                                          AuthKeyspace.ROLE_PERMISSIONS,
                                                          escape(revokee.getRoleName())));

            List<CQLStatement> statements = new ArrayList<>();
            for (UntypedResultSet.Row row : rows)
            {
                statements.add(
                    QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE resource = '%s' AND role = '%s'",
                                                              SchemaConstants.AUTH_KEYSPACE_NAME,
                                                              AuthKeyspace.RESOURCE_ROLE_INDEX,
                                                              escape(row.getString("resource")),
                                                              escape(revokee.getRoleName())),
                                                ClientState.forInternalCalls()).statement);

            }

            statements.add(QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE role = '%s'",
                                                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                     AuthKeyspace.ROLE_PERMISSIONS,
                                                                     escape(revokee.getRoleName())),
                                                       ClientState.forInternalCalls()).statement);

            executeLoggedBatch(statements);
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            logger.warn("CassandraAuthorizer failed to revoke all permissions of {}: {}",  revokee.getRoleName(), e);
        }
    }

    // Called after a resource is removed (DROP KEYSPACE, DROP TABLE, etc.).
    // Execute a logged batch removing all the permissions for the resource
    // as well as the index table entry
    public void revokeAllOn(IResource droppedResource)
    {
        try
        {
            UntypedResultSet rows = process(String.format("SELECT role FROM %s.%s WHERE resource = '%s'",
                                                          SchemaConstants.AUTH_KEYSPACE_NAME,
                                                          AuthKeyspace.RESOURCE_ROLE_INDEX,
                                                          escape(droppedResource.getName())));

            List<CQLStatement> statements = new ArrayList<>();
            for (UntypedResultSet.Row row : rows)
            {
                statements.add(QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE role = '%s' AND resource = '%s'",
                                                                         SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                         AuthKeyspace.ROLE_PERMISSIONS,
                                                                         escape(row.getString("role")),
                                                                         escape(droppedResource.getName())),
                                                           ClientState.forInternalCalls()).statement);
            }

            statements.add(QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE resource = '%s'",
                                                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                     AuthKeyspace.RESOURCE_ROLE_INDEX,
                                                                     escape(droppedResource.getName())),
                                                                               ClientState.forInternalCalls()).statement);

            executeLoggedBatch(statements);
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            logger.warn("CassandraAuthorizer failed to revoke all permissions on {}: {}", droppedResource, e);
            return;
        }
    }

    private void executeLoggedBatch(List<CQLStatement> statements)
    throws RequestExecutionException, RequestValidationException
    {
        BatchStatement batch = new BatchStatement(0,
                                                  BatchStatement.Type.LOGGED,
                                                  Lists.newArrayList(Iterables.filter(statements, ModificationStatement.class)),
                                                  Attributes.none());
        QueryProcessor.instance.processBatch(batch,
                                             QueryState.forInternalCalls(),
                                             BatchQueryOptions.withoutPerStatementVariables(QueryOptions.DEFAULT),
                                             System.nanoTime());

    }

    // Add every permission on the resource granted to the role
    private void addPermissionsForRole(Set<Permission> permissions, IResource resource, RoleResource role)
    throws RequestExecutionException, RequestValidationException
    {
        QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE,
                                                             Lists.newArrayList(ByteBufferUtil.bytes(role.getRoleName()),
                                                                                ByteBufferUtil.bytes(resource.getName())));

        SelectStatement statement;
        // If it exists, read from the legacy user permissions table to handle the case where the cluster
        // is being upgraded and so is running with mixed versions of the authz schema
        if (Schema.instance.getCFMetaData(SchemaConstants.AUTH_KEYSPACE_NAME, USER_PERMISSIONS) == null)
            statement = authorizeRoleStatement;
        else
        {
            // If the permissions table was initialised only after the statement got prepared, re-prepare (CASSANDRA-12813)
            if (legacyAuthorizeRoleStatement == null)
                legacyAuthorizeRoleStatement = prepare(USERNAME, USER_PERMISSIONS);
            statement = legacyAuthorizeRoleStatement;
        }
        ResultMessage.Rows rows = statement.execute(QueryState.forInternalCalls(), options, System.nanoTime());
        UntypedResultSet result = UntypedResultSet.create(rows.result);

        if (!result.isEmpty() && result.one().has(PERMISSIONS))
        {
            for (String perm : result.one().getSet(PERMISSIONS, UTF8Type.instance))
            {
                permissions.add(Permission.valueOf(perm));
            }
        }
    }

    // Adds or removes permissions from a role_permissions table (adds if op is "+", removes if op is "-")
    private void modifyRolePermissions(Set<Permission> permissions, IResource resource, RoleResource role, String op)
            throws RequestExecutionException
    {
        process(String.format("UPDATE %s.%s SET permissions = permissions %s {%s} WHERE role = '%s' AND resource = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.ROLE_PERMISSIONS,
                              op,
                              "'" + StringUtils.join(permissions, "','") + "'",
                              escape(role.getRoleName()),
                              escape(resource.getName())));
    }

    // Removes an entry from the inverted index table (from resource -> role with defined permissions)
    private void removeLookupEntry(IResource resource, RoleResource role) throws RequestExecutionException
    {
        process(String.format("DELETE FROM %s.%s WHERE resource = '%s' and role = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.RESOURCE_ROLE_INDEX,
                              escape(resource.getName()),
                              escape(role.getRoleName())));
    }

    // Adds an entry to the inverted index table (from resource -> role with defined permissions)
    private void addLookupEntry(IResource resource, RoleResource role) throws RequestExecutionException
    {
        process(String.format("INSERT INTO %s.%s (resource, role) VALUES ('%s','%s')",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.RESOURCE_ROLE_INDEX,
                              escape(resource.getName()),
                              escape(role.getRoleName())));
    }

    // 'of' can be null - in that case everyone's permissions have been requested. Otherwise only single user's.
    // If the user requesting 'LIST PERMISSIONS' is not a superuser OR their username doesn't match 'of', we
    // throw UnauthorizedException. So only a superuser can view everybody's permissions. Regular users are only
    // allowed to see their own permissions.
    public Set<PermissionDetails> list(AuthenticatedUser performer,
                                       Set<Permission> permissions,
                                       IResource resource,
                                       RoleResource grantee)
    throws RequestValidationException, RequestExecutionException
    {
        if (!(performer.isSuper() || performer.isSystem()) && !performer.getRoles().contains(grantee))
            throw new UnauthorizedException(String.format("You are not authorized to view %s's permissions",
                                                          grantee == null ? "everyone" : grantee.getRoleName()));

        if (null == grantee)
            return listPermissionsForRole(permissions, resource, grantee);

        Set<RoleResource> roles = DatabaseDescriptor.getRoleManager().getRoles(grantee, true);
        Set<PermissionDetails> details = new HashSet<>();
        for (RoleResource role : roles)
            details.addAll(listPermissionsForRole(permissions, resource, role));

        return details;
    }

    private Set<PermissionDetails> listPermissionsForRole(Set<Permission> permissions,
                                                          IResource resource,
                                                          RoleResource role)
    throws RequestExecutionException
    {
        Set<PermissionDetails> details = new HashSet<>();
        // If it exists, try the legacy user permissions table first. This is to handle the case
        // where the cluster is being upgraded and so is running with mixed versions of the perms table
        boolean useLegacyTable = Schema.instance.getCFMetaData(SchemaConstants.AUTH_KEYSPACE_NAME, USER_PERMISSIONS) != null;
        String entityColumnName = useLegacyTable ? USERNAME : ROLE;
        for (UntypedResultSet.Row row : process(buildListQuery(resource, role, useLegacyTable)))
        {
            if (row.has(PERMISSIONS))
            {
                for (String p : row.getSet(PERMISSIONS, UTF8Type.instance))
                {
                    Permission permission = Permission.valueOf(p);
                    if (permissions.contains(permission))
                        details.add(new PermissionDetails(row.getString(entityColumnName),
                                                          Resources.fromName(row.getString(RESOURCE)),
                                                          permission));
                }
            }
        }
        return details;
    }

    private String buildListQuery(IResource resource, RoleResource grantee, boolean useLegacyTable)
    {
        String tableName = useLegacyTable ? USER_PERMISSIONS : AuthKeyspace.ROLE_PERMISSIONS;
        String entityName = useLegacyTable ? USERNAME : ROLE;
        List<String> vars = Lists.newArrayList(SchemaConstants.AUTH_KEYSPACE_NAME, tableName);
        List<String> conditions = new ArrayList<>();

        if (resource != null)
        {
            conditions.add("resource = '%s'");
            vars.add(escape(resource.getName()));
        }

        if (grantee != null)
        {
            conditions.add(entityName + " = '%s'");
            vars.add(escape(grantee.getRoleName()));
        }

        String query = "SELECT " + entityName + ", resource, permissions FROM %s.%s";

        if (!conditions.isEmpty())
            query += " WHERE " + StringUtils.join(conditions, " AND ");

        if (resource != null && grantee == null)
            query += " ALLOW FILTERING";

        return String.format(query, vars.toArray());
    }


    public Set<DataResource> protectedResources()
    {
        return ImmutableSet.of(DataResource.table(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS));
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
        authorizeRoleStatement = prepare(ROLE, AuthKeyspace.ROLE_PERMISSIONS);

        // If old user permissions table exists, migrate the legacy authz data to the new table
        // The delay is to give the node a chance to see its peers before attempting the conversion
        if (Schema.instance.getCFMetaData(SchemaConstants.AUTH_KEYSPACE_NAME, "permissions") != null)
        {
            legacyAuthorizeRoleStatement = prepare(USERNAME, USER_PERMISSIONS);

            ScheduledExecutors.optionalTasks.schedule(new Runnable()
            {
                public void run()
                {
                    convertLegacyData();
                }
            }, AuthKeyspace.SUPERUSER_SETUP_DELAY, TimeUnit.MILLISECONDS);
        }
    }

    private SelectStatement prepare(String entityname, String permissionsTable)
    {
        String query = String.format("SELECT permissions FROM %s.%s WHERE %s = ? AND resource = ?",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     permissionsTable,
                                     entityname);
        return (SelectStatement) QueryProcessor.getStatement(query, ClientState.forInternalCalls()).statement;
    }

    /**
     * Copy legacy authz data from the system_auth.permissions table to the new system_auth.role_permissions table and
     * also insert entries into the reverse lookup table.
     * In theory, we could simply rename the existing table as the schema is structurally the same, but this would
     * break mixed clusters during a rolling upgrade.
     * This setup is not performed if AllowAllAuthenticator is configured (see Auth#setup).
     */
    private void convertLegacyData()
    {
        try
        {
            if (Schema.instance.getCFMetaData("system_auth", "permissions") != null)
            {
                logger.info("Converting legacy permissions data");
                CQLStatement insertStatement =
                    QueryProcessor.getStatement(String.format("INSERT INTO %s.%s (role, resource, permissions) " +
                                                              "VALUES (?, ?, ?)",
                                                              SchemaConstants.AUTH_KEYSPACE_NAME,
                                                              AuthKeyspace.ROLE_PERMISSIONS),
                                                ClientState.forInternalCalls()).statement;
                CQLStatement indexStatement =
                    QueryProcessor.getStatement(String.format("INSERT INTO %s.%s (resource, role) VALUES (?,?)",
                                                              SchemaConstants.AUTH_KEYSPACE_NAME,
                                                              AuthKeyspace.RESOURCE_ROLE_INDEX),
                                                ClientState.forInternalCalls()).statement;

                UntypedResultSet permissions = process("SELECT * FROM system_auth.permissions");
                for (UntypedResultSet.Row row : permissions)
                {
                    final IResource resource = Resources.fromName(row.getString("resource"));
                    Predicate<String> isApplicable = new Predicate<String>()
                    {
                        public boolean apply(String s)
                        {
                            return resource.applicablePermissions().contains(Permission.valueOf(s));
                        }
                    };
                    SetSerializer<String> serializer = SetSerializer.getInstance(UTF8Serializer.instance, UTF8Type.instance);
                    Set<String> originalPerms = serializer.deserialize(row.getBytes("permissions"));
                    Set<String> filteredPerms = ImmutableSet.copyOf(Iterables.filter(originalPerms, isApplicable));
                    insertStatement.execute(QueryState.forInternalCalls(),
                                            QueryOptions.forInternalCalls(ConsistencyLevel.ONE,
                                                                          Lists.newArrayList(row.getBytes("username"),
                                                                                             row.getBytes("resource"),
                                                                                             serializer.serialize(filteredPerms))),
                                            System.nanoTime());

                    indexStatement.execute(QueryState.forInternalCalls(),
                                           QueryOptions.forInternalCalls(ConsistencyLevel.ONE,
                                                                         Lists.newArrayList(row.getBytes("resource"),
                                                                                            row.getBytes("username"))),
                                           System.nanoTime());

                }
                logger.info("Completed conversion of legacy permissions");
            }
        }
        catch (Exception e)
        {
            logger.info("Unable to complete conversion of legacy permissions data (perhaps not enough nodes are upgraded yet). " +
                        "Conversion should not be considered complete");
            logger.trace("Conversion error", e);
        }
    }

    // We only worry about one character ('). Make sure it's properly escaped.
    private String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    private UntypedResultSet process(String query) throws RequestExecutionException
    {
        return QueryProcessor.process(query, ConsistencyLevel.LOCAL_ONE);
    }
}

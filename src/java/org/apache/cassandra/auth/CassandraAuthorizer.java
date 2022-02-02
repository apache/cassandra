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
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

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

    private SelectStatement authorizeRoleStatement;

    public CassandraAuthorizer()
    {
    }

    // Returns every permission on the resource granted to the user either directly
    // or indirectly via roles granted to the user.
    public Set<Permission> authorize(AuthenticatedUser user, IResource resource)
    {
        try
        {
            if (user.isSuper())
                return resource.applicablePermissions();

            Set<Permission> permissions = EnumSet.noneOf(Permission.class);

            // Even though we only care about the RoleResource here, we use getRoleDetails as
            // it saves a Set creation in RolesCache
            for (Role role: user.getRoleDetails())
                addPermissionsForRole(permissions, resource, role.resource);
            return permissions;
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            logger.debug("Failed to authorize {} for {}", user, resource);
            throw new UnauthorizedException("Unable to perform authorization of permissions: " + e.getMessage(), e);
        }
    }

    public Set<Permission> grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource grantee)
    throws RequestValidationException, RequestExecutionException
    {
        String roleName = escape(grantee.getRoleName());
        String resourceName = escape(resource.getName());
        Set<Permission> existingPermissions = getExistingPermissions(roleName, resourceName, permissions);
        Set<Permission> nonExistingPermissions = Sets.difference(permissions, existingPermissions);

        if (!nonExistingPermissions.isEmpty())
        {
            modifyRolePermissions(nonExistingPermissions, resource, grantee, "+");
            addLookupEntry(resource, grantee);
        }

        return nonExistingPermissions;
    }

    public Set<Permission> revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource revokee)
    throws RequestValidationException, RequestExecutionException
    {
        String roleName = escape(revokee.getRoleName());
        String resourceName = escape(resource.getName());
        Set<Permission> existingPermissions = getExistingPermissions(roleName, resourceName, permissions);

        if (!existingPermissions.isEmpty())
        {
            modifyRolePermissions(existingPermissions, resource, revokee, "-");
            removeLookupEntry(resource, revokee);
        }

        return existingPermissions;
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
                                                          escape(revokee.getRoleName())),
                                            authReadConsistencyLevel());

            List<CQLStatement> statements = new ArrayList<>();
            for (UntypedResultSet.Row row : rows)
            {
                statements.add(
                    QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE resource = '%s' AND role = '%s'",
                                                              SchemaConstants.AUTH_KEYSPACE_NAME,
                                                              AuthKeyspace.RESOURCE_ROLE_INDEX,
                                                              escape(row.getString("resource")),
                                                              escape(revokee.getRoleName())),
                                                ClientState.forInternalCalls()));

            }

            statements.add(QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE role = '%s'",
                                                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                     AuthKeyspace.ROLE_PERMISSIONS,
                                                                     escape(revokee.getRoleName())),
                                                       ClientState.forInternalCalls()));

            executeLoggedBatch(statements);
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            logger.warn(String.format("CassandraAuthorizer failed to revoke all permissions of %s", revokee.getRoleName()), e);
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
                                                          escape(droppedResource.getName())),
                                            authReadConsistencyLevel());

            List<CQLStatement> statements = new ArrayList<>();
            for (UntypedResultSet.Row row : rows)
            {
                statements.add(QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE role = '%s' AND resource = '%s'",
                                                                         SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                         AuthKeyspace.ROLE_PERMISSIONS,
                                                                         escape(row.getString("role")),
                                                                         escape(droppedResource.getName())),
                                                           ClientState.forInternalCalls()));
            }

            statements.add(QueryProcessor.getStatement(String.format("DELETE FROM %s.%s WHERE resource = '%s'",
                                                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                     AuthKeyspace.RESOURCE_ROLE_INDEX,
                                                                     escape(droppedResource.getName())),
                                                      ClientState.forInternalCalls()));

            executeLoggedBatch(statements);
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            logger.warn(String.format("CassandraAuthorizer failed to revoke all permissions on %s", droppedResource), e);
        }
    }

    /**
     * Checks that the specified role has at least one of the expected permissions on the resource.
     *
     * @param roleName the role name
     * @param resourceName the resource name
     * @param expectedPermissions the permissions to check for
     * @return The existing permissions
     */
    private Set<Permission> getExistingPermissions(String roleName,
                                                   String resourceName,
                                                   Set<Permission> expectedPermissions)
    {
        UntypedResultSet rs = process(String.format("SELECT permissions FROM %s.%s WHERE role = '%s' AND resource = '%s'",
                                                    SchemaConstants.AUTH_KEYSPACE_NAME,
                                                    AuthKeyspace.ROLE_PERMISSIONS,
                                                    roleName,
                                                    resourceName),
                                      ConsistencyLevel.LOCAL_ONE);

        if (rs.isEmpty())
            return Collections.emptySet();

        Row one = rs.one();

        Set<Permission> existingPermissions = Sets.newHashSetWithExpectedSize(expectedPermissions.size());
        for (String permissionName : one.getSet("permissions", UTF8Type.instance))
        {
            Permission permission = Permission.valueOf(permissionName);
            if (expectedPermissions.contains(permission))
                existingPermissions.add(permission);
        }
        return existingPermissions;
    }

    private void executeLoggedBatch(List<CQLStatement> statements)
    throws RequestExecutionException, RequestValidationException
    {
        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED,
                                                  VariableSpecifications.empty(),
                                                  Lists.newArrayList(Iterables.filter(statements, ModificationStatement.class)),
                                                  Attributes.none());
        processBatch(batch);
    }

    // Add every permission on the resource granted to the role
    private void addPermissionsForRole(Set<Permission> permissions, IResource resource, RoleResource role)
    throws RequestExecutionException, RequestValidationException
    {
        QueryOptions options = QueryOptions.forInternalCalls(authReadConsistencyLevel(),
                                                             Lists.newArrayList(ByteBufferUtil.bytes(role.getRoleName()),
                                                                                ByteBufferUtil.bytes(resource.getName())));

        ResultMessage.Rows rows = select(authorizeRoleStatement, options);

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
                              escape(resource.getName())),
                authWriteConsistencyLevel());
    }

    // Removes an entry from the inverted index table (from resource -> role with defined permissions)
    private void removeLookupEntry(IResource resource, RoleResource role) throws RequestExecutionException
    {
        process(String.format("DELETE FROM %s.%s WHERE resource = '%s' and role = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.RESOURCE_ROLE_INDEX,
                              escape(resource.getName()),
                              escape(role.getRoleName())),
                authWriteConsistencyLevel());
    }

    // Adds an entry to the inverted index table (from resource -> role with defined permissions)
    private void addLookupEntry(IResource resource, RoleResource role) throws RequestExecutionException
    {
        process(String.format("INSERT INTO %s.%s (resource, role) VALUES ('%s','%s')",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.RESOURCE_ROLE_INDEX,
                              escape(resource.getName()),
                              escape(role.getRoleName())),
                authWriteConsistencyLevel());
    }

    // 'grantee' can be null - in that case everyone's permissions have been requested. Otherwise, only single user's.
    // If the 'performer' requesting 'LIST PERMISSIONS' is not a superuser OR their username doesn't match 'grantee' OR
    // they have no permission to describe all roles OR they have no permission to describe 'grantee', then we throw
    // UnauthorizedException.
    public Set<PermissionDetails> list(AuthenticatedUser performer,
                                       Set<Permission> permissions,
                                       IResource resource,
                                       RoleResource grantee)
    throws RequestValidationException, RequestExecutionException
    {
        if (!performer.isSuper()
            && !performer.isSystem()
            && !performer.getRoles().contains(grantee)
            && !performer.getPermissions(RoleResource.root()).contains(Permission.DESCRIBE)
            && (grantee == null || !performer.getPermissions(grantee).contains(Permission.DESCRIBE)))
            throw new UnauthorizedException(String.format("You are not authorized to view %s's permissions",
                                                          grantee == null ? "everyone" : grantee.getRoleName()));

        if (null == grantee)
            return listPermissionsForRole(permissions, resource, null);

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
        for (UntypedResultSet.Row row : process(buildListQuery(resource, role), authReadConsistencyLevel()))
        {
            if (row.has(PERMISSIONS))
            {
                for (String p : row.getSet(PERMISSIONS, UTF8Type.instance))
                {
                    Permission permission = Permission.valueOf(p);
                    if (permissions.contains(permission))
                        details.add(new PermissionDetails(row.getString(ROLE),
                                                          Resources.fromName(row.getString(RESOURCE)),
                                                          permission));
                }
            }
        }
        return details;
    }

    private String buildListQuery(IResource resource, RoleResource grantee)
    {
        List<String> vars = Lists.newArrayList(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS);
        List<String> conditions = new ArrayList<>();

        if (resource != null)
        {
            conditions.add("resource = '%s'");
            vars.add(escape(resource.getName()));
        }

        if (grantee != null)
        {
            conditions.add(ROLE + " = '%s'");
            vars.add(escape(grantee.getRoleName()));
        }

        String query = "SELECT " + ROLE + ", resource, permissions FROM %s.%s";

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
    }

    private SelectStatement prepare(String entityname, String permissionsTable)
    {
        String query = String.format("SELECT permissions FROM %s.%s WHERE %s = ? AND resource = ?",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     permissionsTable,
                                     entityname);
        return (SelectStatement) QueryProcessor.getStatement(query, ClientState.forInternalCalls());
    }

    // We only worry about one character ('). Make sure it's properly escaped.
    private String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
    {
        return statement.execute(QueryState.forInternalCalls(), options, nanoTime());
    }

    /**
     * This is exposed so we can override the consistency level for tests that are single node
     */
    @VisibleForTesting
    UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        return QueryProcessor.process(query, cl);
    }

    void processBatch(BatchStatement statement)
    {
        QueryOptions options = QueryOptions.forInternalCalls(authWriteConsistencyLevel(), Collections.emptyList());
        QueryProcessor.instance.processBatch(statement,
                                             QueryState.forInternalCalls(),
                                             BatchQueryOptions.withoutPerStatementVariables(options),
                                             nanoTime());
    }

    public static ConsistencyLevel authWriteConsistencyLevel()
    {
        return AuthProperties.instance.getWriteConsistencyLevel();
    }

    public static ConsistencyLevel authReadConsistencyLevel()
    {
        return AuthProperties.instance.getReadConsistencyLevel();
    }

    /**
     * Get an initial set of permissions to load into the PermissionsCache at startup
     * @return map of User/Resource -> Permissions for cache initialisation
     */
    public Supplier<Map<Pair<AuthenticatedUser, IResource>, Set<Permission>>> bulkLoader()
    {
        return () ->
        {
            Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> entries = new HashMap<>();
            String cqlTemplate = "SELECT %s, %s, %s FROM %s.%s";

            logger.info("Warming permissions cache from role_permissions table");
            UntypedResultSet results = process(String.format(cqlTemplate,
                                                             ROLE, RESOURCE, PERMISSIONS,
                                                             SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS),
                                                             AuthProperties.instance.getReadConsistencyLevel());

            // role_name -> (resource, permissions)
            Table<String, IResource, Set<Permission>> individualRolePermissions = HashBasedTable.create();
            results.forEach(row -> {
                if (row.has(PERMISSIONS))
                {
                    individualRolePermissions.put(row.getString(ROLE),
                                                  Resources.fromName(row.getString(RESOURCE)),
                                                  permissions(row.getSet(PERMISSIONS, UTF8Type.instance)));
                }
            });

            // Iterate all user level roles in the system and accumulate the permissions of their granted roles
            Roles.getAllRoles().forEach(roleResource -> {
                // If the role has login priv, accumulate the permissions of all its granted roles
                if (Roles.canLogin(roleResource))
                {
                    // Structure to accumulate the resource -> permission mappings for the closure of granted roles
                    Map<IResource, ImmutableSet.Builder<Permission>> userPermissions = new HashMap<>();
                    BiConsumer<IResource, Set<Permission>> accumulator = accumulator(userPermissions);

                    // For each role granted to this primary, lookup the specific resource/permissions grants
                    // we read in the first step. We'll accumlate those in the userPermissions map, which we'll turn
                    // into cache entries when we're done.
                    // Note: we need to provide a default empty set of permissions for roles without any explicitly
                    // granted to them (e.g. superusers or roles with no direct perms).
                    Roles.getRoleDetails(roleResource).forEach(grantedRole ->
                                                               individualRolePermissions.rowMap()
                                                                                        .getOrDefault(grantedRole.resource.getRoleName(), Collections.emptyMap())
                                                                                        .forEach(accumulator));

                    // Having iterated all the roles granted to this user, finalize the transitive permissions
                    // (i.e. turn them into entries for the PermissionsCache)
                    userPermissions.forEach((resource, builder) -> entries.put(cacheKey(roleResource, resource),
                                                                               builder.build()));
                }
            });

            return entries;
        };
    }

    // Helper function to group the transitive set of permissions granted
    // to user by the specific resources to which they apply
    private static BiConsumer<IResource, Set<Permission>> accumulator(Map<IResource, ImmutableSet.Builder<Permission>> accumulator)
    {
        return (resource, permissions) -> accumulator.computeIfAbsent(resource, k -> new ImmutableSet.Builder<>()).addAll(permissions);
    }

    private static Set<Permission> permissions(Set<String> permissionNames)
    {
        return permissionNames.stream().map(Permission::valueOf).collect(Collectors.toSet());
    }

    private static Pair<AuthenticatedUser, IResource> cacheKey(RoleResource role, IResource resource)
    {
        return cacheKey(role.getRoleName(), resource);
    }

    private static Pair<AuthenticatedUser, IResource> cacheKey(String roleName, IResource resource)
    {
        return Pair.create(new AuthenticatedUser(roleName), resource);
    }
}

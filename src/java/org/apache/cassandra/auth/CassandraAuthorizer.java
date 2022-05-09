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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.ResultMessage.Rows;
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
    private static final String GRANTABLES = "grantables";
    private static final String RESTRICTED = "restricted";

    private static final String ROLE_PERMISSIONS_TABLE = SchemaConstants.AUTH_KEYSPACE_NAME + "." + AuthKeyspace.ROLE_PERMISSIONS;

    private SelectStatement authorizeRoleStatement;

    public CassandraAuthorizer()
    {
    }

    @Override
    public PermissionSets allPermissionSets(AuthenticatedUser user, IResource resource)
    {
        if (user.isSuper())
            // superuser can do everything
            return PermissionSets.builder()
                                 .addGranted(resource.applicablePermissions())
                                 .addGrantables(resource.applicablePermissions())
                                 .build();

        try
        {
            return getPermissionsFor(user, resource);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            logger.warn("CassandraAuthorizer failed to authorize {} for {}", user, resource);
            throw e;
        }
    }

    public Set<Permission> grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource grantee, GrantMode grantMode)
    throws RequestValidationException, RequestExecutionException
    {
        String roleName = escape(grantee.getRoleName());
        String resourceName = escape(resource.getName());
        Set<Permission> existingPermissions = getExistingPermissions(roleName, resourceName, permissions);
        Set<Permission> nonExistingPermissions = Sets.difference(permissions, existingPermissions);

        if (!nonExistingPermissions.isEmpty())
        {
            grantRevoke(nonExistingPermissions, resource, grantee, grantMode, "+");
            addLookupEntry(resource, grantee);
        }

        return nonExistingPermissions;
    }

    public Set<Permission> revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource revokee, GrantMode grantMode)
    throws RequestValidationException, RequestExecutionException
    {
        String roleName = escape(revokee.getRoleName());
        String resourceName = escape(resource.getName());
        Set<Permission> existingPermissions = getExistingPermissions(roleName, resourceName, permissions);

        if (!existingPermissions.isEmpty())
        {
            grantRevoke(existingPermissions, resource, revokee, grantMode, "-");
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
     * Returns the permissions for the specified user and resource
     * @param user the user
     * @param resource the resource
     * @return the permissions for the specified user and resource
     */
    private PermissionSets getPermissionsFor(AuthenticatedUser user, IResource resource)
    {
        PermissionSets.Builder permissions = PermissionSets.builder();
        for (UntypedResultSet.Row row : fetchPermissions(user, resource))
            addPermissionsFromRow(row, permissions);

        return permissions.build();
    }

    /**
     * Fetch the permissions of the user for the specified resources from the {@code role_permissions} table.
     * @param user the user
     * @param resource the resource
     * @return the permissions of the user for the resources
     */
    private UntypedResultSet fetchPermissions(AuthenticatedUser user, IResource resource)
    {
        // Query looks like this:
        // SELECT permissions, restricted, grantables
        // FROM system_auth.role-permissions
        // WHERE resource = ? AND role IN ?
        // Purpose is to fetch permissions for all role with one query and not one query per role.

        ByteBuffer resourceName = UTF8Serializer.instance.serialize(resource.getName());
        ByteBuffer roleNames = ListSerializer.getInstance(UTF8Serializer.instance).serialize(user.getRoleNames());

        QueryOptions options = QueryOptions.forInternalCalls(authReadConsistencyLevel(),
                                                             Lists.newArrayList(resourceName, roleNames));
        Rows rows = authorizeRoleStatement.execute(QueryState.forInternalCalls(), options, nanoTime());
        return UntypedResultSet.create(rows.result);
    }

    private static void addPermissionsFromRow(UntypedResultSet.Row row, PermissionSets.Builder perms)
    {
        permissionsFromRow(row, PERMISSIONS, perms::addGranted);
        permissionsFromRow(row, RESTRICTED, perms::addRestricted);
        permissionsFromRow(row, GRANTABLES, perms::addGrantable);
    }

    private static void permissionsFromRow(UntypedResultSet.Row row, String column, Consumer<Permission> perms)
    {
        if (!row.has(column))
            return;

        row.getSet(column, UTF8Type.instance)
           .stream()
           .map(Permission::valueOf)
           .forEach(perms);
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
                                      authReadConsistencyLevel());

        if (rs.isEmpty())
            return Collections.emptySet();
        Row one = rs.one();
        Set<Permission> existingPermissions = Sets.newHashSetWithExpectedSize(expectedPermissions.size());
        Set<String> row = one.getSet("permissions", UTF8Type.instance);
        if (row == null)
            return Collections.emptySet();

        for (String permissionName : row)
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

    // Adds or removes permissions from a role_permissions table (adds if op is "+", removes if op is "-")
    private void grantRevoke(Set<Permission> permissions,
                             IResource resource,
                             RoleResource role,
                             GrantMode grantMode,
                             String op)
    {
        // Construct a CQL command like the following. The updated columns are variable (depend on grantMode).
        //
        // UPDATE system_auth.role_permissions
        // SET permissions = permissions + {<permissions>}
        // WHERE role = <role-name>
        // AND resource = <resource-name>
        //

        String column = columnForGrantMode(grantMode);
        process(String.format("UPDATE " + ROLE_PERMISSIONS_TABLE
                              + " SET %s = %s %s { %s } WHERE role = '%s' AND resource = '%s'",
                              column,
                              column,
                              op,
                              "'" + StringUtils.join(permissions, "','") + "'",
                              escape(role.getRoleName()),
                              escape(resource.getName())),
                authWriteConsistencyLevel());
    }

    private static String columnForGrantMode(GrantMode grantMode)
    {
        switch (grantMode)
        {
            case GRANT:
                return PERMISSIONS;
            case RESTRICT:
                return RESTRICTED;
            case GRANTABLE:
                return GRANTABLES;
            default:
                throw new AssertionError(); // make compiler happy
        }
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

    public Set<PermissionDetails> list(Set<Permission> permissions, IResource resource, RoleResource grantee)
    {
        // 'grantee' can be null - in that case everyone's permissions have been requested. Otherwise only single
        // user's.
        Set<RoleResource> roles = grantee != null ? DatabaseDescriptor.getRoleManager().getRoles(grantee, true)
                                                  : Collections.emptySet();

        Set<PermissionDetails> details = new HashSet<>();
        // If it exists, try the legacy user permissions table first. This is to handle the case
        // where the cluster is being upgraded and so is running with mixed versions of the perms table
        for (UntypedResultSet.Row row : process(buildListQuery(resource, roles), authReadConsistencyLevel()))
        {
            PermissionSets.Builder permsBuilder = PermissionSets.builder();
            addPermissionsFromRow(row, permsBuilder);
            PermissionSets perms = permsBuilder.build();

            String rowRole = row.getString(ROLE);
            IResource rowResource = Resources.fromName(row.getString(RESOURCE));

            for (Permission p : perms.allContainedPermissions())
            {
                if (permissions.contains(p))
                {
                    details.add(new PermissionDetails(rowRole, rowResource, p, perms.grantModesFor(p)));
                }
            }
        }
        return details;
    }

    private String buildListQuery(IResource resource, Set<RoleResource> roles)
    {
        StringBuilder builder = new StringBuilder("SELECT " + ROLE
                                                  + ", "
                                                  + RESOURCE
                                                  + ", "
                                                  + PERMISSIONS
                                                  + ", "
                                                  + RESTRICTED
                                                  + ", "
                                                  + GRANTABLES
                                                  + " FROM "
                                                  + ROLE_PERMISSIONS_TABLE);

        boolean hasResource = resource != null;
        boolean hasRoles = roles != null && !roles.isEmpty();

        if (hasResource)
        {
            builder.append(" WHERE resource = '").append(escape(resource.getName())).append('\'');
        }
        if (hasRoles)
        {
            builder.append(hasResource ? " AND " : " WHERE ")
                   .append(ROLE + " IN ")
                   .append(roles.stream()
                                .map(r -> escape(r.getRoleName()))
                                .collect(Collectors.joining("', '", "('", "')")));
        }
        builder.append(" ALLOW FILTERING");
        return builder.toString();
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
        String query = String.format("SELECT %s, %s, %s FROM %s.%s WHERE resource = ? AND %s IN ?",
                                              PERMISSIONS,
                                              RESTRICTED,
                                              GRANTABLES,
                                              SchemaConstants.AUTH_KEYSPACE_NAME,
                                              AuthKeyspace.ROLE_PERMISSIONS,
                                              ROLE);
        authorizeRoleStatement = (SelectStatement) QueryProcessor.getStatement(query, ClientState.forInternalCalls());
    }

    // We only worry about one character ('). Make sure it's properly escaped.
    private static String escape(String name)
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
        logger.info("EXECUTING {}", query);
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

    public Supplier<Map<Pair<AuthenticatedUser, IResource>, PermissionSets>> bulkLoader()
    {
        return () -> {
            Map<Pair<AuthenticatedUser, IResource>, PermissionSets> entries = new HashMap<>();

            String cqlTemplate = "SELECT %s, %s, %s, %s, %s FROM %s.%s";
            logger.info("Warming permissions cache from role_permissions table");
            UntypedResultSet results = process(String.format(cqlTemplate,
                                                             ROLE,
                                                             RESOURCE,
                                                             PERMISSIONS,
                                                             RESTRICTED,
                                                             GRANTABLES,
                                                             SchemaConstants.AUTH_KEYSPACE_NAME,
                                                             AuthKeyspace.ROLE_PERMISSIONS),
                                               authReadConsistencyLevel());

          // role_name -> (resource, permissions)
          Table<String, IResource, PermissionSets> individualRolePermissions = HashBasedTable.create();
            results.forEach(row -> {
                if (row.has(PERMISSIONS) || row.has(RESTRICTED) || row.has(GRANTABLES))
                {
                    PermissionSets.Builder perms = PermissionSets.builder();
                    perms.addGranted(permissions(row.getSet(PERMISSIONS, UTF8Type.instance)));
                    perms.addRestricted(permissions(row.getSet(RESTRICTED, UTF8Type.instance)));
                    perms.addGrantables(permissions(row.getSet(GRANTABLES, UTF8Type.instance)));
                    individualRolePermissions.put(row.getString(ROLE),
                                                  Resources.fromName(row.getString(RESOURCE)),
                                                  perms.build());
                }
            });

          // Iterate all user level roles in the system and accumulate the permissions of their granted roles
          Roles.getAllRoles().forEach(roleResource -> {
              // If the role has login priv, accumulate the permissions of all its granted roles
              if (Roles.canLogin(roleResource))
              {
                  // Structure to accumulate the resource -> permission mappings for the closure of granted roles
                  Map<IResource, PermissionSets.Builder> userPermissions = new HashMap<>();
                  BiConsumer<IResource, PermissionSets> accumulator = accumulator(userPermissions);

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
    private static BiConsumer<IResource, PermissionSets> accumulator(Map<IResource, PermissionSets.Builder> accumulator)
    {
        return (resource, permissions) -> accumulator.computeIfAbsent(resource, k -> PermissionSets.builder()).accumulate(permissions);
    }

    private static Set<Permission> permissions(Set<String> permissionNames)
    {
        if (permissionNames == null)
            return Collections.emptySet();
        else
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

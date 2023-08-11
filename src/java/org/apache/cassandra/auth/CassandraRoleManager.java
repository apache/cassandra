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
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.NoSpamLogger;
import org.mindrot.jbcrypt.BCrypt;

import static org.apache.cassandra.config.CassandraRelevantProperties.AUTH_BCRYPT_GENSALT_LOG2_ROUNDS;
import static org.apache.cassandra.service.QueryState.forInternalCalls;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Responsible for the creation, maintenance and deletion of roles
 * for the purposes of authentication and authorization.
 * Role data is stored internally, using the roles and role_members tables
 * in the system_auth keyspace.
 *
 * Additionally, if org.apache.cassandra.auth.PasswordAuthenticator is used,
 * encrypted passwords are also stored in the system_auth.roles table. This
 * coupling between the IAuthenticator and IRoleManager implementations exists
 * because setting a role's password via CQL is done with a CREATE ROLE or
 * ALTER ROLE statement, the processing of which is handled by IRoleManager.
 * As IAuthenticator is concerned only with credentials checking and has no
 * means to modify passwords, PasswordAuthenticator depends on
 * CassandraRoleManager for those functions.
 *
 * Alternative IAuthenticator implementations may be used in conjunction with
 * CassandraRoleManager, but WITH PASSWORD = 'password' will not be supported
 * in CREATE/ALTER ROLE statements.
 *
 * Such a configuration could be implemented using a custom IRoleManager that
 * extends CassandraRoleManager and which includes Option.PASSWORD in the {@code Set<Option>}
 * returned from supportedOptions/alterableOptions. Any additional processing
 * of the password itself (such as storing it in an alternative location) would
 * be added in overridden createRole and alterRole implementations.
 */
public class CassandraRoleManager implements IRoleManager
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraRoleManager.class);
    private static final NoSpamLogger nospamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);

    public static final String DEFAULT_SUPERUSER_NAME = "cassandra";
    public static final String DEFAULT_SUPERUSER_PASSWORD = "cassandra";

    /**
     * We need to treat the default superuser as a special case since during initial node startup, we may end up with
     * duplicate creation or deletion + re-creation of this user on different nodes unless we check at quorum to see if
     * it's already been done.
     */
    static final ConsistencyLevel DEFAULT_SUPERUSER_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    // Transform a row in the AuthKeyspace.ROLES to a Role instance
    private static final Function<UntypedResultSet.Row, Role> ROW_TO_ROLE = row ->
    {
        try
        {
            return new Role(row.getString("role"),
                            row.getBoolean("is_superuser"),
                            row.getBoolean("can_login"),
                            Collections.emptyMap(),
                            row.has("member_of") ? row.getSet("member_of", UTF8Type.instance)
                                                 : Collections.<String>emptySet());
        }
        // Failing to deserialize a boolean in is_superuser or can_login will throw an NPE
        catch (NullPointerException e)
        {
            logger.warn("An invalid value has been detected in the {} table for role {}. If you are " +
                        "unable to login, you may need to disable authentication and confirm " +
                        "that values in that table are accurate", AuthKeyspace.ROLES, row.getString("role"));
            throw new RuntimeException(String.format("Invalid metadata has been detected for role %s", row.getString("role")), e);
        }
    };

    private static final int GENSALT_LOG2_ROUNDS = getGensaltLogRounds();

    static int getGensaltLogRounds()
    {
        int rounds = AUTH_BCRYPT_GENSALT_LOG2_ROUNDS.getInt(10);
        if (rounds < 4 || rounds > 30)
            throw new ConfigurationException(String.format("Bad value for system property %s." +
                                                           "Please use a value between 4 and 30 inclusively", AUTH_BCRYPT_GENSALT_LOG2_ROUNDS.getKey()));
        return rounds;
    }

    private SelectStatement loadRoleStatement;
    private SelectStatement loadIdentityStatement;

    private final Set<Option> supportedOptions;
    private final Set<Option> alterableOptions;

    public CassandraRoleManager()
    {
        supportedOptions = DatabaseDescriptor.getAuthenticator() instanceof PasswordAuthenticator
                         ? ImmutableSet.of(Option.LOGIN, Option.SUPERUSER, Option.PASSWORD, Option.HASHED_PASSWORD)
                         : ImmutableSet.of(Option.LOGIN, Option.SUPERUSER);
        alterableOptions = DatabaseDescriptor.getAuthenticator() instanceof PasswordAuthenticator
                         ? ImmutableSet.of(Option.PASSWORD, Option.HASHED_PASSWORD)
                         : ImmutableSet.<Option>of();
    }

    @Override
    public void setup()
    {
        loadRoleStatement();
        loadIdentityStatement();
        scheduleSetupTask(() -> {
            setupDefaultRole();
            return null;
        });
    }

    @Override
    public String roleForIdentity(String identity)
    {
        QueryOptions options = QueryOptions.forInternalCalls(CassandraAuthorizer.authReadConsistencyLevel(),
                                                             Collections.singletonList(byteBuf(identity)));
        ResultMessage.Rows rows = select(loadIdentityStatement, options);
        if (rows.result.isEmpty())
        {
            nospamLogger.warn("No such identity {} in the identity_to_roles table", identity);
            return null;
        }
        return UntypedResultSet.create(rows.result).one().getString("role");
    }

    @Override
    public Map<String, String> authorizedIdentities()
    {
        Map<String, String> validIdentities = new HashMap<>();
        String query = String.format("SELECT identity, role from %s.%s",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.IDENTITY_TO_ROLES);
        UntypedResultSet rows = process(query, CassandraAuthorizer.authReadConsistencyLevel());
        rows.forEach(row -> validIdentities.put(row.getString("identity"), row.getString("role")));
        return validIdentities;
    }

    @Override
    public void addIdentity(String identity, String role)
    {
        if (isExistingIdentity(identity))
        {
            throw new IllegalStateException("Identity is already associated with another role, cannot associate it with role " + role);
        }

        String query = String.format("INSERT INTO %s.%s (identity, role) VALUES (?, ?)",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.IDENTITY_TO_ROLES);
        process(query, CassandraAuthorizer.authWriteConsistencyLevel(), byteBuf(identity), byteBuf(role));
    }

    @Override
    public boolean isExistingIdentity(String identity)
    {
        String query = String.format("SELECT identity from %s.%s where identity=?",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.IDENTITY_TO_ROLES);
        UntypedResultSet rows = process(query, CassandraAuthorizer.authReadConsistencyLevel(), byteBuf(identity));
        return !rows.isEmpty();
    }

    @Override
    public void dropIdentity(String identity)
    {
        String query = String.format("DELETE FROM %s.%s WHERE identity = ?",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.IDENTITY_TO_ROLES);
        process(query, CassandraAuthorizer.authWriteConsistencyLevel(), byteBuf(identity));
    }

    protected final void loadRoleStatement()
    {
        loadRoleStatement = (SelectStatement) prepare("SELECT * from %s.%s WHERE role = ?",
                                                      SchemaConstants.AUTH_KEYSPACE_NAME,
                                                      AuthKeyspace.ROLES);
    }


    protected void loadIdentityStatement()
    {
        loadIdentityStatement = (SelectStatement) prepare("SELECT role from %s.%s where identity=?",
                                                          SchemaConstants.AUTH_KEYSPACE_NAME,
                                                          AuthKeyspace.IDENTITY_TO_ROLES);
    }

    public Set<Option> supportedOptions()
    {
        return supportedOptions;
    }

    public Set<Option> alterableOptions()
    {
        return alterableOptions;
    }

    public void createRole(AuthenticatedUser performer, RoleResource role, RoleOptions options)
    throws RequestValidationException, RequestExecutionException
    {
        List<String> identitiesOfRole = identitiesForRole(role.getRoleName());
        if (!identitiesOfRole.isEmpty())
        {
            throw new IllegalStateException(String.format("Cannot create a role '%s' when identities already exists for it", role.getRoleName()));
        }
        String insertCql = options.getPassword().isPresent() || options.getHashedPassword().isPresent()
                         ? String.format("INSERT INTO %s.%s (role, is_superuser, can_login, salted_hash) VALUES ('%s', %s, %s, '%s')",
                                         SchemaConstants.AUTH_KEYSPACE_NAME,
                                         AuthKeyspace.ROLES,
                                         escape(role.getRoleName()),
                                         options.getSuperuser().orElse(false),
                                         options.getLogin().orElse(false),
                                         options.getHashedPassword().orElseGet(() -> escape(hashpw(options.getPassword().get()))))
                         : String.format("INSERT INTO %s.%s (role, is_superuser, can_login) VALUES ('%s', %s, %s)",
                                         SchemaConstants.AUTH_KEYSPACE_NAME,
                                         AuthKeyspace.ROLES,
                                         escape(role.getRoleName()),
                                         options.getSuperuser().orElse(false),
                                         options.getLogin().orElse(false));
        process(insertCql, consistencyForRoleWrite(role.getRoleName()));
    }

    public void dropRole(AuthenticatedUser performer, RoleResource role) throws RequestValidationException, RequestExecutionException
    {
        process(String.format("DELETE FROM %s.%s WHERE role = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.ROLES,
                              escape(role.getRoleName())),
                consistencyForRoleWrite(role.getRoleName()));
        removeAllMembers(role.getRoleName());
        removeAllIdentitiesOfRole(role.getRoleName());
    }

    public void alterRole(AuthenticatedUser performer, RoleResource role, RoleOptions options)
    {
        // Unlike most of the other data access methods here, this does not use a
        // prepared statement in order to allow the set of assignments to be variable.
        String assignments = optionsToAssignments(options.getOptions());
        if (!Strings.isNullOrEmpty(assignments))
        {
            process(String.format("UPDATE %s.%s SET %s WHERE role = '%s'",
                                  SchemaConstants.AUTH_KEYSPACE_NAME,
                                  AuthKeyspace.ROLES,
                                  assignments,
                                  escape(role.getRoleName())),
                    consistencyForRoleWrite(role.getRoleName()));
        }
    }

    public void grantRole(AuthenticatedUser performer, RoleResource role, RoleResource grantee)
    throws RequestValidationException, RequestExecutionException
    {
        if (getRoles(grantee, true).contains(role))
            throw new InvalidRequestException(String.format("%s is a member of %s",
                                                            grantee.getRoleName(),
                                                            role.getRoleName()));
        if (getRoles(role, true).contains(grantee))
            throw new InvalidRequestException(String.format("%s is a member of %s",
                                                            role.getRoleName(),
                                                            grantee.getRoleName()));

        modifyRoleMembership(grantee.getRoleName(), role.getRoleName(), "+");
        process(String.format("INSERT INTO %s.%s (role, member) values ('%s', '%s')",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.ROLE_MEMBERS,
                              escape(role.getRoleName()),
                              escape(grantee.getRoleName())),
                consistencyForRoleWrite(role.getRoleName()));
    }

    public void revokeRole(AuthenticatedUser performer, RoleResource role, RoleResource revokee)
    throws RequestValidationException, RequestExecutionException
    {
        if (!getRoles(revokee, false).contains(role))
            throw new InvalidRequestException(String.format("%s is not a member of %s",
                                                            revokee.getRoleName(),
                                                            role.getRoleName()));

        modifyRoleMembership(revokee.getRoleName(), role.getRoleName(), "-");
        process(String.format("DELETE FROM %s.%s WHERE role = '%s' and member = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.ROLE_MEMBERS,
                              escape(role.getRoleName()),
                              escape(revokee.getRoleName())),
                consistencyForRoleWrite(role.getRoleName()));
    }

    public Set<RoleResource> getRoles(RoleResource grantee, boolean includeInherited)
    throws RequestValidationException, RequestExecutionException
    {
        return collectRoles(getRole(grantee.getRoleName()),
                            includeInherited,
                            filter(),
                            this::getRole)
               .map(r -> r.resource)
               .collect(Collectors.toSet());
    }

    public Set<Role> getRoleDetails(RoleResource grantee)
    {
        return collectRoles(getRole(grantee.getRoleName()),
                            true,
                            filter(),
                            this::getRole)
               .collect(Collectors.toSet());
    }

    /**
     * We hard-code this query to Quorum regardless of the role or auth credentials of the queryer given the nature of
     * this query: we expect to know *all* roles across the entire cluster when we query this, not just local quorum or
     * on a single node.
     */
    public Set<RoleResource> getAllRoles() throws RequestValidationException, RequestExecutionException
    {
        ImmutableSet.Builder<RoleResource> builder = ImmutableSet.builder();
        UntypedResultSet rows = process(String.format("SELECT role from %s.%s",
                                                      SchemaConstants.AUTH_KEYSPACE_NAME,
                                                      AuthKeyspace.ROLES),
                                        ConsistencyLevel.QUORUM);
        rows.forEach(row -> builder.add(RoleResource.role(row.getString("role"))));
        return builder.build();
    }

    public boolean isSuper(RoleResource role)
    {
        try
        {
            return getRole(role.getRoleName()).isSuper;
        }
        catch (RequestExecutionException e)
        {
            logger.debug("Failed to authorize {} for super-user permission", role.getRoleName());
            throw new UnauthorizedException("Unable to perform authorization of super-user permission: " + e.getMessage(), e);
        }
    }

    public boolean canLogin(RoleResource role)
    {
        try
        {
            return getRole(role.getRoleName()).canLogin;
        }
        catch (RequestExecutionException e)
        {
            logger.debug("Failed to authorize {} for login permission", role.getRoleName());
            throw new UnauthorizedException("Unable to perform authorization of login permission: " + e.getMessage(), e);
        }
    }

    public Map<String, String> getCustomOptions(RoleResource role)
    {
        return Collections.emptyMap();
    }

    public boolean isExistingRole(RoleResource role)
    {
        return !Roles.isNullRole(getRole(role.getRoleName()));
    }

    public Set<? extends IResource> protectedResources()
    {
        return ImmutableSet.of(DataResource.table(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES),
                               DataResource.table(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_MEMBERS));
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    /*
     * Create the default superuser role to bootstrap role creation on a clean system. Preemptively
     * gives the role the default password so PasswordAuthenticator can be used to log in (if
     * configured)
     */
    private static void setupDefaultRole()
    {
        if (StorageService.instance.getTokenMetadata().sortedTokens().isEmpty())
            throw new IllegalStateException("CassandraRoleManager skipped default role setup: no known tokens in ring");

        try
        {
            if (!hasExistingRoles())
            {
                QueryProcessor.process(createDefaultRoleQuery(),
                                       consistencyForRoleWrite(DEFAULT_SUPERUSER_NAME));
                logger.info("Created default superuser role '{}'", DEFAULT_SUPERUSER_NAME);
            }
        }
        catch (RequestExecutionException e)
        {
            logger.warn("CassandraRoleManager skipped default role setup: some nodes were not ready");
            throw e;
        }
    }

    @VisibleForTesting
    public static String createDefaultRoleQuery()
    {
        return String.format("INSERT INTO %s.%s (role, is_superuser, can_login, salted_hash) VALUES ('%s', true, true, '%s') USING TIMESTAMP 0",
                             SchemaConstants.AUTH_KEYSPACE_NAME,
                             AuthKeyspace.ROLES,
                             DEFAULT_SUPERUSER_NAME,
                             escape(hashpw(DEFAULT_SUPERUSER_PASSWORD)));
    }

    @VisibleForTesting
    public static boolean hasExistingRoles() throws RequestExecutionException
    {
        // Try looking up the 'cassandra' default role first, to avoid the range query if possible.
        String defaultSUQuery = String.format("SELECT * FROM %s.%s WHERE role = '%s'", SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES, DEFAULT_SUPERUSER_NAME);
        String allUsersQuery = String.format("SELECT * FROM %s.%s LIMIT 1", SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES);
        return !QueryProcessor.process(defaultSUQuery, ConsistencyLevel.ONE).isEmpty()
               || !QueryProcessor.process(defaultSUQuery, ConsistencyLevel.QUORUM).isEmpty()
               || !QueryProcessor.process(allUsersQuery, ConsistencyLevel.QUORUM).isEmpty();
    }

    protected void scheduleSetupTask(final Callable<Void> setupTask)
    {
        // The delay is to give the node a chance to see its peers before attempting the operation
        ScheduledExecutors.optionalTasks.scheduleSelfRecurring(() -> {
            if (!StorageProxy.isSafeToPerformRead())
            {
                logger.trace("Setup task may not run due to it not being safe to perform reads... rescheduling");
                scheduleSetupTask(setupTask);
                return;
            }
            try
            {
                setupTask.call();
            }
            catch (Exception e)
            {
                logger.info("Setup task failed with error, rescheduling");
                scheduleSetupTask(setupTask);
            }
        }, AuthKeyspace.SUPERUSER_SETUP_DELAY, TimeUnit.MILLISECONDS);
    }

    private CQLStatement prepare(String template, String keyspace, String table)
    {
        try
        {
            return QueryProcessor.parseStatement(String.format(template, keyspace, table)).prepare(ClientState.forInternalCalls());
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
    }

    // Providing a function to fetch the details of granted roles allows us to read from the underlying tables during
    // normal usage and fetch from a prepopulated in memory structure when building an initial set of roles to warm
    // the RolesCache at startup
    private Stream<Role> collectRoles(Role role, boolean includeInherited, Predicate<String> distinctFilter, Function<String, Role> loaderFunction)
    {
        if (Roles.isNullRole(role))
            return Stream.empty();

        if (!includeInherited)
            return Stream.concat(Stream.of(role), role.memberOf.stream().map(loaderFunction));


        return Stream.concat(Stream.of(role),
                             role.memberOf.stream()
                                          .filter(distinctFilter)
                                          .flatMap(r -> collectRoles(loaderFunction.apply(r), true, distinctFilter, loaderFunction)));
    }

    // Used as a stateful filtering function when recursively collecting granted roles
    private static Predicate<String> filter()
    {
        final Set<String> seen = new HashSet<>();
        return seen::add;
    }

    /*
     * Get a single Role instance given the role name. This never returns null, instead it
     * uses a null object when a role with the given name cannot be found. So
     * it's always safe to call methods on the returned object without risk of NPE.
     */
    private Role getRole(String name)
    {
        QueryOptions options = QueryOptions.forInternalCalls(consistencyForRoleRead(name),
                                                             Collections.singletonList(ByteBufferUtil.bytes(name)));
        ResultMessage.Rows rows = select(loadRoleStatement, options);
        if (rows.result.isEmpty())
            return Roles.nullRole();

        return ROW_TO_ROLE.apply(UntypedResultSet.create(rows.result).one());
    }

    /*
     * Adds or removes a role name from the membership list of an entry in the roles table table
     * (adds if op is "+", removes if op is "-")
     */
    private void modifyRoleMembership(String grantee, String role, String op)
    throws RequestExecutionException
    {
        process(String.format("UPDATE %s.%s SET member_of = member_of %s {'%s'} WHERE role = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.ROLES,
                              op,
                              escape(role),
                              escape(grantee)),
                consistencyForRoleWrite(grantee));
    }

    private List<String> identitiesForRole(String role)
    {
        // Get all identities associated with a given role
        String query = String.format("SELECT identity FROM %s.%s WHERE role = ? ALLOW FILTERING",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.IDENTITY_TO_ROLES);
        UntypedResultSet rows = process(query, consistencyForRoleRead(role), byteBuf(role));
        List<String> identities = new ArrayList<>();
        rows.forEach(row -> identities.add(row.getString("identity")));
        return identities;
    }

    private void removeAllIdentitiesOfRole(String role)
    {
        List<String> identities = identitiesForRole(role);
        String query = String.format("DELETE FROM %s.%s WHERE identity = ?",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.IDENTITY_TO_ROLES);
        // Remove all the identities associated with the role from the table
        for (String identity : identities)
        {
            process(query, consistencyForRoleWrite(role), byteBuf(identity));
        }
    }

    /*
     * Clear the membership list of the given role
     */
    private void removeAllMembers(String role) throws RequestValidationException, RequestExecutionException
    {
        // Get the membership list of the the given role
        UntypedResultSet rows = process(String.format("SELECT member FROM %s.%s WHERE role = '%s'",
                                                      SchemaConstants.AUTH_KEYSPACE_NAME,
                                                      AuthKeyspace.ROLE_MEMBERS,
                                                      escape(role)),
                                        consistencyForRoleRead(role));
        if (rows.isEmpty())
            return;

        // Update each member in the list, removing this role from its own list of granted roles
        for (UntypedResultSet.Row row : rows)
            modifyRoleMembership(row.getString("member"), role, "-");

        // Finally, remove the membership list for the dropped role
        process(String.format("DELETE FROM %s.%s WHERE role = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME,
                              AuthKeyspace.ROLE_MEMBERS,
                              escape(role)),
                consistencyForRoleWrite(role));
    }

    /*
     * Convert a map of Options from a CREATE/ALTER statement into
     * assignment clauses used to construct a CQL UPDATE statement
     */
    private String optionsToAssignments(Map<Option, Object> options)
    {
        return options.entrySet()
                      .stream()
                      .map(entry ->
                           {
                               switch (entry.getKey())
                               {
                                   case LOGIN:
                                       return String.format("can_login = %s", entry.getValue());
                                   case SUPERUSER:
                                       return String.format("is_superuser = %s", entry.getValue());
                                   case PASSWORD:
                                       return String.format("salted_hash = '%s'", escape(hashpw((String) entry.getValue())));
                                   case HASHED_PASSWORD:
                                       return String.format("salted_hash = '%s'", (String) entry.getValue());
                                   default:
                                       return null;
                               }
                           })
                      .filter(Objects::nonNull)
                      .collect(Collectors.joining(","));
    }

    private static String hashpw(String password)
    {
        return BCrypt.hashpw(password, BCrypt.gensalt(GENSALT_LOG2_ROUNDS));
    }

    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    private static ByteBuffer byteBuf(String str)
    {
        return UTF8Type.instance.decompose(str);
    }

    /** Allows selective overriding of the consistency level for specific roles. */
    protected static ConsistencyLevel consistencyForRoleWrite(String role)
    {
        return role.equals(DEFAULT_SUPERUSER_NAME) ?
               DEFAULT_SUPERUSER_CONSISTENCY_LEVEL :
               CassandraAuthorizer.authWriteConsistencyLevel();
    }

    protected static ConsistencyLevel consistencyForRoleRead(String role)
    {
        return role.equals(DEFAULT_SUPERUSER_NAME) ?
               DEFAULT_SUPERUSER_CONSISTENCY_LEVEL :
               CassandraAuthorizer.authReadConsistencyLevel();
    }

    /**
     * Executes the provided query.
     * This shouldn't be used during setup as this will directly return an error if the manager is not setup yet. Setup tasks
     * should use QueryProcessor.process directly.
     */
    @VisibleForTesting
    UntypedResultSet process(String query, ConsistencyLevel consistencyLevel)
    throws RequestValidationException, RequestExecutionException
    {
        return QueryProcessor.process(query, consistencyLevel);
    }

    UntypedResultSet process(String query, ConsistencyLevel consistencyLevel, ByteBuffer... values)
    throws RequestValidationException, RequestExecutionException
    {
        return QueryProcessor.process(query, consistencyLevel, Arrays.asList(values));
    }

    @VisibleForTesting
    ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
    {
        return statement.execute(forInternalCalls(), options, nanoTime());
    }

    @Override
    public Supplier<Map<RoleResource, Set<Role>>> bulkLoader()
    {
        return () ->
        {
            Map<RoleResource, Set<Role>> entries = new HashMap<>();

            logger.info("Warming roles cache from roles table");
            UntypedResultSet results = process("SELECT * FROM system_auth.roles", CassandraAuthorizer.authReadConsistencyLevel());

            // Create flat temporary lookup of name -> role mappings
            Map<String, Role> roles = new HashMap<>();
            results.forEach(row -> roles.put(row.getString("role"), ROW_TO_ROLE.apply(row)));

            // Iterate the flat structure and populate the fully hierarchical one
            roles.forEach((key, value) ->
                          entries.put(RoleResource.role(key),
                                      collectRoles(value, true, filter(), roles::get).collect(Collectors.toSet()))
            );
            return entries;
        };
    }
}

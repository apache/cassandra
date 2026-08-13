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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.NoOpGenerator;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.service.QueryState.forInternalCalls;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * Responsible for the creation, maintenance and deletion of roles for the purposes of authentication and
 * authorization. Role data is stored internally, using the roles and role_members tables in the system_auth
 * keyspace.
 *
 * Authenticators (implementations of {@link IAuthenticator}) can specify additional attributes to be stored.
 * For example, {@link org.apache.cassandra.auth.PasswordAuthenticator}, stores encrypted passwords in the
 * system_auth.roles table. This coupling between the IAuthenticator and IRoleManager implementations exists because
 * setting a role's password via CQL is done with a CREATE ROLE or ALTER ROLE statement, the processing of which is
 * handled by IRoleManager. Authenticators depend on CassandraRoleManager for those functions because IAuthenticator
 * is concerned only with credentials checking and has no means to directly modify passwords.
 */
public class CassandraRoleManager implements IRoleManager, CassandraRoleManagerMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraRoleManager.class);
    private static final NoSpamLogger nospamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);

    /**
     * Role options which are supported for all authentication mechanisms. IAuthenticator implementations can declare
     * additional supported role options via {@link IAuthenticator#getSupportedRoleOptions()}.
     */
    @VisibleForTesting
    static final Set<Option> DEFAULT_SUPPORTED_ROLE_OPTIONS = Set.of(Option.LOGIN, Option.SUPERUSER);

    /**
     * User-alterable role options which are supported for all authentication mechanisms. IAuthenticator
     * implementations can declare additional alterable role options via
     * {@link IAuthenticator#getAlterableRoleOptions()}.
     */
    @VisibleForTesting
    static final Set<Option> DEFAULT_ALTERABLE_ROLE_OPTIONS = Set.of();

    @VisibleForTesting
    static final String PARAM_INVALID_ROLE_DISCONNECT_TASK_PERIOD = "invalid_role_disconnect_task_period";

    @VisibleForTesting
    static final String PARAM_INVALID_ROLE_DISCONNECT_TASK_MAX_JITTER = "invalid_role_disconnect_task_max_jitter";

    public static final String MBEAN_NAME = "org.apache.cassandra.auth:type=CassandraRoleManager";

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

    private static int PASSWORD_UPDATE_MIN_INTERVAL_MS = CassandraRelevantProperties.ROLE_PASSWORD_UPDATE_MIN_INTERVAL_MS.getInt();
    // in-memory protection against excessive loadRoleWithWritetimeStatement queries
    private static Cache<String, Boolean> recentPasswordUpdates = Caffeine.newBuilder()
                                        .expireAfterWrite(PASSWORD_UPDATE_MIN_INTERVAL_MS, TimeUnit.MILLISECONDS)
                                        .build();

    @VisibleForTesting
    public static synchronized void updatePasswordUpdateMinInterval(int newInterval)
    {
        recentPasswordUpdates = Caffeine.newBuilder().expireAfterWrite(newInterval, TimeUnit.MILLISECONDS).build();
        PASSWORD_UPDATE_MIN_INTERVAL_MS = newInterval;
    }

    private SelectStatement loadRoleStatement;
    private SelectStatement loadIdentityStatement;
    private SelectStatement loadRoleWithWritetimeStatement;

    private final Set<Option> supportedOptions;
    private final Set<Option> alterableOptions;

    private volatile ScheduledFuture<?> invalidRoleDisconnectTask;

    private volatile long invalidClientDisconnectPeriodMillis;
    private volatile long invalidClientDisconnectMaxJitterMillis;

    public CassandraRoleManager()
    {
        this(Map.of());
    }

    public CassandraRoleManager(Map<String, String> parameters)
    {
        Set<Option> supportedOptions = Stream.concat(
                                           DEFAULT_SUPPORTED_ROLE_OPTIONS.stream(),
                                           DatabaseDescriptor.getAuthenticator().getSupportedRoleOptions().stream()
                                                             .filter(Objects::nonNull))
                                             .collect(Collectors.toSet());

        if (Guardrails.roleNamePolicy.getGenerator() != NoOpGenerator.INSTANCE)
            supportedOptions.add(Option.OPTIONS);

        this.supportedOptions = Set.copyOf(supportedOptions);

        alterableOptions = Stream.concat(DEFAULT_ALTERABLE_ROLE_OPTIONS.stream(),
                                         DatabaseDescriptor.getAuthenticator().getAlterableRoleOptions().stream()
                                                           .filter(Objects::nonNull))
                                 .collect(Collectors.toUnmodifiableSet());

        // Inherit parsing and validation from existing config parser
        invalidClientDisconnectPeriodMillis = new DurationSpec.LongMillisecondsBound(parameters.getOrDefault(PARAM_INVALID_ROLE_DISCONNECT_TASK_PERIOD, "0h")).toMilliseconds();
        invalidClientDisconnectMaxJitterMillis = new DurationSpec.LongMillisecondsBound(parameters.getOrDefault(PARAM_INVALID_ROLE_DISCONNECT_TASK_MAX_JITTER, "0h")).toMilliseconds();

        if (!MBeanWrapper.instance.isRegistered(MBEAN_NAME))
            MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    /**
     * The default role initializer is configured as a top-level {@code default_role_initializer} option and
     * applied by {@link AuthConfig#applyAuth()}. Returning it here lets the startup logic (see
     * {@link #setup(boolean)}, {@link #hasExistingRoles()} and {@link #consistencyForRoleWrite(String)}) reach the
     * configured initializer through the role manager, and lets custom {@link IRoleManager} implementations
     * override how they integrate it. Falls back to the historical password initializer when auth setup has not
     * run, e.g. in tests which do not call {@link AuthConfig#applyAuth()}.
     */
    @Override
    public IDefaultRoleInitializer defaultRoleInitializer()
    {
        IDefaultRoleInitializer initializer = DatabaseDescriptor.getDefaultRoleInitializer();
        return initializer == null ? PasswordDefaultRoleInitializer.instance : initializer;
    }

    @Override
    public void setup(boolean asyncRoleSetup)
    {
        loadRoleStatement();
        loadIdentityStatement();
        scheduleDisconnectInvalidRoleTask();
        if (!asyncRoleSetup)
        {
            try
            {
                // Try to set up synchronously
                defaultRoleInitializer().initializeDefaultRoleIfNeeded();
                return;
            }
            catch (Throwable t)
            {
                // We tried to execute the task in a sync way, but failed. Try asynchronous setup.
            }
        }
        scheduleSetupTask(() -> {
            defaultRoleInitializer().initializeDefaultRoleIfNeeded();
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

        loadRoleWithWritetimeStatement = (SelectStatement) prepare("SELECT writetime(salted_hash) AS salted_hash_writetime from %s.%s WHERE role = ?",
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
        if (options.getPassword().isPresent())
            enforcePasswordUpdateRateLimit(performer, role.getRoleName());

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

    @Override
    public ResultMessage alterRoleWithResult(AuthenticatedUser performer, RoleResource role, RoleOptions options)
    {
        alterRole(performer, role, options);
        return getResultMessageForRoleCreatedOrAltered(role, options);
    }

    @Override
    public ResultMessage createRoleWithResult(AuthenticatedUser performer, RoleResource role, RoleOptions options)
    {
        createRole(performer, role, options);
        return getResultMessageForRoleCreatedOrAltered(role, options);
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
        return Set.of(DataResource.table(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES),
                      DataResource.table(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_MEMBERS));
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    @VisibleForTesting
    public static boolean hasExistingRoles() throws RequestExecutionException
    {
        return DatabaseDescriptor.getRoleManager().defaultRoleInitializer().hasExistingRoles();
    }

    protected void scheduleSetupTask(final Callable<Void> setupTask)
    {
        // The delay is to give the node a chance to see its peers before attempting the operation
        ScheduledExecutors.optionalTasks.scheduleSelfRecurring(() -> {
            if (!StorageProxy.hasJoined())
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
            throw new AssertionError(e + " " + FBUtilities.getJustLocalAddress()); // not supposed to happen
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

    /**
     * Rate limit password updates on each role.
     * @throws OverloadedException if the password was changed within ROLE_PASSWORD_UPDATE_INTERVAL
     */
    private void enforcePasswordUpdateRateLimit(AuthenticatedUser performer, String roleName)
    {
        if (PASSWORD_UPDATE_MIN_INTERVAL_MS <= 0)
            return;

        if (Boolean.TRUE != recentPasswordUpdates.getIfPresent(roleName))
        {
            QueryOptions options = QueryOptions.forInternalCalls(consistencyForRoleRead(roleName),
                                                                 Collections.singletonList(ByteBufferUtil.bytes(roleName)));

            ResultMessage.Rows rows = select(loadRoleWithWritetimeStatement, options);
            boolean hasRecentPasswordUpdates = !rows.result.isEmpty();
            if (hasRecentPasswordUpdates)
            {
                UntypedResultSet.Row row = UntypedResultSet.create(rows.result).one();

                hasRecentPasswordUpdates = row.has("salted_hash_writetime")
                                           && PASSWORD_UPDATE_MIN_INTERVAL_MS >= (Clock.Global.currentTimeMillis() - TimeUnit.MICROSECONDS.toMillis(row.getLong("salted_hash_writetime")));
            }
            if (!hasRecentPasswordUpdates)
            {
                recentPasswordUpdates.put(roleName, Boolean.TRUE);
                logger.info(String.format("Password changing for role %s by %s", roleName, performer.getName()));
                return;
            }
        }
        String failure = String.format("Password for role %s can only be changed every %sms.", roleName, PASSWORD_UPDATE_MIN_INTERVAL_MS);
        logger.warn(String.format("%s [performer: %s]", failure, performer.getName()));
        throw new OverloadedException(failure);
    }

    static String hashpw(String password)
    {
        return AuthUtils.hashpw(password);
    }

    static String escape(String name)
    {
        return AuthUtils.escape(name);
    }

    private static ByteBuffer byteBuf(String str)
    {
        return UTF8Type.instance.decompose(str);
    }

    /** Allows selective overriding of the consistency level for specific roles. */
    protected static ConsistencyLevel consistencyForRoleWrite(String role)
    {
        return AuthUtils.consistencyForRoleWrite(role);
    }

    protected static ConsistencyLevel consistencyForRoleRead(String role)
    {
        return AuthUtils.consistencyForRoleRead(role);
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
        return statement.execute(forInternalCalls(), options, Dispatcher.RequestTime.forImmediateExecution());
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

    protected void disconnectInvalidRoles()
    {
        // This should always run with jitter, otherwise there's a risk that all nodes disconnect clients at the same time
        StorageService.instance.disconnectInvalidRoles();
    }

    protected void invalidRoleDisconnectTask(LongSupplier delayMillis, ScheduledExecutorService executor)
    {
        try
        {
            disconnectInvalidRoles();
        }
        catch (Exception e)
        {
            logger.warn("Failed to disconnect invalid roles", e);
        }

        long nextDelayMillis = delayMillis.getAsLong();
        logger.info("Scheduling next invalid role disconnection in {} millis", nextDelayMillis);
        this.invalidRoleDisconnectTask = executor.schedule(() -> invalidRoleDisconnectTask(delayMillis, executor), nextDelayMillis, TimeUnit.MILLISECONDS);
    }

    protected void scheduleDisconnectInvalidRoleTask()
    {
        // Cancel any pending execution if it exists, since we may have changed period / jitter parameters
        if (this.invalidRoleDisconnectTask != null)
        {
            logger.debug("Canceling previous invalidRoleDisconnectTask");
            this.invalidRoleDisconnectTask.cancel(true);
        }

        long period = getInvalidClientDisconnectPeriodMillis();
        long jitter = getInvalidClientDisconnectMaxJitterMillis();
        if (period <= 0)
        {
            logger.info("Invalid role disconnection is disabled");
            return;
        }
        LongSupplier delayMillis = () -> period + ThreadLocalRandom.current().nextLong(0, jitter);
        long firstDelayMillis = delayMillis.getAsLong();
        ScheduledExecutorPlus executor = ScheduledExecutors.optionalTasks;

        logger.debug("Scheduling first invalid role disconnection in {} millis", firstDelayMillis);
        this.invalidRoleDisconnectTask = executor.schedule(() -> invalidRoleDisconnectTask(delayMillis, executor), firstDelayMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public long getInvalidClientDisconnectPeriodMillis()
    {
        return this.invalidClientDisconnectPeriodMillis;
    }

    @Override
    public void setInvalidClientDisconnectPeriodMillis(long duration)
    {
        this.invalidClientDisconnectPeriodMillis = duration;
        scheduleDisconnectInvalidRoleTask();
    }

    @Override
    public long getInvalidClientDisconnectMaxJitterMillis()
    {
        return this.invalidClientDisconnectMaxJitterMillis;
    }

    @Override
    public void setInvalidClientDisconnectMaxJitterMillis(long duration)
    {
        this.invalidClientDisconnectMaxJitterMillis = duration;
        scheduleDisconnectInvalidRoleTask();
    }

    private static final ColumnSpecification GENERATED_PASSWORD_METADATA = new ColumnSpecification(SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                                                   "generated_password",
                                                                                                   new ColumnIdentifier("generated_password", true),
                                                                                                   UTF8Type.instance);

    private static final ColumnSpecification GENERATED_ROLE_NAME_METADATA = new ColumnSpecification(SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                                                    "generated_role_name",
                                                                                                    new ColumnIdentifier("generated_role_name", true),
                                                                                                    UTF8Type.instance);

    protected ResultMessage getResultMessageForRoleCreatedOrAltered(RoleResource role, RoleOptions opts)
    {
        if (!opts.isGeneratedPassword() && !opts.isGeneratedName())
            return null;

        ResultSet resultSet = null;

        if (opts.isGeneratedPassword() && !opts.isGeneratedName())
        {
            if (opts.getPassword().isEmpty())
                return null;

            resultSet = new ResultSet(new ResultSet.ResultMetadata(List.of(GENERATED_PASSWORD_METADATA)));
            resultSet.addColumnValue(bytes(opts.getPassword().get()));
        }
        else if (!opts.isGeneratedPassword() && opts.isGeneratedName())
        {
            resultSet = new ResultSet(new ResultSet.ResultMetadata(List.of(GENERATED_ROLE_NAME_METADATA)));
            resultSet.addColumnValue(bytes(role.getRoleName()));
        }
        else if (opts.isGeneratedName() && opts.isGeneratedPassword())
        {
            if (opts.getPassword().isEmpty())
            {
                resultSet = new ResultSet(new ResultSet.ResultMetadata(List.of(GENERATED_ROLE_NAME_METADATA)));
                resultSet.addColumnValue(bytes(role.getRoleName()));
            }
            else
            {
                resultSet = new ResultSet(new ResultSet.ResultMetadata(List.of(GENERATED_PASSWORD_METADATA, GENERATED_ROLE_NAME_METADATA)));
                resultSet.addColumnValue(bytes(opts.getPassword().get()));
                resultSet.addColumnValue(bytes(role.getRoleName()));
            }
        }

        return new ResultMessage.Rows(resultSet);
    }
}

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

import com.google.common.base.*;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.mindrot.jbcrypt.BCrypt;

/**
 * Responsible for the creation, maintainance and delation of roles
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
 * extends CassandraRoleManager and which includes Option.PASSWORD in the Set<Option>
 * returned from supportedOptions/alterableOptions. Any additional processing
 * of the password itself (such as storing it in an alternative location) would
 * be added in overriden createRole and alterRole implementations.
 */
public class CassandraRoleManager implements IRoleManager
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraRoleManager.class);

    static final String DEFAULT_SUPERUSER_NAME = "cassandra";
    static final String DEFAULT_SUPERUSER_PASSWORD = "cassandra";

    // Transform a row in the AuthKeyspace.ROLES to a Role instance
    private static final Function<UntypedResultSet.Row, Role> ROW_TO_ROLE = new Function<UntypedResultSet.Row, Role>()
    {
        public Role apply(UntypedResultSet.Row row)
        {
            return new Role(row.getString("role"),
                            row.getBoolean("is_superuser"),
                            row.getBoolean("can_login"),
                            row.has("member_of") ? row.getSet("member_of", UTF8Type.instance)
                                                 : Collections.<String>emptySet());
        }
    };

    public static final String LEGACY_USERS_TABLE = "users";
    // Transform a row in the legacy system_auth.users table to a Role instance,
    // used to fallback to previous schema on a mixed cluster during an upgrade
    private static final Function<UntypedResultSet.Row, Role> LEGACY_ROW_TO_ROLE = new Function<UntypedResultSet.Row, Role>()
    {
        public Role apply(UntypedResultSet.Row row)
        {
            return new Role(row.getString("name"),
                            row.getBoolean("super"),
                            true,
                            Collections.<String>emptySet());
        }
    };

    // 2 ** GENSALT_LOG2_ROUNS rounds of hashing will be performed.
    private static final int GENSALT_LOG2_ROUNDS = 10;

    // NullObject returned when a supplied role name not found in AuthKeyspace.ROLES
    private static final Role NULL_ROLE = new Role(null, false, false, Collections.<String>emptySet());

    private SelectStatement loadRoleStatement;
    private SelectStatement legacySelectUserStatement;

    private final Set<Option> supportedOptions;
    private final Set<Option> alterableOptions;

    public CassandraRoleManager()
    {
        supportedOptions = DatabaseDescriptor.getAuthenticator().getClass() == PasswordAuthenticator.class
                         ? ImmutableSet.of(Option.LOGIN, Option.SUPERUSER, Option.PASSWORD)
                         : ImmutableSet.of(Option.LOGIN, Option.SUPERUSER);
        alterableOptions = DatabaseDescriptor.getAuthenticator().getClass().equals(PasswordAuthenticator.class)
                         ? ImmutableSet.of(Option.PASSWORD)
                         : ImmutableSet.<Option>of();
    }

    public void setup()
    {
        loadRoleStatement = (SelectStatement) prepare("SELECT * from %s.%s WHERE role = ?",
                                                      AuthKeyspace.NAME,
                                                      AuthKeyspace.ROLES);
        // If the old users table exists, we may need to migrate the legacy authn
        // data to the new table. We also need to prepare a statement to read from
        // it, so we can continue to use the old tables while the cluster is upgraded.
        // Otherwise, we may need to create a default superuser role to enable others
        // to be added.
        if (Schema.instance.getCFMetaData(AuthKeyspace.NAME, "users") != null)
        {
             legacySelectUserStatement = (SelectStatement) prepare("SELECT * FROM %s.%s WHERE name = ?",
                                                                   AuthKeyspace.NAME,
                                                                   LEGACY_USERS_TABLE);
            scheduleSetupTask(new Runnable()
            {
                public void run()
                {
                    convertLegacyData();
                }
            });
        }
        else
        {
            scheduleSetupTask(new Runnable()
            {
                public void run()
                {
                    setupDefaultRole();
                }
            });
        }
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
        String insertCql = options.getPassword().isPresent()
                         ? String.format("INSERT INTO %s.%s (role, is_superuser, can_login, salted_hash) VALUES ('%s', %s, %s, '%s')",
                                         AuthKeyspace.NAME,
                                         AuthKeyspace.ROLES,
                                         escape(role.getRoleName()),
                                         options.getSuperuser().get(),
                                         options.getLogin().get(),
                                         escape(hashpw(options.getPassword().get())))
                         : String.format("INSERT INTO %s.%s (role, is_superuser, can_login) VALUES ('%s', %s, %s)",
                                         AuthKeyspace.NAME,
                                         AuthKeyspace.ROLES,
                                         escape(role.getRoleName()),
                                         options.getSuperuser().get(),
                                         options.getLogin().get());
        process(insertCql, consistencyForRole(role.getRoleName()));
    }

    public void dropRole(AuthenticatedUser performer, RoleResource role) throws RequestValidationException, RequestExecutionException
    {
        process(String.format("DELETE FROM %s.%s WHERE role = '%s'",
                              AuthKeyspace.NAME,
                              AuthKeyspace.ROLES,
                              escape(role.getRoleName())),
                consistencyForRole(role.getRoleName()));
        removeAllMembers(role.getRoleName());
    }

    public void alterRole(AuthenticatedUser performer, RoleResource role, RoleOptions options)
    {
        // Unlike most of the other data access methods here, this does not use a
        // prepared statement in order to allow the set of assignments to be variable.
        String assignments = Joiner.on(',').join(Iterables.filter(optionsToAssignments(options.getOptions()),
                                                                  Predicates.notNull()));
        if (!Strings.isNullOrEmpty(assignments))
        {
            QueryProcessor.process(String.format("UPDATE %s.%s SET %s WHERE role = '%s'",
                                                 AuthKeyspace.NAME,
                                                 AuthKeyspace.ROLES,
                                                 assignments,
                                                 escape(role.getRoleName())),
                                   consistencyForRole(role.getRoleName()));
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
                              AuthKeyspace.NAME,
                              AuthKeyspace.ROLE_MEMBERS,
                              escape(role.getRoleName()),
                              escape(grantee.getRoleName())),
                consistencyForRole(role.getRoleName()));
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
                              AuthKeyspace.NAME,
                              AuthKeyspace.ROLE_MEMBERS,
                              escape(role.getRoleName()),
                              escape(revokee.getRoleName())),
                consistencyForRole(role.getRoleName()));
    }

    public Set<RoleResource> getRoles(RoleResource grantee, boolean includeInherited) throws RequestValidationException, RequestExecutionException
    {
        Set<RoleResource> roles = new HashSet<>();
        Role role = getRole(grantee.getRoleName());
        if (!role.equals(NULL_ROLE))
        {
            roles.add(RoleResource.role(role.name));
            collectRoles(role, roles, includeInherited);
        }
        return roles;
    }

    public Set<RoleResource> getAllRoles() throws RequestValidationException, RequestExecutionException
    {
        UntypedResultSet rows = QueryProcessor.process(String.format("SELECT role from %s.%s",
                                                                     AuthKeyspace.NAME,
                                                                     AuthKeyspace.ROLES),
                                                       ConsistencyLevel.QUORUM);
        Iterable<RoleResource> roles = Iterables.transform(rows, new Function<UntypedResultSet.Row, RoleResource>()
        {
            public RoleResource apply(UntypedResultSet.Row row)
            {
                return RoleResource.role(row.getString("role"));
            }
        });
        return ImmutableSet.<RoleResource>builder().addAll(roles).build();
    }

    public boolean isSuper(RoleResource role)
    {
        return getRole(role.getRoleName()).isSuper;
    }

    public boolean canLogin(RoleResource role)
    {
        return getRole(role.getRoleName()).canLogin;
    }

    public Map<String, String> getCustomOptions(RoleResource role)
    {
        return Collections.emptyMap();
    }

    public boolean isExistingRole(RoleResource role)
    {
        return getRole(role.getRoleName()) != NULL_ROLE;
    }

    public Set<? extends IResource> protectedResources()
    {
        return ImmutableSet.of(DataResource.table(AuthKeyspace.NAME, AuthKeyspace.ROLES),
                               DataResource.table(AuthKeyspace.NAME, AuthKeyspace.ROLE_MEMBERS));
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
        try
        {
            if (!hasExistingRoles())
            {
                QueryProcessor.process(String.format("INSERT INTO %s.%s (role, is_superuser, can_login, salted_hash) " +
                                                     "VALUES ('%s', true, true, '%s')",
                                                     AuthKeyspace.NAME,
                                                     AuthKeyspace.ROLES,
                                                     DEFAULT_SUPERUSER_NAME,
                                                     escape(hashpw(DEFAULT_SUPERUSER_PASSWORD))),
                                       consistencyForRole(DEFAULT_SUPERUSER_NAME));
                logger.info("Created default superuser role '{}'", DEFAULT_SUPERUSER_NAME);
            }
        }
        catch (RequestExecutionException e)
        {
            logger.warn("CassandraRoleManager skipped default role setup: some nodes were not ready");
        }
    }

    private static boolean hasExistingRoles() throws RequestExecutionException
    {
        // Try looking up the 'cassandra' default role first, to avoid the range query if possible.
        String defaultSUQuery = String.format("SELECT * FROM %s.%s WHERE role = '%s'", AuthKeyspace.NAME, AuthKeyspace.ROLES, DEFAULT_SUPERUSER_NAME);
        String allUsersQuery = String.format("SELECT * FROM %s.%s LIMIT 1", AuthKeyspace.NAME, AuthKeyspace.ROLES);
        return !process(defaultSUQuery, ConsistencyLevel.ONE).isEmpty()
               || !process(defaultSUQuery, ConsistencyLevel.QUORUM).isEmpty()
               || !process(allUsersQuery, ConsistencyLevel.QUORUM).isEmpty();
    }

    private void scheduleSetupTask(Runnable runnable)
    {
        // The delay is to give the node a chance to see its peers before attempting the operation
        ScheduledExecutors.optionalTasks.schedule(runnable, AuthKeyspace.SUPERUSER_SETUP_DELAY, TimeUnit.MILLISECONDS);
    }

    /*
     * Copy legacy auth data from the system_auth.users & system_auth.credentials tables to
     * the new system_auth.roles table. This setup is not performed if AllowAllAuthenticator
     * is configured (see Auth#setup).
     */
    private void convertLegacyData()
    {
        try
        {
            // read old data at QUORUM as it may contain the data for the default superuser
            if (Schema.instance.getCFMetaData("system_auth", "users") != null)
            {
                logger.info("Converting legacy users");
                UntypedResultSet users = QueryProcessor.process("SELECT * FROM system_auth.users",
                                                                ConsistencyLevel.QUORUM);
                for (UntypedResultSet.Row row : users)
                {
                    RoleOptions options = new RoleOptions();
                    options.setOption(Option.SUPERUSER, row.getBoolean("super"));
                    options.setOption(Option.LOGIN, true);
                    createRole(null, RoleResource.role(row.getString("name")), options);
                }
                logger.info("Completed conversion of legacy users");
            }

            if (Schema.instance.getCFMetaData("system_auth", "credentials") != null)
            {
                logger.info("Migrating legacy credentials data to new system table");
                UntypedResultSet credentials = QueryProcessor.process("SELECT * FROM system_auth.credentials",
                                                                      ConsistencyLevel.QUORUM);
                for (UntypedResultSet.Row row : credentials)
                {
                    // Write the password directly into the table to avoid doubly encrypting it
                    QueryProcessor.process(String.format("UPDATE %s.%s SET salted_hash = '%s' WHERE role = '%s'",
                                                         AuthKeyspace.NAME,
                                                         AuthKeyspace.ROLES,
                                                         row.getString("salted_hash"),
                                                         row.getString("username")),
                                           consistencyForRole(row.getString("username")));
                }
                logger.info("Completed conversion of legacy credentials");
            }
        }
        catch (Exception e)
        {
            logger.info("Unable to complete conversion of legacy auth data (perhaps not enough nodes are upgraded yet). " +
                        "Conversion should not be considered complete");
            logger.debug("Conversion error", e);
        }
    }

    private CQLStatement prepare(String template, String keyspace, String table)
    {
        try
        {
            return QueryProcessor.parseStatement(String.format(template, keyspace, table)).prepare().statement;
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
    }

    /*
     * Retrieve all roles granted to the given role. includeInherited specifies
     * whether to include only those roles granted directly or all inherited roles.
     */
    private void collectRoles(Role role, Set<RoleResource> collected, boolean includeInherited) throws RequestValidationException, RequestExecutionException
    {
        for (String memberOf : role.memberOf)
        {
            Role granted = getRole(memberOf);
            if (role.equals(NULL_ROLE))
                continue;
            collected.add(RoleResource.role(granted.name));
            if (includeInherited)
                collectRoles(granted, collected, true);
        }
    }

    /*
     * Get a single Role instance given the role name. This never returns null, instead it
     * uses the null object NULL_ROLE when a role with the given name cannot be found. So
     * it's always safe to call methods on the returned object without risk of NPE.
     */
    private Role getRole(String name)
    {
        try
        {
            // If it exists, try the legacy users table in case the cluster
            // is in the process of being upgraded and so is running with mixed
            // versions of the authn schema.
            return (Schema.instance.getCFMetaData(AuthKeyspace.NAME, "users") != null)
                    ? getRoleFromTable(name, legacySelectUserStatement, LEGACY_ROW_TO_ROLE)
                    : getRoleFromTable(name, loadRoleStatement, ROW_TO_ROLE);
        }
        catch (RequestExecutionException | RequestValidationException e)
        {
            throw new RuntimeException(e);
        }
    }

    private Role getRoleFromTable(String name, SelectStatement statement, Function<UntypedResultSet.Row, Role> function)
    throws RequestExecutionException, RequestValidationException
    {
        ResultMessage.Rows rows =
            statement.execute(QueryState.forInternalCalls(),
                              QueryOptions.forInternalCalls(consistencyForRole(name),
                                                            Collections.singletonList(ByteBufferUtil.bytes(name))));
        if (rows.result.isEmpty())
            return NULL_ROLE;

        return function.apply(UntypedResultSet.create(rows.result).one());
    }

    /*
     * Adds or removes a role name from the membership list of an entry in the roles table table
     * (adds if op is "+", removes if op is "-")
     */
    private void modifyRoleMembership(String grantee, String role, String op)
    throws RequestExecutionException
    {
        process(String.format("UPDATE %s.%s SET member_of = member_of %s {'%s'} WHERE role = '%s'",
                              AuthKeyspace.NAME,
                              AuthKeyspace.ROLES,
                              op,
                              escape(role),
                              escape(grantee)),
                consistencyForRole(grantee));
    }

    /*
     * Clear the membership list of the given role
     */
    private void removeAllMembers(String role) throws RequestValidationException, RequestExecutionException
    {
        // Get the membership list of the the given role
        UntypedResultSet rows = process(String.format("SELECT member FROM %s.%s WHERE role = '%s'",
                                                      AuthKeyspace.NAME,
                                                      AuthKeyspace.ROLE_MEMBERS,
                                                      escape(role)),
                                        consistencyForRole(role));
        if (rows.isEmpty())
            return;

        // Update each member in the list, removing this role from its own list of granted roles
        for (UntypedResultSet.Row row : rows)
            modifyRoleMembership(row.getString("member"), role, "-");

        // Finally, remove the membership list for the dropped role
        process(String.format("DELETE FROM %s.%s WHERE role = '%s'",
                              AuthKeyspace.NAME,
                              AuthKeyspace.ROLE_MEMBERS,
                              escape(role)),
                consistencyForRole(role));
    }

    /*
     * Convert a map of Options from a CREATE/ALTER statement into
     * assignment clauses used to construct a CQL UPDATE statement
     */
    private Iterable<String> optionsToAssignments(Map<Option, Object> options)
    {
        return Iterables.transform(
                                  options.entrySet(),
                                  new Function<Map.Entry<Option, Object>, String>()
                                  {
                                      public String apply(Map.Entry<Option, Object> entry)
                                      {
                                          switch (entry.getKey())
                                          {
                                              case LOGIN:
                                                  return String.format("can_login = %s", entry.getValue());
                                              case SUPERUSER:
                                                  return String.format("is_superuser = %s", entry.getValue());
                                              case PASSWORD:
                                                  return String.format("salted_hash = '%s'", escape(hashpw((String) entry.getValue())));
                                              default:
                                                  return null;
                                          }
                                      }
                                  });
    }

    protected static ConsistencyLevel consistencyForRole(String role)
    {
        if (role.equals(DEFAULT_SUPERUSER_NAME))
            return ConsistencyLevel.QUORUM;
        else
            return ConsistencyLevel.LOCAL_ONE;
    }

    private static String hashpw(String password)
    {
        return BCrypt.hashpw(password, BCrypt.gensalt(GENSALT_LOG2_ROUNDS));
    }

    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    private static UntypedResultSet process(String query, ConsistencyLevel consistencyLevel) throws RequestExecutionException
    {
        return QueryProcessor.process(query, consistencyLevel);
    }

    private static final class Role
    {
        private String name;
        private final boolean isSuper;
        private final boolean canLogin;
        private Set<String> memberOf;

        private Role(String name, boolean isSuper, boolean canLogin, Set<String> memberOf)
        {
            this.name = name;
            this.isSuper = isSuper;
            this.canLogin = canLogin;
            this.memberOf = memberOf;
        }

        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof Role))
                return false;

            Role r = (Role) o;
            return Objects.equal(name, r.name);
        }

        public int hashCode()
        {
            return Objects.hashCode(name);
        }
    }
}

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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.IndexType;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.*;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class Auth
{
    private static final Logger logger = LoggerFactory.getLogger(Auth.class);

    public static final String DEFAULT_SUPERUSER_NAME = "cassandra";

    public static final long SUPERUSER_SETUP_DELAY = Long.getLong("cassandra.superuser_setup_delay_ms", 10000);

    public static final int THREE_MONTHS = 90 * 24 * 60 * 60;

    public static final String AUTH_KS = "system_auth";
    public static final String USERS_CF = "users";
    public static final String ROLES_CF = "roles";
    public static final String GRANTS_CF = "grants";

    private static final String USERS_CF_SCHEMA = String.format("CREATE TABLE %s.%s ("
                                                                + "name text,"
                                                                + "super boolean,"
                                                                + "PRIMARY KEY(name)"
                                                                + ") WITH gc_grace_seconds=%d",
                                                                AUTH_KS,
                                                                USERS_CF,
                                                                THREE_MONTHS);

    private static final String ROLES_CF_SCHEMA = String.format("CREATE TABLE %s.%s ("
                                                                + "role text,"
                                                                + "grantees set<text>,"
                                                                + "PRIMARY KEY(role)"
                                                                + ") WITH gc_grace_seconds=%d",
                                                                AUTH_KS,
                                                                ROLES_CF,
                                                                THREE_MONTHS);

    private static final String GRANTS_CF_SCHEMA = String.format("CREATE TABLE %s.%s ("
                                                                 + "grantee text,"
                                                                 + "roles set<text>,"
                                                                 + "PRIMARY KEY(grantee)"
                                                                 + ") WITH gc_grace_seconds=%d",
                                                                 Auth.AUTH_KS,
                                                                 GRANTS_CF,
                                                                 THREE_MONTHS);

    private static SelectStatement selectUserStatement;
    private static SelectStatement selectRoleStatement;
    private static SelectStatement selectGrantStatement;

    /**
     * Checks if the username is stored in AUTH_KS.USERS_CF.
     *
     * @param username Username to query.
     * @return whether or not Cassandra knows about the user.
     */
    public static boolean isExistingUser(String username)
    {
        return !selectUser(username).isEmpty();
    }

    /**
     * Checks if the user is a known superuser.
     *
     * @param username Username to query.
     * @return true is the user is a superuser, false if they aren't or don't exist at all.
     */
    public static boolean isSuperuser(String username)
    {
        UntypedResultSet result = selectUser(username);
        return !result.isEmpty() && result.one().getBoolean("super");
    }

    /**
     * Inserts the user into AUTH_KS.USERS_CF (or overwrites their superuser status as a result of an ALTER USER query).
     *
     * @param username Username to insert.
     * @param isSuper User's new status.
     * @throws RequestExecutionException
     */
    public static void insertUser(String username, boolean isSuper) throws RequestExecutionException
    {
        QueryProcessor.process(String.format("INSERT INTO %s.%s (name, super) VALUES ('%s', %s)",
                                             AUTH_KS,
                                             USERS_CF,
                                             escape(username),
                                             isSuper),
                               consistencyForUser(username));
    }

    /**
     * Deletes the user from AUTH_KS.USERS_CF and removes any grants from the AUTH_KS.GRANTS_CF.
     *
     * @param username Username to delete.
     * @throws RequestExecutionException
     */
    public static void deleteUser(String username) throws RequestExecutionException
    {
        QueryProcessor.process(String.format("DELETE FROM %s.%s WHERE name = '%s'",
                                             AUTH_KS,
                                             USERS_CF,
                                             escape(username)),
                               consistencyForUser(username));
        QueryProcessor.process(String.format("DELETE FROM %s.%s WHERE grantee = '%s'",
                                             AUTH_KS,
                                             GRANTS_CF,
                                             Grantee.asUser(username).getId()),
                               ConsistencyLevel.LOCAL_ONE);
    }

    /**
     * Checks if the role is stored in AUTH_KS.ROLES_CF.
     *
     * @param role Role to query.
     * @return whether or not Cassandra knows about the role.
     */
    public static boolean isExistingRole(Role role)
    {
        return !selectRole(role).isEmpty();
    }

    /**
     * Inserts the role into AUTH_KS.ROLES_CF.
     *
     * @param role Role to insert.
     * @throws RequestExecutionException
     */
    public static void insertRole(Role role) throws RequestExecutionException
    {
        QueryProcessor.process(String.format("INSERT INTO %s.%s (role) VALUES ('%s')",
                                             AUTH_KS,
                                             ROLES_CF,
                                             escape(role.getName())),
                               ConsistencyLevel.LOCAL_ONE);
    }

    /**
     * Deletes the role by:
     *
     *   Removing any role entries in AUTH_KS.GRANTS_CF
     *   Removing any grantee entries in AUTH_KS.ROLES_CF
     *   Removing it's entry from AUTH_KS.ROLES_CF
     *   Removing it's entry from AUTH_KS.GRANTS_CF
     *
     * @param role Role to delete.
     * @throws RequestExecutionException
     */
    public static void deleteRole(Role role) throws RequestExecutionException
    {
        for (IGrantee grantee : getRoleGrantees(role))
            modifyGrants(role, grantee, "-");
        for (Role grantedRole : getGrantedRoles(new HashSet<Role>(), role, false))
            modifyRoles(grantedRole, role, "-");
        QueryProcessor.process(String.format("DELETE FROM %s.%s WHERE role = '%s'",
                                             AUTH_KS,
                                             ROLES_CF,
                                             escape(role.getName())),
                               ConsistencyLevel.LOCAL_ONE);
        QueryProcessor.process(String.format("DELETE FROM %s.%s WHERE grantee = '%s'",
                                             AUTH_KS,
                                             GRANTS_CF,
                                             role.getId()),
                               ConsistencyLevel.LOCAL_ONE);
    }

    /**
     * Grant a role to another role or user
     *
     * @param role Role to grant
     * @param grantee Grantee that will get role
     * @throws RequestExecutionException
     */
    public static void grantRole(Role role, IGrantee grantee) throws RequestExecutionException
    {
        modifyRoles(role, grantee, "+");
        modifyGrants(role, grantee, "+");
    }

    /**
     * Revoke a role from another role or user
     *
     * @param role Role to revoke
     * @param grantee Grantee that will have the role removed
     * @throws RequestExecutionException
     */
    public static void revokeRole(Role role, IGrantee grantee) throws RequestExecutionException
    {
        modifyRoles(role, grantee, "-");
        modifyGrants(role, grantee, "-");
    }

    /**
     * Get a set of roles that the role or user has been granted
     *
     * @param grantee Role or User
     * @param recursive If true return all roles in the role heirarchy that this
     *                  grantee has been granted
     * @return
     */
    public static Set<Role> getRoles(IGrantee grantee, boolean recursive)
    {
        Set<Role> roles = new HashSet<Role>();
        if (grantee == null)
        {
            for (Row row : selectRoles())
                roles.add(Grantee.asRole(row.getString("role")));
        }
        else
        {
            getGrantedRoles(roles, grantee, recursive);
        }
        return roles;
    }

    /**
     * Sets up Authenticator and Authorizer.
     */
    public static void setup()
    {
        if (DatabaseDescriptor.getAuthenticator() instanceof AllowAllAuthenticator)
            return;

        setupAuthKeyspace();
        setupTable(USERS_CF, USERS_CF_SCHEMA);
        setupTable(ROLES_CF, ROLES_CF_SCHEMA);
        setupTable(GRANTS_CF, GRANTS_CF_SCHEMA);

        DatabaseDescriptor.getAuthenticator().setup();
        DatabaseDescriptor.getAuthorizer().setup();

        // register a custom MigrationListener for permissions cleanup after dropped keyspaces/cfs.
        MigrationManager.instance.register(new MigrationListener());

        // the delay is here to give the node some time to see its peers - to reduce
        // "Skipped default superuser setup: some nodes were not ready" log spam.
        // It's the only reason for the delay.
        if (DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress()) || !DatabaseDescriptor.isAutoBootstrap())
        {
            StorageService.tasks.schedule(new Runnable()
                                          {
                                              public void run()
                                              {
                                                  setupDefaultSuperuser();
                                              }
                                          },
                                          SUPERUSER_SETUP_DELAY,
                                          TimeUnit.MILLISECONDS);
        }

        try
        {
            String query = String.format("SELECT * FROM %s.%s WHERE name = ?", AUTH_KS, USERS_CF);
            selectUserStatement = (SelectStatement) QueryProcessor.parseStatement(query).prepare().statement;
            query = String.format("SELECT * FROM %s.%s WHERE role = ?", AUTH_KS, ROLES_CF);
            selectRoleStatement = (SelectStatement) QueryProcessor.parseStatement(query).prepare().statement;
            query = String.format("SELECT * FROM %s.%s WHERE grantee = ?", AUTH_KS, GRANTS_CF);
            selectGrantStatement = (SelectStatement) QueryProcessor.parseStatement(query).prepare().statement;
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
    }

    public static Set<DataResource> protectedResources()
    {
        return ImmutableSet.of(DataResource.columnFamily(Auth.AUTH_KS, USERS_CF),
                               DataResource.columnFamily(AUTH_KS, ROLES_CF),
                               DataResource.columnFamily(AUTH_KS, GRANTS_CF));
    }

    // Only use QUORUM cl for the default superuser.
    private static ConsistencyLevel consistencyForUser(String username)
    {
        if (username.equals(DEFAULT_SUPERUSER_NAME))
            return ConsistencyLevel.QUORUM;
        else
            return ConsistencyLevel.LOCAL_ONE;
    }

    private static void setupAuthKeyspace()
    {
        if (Schema.instance.getKSMetaData(AUTH_KS) == null)
        {
            try
            {
                KSMetaData ksm = KSMetaData.newKeyspace(AUTH_KS, SimpleStrategy.class.getName(), ImmutableMap.of("replication_factor", "1"), true);
                MigrationManager.announceNewKeyspace(ksm, 0, false);
            }
            catch (Exception e)
            {
                throw new AssertionError(e); // shouldn't ever happen.
            }
        }
    }

    /**
     * Set up table from given CREATE TABLE statement under system_auth keyspace, if not already done so.
     *
     * @param name name of the table
     * @param cql CREATE TABLE statement
     */
    public static void setupTable(String name, String cql)
    {
        if (Schema.instance.getCFMetaData(AUTH_KS, name) == null)
        {
            try
            {
                CFStatement parsed = (CFStatement)QueryProcessor.parseStatement(cql);
                parsed.prepareKeyspace(AUTH_KS);
                CreateTableStatement statement = (CreateTableStatement) parsed.prepare().statement;
                CFMetaData cfm = statement.getCFMetaData().copy(CFMetaData.generateLegacyCfId(AUTH_KS, name));
                assert cfm.cfName.equals(name);
                MigrationManager.announceNewColumnFamily(cfm);
            }
            catch (Exception e)
            {
                throw new AssertionError(e);
            }
        }
    }

    /**
     * Set up an index for the column on the given table.
     *
     * @param tableName name of the table
     * @param column the column to index
     */
    public static void setupIndex(String tableName, ColumnIdentifier column)
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(AUTH_KS, tableName).copy();
        ColumnDefinition cd = cfm.getColumnDefinition(column);
        if (cd.getIndexType() != null)
            return;

        cd.setIndexType(IndexType.KEYS, Collections.<String, String>emptyMap());
        try
        {
            cfm.addDefaultIndexNames();
            MigrationManager.announceColumnFamilyUpdate(cfm, false, false);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    private static void setupDefaultSuperuser()
    {
        try
        {
            // insert a default superuser if AUTH_KS.USERS_CF is empty.
            if (!hasExistingUsers())
            {
                QueryProcessor.process(String.format("INSERT INTO %s.%s (name, super) VALUES ('%s', %s) USING TIMESTAMP 0",
                                                     AUTH_KS,
                                                     USERS_CF,
                                                     DEFAULT_SUPERUSER_NAME,
                                                     true),
                                       ConsistencyLevel.QUORUM);
                logger.info("Created default superuser '{}'", DEFAULT_SUPERUSER_NAME);
            }
        }
        catch (RequestExecutionException e)
        {
            logger.warn("Skipped default superuser setup: some nodes were not ready");
        }
    }

    private static void modifyRoles(Role role, IGrantee grantee, String op) throws RequestExecutionException
    {
        QueryProcessor.process(String.format("UPDATE %s.%s SET grantees = grantees %s {%s} WHERE role = '%s'",
                                             Auth.AUTH_KS,
                                             ROLES_CF,
                                             op,
                                             "'" + grantee.getId() + "'",
                                             escape(role.getName())),
                               ConsistencyLevel.QUORUM);
    }

    private static void modifyGrants(Role role, IGrantee grantee, String op) throws RequestExecutionException
    {
        QueryProcessor.process(String.format("UPDATE %s.%s SET roles = roles %s {%s} WHERE grantee = '%s'",
                                             Auth.AUTH_KS,
                                             GRANTS_CF,
                                             op,
                                             "'" + escape(role.getName()) + "'",
                                             grantee.getId()),
                               ConsistencyLevel.QUORUM);
    }

    private static boolean hasExistingUsers() throws RequestExecutionException
    {
        // Try looking up the 'cassandra' default super user first, to avoid the range query if possible.
        String defaultSUQuery = String.format("SELECT * FROM %s.%s WHERE name = '%s'", AUTH_KS, USERS_CF, DEFAULT_SUPERUSER_NAME);
        String allUsersQuery = String.format("SELECT * FROM %s.%s LIMIT 1", AUTH_KS, USERS_CF);
        return !QueryProcessor.process(defaultSUQuery, ConsistencyLevel.QUORUM).isEmpty()
            || !QueryProcessor.process(allUsersQuery, ConsistencyLevel.QUORUM).isEmpty();
    }

    // we only worry about one character ('). Make sure it's properly escaped.
    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    private static UntypedResultSet selectUser(String username)
    {
        try
        {
            ResultMessage.Rows rows = selectUserStatement.execute(QueryState.forInternalCalls(),
                                                                  QueryOptions.forInternalCalls(consistencyForUser(username),
                                                                                                Lists.newArrayList(ByteBufferUtil.bytes(username))));
            return UntypedResultSet.create(rows.result);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static UntypedResultSet selectRole(Role role)
    {
        try
        {
            ResultMessage.Rows rows = selectRoleStatement.execute(QueryState.forInternalCalls(),
                                                                  QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE,
                                                                                                Lists.newArrayList(ByteBufferUtil.bytes(role.getName()))));
            return UntypedResultSet.create(rows.result);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Set<IGrantee> getRoleGrantees(Role role)
    {
        Set<IGrantee> grantees = new HashSet<IGrantee>();
        UntypedResultSet resultSet = selectRole(role);
        if (!resultSet.isEmpty())
        {
            Set<String> granteeIds = resultSet.one().getSet("grantees", UTF8Type.instance);
            if (granteeIds != null)
                for (String granteeId : granteeIds)
                    grantees.add(Grantee.fromId(granteeId));
        }
        return grantees;
    }

    private static UntypedResultSet selectRoles()
    {
        try
        {
            return QueryProcessor.process(String.format("SELECT role FROM %s.%s", Auth.AUTH_KS, ROLES_CF), ConsistencyLevel.LOCAL_ONE);
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static UntypedResultSet selectGrant(IGrantee grantee)
    {
        try
        {
            ResultMessage.Rows rows = selectGrantStatement.execute(QueryState.forInternalCalls(),
                                                                   QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE,
                                                                                                 Lists.newArrayList(ByteBufferUtil.bytes(grantee.getId()))));
            return UntypedResultSet.create(rows.result);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * This will recursively (optional) go through the role heirarchy and produce a set of all
     * the roles that the grantee supports
     */
    private static Set<Role> getGrantedRoles(Set<Role> roles, IGrantee grantee, boolean recursive)
    {
        UntypedResultSet grant = selectGrant(grantee);
        if (!grant.isEmpty())
        {
            for (String rolename : grant.one().getSet("roles", UTF8Type.instance))
            {
                Role role = Grantee.asRole(rolename);
                // Protect against circular dependencies. This shouldn't be possible because of the statement validation
                // but it doesn't stop someone manually adding to the tables
                if (roles.contains(role))
                    throw new RuntimeException(String.format("Detected circular dependency in system_auth.grants. %s is already in the role chain", rolename));
                roles.add(role);
                if (recursive)
                    getGrantedRoles(roles, role, recursive);
            }
        }
        return roles;
    }

    /**
     * IMigrationListener implementation that cleans up permissions on dropped resources.
     */
    public static class MigrationListener implements IMigrationListener
    {
        public void onDropKeyspace(String ksName)
        {
            DatabaseDescriptor.getAuthorizer().revokeAll(DataResource.keyspace(ksName));
        }

        public void onDropColumnFamily(String ksName, String cfName)
        {
            DatabaseDescriptor.getAuthorizer().revokeAll(DataResource.columnFamily(ksName, cfName));
        }

        public void onDropUserType(String ksName, String userType)
        {
        }

        public void onCreateKeyspace(String ksName)
        {
        }

        public void onCreateColumnFamily(String ksName, String cfName)
        {
        }

        public void onCreateUserType(String ksName, String userType)
        {
        }

        public void onUpdateKeyspace(String ksName)
        {
        }

        public void onUpdateColumnFamily(String ksName, String cfName)
        {
        }

        public void onUpdateUserType(String ksName, String userType)
        {
        }
    }
}

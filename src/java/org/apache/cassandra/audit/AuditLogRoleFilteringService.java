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

package org.apache.cassandra.audit;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * {@code AuditLogRoleFilteringService} keeps an in‑memory cache of the <em>audit_users</em> table that tells
 * {@code AuditLogManager} which roles should have their CQL statements written to the audit log – and
 * with which sampling rate.  The cache is refreshed periodically by a task scheduled on
 * {@link ScheduledExecutors#scheduledTasks}.
 *
 * <p>The class is a <strong>singleton</strong> exposed through {@link #instance}.  Its lifecycle is:</p>
 * <ol>
 *   <li>{@link #initialize()} initialise the service.</li>
 *   <li>{@link #setup()} – start the periodic refresher.</li>
 *   <li>{@link #teardown()} – cancel the refresher so the JVM can exit cleanly or the service be
 *       re‑initialised.</li>
 * </ol>
 *
 * <p>Once {@linkplain #setup() set up}, callers can query the cache through
 * {@link #shouldLog(String)} and {@link #getAccountType(String)}.</p>
 */
public class AuditLogRoleFilteringService
{

    private static final String ROLE_COLUMN = "role";
    private static final String ACCOUNT_TYPE_COLUMN = "account_type";
    private static final String FILTER_PERCENT_COLUMN = "filter_percent";

    /**
     * Value object that holds the audit-logging configuration for a role.
     */
    public static class UserProp
    {
        /** Either {@code "SERVICE"} or {@code "DEVELOPER"}. */
        public final String accountType;
        /** Percentage (0.0–100.0) of statements to log for the role. */
        public final Double filterPercent;

        public String role;

        public UserProp(String role, String accountType, Double filterPercent)
        {
            this(accountType, filterPercent);
            this.role = role;
        }

        public UserProp(String accountType, double filterPercent)
        {
            this.accountType = accountType;
            this.filterPercent = filterPercent;
        }

        public List<String> toList()
        {
            return List.of(role, accountType, String.valueOf(filterPercent));
        }
        
        @Override
        public String toString()
        {
            return "{\"accountType\": \"" + accountType + "\", \"filterPercent\"=" + filterPercent + '}';
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof UserProp)) {
                return false;
            }
            UserProp prop = (UserProp) obj;

            return
            this.accountType.equals(prop.accountType) &&
            this.filterPercent.equals(prop.filterPercent);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(accountType, filterPercent);
        }
    };

    public enum State {
        READY,
        INIT,
        CREATED,
        REFRESH_FAILED
    }

    /** Handle to the periodic cache-refresh task. */
    private ScheduledFuture<?> refreshTask;

    private static final Logger logger = LoggerFactory.getLogger(AuditLogRoleFilteringService.class);

    private static final String INSERT_AUDIT_USER_ROLE_CQL = "INSERT INTO %s.%s (role, account_type, filter_percent) " +
                                                             "VALUES ('%s', '%s', %s)";
    private static final String UPDATE_AUDIT_USER_ROLE_CQL = "UPDATE %s.%s " +
                                                             "SET account_type = '%s', filter_percent = %s " +
                                                             "WHERE role = '%s'";
    private static final String DELETE_AUDIT_USER_ROLE_CQL = "DELETE FROM %s.%s WHERE role in (%s)";

    @VisibleForTesting
    protected volatile State state = State.CREATED;

    @VisibleForTesting
    protected final AtomicBoolean initCalled = new AtomicBoolean(false);

    @VisibleForTesting
    protected final AtomicBoolean startingRefresh = new AtomicBoolean(false);

    private final static String SELECT_QUERY = String.format(
        "SELECT * FROM %s.%s",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUDIT_USER);
    private final static String INSERT_CASSANDRA_ROLE_QUERY = String.format(
        "INSERT INTO %s.%s (role, account_type, filter_percent) VALUES ('cassandra', 'SERVICE', 0.01)",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUDIT_USER);
    private final static String INSERT_PINGLESS_ROLE_QUERY = String.format(
        "INSERT INTO %s.%s (role, account_type, filter_percent) VALUES ('pingless', 'SERVICE', 0.01)",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUDIT_USER);
    private final static String INSERT_ODIN_WORKER_ROLE_QUERY = String.format(
        "INSERT INTO %s.%s (role, account_type, filter_percent) VALUES ('odin_worker', 'SERVICE', 0.01)",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUDIT_USER);
    private final static String INSERT_UQL_ROLE_QUERY = String.format(
        "INSERT INTO %s.%s (role, account_type, filter_percent) VALUES ('uql', 'SERVICE', 100.0)",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUDIT_USER);
    private final static String INSERT_DOSA_ROLE_QUERY = String.format(
        "INSERT INTO %s.%s (role, account_type, filter_percent) VALUES ('dosa', 'SERVICE', 0.01)",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUDIT_USER);

    private static ConsistencyLevel cl = ConsistencyLevel.ONE;
    private static SelectStatement selectStatement;
    private static ModificationStatement insertCassandraRoleStatement;
    private static ModificationStatement insertPinglessRoleStatement;
    private static ModificationStatement insertOdinWorkerRoleStatement;
    private static ModificationStatement insertDosaRoleStatement;
    private static ModificationStatement insertUqlRoleStatement;
    private static ConcurrentHashMap<String, UserProp> auditUserCache = new ConcurrentHashMap<>();

    /** The singleton instance of the service. */
    public final static AuditLogRoleFilteringService instance = new AuditLogRoleFilteringService();
    private AuditLogRoleFilteringService() {}

    /** Helper function to atomically update the cache */
    private void updateAuditUserCache(String role, String accountType, double percentage)
    {
        UserProp newProp = new UserProp(role, accountType, percentage);
        // Insert if the cache does not contain the role
        if (!auditUserCache.containsKey(role))
        {
            auditUserCache.putIfAbsent(role, newProp);
            return;
        }

        // Insert if the role value has changed
        if (!auditUserCache.get(role).equals(newProp))
        {
            auditUserCache.replace(role, auditUserCache.get(role), newProp);
        }
    }

    /**
     * Convert the auditUserCache into a List<String>, but only for roles
     * whose key is present in the input roles list
     *
     * @param roles list of role keys that we want to filter form the
     *              cache
     * @return      list of roles in the cahce filtered by roles; empty if
     *              the cache is not READY; all roles in the cahce if
     *              roles is nil or empty
     */
    public List<String> filterRoles(List<String> roles)
    {
        if (roles == null) {
            roles = new ArrayList<>();
        }
        if (state != State.READY) {
            return new ArrayList<>();
        }

        Set<String> roleMap = new HashSet<>(roles);

        return auditUserCache.keySet()
                             .stream()
                             .filter(userProp -> roleMap.isEmpty() || roleMap.contains(userProp))
                             .collect(Collectors.toCollection(ArrayList::new));
    }

    /**
     * Convert the auditUserCache into a List<List<String>> of the shape
     * List.of(Role, accountType, filterPercent), but only for oles whose
     * key is present in the input roles list.
     *
     * @param roles  list of role keys that we want to filter form the
     *               cache
     * @return       nested list sorted alphabetically by role name,
     *               filtered by roles; empty if the cache is not READY
     *               up; all roles in the cahce if roles is nil or empty
     */
    public List<List<String>> toNestedList(List<String> roles) {
        if (roles == null) {
            roles = new ArrayList<>();
        }
        if (state != State.READY) {
            return new ArrayList<>();
        }

        Set<String> roleMap = new HashSet<>(roles);

        return auditUserCache.entrySet()
                             .stream()
                             /* If roles is empty, include all results */
                             .filter(e -> roleMap.isEmpty() || roleMap.contains(e.getKey()))
                             .sorted(Map.Entry.comparingByKey())
                             .map(e -> e.getValue().toList())
                             .collect(Collectors.toCollection(ArrayList::new));

    }

    /**
     * Insert a role into the `system_distributed.audit_users` table
     *
     * @param role          the role to insert
     * @param accountType   the account type
     * @param percentage    the filter percentage
     */
    public void insertRole(String role, String accountType, double percentage)
    {
        String cql = String.format(INSERT_AUDIT_USER_ROLE_CQL,
                                   SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUDIT_USER,
                                   role, accountType, percentage);
        ModificationStatement stmt = (ModificationStatement) QueryProcessor.getStatement(cql,
                                                                                         ClientState.forInternalCalls());
        stmt.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null),
                      Dispatcher.RequestTime.forImmediateExecution());
    }

    /**
     * Update a role into the `system_distributed.audit_users` table
     *
     * @param role          the role to insert
     * @param accountType   the account type
     * @param percentage    the filter percentage
     */
    public void updateRole(String role, String accountType, double percentage)
    {
        String cql = String.format(UPDATE_AUDIT_USER_ROLE_CQL,
                                   SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUDIT_USER,
                                   accountType, percentage, role);
        ModificationStatement stmt = (ModificationStatement) QueryProcessor.getStatement(cql,
                                                                                         ClientState.forInternalCalls());
        stmt.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null),
                     Dispatcher.RequestTime.forImmediateExecution());
    }

    /**
     * Removes a role from the `system_distributed.audit_users` table
     *
     * @param roles     a comma seperated string of roles to remove
     * @param ifExists  if true, will enforce deletion using "if exists"
     */
    public void deleteRoles(List<String> roles, boolean ifExists)
    {
        String rolesToDelete = roles.stream()
                                            .map(e -> '\'' + e + '\'')
                                            .collect(Collectors.joining(","));

        String cql = String.format(DELETE_AUDIT_USER_ROLE_CQL,
                                   SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUDIT_USER,
                                   rolesToDelete + (ifExists ? "IF EXISTS" : ""));
        ModificationStatement stmt = (DeleteStatement) QueryProcessor.getStatement(cql, ClientState.forInternalCalls());
        stmt.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null),
                     Dispatcher.RequestTime.forImmediateExecution());
    }

    /**
     * Initializes the service
     *
     * <p>The method performs these steps:</p>
     * <ol>
     *   <li>Prepare CQL {@link SelectStatement} / {@link ModificationStatement modification statements}.</li>
     *   <li>Populate <em>audit_users</em> with a handful of essential service roles (idempotent <code>IF NOT EXISTS</code>).</li>
     *   <li>Choose an appropriate {@link ConsistencyLevel} depending on the replication strategy of
     *       {@code system_distributed}.</li>
     * </ol>
     */
    public void initialize()
    {
        if (!initCalled.getAndSet(true))
        {
            try
            {
                selectStatement = (SelectStatement) QueryProcessor.getStatement(SELECT_QUERY, ClientState.forInternalCalls());
                insertCassandraRoleStatement = (ModificationStatement) QueryProcessor.getStatement(INSERT_CASSANDRA_ROLE_QUERY,
                                                                                                   ClientState.forInternalCalls());
                insertPinglessRoleStatement = (ModificationStatement) QueryProcessor.getStatement(INSERT_PINGLESS_ROLE_QUERY,
                                                                                                  ClientState.forInternalCalls());
                insertOdinWorkerRoleStatement = (ModificationStatement) QueryProcessor.getStatement(INSERT_ODIN_WORKER_ROLE_QUERY,
                                                                                                    ClientState.forInternalCalls());
                insertUqlRoleStatement = (ModificationStatement) QueryProcessor.getStatement(INSERT_UQL_ROLE_QUERY,
                                                                                             ClientState.forInternalCalls());
                insertDosaRoleStatement = (ModificationStatement) QueryProcessor.getStatement(INSERT_DOSA_ROLE_QUERY,
                                                                                              ClientState.forInternalCalls());

                insertCassandraRoleStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null),
                                                     Dispatcher.RequestTime.forImmediateExecution());
                insertPinglessRoleStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null),
                                                    Dispatcher.RequestTime.forImmediateExecution());
                insertOdinWorkerRoleStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null),
                                                      Dispatcher.RequestTime.forImmediateExecution());
                insertUqlRoleStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null),
                                               Dispatcher.RequestTime.forImmediateExecution());
                insertDosaRoleStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null),
                                                Dispatcher.RequestTime.forImmediateExecution());

                Keyspace ks = Schema.instance.getKeyspaceInstance(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME);
                if (ks.getReplicationStrategy().getClass() == NetworkTopologyStrategy.class)
                    cl = ConsistencyLevel.LOCAL_ONE;
                initCalled.set(true);
                logger.info("Initialized");
                state = State.INIT;
            }
            catch (Exception e)
            {
                logger.error("Failed to initialize:", e);
                initCalled.compareAndSet(true,false);
            }
        }


    }

    @VisibleForTesting
    public State getState() {
        return state;
    }

    /**
     * Starts the background cache refresher
     */
    public synchronized void setup()
    {
        try
        {
            if (!startingRefresh.getAndSet(true))
            {
                // only start the refresh task if it has not beeen started or doesn't exist
                if (refreshTask == null || refreshTask.isCancelled() || refreshTask.isDone())
                {
                    // Refresh cache every 5 minutes with an initial delay of 2 seconds.
                    refreshTask = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(() -> refresh(),
                                                                                           2,
                                                                                           300,
                                                                                           TimeUnit.SECONDS);
                    logger.info("RefreshTask started");
                }
                startingRefresh.set(false);
            }
        }
        catch (Exception e)
        {
            logger.error("Exception in audit user refresh task setup:", e);
        }
    }

    /**
     * Reloads the <em>audit_users</em> table into the in‑memory cache.
     * <p>The method is called automatically by the scheduled refresher but can also be invoked manually.
     * Any CQL or runtime exception is caught and logged; the cache is left unchanged in that case.</p>
     */
    public void refresh()
    {
        if (state == State.CREATED) {
            return;
        }
        try
        {
            // Refresh cache
            ResultMessage.Rows rows = selectStatement.execute(QueryState.forInternalCalls(),
                    QueryOptions.forInternalCalls(cl, null), Dispatcher.RequestTime.forImmediateExecution());
            UntypedResultSet result = UntypedResultSet.create(rows.result);
            Set<String> rolesInTable = new HashSet<>();
            for (UntypedResultSet.Row row : result)
            {
                if (!row.has(ROLE_COLUMN) || !row.has(ACCOUNT_TYPE_COLUMN) || !row.has(FILTER_PERCENT_COLUMN))
                {
                    logger.warn("Skipping row - " + row);
                    continue;
                }

                String role = row.getString(ROLE_COLUMN);
                String accountType = row.getString(ACCOUNT_TYPE_COLUMN);
                double percentage = row.getDouble(FILTER_PERCENT_COLUMN);

                rolesInTable.add(role);

                // Atomically update the cache
                updateAuditUserCache(role, accountType, percentage);
            }
            auditUserCache.keySet().removeIf(role -> !rolesInTable.contains(role));
            state = State.READY;
        }
        catch (Exception e)
        {
            state = State.REFRESH_FAILED;
            logger.error("Exception in audit user cache refresh:", e);
        }

        logger.info("FOOBAR: The cache now has - "+ auditUserCache);
    }

    /**
     * Stops the background refresher task. If the task does not exist, perfoms a no-op
     */
    public synchronized void teardown() {
        if (refreshTask != null && !refreshTask.isCancelled())
            refreshTask.cancel(true);
        refreshTask = null;

        if (state != State.CREATED)
            state = State.INIT;
        this.initCalled.set(false);
        logger.info("Audit users cache teardown complete");
    }

    /**
     * Decide probabilistically whether a statement executed by the supplied role should be written to the audit log.
     *
     * @param role the CQL role that executed the statement (maybe {@code null})
     * @return {@code true} if the statement should be logged; {@code false} otherwise
     */
    public boolean shouldLog(String role)
    {
        // TODO: remove once conf.audit_user_cache_enabled is deprecated and removed
        if (!DatabaseDescriptor.getAuditUserCacheEnabled()) {
            return true;
        }

        // TODO: while the cache is not ready, fail open for specific event types
        if (state != State.READY || role == null)
            return false;

        UserProp prop = auditUserCache.get(role);
        if (prop == null)
            return false;

        // Log with probability - clamp filterPercent
        return ThreadLocalRandom.current().nextDouble(0, 1) <
               ((Math.max(0.0, Math.min(100.0, prop.filterPercent))) / 100.0);
    }

    /**
     * Get the account type of role as cached by this service.
     *
     * @param role the CQL role name – case‑sensitive and must match the entry in <em>audit_users</em>
     * @return the {@code account_type} string or an empty string if the role is unknown or the cache has not READY
     */
    public String getAccountType(String role)
    {
        if (role == null || state != State.READY)
            return "";

        UserProp prop = auditUserCache.get(role);
        return prop != null ? prop.accountType : "";
    }

    /**
     * Insert an entry directly into the in‑memory cache – <strong>for testing only</strong>.
     *
     * @param role    role name
     * @param type    either {@code "SERVICE"} or {@code "DEVELOPER"}
     * @param percent sampling rate (0–100)
     */
    protected void insert(String role, String type, double percent)
    {
        auditUserCache.put(role, new UserProp(role, type, percent));
    }
}

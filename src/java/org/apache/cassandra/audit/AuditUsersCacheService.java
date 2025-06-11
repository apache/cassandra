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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;

/**
 * {@code AuditUsersCacheService} keeps an in‑memory cache of the <em>audit_users</em> table that tells
 * {@code AuditLogManager} which roles should have their CQL statements written to the audit log – and
 * with which sampling rate.  The cache is refreshed periodically by a task scheduled on
 * {@link ScheduledExecutors#scheduledTasks}.
 *
 * <p>The class is a <strong>singleton</strong> exposed through {@link #instance}.  Its lifecycle is:</p>
 * <ol>
 *   <li>{@link #setup()} – initialise the service and start the periodic refresher.</li>
 *   <li>{@link #teardown()} – cancel the refresher so the JVM can exit cleanly or the service be
 *       re‑initialised.</li>
 * </ol>
 *
 * <p>Once {@linkplain #setup() set up}, callers can query the cache through
 * {@link #shouldLog(String)} and {@link #getAccountType(String)}.</p>
 */
public class AuditUsersCacheService
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
        public String accountType;
        /** Percentage (0.0–100.0) of statements to log for the role. */
        public Double filterPercent;

        public UserProp(String type, double percent)
        {
            accountType = type;
            filterPercent = percent;
        }
    };

    /** Handle to the periodic cache-refresh task. */
    private ScheduledFuture<?> refreshTask;

    private static final Logger logger = LoggerFactory.getLogger(AuditUsersCacheService.class);

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

    /** Flag that becomes {@code true} once the first refresh cycle has completed. */
    private volatile boolean cacheWarmedUp = false;

    /** The singleton instance of the service. */
    public final static AuditUsersCacheService instance = new AuditUsersCacheService();
    private AuditUsersCacheService() {}

    /** Helper function to atomically update the cache */
    private void updateAuditUserCache(String role, String accountType, double percentage)
    {
        UserProp newProp = new UserProp(accountType, percentage);
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
     * Initialises the service and starts the background cache refresher.
     * <p>The method performs these steps:</p>
     * <ol>
     *   <li>Prepare CQL {@link SelectStatement} / {@link ModificationStatement modification statements}.</li>
     *   <li>Populate <em>audit_users</em> with a handful of essential service roles (idempotent <code>IF NOT EXISTS</code>).</li>
     *   <li>Choose an appropriate {@link ConsistencyLevel} depending on the replication strategy of
     *       {@code system_distributed}.</li>
     *   <li>Schedule {@link #refresh()} to execute every five minutes if a task does not already exist.</li>
     * </ol>
     */
    public synchronized void setup()
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

        insertCassandraRoleStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null), Dispatcher.RequestTime.forImmediateExecution());
        insertPinglessRoleStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null), Dispatcher.RequestTime.forImmediateExecution());
        insertOdinWorkerRoleStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null), Dispatcher.RequestTime.forImmediateExecution());
        insertUqlRoleStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null), Dispatcher.RequestTime.forImmediateExecution());
        insertDosaRoleStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null), Dispatcher.RequestTime.forImmediateExecution());

        Keyspace ks = Schema.instance.getKeyspaceInstance(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME);
        if (ks.getReplicationStrategy().getClass() == NetworkTopologyStrategy.class)
            cl = ConsistencyLevel.LOCAL_ONE;

        // only start the refresh task if it has not beeen started
        if (refreshTask == null)
        {
            // Refresh cache every 5 minutes with an initial delay of 2 seconds.
            refreshTask = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(() -> refresh(),
                                                                                   2,
                                                                                   300,
                                                                                   TimeUnit.SECONDS);
        }
    }

    /**
     * Reloads the <em>audit_users</em> table into the in‑memory cache.
     * <p>The method is called automatically by the scheduled refresher but can also be invoked manually.
     * Any CQL or runtime exception is caught and logged; the cache is left unchanged in that case.</p>
     */
    public void refresh()
    {
        try
        {
            // Refresh cache
            ResultMessage.Rows rows = selectStatement.execute(QueryState.forInternalCalls(),
                    QueryOptions.forInternalCalls(cl, null), Dispatcher.RequestTime.forImmediateExecution());
            UntypedResultSet result = UntypedResultSet.create(rows.result);
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

                // Atomically update the cache
                updateAuditUserCache(role, accountType, percentage);
            }
            cacheWarmedUp = true;
        }
        catch (Exception e)
        {
            logger.error("Exception in audit user cache refresh:", e);
        }
    }

    /**
     * Stops the background refresher task.
     */
    public synchronized void teardown() {
        if (refreshTask != null && !refreshTask.isCancelled())
            refreshTask.cancel(false);
        refreshTask = null;
        cacheWarmedUp = false;
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

        if (role == null || !cacheWarmedUp)
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
     * @return the {@code account_type} string or an empty string if the role is unknown or the cache has not warmed up
     */
    public String getAccountType(String role)
    {
        if (role == null || !cacheWarmedUp)
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
        auditUserCache.put(role, new UserProp(type, percent));
    }
}

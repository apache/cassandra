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
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;

/*
 * Create cache of audit users with filter percentage indicates percentage of logs to be filtered for each user/role
 * Cacahe is built by thread by reading table audit_users in system_distributed keyspace
 * Cache will keep following information
 *  a. role / User
 *  b. type of role (developer/service)
 *  b. filter percentage
 *  <p>
 *  Thread will periodically read table system_distributed.audit_users and update cache.
 *  It is not mandatory to have each configured role to be present in cache/table. It is possible that roles in audit_users and
 *  auth_roles are out of sync. If filter_percentage is not configured for role then by default Audit log manager will log
 *  all queries for role.
 *  This cache is referred only by AuditLogManager to decide whether to filter audit log for user.
 */
public class AuditUsersCacheService
{
    /*
     * class to store properties of user
     * 1. Type of account (developer/service)
     * 2. Filter percentage
     */
    public class UserProp
    {
        public String accountType;
        public Double filterPercent;

        public UserProp(String type, double percent)
        {
            accountType = type;
            filterPercent = percent;
        }
    };

    private static final Logger logger = LoggerFactory.getLogger(AuditUsersCacheService.class);

    private final static String SELECT_QUERY = String.format(
    "SELECT * FROM %s.%s"
            , SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUDIT_USER);
    private final static String INSERT_CASSANDRA_ROLE_QUERY = String.format(
            "INSERT INTO %s.%s (role, account_type, filter_percent) VALUES " +
                    "('cassandra', 'developer', 100.0)"
            , SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUDIT_USER);
    private final static String INSERT_PINGLESS_ROLE_QUERY = String.format(
            "INSERT INTO %s.%s (role, account_type, filter_percent) VALUES " +
                    "('pingless', 'service', 0.01)"
            , SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUDIT_USER);
    private final static String INSERT_ODIN_WORKER_ROLE_QUERY = String.format(
            "INSERT INTO %s.%s (role, account_type, filter_percent) VALUES " +
                    "('odin_worker', 'service', 0.01)"
            , SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUDIT_USER);

    private static ConsistencyLevel cl = ConsistencyLevel.ONE;
    private static SelectStatement selectStatement;
    private static ModificationStatement insertCassandraRoleStatement;
    private static ModificationStatement insertPinglessRoleStatement;
    private static ModificationStatement insertOdinWorkerRoleStatement;
    private static ConcurrentHashMap<String, UserProp> auditUserCache = new ConcurrentHashMap<String, UserProp>();
    /*
     * When node restarts, there will be time when cache is not warmed up, this indicates
     * status of cache warmup
    */
    private boolean cacheWarmedUp = false;

    public final static AuditUsersCacheService instance = new AuditUsersCacheService();
    private AuditUsersCacheService()
    {
    }

    public void setup()
    {
        selectStatement = (SelectStatement) QueryProcessor.getStatement(SELECT_QUERY, ClientState.forInternalCalls());
        insertCassandraRoleStatement = (ModificationStatement) QueryProcessor.getStatement(INSERT_CASSANDRA_ROLE_QUERY,
                ClientState.forInternalCalls());
        insertPinglessRoleStatement = (ModificationStatement) QueryProcessor.getStatement(INSERT_PINGLESS_ROLE_QUERY,
                ClientState.forInternalCalls());
        insertOdinWorkerRoleStatement = (ModificationStatement) QueryProcessor.getStatement(INSERT_ODIN_WORKER_ROLE_QUERY,
                ClientState.forInternalCalls());

        insertCassandraRoleStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null), System.nanoTime());
        insertPinglessRoleStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null), System.nanoTime());
        insertOdinWorkerRoleStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, null), System.nanoTime());

        Keyspace ks = Schema.instance.getKeyspaceInstance(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME);
        if (ks.getReplicationStrategy().getClass() == NetworkTopologyStrategy.class)
        {
            cl = ConsistencyLevel.LOCAL_ONE;
        }

        //refresh cache every 5 minutes
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(() -> refresh(),
                2,
                300,
                TimeUnit.SECONDS);

    }

    public void refresh()
    {
        try
        {
            // Refresh cache
            ResultMessage.Rows rows = selectStatement.execute(QueryState.forInternalCalls(),
                    QueryOptions.forInternalCalls(cl, null), System.nanoTime());
            UntypedResultSet result = UntypedResultSet.create(rows.result);
            for (UntypedResultSet.Row row : result)
            {
                String role = row.getString("role");
                String accountType = row.getString("account_type");
                Double percentage = row.getDouble("filter_percent");


                // If role not present in cache
                if (!auditUserCache.containsKey(role))
                {
                    AuditUsersCacheService.UserProp value = new AuditUsersCacheService.UserProp(accountType, percentage);
                    auditUserCache.putIfAbsent(role, value);
                    continue;
                }


                // Ignore if its in AuditCache and filter_percent is same
                AuditUsersCacheService.UserProp currVal = auditUserCache.get(role);
                if (currVal.filterPercent == percentage)
                {
                    continue;
                }

                // Update filter_percent
                AuditUsersCacheService.UserProp newValue = new AuditUsersCacheService.UserProp(accountType, percentage);
                auditUserCache.replace(role, currVal, newValue);
            }

            if (!cacheWarmedUp)
            {
                cacheWarmedUp = true;
            }
        }

        catch (Exception e)
        {
            logger.error("Exception in audit user cache refresh:", e);
        }

    }

    /**
     * shouldLog returns if query should be logged for user
     * @param : role - user
     * returns: true if query should be logged else false
     *
     * function checks of role/user is present in cache.
     * If role is absent then return false
     * If role is present then check filterPercent_percentage and decide whether to
     * log or not based on probability.
     */
    public boolean shouldLog(String role)
    {
        if (role == null || !cacheWarmedUp || !auditUserCache.containsKey(role))
        {
            return false;
        }

        double filter_percent = Double.valueOf(auditUserCache.get(role).filterPercent);

        // Log with probability
        double prob = filter_percent / 100.0;
        double random_value = ThreadLocalRandom.current().nextDouble(0, 1);
        if (random_value >= prob)
        {
            return false;
        }

        return true;
    }

    /**
     * getAccountType returns type of account (service/developer) for given user
     * @param: role(username)
     * returns: accountType if user is present in cache else returns ""
     */
    public String getAccountType(String role)
    {
        String accountType = "";

        if (role == null || !cacheWarmedUp || !auditUserCache.containsKey(role))
        {
            return accountType;
        }

        return auditUserCache.get(role).accountType;
    }

    /**
     *  Should be used only for Testing
     */
    public void insert(String role, String type, double percent)
    {
        AuditUsersCacheService.UserProp value = new AuditUsersCacheService.UserProp(type, percent);
        auditUserCache.putIfAbsent(role, value);
    }
}

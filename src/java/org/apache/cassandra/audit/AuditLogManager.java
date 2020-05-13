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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.AuthEvents;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryEvents;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Central location for managing the logging of client/user-initated actions (like queries, log in commands, and so on).
 *
 */
public class AuditLogManager implements QueryEvents.Listener, AuthEvents.Listener
{
    private static final Logger logger = LoggerFactory.getLogger(AuditLogManager.class);
    public static final AuditLogManager instance = new AuditLogManager();

    // auditLogger can write anywhere, as it's pluggable (logback, BinLog, DiagnosticEvents, etc ...)
    private volatile IAuditLogger auditLogger;

    private volatile AuditLogFilter filter;

    private AuditLogManager()
    {
        final AuditLogOptions auditLogOptions = DatabaseDescriptor.getAuditLoggingOptions();

        if (auditLogOptions.enabled)
        {
            logger.info("Audit logging is enabled.");
            auditLogger = getAuditLogger(auditLogOptions.logger);
        }
        else
        {
            logger.debug("Audit logging is disabled.");
            auditLogger = new NoOpAuditLogger(Collections.emptyMap());
        }

        filter = AuditLogFilter.create(auditLogOptions);
    }

    public void initialize()
    {
        if (DatabaseDescriptor.getAuditLoggingOptions().enabled)
            registerAsListener();
    }

    private IAuditLogger getAuditLogger(ParameterizedClass logger) throws ConfigurationException
    {
        if (logger.class_name != null)
        {
            return FBUtilities.newAuditLogger(logger.class_name, logger.parameters == null ? Collections.emptyMap() : logger.parameters);
        }

        return FBUtilities.newAuditLogger(BinAuditLogger.class.getName(), Collections.emptyMap());
    }

    @VisibleForTesting
    public IAuditLogger getLogger()
    {
        return auditLogger;
    }

    public boolean isEnabled()
    {
        return auditLogger.isEnabled();
    }

    /**
     * Logs AudigLogEntry to standard audit logger
     * @param logEntry AuditLogEntry to be logged
     */
    private void log(AuditLogEntry logEntry)
    {
        if (!filter.isFiltered(logEntry))
        {
            auditLogger.log(logEntry);
        }
    }

    private void log(AuditLogEntry logEntry, Exception e)
    {
        AuditLogEntry.Builder builder = new AuditLogEntry.Builder(logEntry);

        if (e instanceof UnauthorizedException)
        {
            builder.setType(AuditLogEntryType.UNAUTHORIZED_ATTEMPT);
        }
        else if (e instanceof AuthenticationException)
        {
            builder.setType(AuditLogEntryType.LOGIN_ERROR);
        }
        else
        {
            builder.setType(AuditLogEntryType.REQUEST_FAILURE);
        }

        builder.appendToOperation(e.getMessage());

        log(builder.build());
    }

    /**
     * Disables AuditLog, designed to be invoked only via JMX/ Nodetool, not from anywhere else in the codepath.
     */
    public synchronized void disableAuditLog()
    {
        unregisterAsListener();
        IAuditLogger oldLogger = auditLogger;
        auditLogger = new NoOpAuditLogger(Collections.emptyMap());
        oldLogger.stop();
    }

    /**
     * Enables AuditLog, designed to be invoked only via JMX/ Nodetool, not from anywhere else in the codepath.
     * @param auditLogOptions AuditLogOptions to be used for enabling AuditLog
     * @throws ConfigurationException It can throw configuration exception when provided logger class does not exist in the classpath
     */
    public synchronized void enable(AuditLogOptions auditLogOptions) throws ConfigurationException
    {
        // always reload the filters
        filter = AuditLogFilter.create(auditLogOptions);

        // next, check to see if we're changing the logging implementation; if not, keep the same instance and bail.
        // note: auditLogger should never be null
        IAuditLogger oldLogger = auditLogger;
        if (oldLogger.getClass().getSimpleName().equals(auditLogOptions.logger.class_name))
            return;

        auditLogger = getAuditLogger(auditLogOptions.logger);

        // note that we might already be registered here and we rely on the fact that Query/AuthEvents have a Set of listeners
        registerAsListener();

        // ensure oldLogger's stop() is called after we swap it with new logger,
        // otherwise, we might be calling log() on the stopped logger.
        oldLogger.stop();
    }

    private void registerAsListener()
    {
        QueryEvents.instance.registerListener(this);
        AuthEvents.instance.registerListener(this);
    }

    private void unregisterAsListener()
    {
        QueryEvents.instance.unregisterListener(this);
        AuthEvents.instance.unregisterListener(this);
    }

    public void querySuccess(CQLStatement statement, String query, QueryOptions options, QueryState state, long queryTime, Message.Response response)
    {
        AuditLogEntry entry = new AuditLogEntry.Builder(state).setType(statement.getAuditLogContext().auditLogEntryType)
                                                              .setOperation(query)
                                                              .setTimestamp(queryTime)
                                                              .setScope(statement)
                                                              .setKeyspace(state, statement)
                                                              .setOptions(options)
                                                              .build();
        log(entry);
    }

    public void queryFailure(CQLStatement stmt, String query, QueryOptions options, QueryState state, Exception cause)
    {
        AuditLogEntry entry = new AuditLogEntry.Builder(state).setOperation(query)
                                                              .setOptions(options)
                                                              .build();
        log(entry, cause);
    }

    public void executeSuccess(CQLStatement statement, String query, QueryOptions options, QueryState state, long queryTime, Message.Response response)
    {
        AuditLogEntry entry = new AuditLogEntry.Builder(state).setType(statement.getAuditLogContext().auditLogEntryType)
                                                              .setOperation(query)
                                                              .setTimestamp(queryTime)
                                                              .setScope(statement)
                                                              .setKeyspace(state, statement)
                                                              .setOptions(options)
                                                              .build();
        log(entry);
    }

    public void executeFailure(CQLStatement statement, String query, QueryOptions options, QueryState state, Exception cause)
    {
        AuditLogEntry entry = null;
        if (cause instanceof PreparedQueryNotFoundException)
        {
            entry = new AuditLogEntry.Builder(state).setOperation(query == null ? "null" : query)
                                                                  .setOptions(options)
                                                                  .build();
        }
        else if (statement != null)
        {
            entry = new AuditLogEntry.Builder(state).setOperation(query == null ? statement.toString() : query)
                                                                  .setType(statement.getAuditLogContext().auditLogEntryType)
                                                                  .setScope(statement)
                                                                  .setKeyspace(state, statement)
                                                                  .setOptions(options)
                                                                  .build();
        }
        if (entry != null)
            log(entry, cause);
    }

    public void batchSuccess(BatchStatement.Type batchType, List<? extends CQLStatement> statements, List<String> queries, List<List<ByteBuffer>> values, QueryOptions options, QueryState state, long queryTime, Message.Response response)
    {
        List<AuditLogEntry> entries = buildEntriesForBatch(statements, queries, state, options, queryTime);
        for (AuditLogEntry auditLogEntry : entries)
        {
            log(auditLogEntry);
        }
    }

    public void batchFailure(BatchStatement.Type batchType, List<? extends CQLStatement> statements, List<String> queries, List<List<ByteBuffer>> values, QueryOptions options, QueryState state, Exception cause)
    {
        String auditMessage = String.format("BATCH of %d statements at consistency %s", statements.size(), options.getConsistency());
        AuditLogEntry entry = new AuditLogEntry.Builder(state).setOperation(auditMessage)
                                                              .setOptions(options)
                                                              .setType(AuditLogEntryType.BATCH)
                                                              .build();
        log(entry, cause);
    }

    private static List<AuditLogEntry> buildEntriesForBatch(List<? extends CQLStatement> statements, List<String> queries, QueryState state, QueryOptions options, long queryStartTimeMillis)
    {
        List<AuditLogEntry> auditLogEntries = new ArrayList<>(statements.size() + 1);
        UUID batchId = UUID.randomUUID();
        String queryString = String.format("BatchId:[%s] - BATCH of [%d] statements", batchId, statements.size());
        AuditLogEntry entry = new AuditLogEntry.Builder(state)
                              .setOperation(queryString)
                              .setOptions(options)
                              .setTimestamp(queryStartTimeMillis)
                              .setBatch(batchId)
                              .setType(AuditLogEntryType.BATCH)
                              .build();
        auditLogEntries.add(entry);

        for (int i = 0; i < statements.size(); i++)
        {
            CQLStatement statement = statements.get(i);
            entry = new AuditLogEntry.Builder(state)
                    .setType(statement.getAuditLogContext().auditLogEntryType)
                    .setOperation(queries.get(i))
                    .setTimestamp(queryStartTimeMillis)
                    .setScope(statement)
                    .setKeyspace(state, statement)
                    .setOptions(options)
                    .setBatch(batchId)
                    .build();
            auditLogEntries.add(entry);
        }

        return auditLogEntries;
    }

    public void prepareSuccess(CQLStatement statement, String query, QueryState state, long queryTime, ResultMessage.Prepared response)
    {
        AuditLogEntry entry = new AuditLogEntry.Builder(state).setOperation(query)
                                                              .setType(AuditLogEntryType.PREPARE_STATEMENT)
                                                              .setScope(statement)
                                                              .setKeyspace(statement)
                                                              .build();
        log(entry);
    }

    public void prepareFailure(@Nullable CQLStatement stmt, @Nullable String query, QueryState state, Exception cause)
    {
        AuditLogEntry entry = new AuditLogEntry.Builder(state).setOperation(query)
//                                                              .setKeyspace(keyspace) // todo: do we need this? very much special case compared to the others
                                                              .setType(AuditLogEntryType.PREPARE_STATEMENT)
                                                              .build();
        log(entry, cause);
    }

    public void authSuccess(QueryState state)
    {
        AuditLogEntry entry = new AuditLogEntry.Builder(state).setOperation("LOGIN SUCCESSFUL")
                                                              .setType(AuditLogEntryType.LOGIN_SUCCESS)
                                                              .build();
        log(entry);
    }

    public void authFailure(QueryState state, Exception cause)
    {
        AuditLogEntry entry = new AuditLogEntry.Builder(state).setOperation("LOGIN FAILURE")
                                                              .setType(AuditLogEntryType.LOGIN_ERROR)
                                                              .build();
        log(entry, cause);
    }
}

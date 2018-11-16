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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Central location for managing the logging of client/user-initated actions (like queries, log in commands, and so on).
 *
 * We can run multiple {@link IAuditLogger}s at the same time, including the standard audit logger ({@link #auditLogger}
 * and the full query logger ({@link #fullQueryLogger}.
 */
public class AuditLogManager
{
    private static final Logger logger = LoggerFactory.getLogger(AuditLogManager.class);
    private static final AuditLogManager instance = new AuditLogManager();

    // FQL always writes to a BinLog, but it is a type of IAuditLogger
    private final FullQueryLogger fullQueryLogger;
    private final ImmutableSet<AuditLogEntryCategory> fqlIncludeFilter = ImmutableSet.of(AuditLogEntryCategory.OTHER,
                                                                                         AuditLogEntryCategory.QUERY,
                                                                                         AuditLogEntryCategory.DCL,
                                                                                         AuditLogEntryCategory.DML,
                                                                                         AuditLogEntryCategory.DDL);

    // auditLogger can write anywhere, as it's pluggable (logback, BinLog, DiagnosticEvents, etc ...)
    private volatile IAuditLogger auditLogger;

    private volatile AuditLogFilter filter;
    private volatile boolean isAuditLogEnabled;

    private AuditLogManager()
    {
        fullQueryLogger = new FullQueryLogger();

        if (DatabaseDescriptor.getAuditLoggingOptions().enabled)
        {
            logger.info("Audit logging is enabled.");
            auditLogger = getAuditLogger(DatabaseDescriptor.getAuditLoggingOptions().logger);
            isAuditLogEnabled = true;
        }
        else
        {
            logger.debug("Audit logging is disabled.");
            isAuditLogEnabled = false;
            auditLogger = new NoOpAuditLogger();
        }

        filter = AuditLogFilter.create(DatabaseDescriptor.getAuditLoggingOptions());
    }

    public static AuditLogManager getInstance()
    {
        return instance;
    }

    private IAuditLogger getAuditLogger(String loggerClassName) throws ConfigurationException
    {
        if (loggerClassName != null)
        {
            return FBUtilities.newAuditLogger(loggerClassName);
        }

        return FBUtilities.newAuditLogger(BinAuditLogger.class.getName());
    }

    @VisibleForTesting
    public IAuditLogger getLogger()
    {
        return auditLogger;
    }

    public boolean isAuditingEnabled()
    {
        return isAuditLogEnabled;
    }

    public boolean isLoggingEnabled()
    {
        return isAuditingEnabled() || isFQLEnabled();
    }

    private boolean isFQLEnabled()
    {
        return fullQueryLogger.enabled();
    }

    /**
     * Logs AuditLogEntry to standard audit logger
     * @param logEntry AuditLogEntry to be logged
     */
    private void logAuditLoggerEntry(AuditLogEntry logEntry)
    {
        if (!filter.isFiltered(logEntry))
        {
            auditLogger.log(logEntry);
        }
    }

    /**
     * Logs AudigLogEntry to both FQL and standard audit logger
     * @param logEntry AuditLogEntry to be logged
     */
    public void log(AuditLogEntry logEntry)
    {
        if (logEntry == null)
            return;

        if (isAuditingEnabled())
        {
            logAuditLoggerEntry(logEntry);
        }

        if (isFQLEnabled() && fqlIncludeFilter.contains(logEntry.getType().getCategory()))
        {
            fullQueryLogger.log(logEntry);
        }
    }

    public void log(AuditLogEntry logEntry, Exception e)
    {
        if ((logEntry != null) && (isAuditingEnabled()))
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
    }

    /**
     * Logs Batch queries to both FQL and standard audit logger.
     */
    public void logBatch(BatchStatement.Type type,
                         List<Object> queryOrIdList,
                         List<List<ByteBuffer>> values,
                         List<QueryHandler.Prepared> prepared,
                         QueryOptions options,
                         QueryState state,
                         long queryStartTimeMillis)
    {
        if (isAuditingEnabled())
        {
            List<AuditLogEntry> entries = buildEntriesForBatch(queryOrIdList, prepared, state, options, queryStartTimeMillis);
            for (AuditLogEntry auditLogEntry : entries)
            {
                logAuditLoggerEntry(auditLogEntry);
            }
        }

        if (isFQLEnabled())
        {
            List<String> queryStrings = new ArrayList<>(queryOrIdList.size());
            for (QueryHandler.Prepared prepStatment : prepared)
            {
                queryStrings.add(prepStatment.rawCQLStatement);
            }
            fullQueryLogger.logBatch(type, queryStrings, values, options, state, queryStartTimeMillis);
        }
    }

    private static List<AuditLogEntry> buildEntriesForBatch(List<Object> queryOrIdList, List<QueryHandler.Prepared> prepared, QueryState state, QueryOptions options, long queryStartTimeMillis)
    {
        List<AuditLogEntry> auditLogEntries = new ArrayList<>(queryOrIdList.size() + 1);
        UUID batchId = UUID.randomUUID();
        String queryString = String.format("BatchId:[%s] - BATCH of [%d] statements", batchId, queryOrIdList.size());
        AuditLogEntry entry = new AuditLogEntry.Builder(state)
                              .setOperation(queryString)
                              .setOptions(options)
                              .setTimestamp(queryStartTimeMillis)
                              .setBatch(batchId)
                              .setType(AuditLogEntryType.BATCH)
                              .build();
        auditLogEntries.add(entry);

        for (int i = 0; i < queryOrIdList.size(); i++)
        {
            CQLStatement statement = prepared.get(i).statement;
            entry = new AuditLogEntry.Builder(state)
                    .setType(statement.getAuditLogContext().auditLogEntryType)
                    .setOperation(prepared.get(i).rawCQLStatement)
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

    /**
     * Disables AuditLog, designed to be invoked only via JMX/ Nodetool, not from anywhere else in the codepath.
     */
    public synchronized void disableAuditLog()
    {
        if (isAuditLogEnabled)
        {
            // Disable isAuditLogEnabled before attempting to cleanup/ stop AuditLogger so that any incoming log() requests will be dropped.
            isAuditLogEnabled = false;
            IAuditLogger oldLogger = auditLogger;
            auditLogger = new NoOpAuditLogger();
            oldLogger.stop();
        }
    }

    /**
     * Enables AuditLog, designed to be invoked only via JMX/ Nodetool, not from anywhere else in the codepath.
     * @param auditLogOptions AuditLogOptions to be used for enabling AuditLog
     * @throws ConfigurationException It can throw configuration exception when provided logger class does not exist in the classpath
     */
    public synchronized void enableAuditLog(AuditLogOptions auditLogOptions) throws ConfigurationException
    {
        if (isFQLEnabled() && fullQueryLogger.path().toString().equals(auditLogOptions.audit_logs_dir))
            throw new IllegalArgumentException(String.format("audit log path (%s) cannot be the same as the " +
                                                             "running full query logger (%s)",
                                                             auditLogOptions.audit_logs_dir,
                                                             fullQueryLogger.path()));

        // always reload the filters
        filter = AuditLogFilter.create(auditLogOptions);

        // next, check to see if we're changing the logging implementation; if not, keep the same instance and bail.
        // note: auditLogger should never be null
        IAuditLogger oldLogger = auditLogger;
        if (oldLogger.getClass().getSimpleName().equals(auditLogOptions.logger))
            return;

        auditLogger = getAuditLogger(auditLogOptions.logger);
        isAuditLogEnabled = true;

        // ensure oldLogger's stop() is called after we swap it with new logger,
        // otherwise, we might be calling log() on the stopped logger.
        oldLogger.stop();
    }

    public void configureFQL(Path path, String rollCycle, boolean blocking, int maxQueueWeight, long maxLogSize, String archiveCommand, int maxArchiveRetries)
    {
        if (path.equals(auditLogger.path()))
            throw new IllegalArgumentException(String.format("fullquerylogger path (%s) cannot be the same as the " +
                                                             "running audit logger (%s)",
                                                             path,
                                                             auditLogger.path()));

        fullQueryLogger.configure(path, rollCycle, blocking, maxQueueWeight, maxLogSize, archiveCommand, maxArchiveRetries);
    }

    public void resetFQL(String fullQueryLogPath)
    {
        fullQueryLogger.reset(fullQueryLogPath);
    }

    public void disableFQL()
    {
        fullQueryLogger.stop();
    }

    /**
     * ONLY FOR TESTING
     */
    FullQueryLogger getFullQueryLogger()
    {
        return fullQueryLogger;
    }
}

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

package org.apache.cassandra.sqel;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryEvents;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.PasswordObfuscator;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.apache.cassandra.utils.concurrent.WeightedQueue;
import org.apache.cassandra.utils.ObjectSizes;


// reuse the audit log entry - it is a well engineered class with all values we wish to capture
import org.apache.cassandra.audit.AuditLogEntry;
import org.apache.cassandra.audit.AuditLogEntryType;

public class SampledQueryEventLogger implements QueryEvents.Listener {
    
    protected static final Logger logger = LoggerFactory.getLogger(SampledQueryEventLogger.class);
    private volatile SampledQueryEventLoggerOptions sampledQueryEventLoggerOptions;

    public static final SampledQueryEventLogger instance = new SampledQueryEventLogger();

    private SampledQueryEventLogger() {
        sampledQueryEventLoggerOptions = DatabaseDescriptor.getSampledQueryEventLoggingOptions();
    }

    private void registerAsListener()
    {
        QueryEvents.instance.registerListener(this);
    }

    private void unregisterAsListener()
    {
        QueryEvents.instance.unregisterListener(this);
    }

    private String obfuscatePasswordInformation(Exception e, List<String> queries)
    {
        // A syntax error may reveal the password in the form of 'line 1:33 mismatched input 'secret_password''
        if (e instanceof SyntaxException && queries != null && !queries.isEmpty())
        {
            for (String query : queries)
            {
                if (query.toLowerCase().contains(PasswordObfuscator.PASSWORD_TOKEN))
                    return "Syntax Exception. Obscured for security reasons.";
            }
        }

        return PasswordObfuscator.obfuscate(e.getMessage());
    }

    private void log(AuditLogEntry logEntry, Exception e)
    {
        this.log(logEntry, e, null);
    }

    private void log(AuditLogEntry logEntry, Exception e, List<String> queries)
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

        builder.appendToOperation(obfuscatePasswordInformation(e, queries));

        this.log(builder.build());
    }

    public void log(AuditLogEntry logEntry)
    {
        this.logger.info(logEntry.getLogString());
    }
    
    public boolean shouldLog(double rate)
    {
        return ThreadLocalRandom.current().nextDouble() < rate;
    }

    public synchronized void stop()
    {
        logger.info("Stopping sampled query event logger");
        unregisterAsListener();
    }

    public synchronized void enable(SampledQueryEventLoggerOptions newOptions)
    {
        logger.info("Enabling sampled query event logger");
        registerAsListener();
        update(newOptions);
    }

    public synchronized void update(SampledQueryEventLoggerOptions newOptions)
    {
        logger.info("Updating sampled query event logger");
        sampledQueryEventLoggerOptions = newOptions;
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
    
    public void batchSuccess(BatchStatement.Type batchType, List<? extends CQLStatement> statements, List<String> queries, List<List<ByteBuffer>> values, QueryOptions options, QueryState queryState, long queryTime, Message.Response response)
    {
        if (shouldLog(sampledQueryEventLoggerOptions.batch_success_sample_rate)) {
            List<AuditLogEntry> entries = buildEntriesForBatch(statements, queries, queryState, options, queryTime);
            for (AuditLogEntry auditLogEntry : entries)
            {
                log(auditLogEntry);
            }
        }
    }

    public void batchFailure(BatchStatement.Type batchType, List<? extends CQLStatement> statements, List<String> queries, List<List<ByteBuffer>> values, QueryOptions options, QueryState queryState, Exception cause)
    {
        if (shouldLog(sampledQueryEventLoggerOptions.batch_failure_sample_rate))
        {
            String auditMessage = String.format("BATCH of %d statements at consistency %s", statements.size(), options.getConsistency());
            AuditLogEntry entry = new AuditLogEntry.Builder(queryState).setOperation(auditMessage)
                                                                  .setOptions(options)
                                                                  .setType(AuditLogEntryType.BATCH)
                                                                  .build();
            log(entry, cause, queries);
        }
    }

    public void executeSuccess(CQLStatement statement, String query, QueryOptions options, QueryState queryState, long queryTime, Message.Response response)
    {
        if (shouldLog(sampledQueryEventLoggerOptions.execute_success_sample_rate))
        {
            AuditLogEntry entry = new AuditLogEntry.Builder(queryState).setType(statement.getAuditLogContext().auditLogEntryType)
                                                        .setOperation(query)
                                                        .setTimestamp(queryTime)
                                                        .setScope(statement)
                                                        .setKeyspace(queryState, statement)
                                                        .setOptions(options)
                                                        .build();
            log(entry);
        }
    }

    public void executeFailure(CQLStatement statement, String query, QueryOptions options, QueryState queryState, Exception cause)
    {
        if (shouldLog(sampledQueryEventLoggerOptions.execute_failure_sample_rate))
        {
            AuditLogEntry entry = null;
            if (cause instanceof PreparedQueryNotFoundException)
            {
                entry = new AuditLogEntry.Builder(queryState).setOperation(query == null ? "null" : query)
                                                        .setOptions(options)
                                                        .build();
            }
            else if (statement != null)
            {
                entry = new AuditLogEntry.Builder(queryState).setOperation(query == null ? statement.toString() : query)
                                                        .setType(statement.getAuditLogContext().auditLogEntryType)
                                                        .setScope(statement)
                                                        .setKeyspace(queryState, statement)
                                                        .setOptions(options)
                                                        .build();
            }
            if (entry != null) {
                log(entry, cause, query == null ? null : ImmutableList.of(query));
            }
        }
    }

    public void querySuccess(CQLStatement statement, String query, QueryOptions options, QueryState queryState, long queryTime, Message.Response response)
    {
        if (shouldLog(sampledQueryEventLoggerOptions.query_success_sample_rate))
        {
            AuditLogEntry entry = new AuditLogEntry.Builder(queryState).setType(statement.getAuditLogContext().auditLogEntryType)
                                                                  .setOperation(query)
                                                                  .setTimestamp(queryTime)
                                                                  .setScope(statement)
                                                                  .setKeyspace(queryState, statement)
                                                                  .setOptions(options)
                                                                  .build();
            log(entry);
        }
    }
    public void queryFailure(CQLStatement stmt, String query, QueryOptions options, QueryState queryState, Exception cause)
    {
        if (shouldLog(sampledQueryEventLoggerOptions.query_failure_sample_rate))
        {
            AuditLogEntry entry = new AuditLogEntry.Builder(queryState).setOperation(query)
                                                                  .setOptions(options)
                                                                  .build();
            log(entry, cause, query == null ? null : ImmutableList.of(query));
        }
    }

    public void prepareSuccess(CQLStatement statement, String query, QueryState queryState, long queryTime, ResultMessage.Prepared response)
    {
        if (shouldLog(sampledQueryEventLoggerOptions.prepare_success_sample_rate))
        {
            AuditLogEntry entry = new AuditLogEntry.Builder(queryState).setOperation(query)
                                                                  .setType(AuditLogEntryType.PREPARE_STATEMENT)
                                                                  .setScope(statement)
                                                                  .setKeyspace(statement)
                                                                  .build();
            log(entry);
        }
    }

    public void prepareFailure(CQLStatement stmt, String query, QueryState queryState, Exception cause)
    {
        if (shouldLog(sampledQueryEventLoggerOptions.prepare_failure_sample_rate))
        {
             AuditLogEntry entry = new AuditLogEntry.Builder(queryState).setOperation(query)
                                                                .setType(AuditLogEntryType.PREPARE_STATEMENT)
                                                                .build();
            this.log(entry, cause);
        }
    }
}

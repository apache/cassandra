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

package org.apache.cassandra.utils.logging;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.apache.cassandra.audit.FileAuditLogger;
import org.apache.cassandra.db.virtual.LogMessagesTable;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;

import static org.apache.cassandra.db.virtual.LogMessagesTable.LOGS_VIRTUAL_TABLE_DEFAULT_ROWS;
import static org.apache.cassandra.db.virtual.LogMessagesTable.TABLE_NAME;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_VIEWS;

/**
 * Appends Cassandra logs to virtual table system_views.system_logs
 */
public final class VirtualTableAppender extends AppenderBase<LoggingEvent>
{
    public static final String APPENDER_NAME = "CQLLOG";

    private static final Set<String> forbiddenLoggers = ImmutableSet.of(FileAuditLogger.class.getName());

    private LogMessagesTable logs;

    // for holding messages until virtual registry contains logs virtual table
    // as it takes some time during startup of a node to initialise virtual tables but messages are
    // logged already
    private final List<LoggingEvent> messageBuffer = new LinkedList<>();

    @Override
    protected void append(LoggingEvent eventObject)
    {
        if (!forbiddenLoggers.contains(eventObject.getLoggerName()))
        {
            if (logs == null)
            {
                logs = getVirtualTable();
                if (logs == null)
                    addToBuffer(eventObject);
                else
                    logs.add(eventObject);
            }
            else
                logs.add(eventObject);
        }
    }

    @Override
    public void stop()
    {
        messageBuffer.clear();
        super.stop();
    }

    /**
     * Flushes all logs which were appended before virtual table was registered.
     *
     * @see org.apache.cassandra.service.CassandraDaemon#setupVirtualKeyspaces
     */
    public void flushBuffer()
    {
        Optional.ofNullable(getVirtualTable()).ifPresent(vtable -> {
            messageBuffer.forEach(vtable::add);
            messageBuffer.clear();
        });
    }

    private LogMessagesTable getVirtualTable()
    {
        VirtualKeyspace keyspace = VirtualKeyspaceRegistry.instance.getKeyspaceNullable(VIRTUAL_VIEWS);

        if (keyspace == null)
            return null;

        Optional<VirtualTable> logsTable = keyspace.tables()
                                                   .stream()
                                                   .filter(vt -> vt.name().equals(TABLE_NAME))
                                                   .findFirst();

        if (!logsTable.isPresent())
            return null;

        VirtualTable vt = logsTable.get();

        if (!(vt instanceof LogMessagesTable))
            throw new IllegalStateException(String.format("Virtual table %s.%s is not backed by an instance of %s but by %s",
                                                          VIRTUAL_VIEWS,
                                                          TABLE_NAME,
                                                          LogMessagesTable.class.getName(),
                                                          vt.getClass().getName()));

        return (LogMessagesTable) vt;
    }

    private void addToBuffer(LoggingEvent eventObject)
    {
        // we restrict how many logging events we can put into buffer,
        // so we are not growing without any bound when things go south
        if (messageBuffer.size() < LOGS_VIRTUAL_TABLE_DEFAULT_ROWS)
            messageBuffer.add(eventObject);
    }
}

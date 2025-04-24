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

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import ch.qos.logback.classic.spi.LoggingEvent;
import org.apache.cassandra.audit.FileAuditLogger;
import org.apache.cassandra.db.virtual.AbstractLoggerVirtualTable;
import org.apache.cassandra.db.virtual.LogMessagesTable;

/**
 * Appends Cassandra logs to virtual table system_views.system_logs
 */
public final class VirtualTableAppender extends AbstractVirtualTableAppender
{
    public static final String APPENDER_NAME = "CQLLOG";

    private static final Set<String> forbiddenLoggers = ImmutableSet.of(FileAuditLogger.class.getName());

    private AbstractLoggerVirtualTable<?> logs;

    public VirtualTableAppender()
    {
        super(LogMessagesTable.LOGS_VIRTUAL_TABLE_DEFAULT_ROWS);
    }

    @Override
    protected void append(LoggingEvent eventObject)
    {
        if (!forbiddenLoggers.contains(eventObject.getLoggerName()))
            logs = appendToVirtualTable(logs, eventObject, LogMessagesTable.TABLE_NAME);
    }
}

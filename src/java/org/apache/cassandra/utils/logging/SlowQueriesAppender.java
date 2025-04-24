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

import ch.qos.logback.classic.spi.LoggingEvent;
import org.apache.cassandra.db.virtual.AbstractLoggerVirtualTable;
import org.apache.cassandra.db.virtual.SlowQueriesTable;

public final class SlowQueriesAppender extends AbstractVirtualTableAppender
{
    public static final String APPENDER_NAME = "SLOW_QUERIES_APPENDER";

    private AbstractLoggerVirtualTable<?> slowQueries;

    public SlowQueriesAppender()
    {
        super(SlowQueriesTable.LOGS_VIRTUAL_TABLE_DEFAULT_ROWS);
    }

    @Override
    protected void append(LoggingEvent eventObject)
    {
        // slowQueries will be null as long as virtual tables
        // are not registered, and we already try to put queries there.
        // As soon as vtable is registered (as part of node's startup / initialisation),
        // slow queries will never be null again
        slowQueries = appendToVirtualTable(slowQueries, eventObject, SlowQueriesTable.TABLE_NAME);
    }
}

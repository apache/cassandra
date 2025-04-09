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

import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.apache.cassandra.db.virtual.AbstractLoggerVirtualTable;
import org.apache.cassandra.db.virtual.SlowQueriesTable;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;

import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_VIEWS;

public abstract class AbstractVirtualTableAppender extends AppenderBase<LoggingEvent>
{
    private final int defaultRows;

    protected AbstractVirtualTableAppender(int defaultRows)
    {
        this.defaultRows = defaultRows;
    }

    // for holding messages until virtual registry contains logs virtual table
    // as it takes some time during startup of a node to initialise virtual tables but messages are
    // logged already
    protected final List<LoggingEvent> messageBuffer = new LinkedList<>();

    protected <T> T getVirtualTable(Class<T> vtableClass, String tableName)
    {
        VirtualKeyspace keyspace = VirtualKeyspaceRegistry.instance.getKeyspaceNullable(VIRTUAL_VIEWS);

        if (keyspace == null)
            return null;

        Optional<VirtualTable> virtualTable = keyspace.tables()
                                                      .stream()
                                                      .filter(vt -> vt.name().equals(tableName))
                                                      .findFirst();

        if (virtualTable.isEmpty())
            return null;

        VirtualTable vt = virtualTable.get();

        if (!vt.getClass().equals(vtableClass))
            throw new IllegalStateException(String.format("Virtual table %s.%s is not backed by an instance of %s but by %s",
                                                          VIRTUAL_VIEWS,
                                                          tableName,
                                                          vtableClass.getName(),
                                                          vt.getClass().getName()));

        return (T) vt;
    }

    /**
     * This method adds an event to virtual table, when present.
     * When vtable is null, we will attempt to find it among registered ones. Then not found, we add it to internal
     * buffer for later processing. This might happen e.g. for logging tables when log events
     * were appended via logging framework sooner than registration of virtual tables was done so after they are registered,
     * they would miss logging events happened before being so.
     *
     * @param vtable    vtable to append to
     * @param event     event to append to
     * @param tableName table name of virtual table to append to
     * @return vtable or when null, found vtable
     */
    protected AbstractLoggerVirtualTable<?> appendToVirtualTable(AbstractLoggerVirtualTable<?> vtable, LoggingEvent event, String tableName)
    {
        AbstractLoggerVirtualTable<?> foundVtable;
        if (vtable == null)
        {
            foundVtable = getVirtualTable(SlowQueriesTable.class, tableName);
            if (foundVtable == null)
                addToBuffer(event);
            else
                foundVtable.add(event);
        }
        else
        {
            foundVtable = vtable;
            vtable.add(event);
        }

        return foundVtable;
    }

    @Override
    public void stop()
    {
        synchronized (messageBuffer)
        {
            messageBuffer.clear();
            super.stop();
        }
    }

    /**
     * Flushes all log entries which were appended before virtual table was registered.
     *
     * @see org.apache.cassandra.service.CassandraDaemon#setupVirtualKeyspaces
     */
    public void flushBuffer(Class<? extends AbstractLoggerVirtualTable<?>> vtableClass, String tableName)
    {
        synchronized (messageBuffer)
        {
            Optional.ofNullable(getVirtualTable(vtableClass, tableName)).ifPresent(vtable -> {
                messageBuffer.forEach(vtable::add);
                messageBuffer.clear();
            });
        }
    }

    protected void addToBuffer(LoggingEvent eventObject)
    {
        synchronized (messageBuffer)
        {
            // we restrict how many logging events we can put into buffer,
            // so we are not growing without any bound when things go south
            if (messageBuffer.size() < defaultRows)
                messageBuffer.add(eventObject);
        }
    }
}

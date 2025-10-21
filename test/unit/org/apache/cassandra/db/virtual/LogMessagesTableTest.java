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

package org.apache.cassandra.db.virtual;

import java.time.Instant;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import ch.qos.logback.classic.spi.LoggingEvent;
import com.datastax.driver.core.Row;

import static org.apache.cassandra.config.CassandraRelevantProperties.LOGS_VIRTUAL_TABLE_MAX_ROWS;
import static org.apache.cassandra.db.virtual.LogMessagesTable.LOGS_VIRTUAL_TABLE_DEFAULT_ROWS;
import static org.junit.Assert.assertEquals;

public class LogMessagesTableTest extends AbstractLoggerVirtualTableTest<LoggingEvent>
{
    @Test
    public void testMultipleLogsInSameMillisecond()
    {
        registerTable();
        List<LoggingEvent> loggingEvents = getLoggingEvents(10, Instant.now(), 5);
        loggingEvents.forEach(table::add);

        // 2 partitions, 5 rows in each
        assertEquals(2, numberOfPartitions());
    }

    @Test
    public void testLimitedCapacity()
    {
        registerTable(100);

        int numberOfRows = 1000;
        List<LoggingEvent> loggingEvents = getLoggingEvents(numberOfRows);
        loggingEvents.forEach(table::add);

        // even we inserted 1000 rows, only 100 are present as its capacity is bounded
        assertEquals(100, numberOfPartitions());

        // the first record in the table will be the last one which we inserted
        LoggingEvent firstEvent = loggingEvents.get(999);
        assertRowsNet(executeNet(query("select timestamp from %s limit 1")),
                      new Object[]{ new Date(firstEvent.getTimeStamp()) });

        // the last record in the table will be 900th we inserted
        List<Row> all = executeNet(query("select timestamp from %s")).all();
        assertEquals(100, all.size());
        Row row = all.get(all.size() - 1);
        Date timestamp = row.getTimestamp(0);
        assertEquals(loggingEvents.get(900).getTimeStamp(), timestamp.getTime());
    }

    @Test
    public void testResolvingBufferSize()
    {
        LOGS_VIRTUAL_TABLE_MAX_ROWS.setInt(-1);
        assertEquals(LOGS_VIRTUAL_TABLE_DEFAULT_ROWS, resolveBufferSize());

        LOGS_VIRTUAL_TABLE_MAX_ROWS.setInt(0);
        assertEquals(LOGS_VIRTUAL_TABLE_DEFAULT_ROWS, resolveBufferSize());

        LOGS_VIRTUAL_TABLE_MAX_ROWS.setInt(1000001);
        assertEquals(LOGS_VIRTUAL_TABLE_DEFAULT_ROWS, resolveBufferSize());

        LOGS_VIRTUAL_TABLE_MAX_ROWS.setInt(999);
        assertEquals(999, resolveBufferSize());

        LOGS_VIRTUAL_TABLE_MAX_ROWS.setInt(50001);
        assertEquals(50001, resolveBufferSize());
    }

    private int resolveBufferSize()
    {
        return AbstractLoggerVirtualTable.resolveBufferSize(LOGS_VIRTUAL_TABLE_MAX_ROWS.getInt(),
                                                            LogMessagesTable.LOGS_VIRTUAL_TABLE_MAX_ROWS,
                                                            LOGS_VIRTUAL_TABLE_DEFAULT_ROWS);
    }

    @Override
    protected void registerTable(int maxSize)
    {
        registerVirtualTable(new LogMessagesTable(keyspace, maxSize));
    }

    @Override
    protected void registerTable()
    {
        registerTable(1000);
    }

    @Override
    protected String getMessage(long timestamp)
    {
        return "message " + timestamp;
    }
}

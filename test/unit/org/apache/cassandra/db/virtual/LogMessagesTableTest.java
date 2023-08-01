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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.virtual.AbstractVirtualTable.DataSet;
import org.apache.cassandra.db.virtual.AbstractVirtualTable.Partition;
import org.apache.cassandra.dht.LocalPartitioner;

import static org.apache.cassandra.config.CassandraRelevantProperties.LOGS_VIRTUAL_TABLE_MAX_ROWS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogMessagesTableTest extends CQLTester
{
    private String keyspace = createKeyspaceName();
    private LogMessagesTable table;

    @BeforeClass
    public static void setup()
    {
        CQLTester.setUpClass();
    }

    @Test
    public void testTruncate() throws Throwable
    {
        registerVirtualTable();

        int numberOfRows = 100;
        List<LoggingEvent> loggingEvents = getLoggingEvents(numberOfRows);
        loggingEvents.forEach(table::add);

        execute(query("truncate %s"));

        assertTrue(executeNet(query("select timestamp from %s")).all().isEmpty());
    }

    @Test
    public void empty() throws Throwable
    {
        registerVirtualTable();
        assertEmpty(execute(query("select * from %s")));
    }

    @Test
    public void testInsert()
    {
        registerVirtualTable();

        int numberOfRows = 1000;
        List<LoggingEvent> loggingEvents = getLoggingEvents(numberOfRows);
        loggingEvents.forEach(table::add);

        assertEquals(numberOfRows, numberOfPartitions());
    }

    @Test
    public void testLimitedCapacity() throws Throwable
    {
        registerVirtualTable(100);

        int numberOfRows = 1000;
        List<LoggingEvent> loggingEvents = getLoggingEvents(numberOfRows);
        loggingEvents.forEach(table::add);

        // even we inserted 1000 rows, only 100 are present as its capacity is bounded
        assertEquals(100, numberOfPartitions());

        // the first record in the table will be the last one which we inserted
        LoggingEvent firstEvent = loggingEvents.get(999);
        assertRowsNet(executeNet(query("select timestamp from %s limit 1")),
                      new Object[] { new Date(firstEvent.getTimeStamp()) });

        // the last record in the table will be 900th we inserted
        List<Row> all = executeNet(query("select timestamp from %s")).all();
        assertEquals(100, all.size());
        Row row = all.get(all.size() - 1);
        Date timestamp = row.getTimestamp(0);
        assertEquals(loggingEvents.get(900).getTimeStamp(), timestamp.getTime());
    }

    @Test
    public void testMultipleLogsInSameMillisecond()
    {
        registerVirtualTable(10);
        List<LoggingEvent> loggingEvents = getLoggingEvents(10, Instant.now(), 5);
        loggingEvents.forEach(table::add);

        // 2 partitions, 5 rows in each
        assertEquals(2, numberOfPartitions());
    }

    @Test
    public void testResolvingBufferSize()
    {
        LOGS_VIRTUAL_TABLE_MAX_ROWS.setInt(-1);
        assertEquals(LogMessagesTable.LOGS_VIRTUAL_TABLE_DEFAULT_ROWS, LogMessagesTable.resolveBufferSize());

        LOGS_VIRTUAL_TABLE_MAX_ROWS.setInt(0);
        assertEquals(LogMessagesTable.LOGS_VIRTUAL_TABLE_DEFAULT_ROWS, LogMessagesTable.resolveBufferSize());

        LOGS_VIRTUAL_TABLE_MAX_ROWS.setInt(1000001);
        assertEquals(LogMessagesTable.LOGS_VIRTUAL_TABLE_DEFAULT_ROWS, LogMessagesTable.resolveBufferSize());

        LOGS_VIRTUAL_TABLE_MAX_ROWS.setInt(999);
        assertEquals(LogMessagesTable.LOGS_VIRTUAL_TABLE_DEFAULT_ROWS, LogMessagesTable.resolveBufferSize());

        LOGS_VIRTUAL_TABLE_MAX_ROWS.setInt(50001);
        assertEquals(50001, LogMessagesTable.resolveBufferSize());
    }

    private void registerVirtualTable()
    {
        registerVirtualTable(LogMessagesTable.LOGS_VIRTUAL_TABLE_MIN_ROWS);
    }

    private void registerVirtualTable(int size)
    {
        table = new LogMessagesTable(keyspace, size);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(keyspace, ImmutableList.of(table)));
    }

    private int numberOfPartitions()
    {
        DataSet data = table.data();

        Iterator<Partition> partitions = data.getPartitions(DataRange.allData(new LocalPartitioner(TimestampType.instance)));

        int numberOfPartitions = 0;

        while (partitions.hasNext())
        {
            partitions.next();
            numberOfPartitions += 1;
        }

        return numberOfPartitions;
    }

    private String query(String query)
    {
        return String.format(query, table.toString());
    }

    private List<LoggingEvent> getLoggingEvents(int size)
    {
        return getLoggingEvents(size, Instant.now(), 1);
    }

    private List<LoggingEvent> getLoggingEvents(int size, Instant firstTimestamp, int logsInMillisecond)
    {
        List<LoggingEvent> logs = new LinkedList<>();
        int partitions = size / logsInMillisecond;

        for (int i = 0; i < partitions; i++)
        {
            long timestamp = firstTimestamp.toEpochMilli();
            firstTimestamp = firstTimestamp.plusSeconds(1);

            for (int j = 0; j < logsInMillisecond; j++)
                logs.add(getLoggingEvent(timestamp));
        }

        return logs;
    }

    private LoggingEvent getLoggingEvent(long timestamp)
    {
        LoggingEvent event = new LoggingEvent();
        event.setLevel(Level.INFO);
        event.setMessage("message " + timestamp);
        event.setLoggerName("logger " + timestamp);
        event.setThreadName(Thread.currentThread().getName());
        event.setTimeStamp(timestamp);

        return event;
    }
}

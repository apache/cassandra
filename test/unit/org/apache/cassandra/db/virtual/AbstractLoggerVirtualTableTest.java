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
import org.junit.Ignore;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.DataRange;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore
public abstract class AbstractLoggerVirtualTableTest<U> extends CQLTester
{
    protected final String keyspace = createKeyspaceName();

    protected AbstractLoggerVirtualTable<U> table;

    @Test
    public void testTruncate()
    {
        registerTable();

        int numberOfRows = 100;
        List<LoggingEvent> loggingEvents = getLoggingEvents(numberOfRows);
        loggingEvents.forEach(table::add);

        execute(query("truncate %s"));

        assertTrue(executeNet(query("select timestamp from %s")).all().isEmpty());
    }

    @Test
    public void testEmpty() throws Throwable
    {
        registerTable();
        assertEmpty(execute(query("select * from %s")));
    }

    @Test
    public void testInsert()
    {
        registerTable();

        int numberOfRows = 1000;
        List<LoggingEvent> loggingEvents = getLoggingEvents(numberOfRows);
        loggingEvents.forEach(table::add);

        assertEquals(numberOfRows, execute(query("select * from %s")).size());
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

    protected abstract void registerTable(int maxSize);

    protected abstract void registerTable();

    protected void registerVirtualTable(AbstractLoggerVirtualTable<U> table)
    {
        this.table = table;
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(table.metadata.keyspace, ImmutableList.of(this.table)));
    }

    protected String query(String query)
    {
        return String.format(query, table.toString());
    }

    protected List<LoggingEvent> getLoggingEvents(int size)
    {
        return getLoggingEvents(size, Instant.now(), 1);
    }

    protected List<LoggingEvent> getLoggingEvents(int size, Instant firstTimestamp, int logsInMillisecond)
    {
        List<LoggingEvent> logs = new LinkedList<>();
        int partitions = size / logsInMillisecond;

        for (int i = 0; i < partitions; i++)
        {
            firstTimestamp = firstTimestamp.plusSeconds(i);

            for (int j = 0; j < logsInMillisecond; j++)
                logs.add(getLoggingEvent(firstTimestamp.toEpochMilli()));
        }

        return logs;
    }

    protected int numberOfPartitions()
    {
        AbstractVirtualTable.DataSet data = table.data();
        Iterator<AbstractVirtualTable.Partition> partitions = data.getPartitions(DataRange.allData(table.metadata.partitioner));
        int numberOfPartitions = 0;

        while (partitions.hasNext())
        {
            partitions.next();
            numberOfPartitions += 1;
        }

        return numberOfPartitions;
    }

    protected LoggingEvent getLoggingEvent(long timestamp)
    {
        LoggingEvent event = new LoggingEvent();
        event.setLevel(Level.INFO);
        event.setMessage(getMessage(timestamp));
        event.setLoggerName(AbstractLoggerVirtualTableTest.class.getName());
        event.setThreadName(Thread.currentThread().getName());
        event.setTimeStamp(timestamp);

        return event;
    }

    protected abstract String getMessage(long timestamp);
}

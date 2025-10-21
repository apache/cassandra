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

import java.util.List;
import java.util.Random;

import org.junit.Test;

import ch.qos.logback.classic.spi.LoggingEvent;
import org.apache.cassandra.db.monitoring.MonitorableImpl;
import org.apache.cassandra.db.monitoring.MonitoringTask;
import org.apache.cassandra.db.monitoring.MonitoringTask.Operation;
import org.apache.cassandra.utils.Generators;
import org.quicktheories.impl.JavaRandom;

import static org.apache.cassandra.config.CassandraRelevantProperties.LOGS_SLOW_QUERIES_VIRTUAL_TABLE_MAX_ROWS;
import static org.apache.cassandra.db.virtual.SlowQueriesTable.LOGS_VIRTUAL_TABLE_DEFAULT_ROWS;
import static org.apache.cassandra.db.virtual.SlowQueriesTable.LOGS_VIRTUAL_TABLE_MAX_ROWS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SlowQueriesTableTest extends AbstractLoggerVirtualTableTest<Operation>
{
    private final Random random = new Random();
    private final JavaRandom javaRandom = new JavaRandom(random);

    @Override
    protected void registerTable(int maxSize)
    {
        registerVirtualTable(new SlowQueriesTable(keyspace, maxSize));
    }

    @Override
    protected void registerTable()
    {
        registerTable(1000);
    }

    @Test
    public void testLimitedCapacity()
    {
        registerTable(100);

        int numberOfRows = 1000;
        List<LoggingEvent> loggingEvents = getLoggingEvents(numberOfRows);
        assertEquals(1000, loggingEvents.size());
        loggingEvents.forEach(table::add);

        // even we inserted 1000 rows, only 100 are present as its capacity is bounded
        assertEquals(100, executeNet(query("select * from %s")).all().size());
    }

    @Test
    public void testDelete()
    {
        registerTable();

        int numberOfRows = 100;
        List<LoggingEvent> loggingEvents = getLoggingEvents(numberOfRows);
        loggingEvents.forEach(table::add);

        Operation operation = table.buffer.get(0);

        assertEquals(100, executeNet(query("select * from %s")).all().size());
        execute(query("delete from %s where keyspace_name = '" + operation.keyspace() + '\''));
        assertTrue(executeNet(query("select * from %s")).all().size() < 100);
    }

    @Test
    public void testResolvingBufferSize()
    {
        LOGS_SLOW_QUERIES_VIRTUAL_TABLE_MAX_ROWS.setInt(-1);
        assertEquals(LOGS_VIRTUAL_TABLE_DEFAULT_ROWS, resolveBufferSize());

        LOGS_SLOW_QUERIES_VIRTUAL_TABLE_MAX_ROWS.setInt(0);
        assertEquals(LOGS_VIRTUAL_TABLE_DEFAULT_ROWS, resolveBufferSize());

        LOGS_SLOW_QUERIES_VIRTUAL_TABLE_MAX_ROWS.setInt(1000001);
        assertEquals(LOGS_VIRTUAL_TABLE_DEFAULT_ROWS, resolveBufferSize());

        LOGS_SLOW_QUERIES_VIRTUAL_TABLE_MAX_ROWS.setInt(999);
        assertEquals(999, resolveBufferSize());

        LOGS_SLOW_QUERIES_VIRTUAL_TABLE_MAX_ROWS.setInt(50001);
        assertEquals(50001, resolveBufferSize());
    }

    private int resolveBufferSize()
    {
        return AbstractLoggerVirtualTable.resolveBufferSize(LOGS_SLOW_QUERIES_VIRTUAL_TABLE_MAX_ROWS.getInt(),
                                                            LOGS_VIRTUAL_TABLE_MAX_ROWS,
                                                            LOGS_VIRTUAL_TABLE_DEFAULT_ROWS);
    }


    @Override
    protected String getMessage(long timestamp)
    {
        MonitoringTask.SlowOperation slowOperation = new MonitoringTask.SlowOperation(new MonitorableImpl()
        {
            @Override
            public String name()
            {
                return Generators.SYMBOL_GEN.generate(javaRandom);
            }

            @Override
            public String monitoredOnKeyspace()
            {
                return Generators.SYMBOL_GEN.generate(javaRandom);
            }

            @Override
            public String monitoredOnTable()
            {
                return Generators.SYMBOL_GEN.generate(javaRandom);
            }

            @Override
            public boolean isCrossNode()
            {
                return random.nextBoolean();
            }
        }, timestamp);

        return Operation.serialize(List.of(slowOperation));
    }
}

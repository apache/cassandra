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

package org.apache.cassandra.distributed.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import ch.qos.logback.classic.Level;
import org.apache.cassandra.db.virtual.LogMessagesTable;
import org.apache.cassandra.db.virtual.LogMessagesTable.LogMessage;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.logging.VirtualTableAppender;

import static java.lang.String.format;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOGBACK_CONFIGURATION_FILE;
import static org.apache.cassandra.db.virtual.LogMessagesTable.LEVEL_COLUMN_NAME;
import static org.apache.cassandra.db.virtual.LogMessagesTable.LOGGER_COLUMN_NAME;
import static org.apache.cassandra.db.virtual.LogMessagesTable.MESSAGE_COLUMN_NAME;
import static org.apache.cassandra.db.virtual.LogMessagesTable.ORDER_IN_MILLISECOND_COLUMN_NAME;
import static org.apache.cassandra.db.virtual.LogMessagesTable.TIMESTAMP_COLUMN_NAME;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class VirtualTableLogsTest extends TestBaseImpl
{
    @Test
    public void testVTableOutput() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(LOGBACK_CONFIGURATION_FILE, "test/conf/logback-dtest_with_vtable_appender.xml");
             Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c.with(Feature.values()))
                                      .start();
             )
        {
            List<TestingLogMessage> rows = getRows(cluster);
            assertFalse(rows.isEmpty());

            rows.forEach(message -> assertTrue(Level.toLevel(message.level).isGreaterOrEqual(Level.INFO)));
        }
    }

    @Test
    public void testMultipleAppendersFailToStartNode() throws Throwable
    {
        LOGBACK_CONFIGURATION_FILE.setString("test/conf/logback-dtest_with_vtable_appender_invalid.xml");

        // NOTE: Because cluster startup is expected to fail in this case, and can leave things in a weird state
        // for the next state, create without starting, and set failure as shutdown to false,
        // so the try-with-resources can close instances properly.
        try (WithProperties properties = new WithProperties().set(LOGBACK_CONFIGURATION_FILE, "test/conf/logback-dtest_with_vtable_appender_invalid.xml");
             Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c.with(Feature.values())
                                                        .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false))
                                      .createWithoutStarting())
        {
            cluster.startup();
            fail("Node should not start as there is supposed to be invalid logback configuration file.");
        }
        catch (IllegalStateException ex)
        {
            assertEquals(format("There are multiple appenders of class %s " +
                                "of names CQLLOG,CQLLOG2. There is only one appender of such class allowed.",
                                VirtualTableAppender.class.getName()),
                         ex.getMessage());
        }
    }

    private List<TestingLogMessage> getRows(Cluster cluster)
    {
        SimpleQueryResult simpleQueryResult = cluster.coordinator(1).executeWithResult(query("select * from %s"), ONE);
        List<TestingLogMessage> rows = new ArrayList<>();
        simpleQueryResult.forEachRemaining(row -> {
            long timestamp = row.getTimestamp(TIMESTAMP_COLUMN_NAME).getTime();
            String logger = row.getString(LOGGER_COLUMN_NAME);
            String level = row.getString(LEVEL_COLUMN_NAME);
            String message = row.getString(MESSAGE_COLUMN_NAME);
            int order = row.getInteger(ORDER_IN_MILLISECOND_COLUMN_NAME);
            TestingLogMessage logMessage = new TestingLogMessage(timestamp, logger, level, message, order);
            rows.add(logMessage);
        });
        return rows;
    }

    private String query(String template)
    {
        return format(template, getTableName());
    }

    private String getTableName()
    {
        return format("%s.%s", SchemaConstants.VIRTUAL_VIEWS, LogMessagesTable.TABLE_NAME);
    }

    private static class TestingLogMessage extends LogMessage
    {
        private int order;

        public TestingLogMessage(long timestamp, String logger, String level, String message, int order)
        {
            super(timestamp, logger, level, message);
            this.order = order;
        }
    }
}

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

import org.junit.Test;

import org.apache.cassandra.db.virtual.SlowQueriesTable;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.logging.SlowQueriesAppender;

import static java.lang.String.format;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOGBACK_CONFIGURATION_FILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * It is inherently tricky / flaky to make some queries to be slow so we just test
 * the invalid configuration otherwise the table as such is tested in {@link org.apache.cassandra.db.virtual.SlowQueriesTableTest}.
 */
public class SlowQueriesAppenderTest extends AbstractVirtualLogsTableTest
{
    @Test
    public void testMultipleAppendersFailToStartNode() throws Throwable
    {
        LOGBACK_CONFIGURATION_FILE.setString("test/conf/logback-dtest_with_slow_query_appender_invalid.xml");

        // NOTE: Because cluster startup is expected to fail in this case, and can leave things in a weird state
        // for the next state, create without starting, and set failure as shutdown to false,
        // so the try-with-resources can close instances properly.
        try (WithProperties properties = new WithProperties().set(LOGBACK_CONFIGURATION_FILE, "test/conf/logback-dtest_with_slow_query_appender_invalid.xml");
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
                                "of names SLOW_QUERIES_APPENDER,SLOW_QUERIES_APPENDER_2. There is only one appender of such class allowed.",
                                SlowQueriesAppender.class.getName()),
                         ex.getMessage());
        }
    }

    @Override
    public String getTableName()
    {
        return format("%s.%s", SchemaConstants.VIRTUAL_VIEWS, SlowQueriesTable.TABLE_NAME);
    }
}

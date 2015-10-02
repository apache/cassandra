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

package org.apache.cassandra.metrics;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static junit.framework.Assert.assertEquals;

@RunWith(OrderedJUnit4ClassRunner.class)
public class CQLMetricsTest extends SchemaLoader
{
    private static EmbeddedCassandraService cassandra;

    private static Cluster cluster;
    private static Session session;

    @BeforeClass()
    public static void setup() throws ConfigurationException, IOException
    {
        Schema.instance.clear();

        cassandra = new EmbeddedCassandraService();
        cassandra.start();

        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS junit WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("CREATE TABLE IF NOT EXISTS junit.metricstest (id int PRIMARY KEY, val text);");
    }

    @Test
    public void testPreparedStatementsCount()
    {
        int n = (int) QueryProcessor.metrics.preparedStatementsCount.getValue();
        session.prepare("SELECT * FROM junit.metricstest WHERE id = ?");
        assertEquals(n+1, (int) QueryProcessor.metrics.preparedStatementsCount.getValue());
    }

    @Test
    public void testRegularStatementsExecuted()
    {
        clearMetrics();
        PreparedStatement metricsStatement = session.prepare("INSERT INTO junit.metricstest (id, val) VALUES (?, ?)");

        assertEquals(0, QueryProcessor.metrics.preparedStatementsExecuted.getCount());
        assertEquals(0, QueryProcessor.metrics.regularStatementsExecuted.getCount());

        for (int i = 0; i < 10; i++)
            session.execute(String.format("INSERT INTO junit.metricstest (id, val) VALUES (%d, '%s')", i, "val" + i));

        assertEquals(0, QueryProcessor.metrics.preparedStatementsExecuted.getCount());
        assertEquals(10, QueryProcessor.metrics.regularStatementsExecuted.getCount());
    }

    @Test
    public void testPreparedStatementsExecuted()
    {
        clearMetrics();
        PreparedStatement metricsStatement = session.prepare("INSERT INTO junit.metricstest (id, val) VALUES (?, ?)");

        assertEquals(0, QueryProcessor.metrics.preparedStatementsExecuted.getCount());
        assertEquals(0, QueryProcessor.metrics.regularStatementsExecuted.getCount());

        for (int i = 0; i < 10; i++)
            session.execute(metricsStatement.bind(i, "val" + i));

        assertEquals(10, QueryProcessor.metrics.preparedStatementsExecuted.getCount());
        assertEquals(0, QueryProcessor.metrics.regularStatementsExecuted.getCount());
    }

    @Test
    public void testPreparedStatementsRatio()
    {
        clearMetrics();
        PreparedStatement metricsStatement = session.prepare("INSERT INTO junit.metricstest (id, val) VALUES (?, ?)");

        assertEquals(Double.NaN, QueryProcessor.metrics.preparedStatementsRatio.getValue());

        for (int i = 0; i < 10; i++)
            session.execute(metricsStatement.bind(i, "val" + i));
        assertEquals(1.0, QueryProcessor.metrics.preparedStatementsRatio.getValue());

        for (int i = 0; i < 10; i++)
            session.execute(String.format("INSERT INTO junit.metricstest (id, val) VALUES (%d, '%s')", i, "val" + i));
        assertEquals(0.5, QueryProcessor.metrics.preparedStatementsRatio.getValue());
    }

    private void clearMetrics()
    {
        QueryProcessor.metrics.preparedStatementsExecuted.dec(QueryProcessor.metrics.preparedStatementsExecuted.getCount());
        QueryProcessor.metrics.regularStatementsExecuted.dec(QueryProcessor.metrics.regularStatementsExecuted.getCount());
        QueryProcessor.metrics.preparedStatementsEvicted.dec(QueryProcessor.metrics.preparedStatementsEvicted.getCount());
    }
}


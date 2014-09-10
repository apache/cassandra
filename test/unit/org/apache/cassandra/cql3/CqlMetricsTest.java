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
package org.apache.cassandra.cql3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;


@RunWith(OrderedJUnit4ClassRunner.class)
public class CqlMetricsTest
{

    private static EmbeddedCassandraService cassandra;

    private static Cluster cluster;
    private static Session session;
    private static PreparedStatement metricsStatement;

    @BeforeClass()
    public static void setup() throws ConfigurationException, IOException
    {
        cassandra = new EmbeddedCassandraService();
        cassandra.start();

        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("drop keyspace if exists junit;");
        session.execute("create keyspace junit WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("CREATE TABLE junit.metricstest (\n" +
                "  id int PRIMARY KEY,\n" +
                "  val text\n" +
                ");");
    }

    @Test
    public void testActivePreparedStatements()
    {
        assert QueryProcessor.metrics.activePreparedStatements.count() == 0;

        metricsStatement = session.prepare("insert into junit.metricstest(id, val)values(?,?)");

        assert QueryProcessor.metrics.activePreparedStatements.count() == 1;
    }

    @Test
    public void testExecutedPrepared()
    {
        QueryProcessor.metrics.reset();

        assert QueryProcessor.metrics.activePreparedStatements.count() == 1;
        assert QueryProcessor.metrics.executedPrepared.count() == 0;
        assert QueryProcessor.metrics.executedUnprepared.count() == 0;
        assert QueryProcessor.metrics.preparedRatio.value() == 1.0;

        for (int i = 0; i < 10; i++)
        {
            session.execute(metricsStatement.bind(i, "val"+i));
        }

        assert QueryProcessor.metrics.executedPrepared.count() == 10;
        assert QueryProcessor.metrics.executedUnprepared.count() == 0;
        assert QueryProcessor.metrics.preparedRatio.value() == 10d/1d;

    }

    @Test
    public void testExecutedUnPrepared()
    {
        QueryProcessor.metrics.reset();

        assert QueryProcessor.metrics.activePreparedStatements.count() == 1;
        assert QueryProcessor.metrics.executedPrepared.count() == 0;
        assert QueryProcessor.metrics.executedUnprepared.count() == 0;

        for (int i = 0; i < 10; i++)
        {
            session.execute(String.format("insert into junit.metricstest(id, val)values(%d,'%s')",i, "val"+1));
        }

        assert QueryProcessor.metrics.executedPrepared.count() == 0;
        assert QueryProcessor.metrics.executedUnprepared.count() == 10;
        assert QueryProcessor.metrics.preparedRatio.value() == 1d/10d;
    }
}

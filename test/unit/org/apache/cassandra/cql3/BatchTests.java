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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class BatchTests
{
    private static EmbeddedCassandraService cassandra;

    private static Cluster cluster;
    private static Session session;

    private static PreparedStatement counter;
    private static PreparedStatement noncounter;

    @BeforeClass()
    public static void setup() throws ConfigurationException, IOException
    {
        cassandra = new EmbeddedCassandraService();
        cassandra.start();

        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("drop keyspace if exists junit;");
        session.execute("create keyspace junit WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("CREATE TABLE junit.noncounter (\n" +
                "  id int PRIMARY KEY,\n" +
                "  val text\n" +
                ");");
        session.execute("CREATE TABLE junit.counter (\n" +
                "  id int PRIMARY KEY,\n" +
                "  val counter,\n" +
                ");");


        noncounter = session.prepare("insert into junit.noncounter(id, val)values(?,?)");
        counter = session.prepare("update junit.counter set val = val + ? where id = ?");
    }

    @Test(expected = InvalidQueryException.class)
    public void testMixedInCounterBatch()
    {
       sendBatch(BatchStatement.Type.COUNTER, true, true);
    }

    @Test(expected = InvalidQueryException.class)
    public void testMixedInLoggedBatch()
    {
        sendBatch(BatchStatement.Type.LOGGED, true, true);
    }

    @Test(expected = InvalidQueryException.class)
    public void testMixedInUnLoggedBatch()
    {
        sendBatch(BatchStatement.Type.UNLOGGED, true, true);
    }

    @Test(expected = InvalidQueryException.class)
    public void testNonCounterInCounterBatch()
    {
        sendBatch(BatchStatement.Type.COUNTER, false, true);
    }

    @Test
    public void testNonCounterInLoggedBatch()
    {
        sendBatch(BatchStatement.Type.LOGGED, false, true);
    }

    @Test
    public void testNonCounterInUnLoggedBatch()
    {
        sendBatch(BatchStatement.Type.UNLOGGED, false, true);
    }

    @Test
    public void testCounterInCounterBatch()
    {
        sendBatch(BatchStatement.Type.COUNTER, true, false);
    }

    @Test
    public void testCounterInUnLoggedBatch()
    {
        sendBatch(BatchStatement.Type.UNLOGGED, true, false);
    }

    @Test
    public void testEmptyBatch()
    {
        session.execute("BEGIN BATCH APPLY BATCH");
        session.execute("BEGIN UNLOGGED BATCH APPLY BATCH");
    }

    @Test(expected = InvalidQueryException.class)
    public void testCounterInLoggedBatch()
    {
        sendBatch(BatchStatement.Type.LOGGED, true, false);
    }

    @Test(expected = InvalidQueryException.class)
    public void testOversizedBatch()
    {
        int SIZE_FOR_FAILURE = 2500;
        BatchStatement b = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (int i = 0; i < SIZE_FOR_FAILURE; i++)
        {
            b.add(noncounter.bind(i, "foobar"));
        }
        session.execute(b);
    }

    public void sendBatch(BatchStatement.Type type, boolean addCounter, boolean addNonCounter)
    {

        assert addCounter || addNonCounter;
        BatchStatement b = new BatchStatement(type);

        for (int i = 0; i < 10; i++)
        {
            if (addNonCounter)
                b.add(noncounter.bind(i, "foo"));

            if (addCounter)
                b.add(counter.bind((long)i, i));
        }

        session.execute(b);
    }

}

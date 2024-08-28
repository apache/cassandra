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

package org.apache.cassandra.db;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;
import com.datastax.driver.core.exceptions.ReadTimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ArtificialReadCommandTimeoutTest extends CQLTester
{
    private static EmbeddedCassandraService cassandra;

    private static Cluster cluster;
    private static Session session;

    @BeforeClass()
    public static void setup() throws ConfigurationException, IOException
    {
        cassandra = ServerTestUtils.startEmbeddedCassandraService();
        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("drop keyspace if exists readcommand;");
        session.execute("create keyspace readcommand WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("CREATE TABLE readcommand.tbl (\n" +
                        "  id int,\n" +
                        "  a int,\n" +
                        "  b int,\n" +
                        "  c int,\n" +
                        " PRIMARY KEY(id)" +
                        ");");

        // The default should be 0
        assertEquals(0, DatabaseDescriptor.getInjectArtificialDelayReadPath().toMilliseconds());
    }

    @AfterClass
    public static void tearDown()
    {
        try
        {
            DatabaseDescriptor.setInjectArtificialDelayReadPath("0s");
            System.out.println("Shutting down...");
            if (session != null)
                session.close();
            if (cluster != null)
                cluster.close();
            if (cassandra != null)
                cassandra.stop();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testArtificialTimeout()
    {
        DatabaseDescriptor.setInjectArtificialDelayReadPath("0s");
        session.execute(String.format("INSERT INTO readcommand.tbl (id, a, b, c) VALUES (10, 10, 10, 10)"));

        // the read should timeout
        DatabaseDescriptor.setInjectArtificialDelayReadPath("10m");
        try
        {
            session.execute("SELECT * FROM readcommand.tbl WHERE id = 10");
            fail("Expected ReadTimeoutException to be thrown");
        }
        catch (ReadTimeoutException e)
        {
            // Expected exception
        }

        // the read should not timeout
        DatabaseDescriptor.setInjectArtificialDelayReadPath("0s");
        session.execute(String.format("SELECT * FROM readcommand.tbl WHERE id = 10"));
    }
}

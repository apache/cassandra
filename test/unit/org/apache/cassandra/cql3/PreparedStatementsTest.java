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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static junit.framework.Assert.assertEquals;

public class PreparedStatementsTest extends SchemaLoader
{
    private static Cluster cluster;
    private static Session session;

    private static final String KEYSPACE = "prepared_stmt_cleanup";
    private static final String createKsStatement = "CREATE KEYSPACE " + KEYSPACE +
                                                    " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    private static final String dropKsStatement = "DROP KEYSPACE IF EXISTS " + KEYSPACE;

    @BeforeClass
    public static void setup() throws Exception
    {
        Schema.instance.clear();

        EmbeddedCassandraService cassandra = new EmbeddedCassandraService();
        cassandra.start();

        // Currently the native server start method return before the server is fully binded to the socket, so we need
        // to wait slightly before trying to connect to it. We should fix this but in the meantime using a sleep.
        Thread.sleep(500);

        cluster = Cluster.builder().addContactPoint("127.0.0.1")
                                   .withPort(DatabaseDescriptor.getNativeTransportPort())
                                   .build();
        session = cluster.connect();

        session.execute(dropKsStatement);
        session.execute(createKsStatement);
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        cluster.close();
    }

    @Test
    public void testInvalidatePreparedStatementsOnDrop()
    {
        String createTableStatement = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".qp_cleanup (id int PRIMARY KEY, cid int, val text);";
        String dropTableStatement = "DROP TABLE IF EXISTS " + KEYSPACE + ".qp_cleanup;";

        session.execute(createTableStatement);

        PreparedStatement prepared = session.prepare("INSERT INTO " + KEYSPACE + ".qp_cleanup (id, cid, val) VALUES (?, ?, ?)");
        PreparedStatement preparedBatch = session.prepare("BEGIN BATCH " +
                                                          "INSERT INTO " + KEYSPACE + ".qp_cleanup (id, cid, val) VALUES (?, ?, ?);" +
                                                          "APPLY BATCH;");
        session.execute(dropTableStatement);
        session.execute(createTableStatement);
        session.execute(prepared.bind(1, 1, "value"));
        session.execute(preparedBatch.bind(2, 2, "value2"));

        session.execute(dropKsStatement);
        session.execute(createKsStatement);
        session.execute(createTableStatement);

        // The driver will get a response about the prepared statement being invalid, causing it to transparently
        // re-prepare the statement.  We'll rely on the fact that we get no errors while executing this to show that
        // the statements have been invalidated.
        session.execute(prepared.bind(1, 1, "value"));
        session.execute(preparedBatch.bind(2, 2, "value2"));
        session.execute(dropKsStatement);
    }

    @Test
    public void testStatementRePreparationOnReconnect()
    {
        session.execute(dropKsStatement);
        session.execute(createKsStatement);

        session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".qp_test (id int PRIMARY KEY, cid int, val text);");

        String insertCQL = "INSERT INTO " + KEYSPACE + ".qp_test (id, cid, val) VALUES (?, ?, ?)";
        String selectCQL = "Select * from " + KEYSPACE + ".qp_test where id = ?";

        PreparedStatement preparedInsert = session.prepare(insertCQL);
        PreparedStatement preparedSelect = session.prepare(selectCQL);

        session.execute(preparedInsert.bind(1, 1, "value"));
        assertEquals(1, session.execute(preparedSelect.bind(1)).all().size());

        cluster.close();

        cluster = Cluster.builder().addContactPoint("127.0.0.1")
                                   .withPort(DatabaseDescriptor.getNativeTransportPort())
                                   .build();
        session = cluster.connect();

        preparedInsert = session.prepare(insertCQL);
        preparedSelect = session.prepare(selectCQL);
        session.execute(preparedInsert.bind(1, 1, "value"));

        assertEquals(1, session.execute(preparedSelect.bind(1)).all().size());
    }
}

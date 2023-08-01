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

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ClientsTableKeyspaceColTest
{

    private static Cluster cluster;
    private static EmbeddedCassandraService cassandra;
    private static final String KS_NAME = "vts";

    @BeforeClass
    public static void setupClass() throws IOException
    {
        cassandra = ServerTestUtils.startEmbeddedCassandraService();

        cluster = Cluster.builder().addContactPoint("127.0.0.1")
                         .withPort(DatabaseDescriptor.getNativeTransportPort())
                         .build();

        ClientsTable table = new ClientsTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
    }

    @AfterClass
    public static void tearDown()
    {
        try
        {
            if (cluster != null)
                cluster.close();
        }
        finally
        {
            if (cassandra != null)
                cassandra.stop();
        }
    }

    @Test
    public void testWithoutConnectingToKeyspace()
    {
        try (Session session = cluster.connect())
        {
            List<Row> rows = session.execute("SELECT * from " + KS_NAME + ".clients").all();
            assertTrue("At least one client should be returned.", rows.size() > 0);
            for (Row r : rows)
                assertNull(r.getString("keyspace_name")); // No keyspace is specifed while connecting. It should be null.
        }
    }

    @Test
    public void testChangingKeyspace()
    {
        String keyspace1 = "system_distributed";
        String keyspace2 = "system_auth";
        try (Session session = cluster.connect(keyspace1))
        {

            InetAddress poolConnection = null;
            int port = -1;
            for (Row r : session.execute("SELECT * from " + KS_NAME + ".clients").all())
            {
                // Keyspace is used for pool connection only (control connection is not using keyspace).
                // Using r["keyspace_name"] == keyspace1 as a hint to identify a pool connection as we can't identify
                // control connection based on information in this table.
                String keyspace = r.getString("keyspace_name");
                if (keyspace1.equals(keyspace))
                {
                    poolConnection = r.getInet("address");
                    port = r.getInt("port");
                    break;
                }
            }

            assertNotEquals(-1, port);
            assertNotNull(poolConnection);

            session.execute("USE " + keyspace2);

            String usedKeyspace = null;
            for (Row r : session.execute("SELECT * from " + KS_NAME + ".clients").all())
            {
                if (poolConnection.equals(r.getInet("address")) && port == r.getInt("port"))
                {
                    usedKeyspace = r.getString("keyspace_name");
                    break;
                }
            }

            assertEquals(keyspace2, usedKeyspace);
        }
    }
}

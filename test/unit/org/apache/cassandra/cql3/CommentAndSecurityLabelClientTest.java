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

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.transport.ClientNotificationsTest;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.RegisterMessage;
import org.apache.cassandra.transport.messages.ResultMessage;

import static java.lang.String.format;
import static org.apache.cassandra.transport.Event.SchemaChange.Change.UPDATED;
import static org.apache.cassandra.transport.Event.SchemaChange.Target.TABLE;
import static org.apache.cassandra.transport.Event.SchemaChange.Target.TYPE;
import static org.junit.Assert.assertEquals;

public class CommentAndSecurityLabelClientTest extends CQLTester
{
    private static final String KEYSPACE_NAME = "ks_comment";
    private static final String TABLE_NAME = "tbl_comment";
    private static final String COLUMN_NAME = "name";
    private static final String TYPE_NAME = "type_comment";
    private static final String FIELD_NAME = "f1";

    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();
        CQLTester.setUpClass();
    }

    @Before
    public void setup()
    {
        requireNetwork();
    }

    @Test
    public void testClientResponses() throws IOException
    {
        createSchema();
        try (SimpleClient client = newSimpleClient(ProtocolVersion.CURRENT))
        {
            // Register to receive schema change notifications, the client uses a simple event handler to
            // record and inspect these.
            client.execute(new RegisterMessage(List.of(Event.Type.SCHEMA_CHANGE)));
            ClientNotificationsTest.EventHandler handler = new ClientNotificationsTest.EventHandler();
            client.setEventHandler(handler);

            // Comments
            makeRequestAndVerify(client, handler,
                                 format("COMMENT ON KEYSPACE %s IS 'test comment'", KEYSPACE_NAME),
                                 new Event.SchemaChange(UPDATED, KEYSPACE_NAME));
            makeRequestAndVerify(client, handler,
                                 format("COMMENT ON TABLE %s.%s IS 'test comment'", KEYSPACE_NAME, TABLE_NAME),
                                 new Event.SchemaChange(UPDATED, TABLE, KEYSPACE_NAME, TABLE_NAME));
            makeRequestAndVerify(client, handler,
                                 format("COMMENT ON COLUMN %s.%s.%s IS 'test comment'", KEYSPACE_NAME, TABLE_NAME, COLUMN_NAME),
                                 new Event.SchemaChange(UPDATED, TABLE, KEYSPACE_NAME, TABLE_NAME));
            makeRequestAndVerify(client, handler,
                                 format("COMMENT ON TYPE %s.%s IS 'test comment'", KEYSPACE_NAME, TYPE_NAME),
                                 new Event.SchemaChange(UPDATED, TYPE, KEYSPACE_NAME, TYPE_NAME));
            makeRequestAndVerify(client, handler,
                                 format("COMMENT ON FIELD %s.%s.%s IS 'test comment'", KEYSPACE_NAME, TYPE_NAME, FIELD_NAME),
                                 new Event.SchemaChange(UPDATED, TYPE, KEYSPACE_NAME, TYPE_NAME));

            // Security labels
            makeRequestAndVerify(client, handler,
                                 format("SECURITY LABEL ON KEYSPACE %s IS 'test label'", KEYSPACE_NAME),
                                 new Event.SchemaChange(UPDATED, KEYSPACE_NAME));
            makeRequestAndVerify(client, handler,
                                 format("SECURITY LABEL ON TABLE %s.%s IS 'test label'", KEYSPACE_NAME, TABLE_NAME),
                                 new Event.SchemaChange(UPDATED, TABLE, KEYSPACE_NAME, TABLE_NAME));
            makeRequestAndVerify(client, handler,
                                 format("SECURITY LABEL ON COLUMN %s.%s.%s IS 'test label'", KEYSPACE_NAME, TABLE_NAME, COLUMN_NAME),
                                 new Event.SchemaChange(UPDATED, TABLE, KEYSPACE_NAME, TABLE_NAME));
            makeRequestAndVerify(client, handler,
                                 format("SECURITY LABEL ON TYPE %s.%s IS 'test label'", KEYSPACE_NAME, TYPE_NAME),
                                 new Event.SchemaChange(UPDATED, TYPE, KEYSPACE_NAME, TYPE_NAME));
            makeRequestAndVerify(client, handler,
                                 format("SECURITY LABEL ON FIELD %s.%s.%s IS 'test label'", KEYSPACE_NAME, TYPE_NAME, FIELD_NAME),
                                 new Event.SchemaChange(UPDATED, TYPE, KEYSPACE_NAME, TYPE_NAME));
        }
    }

    private void makeRequestAndVerify(SimpleClient client,
                                      ClientNotificationsTest.EventHandler handler,
                                      String cql,
                                      Event.SchemaChange expected)
    {
        // Assert that the correct response type and content is received
        ResultMessage result = client.execute(cql, ConsistencyLevel.ONE);
        assertEquals(result.kind, ResultMessage.Kind.SCHEMA_CHANGE);
        ResultMessage.SchemaChange message = (ResultMessage.SchemaChange) result;
        assertEquals(message.change, expected);
        // Verify that the expected notification was received and passed to the event handler
        handler.assertNextEvent(expected);
    }

    private void createSchema()
    {
        createKeyspace(format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE_NAME));
        createTable(format("CREATE TABLE %s.%s (id int PRIMARY KEY, name text)", KEYSPACE_NAME, TABLE_NAME));
        execute(format("CREATE TYPE %s.%s (%s int)", KEYSPACE_NAME, TYPE_NAME, FIELD_NAME));
    }
}

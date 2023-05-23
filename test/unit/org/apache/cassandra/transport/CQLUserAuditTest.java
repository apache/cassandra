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

package org.apache.cassandra.transport;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.audit.AuditEvent;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static org.apache.cassandra.config.CassandraRelevantProperties.SUPERUSER_SETUP_DELAY_MS;
import static org.apache.cassandra.utils.concurrent.BlockingQueues.newBlockingQueue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class CQLUserAuditTest
{
    private static EmbeddedCassandraService embedded;
    private static final BlockingQueue<AuditEvent> auditEvents = newBlockingQueue();

    @BeforeClass
    public static void setup() throws Exception
    {
        OverrideConfigurationLoader.override((config) -> {
            config.authenticator = new ParameterizedClass("PasswordAuthenticator");
            config.role_manager = "CassandraRoleManager";
            config.diagnostic_events_enabled = true;
            config.audit_logging_options.enabled = true;
            config.audit_logging_options.logger = new ParameterizedClass("DiagnosticEventAuditLogger", null);
        });

        SUPERUSER_SETUP_DELAY_MS.setLong(0);

        embedded = ServerTestUtils.startEmbeddedCassandraService();

        executeAs(Arrays.asList("CREATE ROLE testuser WITH LOGIN = true AND SUPERUSER = false AND PASSWORD = 'foo'",
                                "CREATE ROLE testuser_nologin WITH LOGIN = false AND SUPERUSER = false AND PASSWORD = 'foo'",
                                "CREATE KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
                                "CREATE TABLE testks.table1 (a text, b int, c int, PRIMARY KEY (a, b))",
                                "CREATE TABLE testks.table2 (a text, b int, c int, PRIMARY KEY (a, b))"),
                  "cassandra", "cassandra", null);

        DiagnosticEventService.instance().subscribe(AuditEvent.class, auditEvents::add);
        AuditLogManager.instance.initialize();
    }

    @AfterClass
    public static void shutdown()
    {
        embedded.stop();
    }

    @After
    public void clearQueue()
    {
        auditEvents.clear();
    }

    @Test
    public void loginWrongPasswordTest() throws Throwable
    {
        executeAs(Collections.emptyList(), "testuser", "wrongpassword", AuditLogEntryType.LOGIN_ERROR);
    }

    @Test
    public void loginWrongUsernameTest() throws Throwable
    {
        executeAs(Collections.emptyList(), "wronguser", "foo", AuditLogEntryType.LOGIN_ERROR);
    }

    @Test
    public void loginDeniedTest() throws Throwable
    {
        executeAs(Collections.emptyList(), "testuser_nologin", "foo", AuditLogEntryType.LOGIN_ERROR);
    }

    @Test
    public void loginSuccessfulTest() throws Throwable
    {
        executeAs(Collections.emptyList(), "testuser", "foo", AuditLogEntryType.LOGIN_SUCCESS);
    }

    @Test
    public void querySimpleSelect() throws Throwable
    {
        ArrayList<AuditEvent> events = executeAs(Arrays.asList("SELECT * FROM testks.table1"),
                                                 "testuser", "foo", AuditLogEntryType.LOGIN_SUCCESS);
        assertEquals(1, events.size());
        AuditEvent e = events.get(0);
        Map<String, Serializable> m = e.toMap();
        assertEquals("testuser", m.get("user"));
        assertEquals("SELECT * FROM testks.table1", m.get("operation"));
        assertEquals("testks", m.get("keyspace"));
        assertEquals("table1", m.get("scope"));
        assertEquals(AuditLogEntryType.SELECT, e.getType());
    }

    @Test
    public void queryInsert() throws Throwable
    {
        ArrayList<AuditEvent> events = executeAs(Arrays.asList("INSERT INTO testks.table1 (a, b, c) VALUES ('a', 1, 1)"),
                                                 "testuser", "foo", AuditLogEntryType.LOGIN_SUCCESS);
        assertEquals(1, events.size());
        AuditEvent e = events.get(0);
        Map<String, Serializable> m = e.toMap();
        assertEquals("testuser", m.get("user"));
        assertEquals("INSERT INTO testks.table1 (a, b, c) VALUES ('a', 1, 1)", m.get("operation"));
        assertEquals("testks", m.get("keyspace"));
        assertEquals("table1", m.get("scope"));
        assertEquals(AuditLogEntryType.UPDATE, e.getType());
    }

    @Test
    public void queryBatch() throws Throwable
    {
        String query = "BEGIN BATCH "
                       + "INSERT INTO testks.table1 (a, b, c) VALUES ('a', 1, 1); "
                       + "INSERT INTO testks.table1 (a, b, c) VALUES ('b', 1, 1); "
                       + "INSERT INTO testks.table1 (a, b, c) VALUES ('b', 2, 2); "
                       + "APPLY BATCH;";
        ArrayList<AuditEvent> events = executeAs(Arrays.asList(query),
                                                 "testuser", "foo",
                                                 AuditLogEntryType.LOGIN_SUCCESS);
        assertEquals(1, events.size());
        AuditEvent e = events.get(0);
        Map<String, Serializable> m = e.toMap();
        assertEquals("testuser", m.get("user"));
        assertEquals(query, m.get("operation"));
        assertEquals(AuditLogEntryType.BATCH, e.getType());
    }

    @Test
    public void prepareStmt()
    {
        Cluster cluster = Cluster.builder().addContactPoints(InetAddress.getLoopbackAddress())
                                 .withoutJMXReporting()
                                 .withCredentials("testuser", "foo")
                                 .withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        String spStmt = "INSERT INTO testks.table1 (a, b, c) VALUES (?, ?, ?)";
        try (Session session = cluster.connect())
        {
            PreparedStatement pStmt = session.prepare(spStmt);
            session.execute(pStmt.bind("x", 9, 8));
        }

        List<AuditEvent> events = auditEvents.stream().filter((e) -> e.getType() != AuditLogEntryType.LOGIN_SUCCESS)
                                             .collect(Collectors.toList());
        AuditEvent e = events.get(0);
        Map<String, Serializable> m = e.toMap();
        assertEquals(2, events.size());
        assertEquals("testuser", m.get("user"));
        assertEquals(spStmt, m.get("operation"));
        assertEquals("testks", m.get("keyspace"));
        assertEquals("table1", m.get("scope"));
        assertEquals(AuditLogEntryType.PREPARE_STATEMENT, e.getType());

        e = events.get(1);
        m = e.toMap();
        assertEquals("testuser", m.get("user"));
        assertEquals(spStmt, m.get("operation"));
        assertEquals("testks", m.get("keyspace"));
        assertEquals("table1", m.get("scope"));
        assertEquals(AuditLogEntryType.UPDATE, e.getType());

    }

    private static ArrayList<AuditEvent> executeAs(List<String> queries, String username, String password,
                                                   AuditLogEntryType expectedAuthType) throws Exception
    {
        boolean authFailed = false;
        Cluster cluster = Cluster.builder().addContactPoints(InetAddress.getLoopbackAddress())
                                 .withoutJMXReporting()
                                 .withCredentials(username, password)
                                 .withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        try (Session session = cluster.connect())
        {
            for (String query : queries)
                session.execute(query);
        }
        catch (AuthenticationException e)
        {
            authFailed = true;
        }
        cluster.close();

        if (expectedAuthType == null) return null;

        AuditEvent event = auditEvents.poll(100, TimeUnit.MILLISECONDS);
        assertEquals(expectedAuthType, event.getType());
        assertTrue(!authFailed || event.getType() == AuditLogEntryType.LOGIN_ERROR);
        assertEquals(InetAddressAndPort.getLoopbackAddress().getAddress(),
                     event.getEntry().getSource().getAddress());
        assertTrue(event.getEntry().getSource().getPort() > 0);
        if (event.getType() != AuditLogEntryType.LOGIN_ERROR)
            assertEquals(username, event.toMap().get("user"));

        // drain all remaining login related events, as there's no specification how connections and login attempts
        // should be handled by the driver, so we can't assert a fixed number of login events
        for (AuditEvent e = auditEvents.peek();
             e != null && (e.getType() == AuditLogEntryType.LOGIN_ERROR
                           || e.getType() == AuditLogEntryType.LOGIN_SUCCESS);
             e = auditEvents.peek())
        {
            auditEvents.remove(e);
        }

        ArrayList<AuditEvent> ret = new ArrayList<>(auditEvents.size());
        auditEvents.drainTo(ret);
        return ret;
    }
}

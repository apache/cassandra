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
package org.apache.cassandra.audit;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** AuditUsersCacheTest class is responsible for covering test cases for AuditUsersCacheService */

public class AuditUsersCacheTest
{
    private static EmbeddedCassandraService embedded;

    private static final String TEST_USER = "testuser";
    private static final String TEST_SERVICE = "testservice";
    private static final String TEST_PW = "testpassword";
    private static final String CASS_USER = "cassandra";
    private static final String CASS_PW = "cassandra";

    @BeforeClass
    public static void setup() throws Exception
    {
        OverrideConfigurationLoader.override((config) -> {
            config.authenticator = "PasswordAuthenticator";
            config.role_manager = "CassandraRoleManager";
            config.authorizer = "CassandraAuthorizer";
            config.audit_logging_options.enabled = true;
            config.audit_logging_options.logger = new ParameterizedClass("InMemoryAuditLogger", null);
        });
        CQLTester.prepareServer();

        System.setProperty("cassandra.superuser_setup_delay_ms", "0");
        embedded = new EmbeddedCassandraService();
        embedded.start();

        executeWithCredentials(
                Arrays.asList(getCreateRoleCql(TEST_USER, true, false, TEST_PW),
                        getCreateRoleCql(TEST_SERVICE, true, false, TEST_PW),
                        "CREATE KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
                        "CREATE TABLE testks.table1 (key text PRIMARY KEY, col1 int, col2 int)"),
                "cassandra", "cassandra", null);


        Thread.sleep(5000);
        /**
         * Insert into system_distributed/audit_users table
         */
        AuditUsersCacheService.instance.setup();
        executeWithCredentials(
                Arrays.asList(getInsertAuditRoleCql(TEST_USER, "Developer", 100.0),
                        getInsertAuditRoleCql(TEST_SERVICE, "Service", 0.0),
                        getGrantPermCql(TEST_USER, "testks"), getGrantPermCql(TEST_SERVICE, "testks")),
                "cassandra", "cassandra", null);

        AuditUsersCacheService.instance.insert(TEST_USER, "EMPLOYEE", 100.0);
        AuditUsersCacheService.instance.insert(TEST_SERVICE, "SERVICE", 0.0);
        AuditUsersCacheService.instance.refresh();
    }


    @AfterClass
    public static void shutdown()
    {
        embedded.stop();
    }

    @Before
    public void clearInMemoryLogger()
    {
        getInMemAuditLogger().clear();
    }

    @Test
    public void testAuditCaasUser()
    {
        String cql = "LIST ALL";
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.LIST_PERMISSIONS, cql, CASS_USER);
    }

    @Test
    public void testAuditDeveloper()
    {
        String cql = "SELECT * FROM testks.table1";
        executeWithCredentials(Arrays.asList(cql), TEST_USER, TEST_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.SELECT, cql, TEST_USER);
    }

    @Test
    public void testAuditService()
    {
        String cql = "SELECT * FROM testks.table1";
        executeWithCredentials(Arrays.asList(cql), TEST_SERVICE, TEST_PW, null);
        assertTrue(getInMemAuditLogger().size() == 0);
    }

    /**
     * Helper methods
     */

    private static void executeWithCredentials(List<String> queries, String username, String password,
                                               AuditLogEntryType expectedType)
    {
        boolean authFailed = false;
        try (Cluster cluster = Cluster.builder().addContactPoints(InetAddress.getLoopbackAddress())
                .withoutJMXReporting()
                .withCredentials(username, password)
                .withPort(DatabaseDescriptor.getNativeTransportPort()).build())
        {
            try (Session session = cluster.connect())
            {
                for (String query : queries)
                    session.execute(query);
            }
            catch (AuthenticationException e)
            {
                authFailed = true;
            }
            catch (UnauthorizedException ue)
            {
                //no-op, taken care by caller
            }
        }

        if (expectedType != null)
        {
            assertTrue(getInMemAuditLogger().size() > 0);
            AuditLogEntry logEntry = getInMemAuditLogger().poll();

            assertEquals(expectedType, logEntry.getType());
            assertTrue(!authFailed || logEntry.getType() == AuditLogEntryType.LOGIN_ERROR);
            assertSource(logEntry, username);

            // drain all remaining login related events, as there's no specification how connections and login attempts
            // should be handled by the driver, so we can't assert a fixed number of login events
            getInMemAuditLogger()
                    .removeIf(auditLogEntry -> auditLogEntry.getType() == AuditLogEntryType.LOGIN_ERROR
                            || auditLogEntry.getType() == AuditLogEntryType.LOGIN_SUCCESS);
        }
    }

    private static Queue<AuditLogEntry> getInMemAuditLogger()
    {
        return ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).inMemQueue;
    }

    private static void assertLogEntry(AuditLogEntry logEntry, AuditLogEntryType type, String cql, String username)
    {
        assertSource(logEntry, username);
        assertNotEquals(0, logEntry.getTimestamp());
        assertEquals(type, logEntry.getType());
        if (null != cql && !cql.isEmpty())
        {
            assertThat(logEntry.getOperation(), containsString(cql));
        }
    }

    private static void assertSource(AuditLogEntry logEntry, String username)
    {
        assertEquals(InetAddressAndPort.getLoopbackAddress().getAddress(), logEntry.getSource().getAddress());
        assertTrue(logEntry.getSource().getPort() > 0);
        if (logEntry.getType() != AuditLogEntryType.LOGIN_ERROR)
            assertEquals(username, logEntry.getUser());
    }

    private static String getCreateRoleCql(String role, boolean login, boolean superUser, String password)
    {
        return String.format("CREATE ROLE IF NOT EXISTS %s WITH LOGIN = %s AND SUPERUSER = %s AND PASSWORD = '%s'",
                role, login, superUser, password);
    }


    private static String getInsertAuditRoleCql(String role, String account_type, double percent)
    {
        return String.format("INSERT INTO system_distributed.audit_users (role, account_type, filter_percent) VALUES ('%s', '%s', %f)",
                role, account_type, percent);
    }

    private static String getGrantPermCql(String role, String keyspace)
    {
        return String.format("GRANT ALL ON KEYSPACE %s TO %s", keyspace, role);
    }
}
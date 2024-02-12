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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.PasswordObfuscator;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.hamcrest.CoreMatchers;

import static org.apache.cassandra.config.CassandraRelevantProperties.SUPERUSER_SETUP_DELAY_MS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * AuditLoggerAuthTest class is responsible for covering test cases for Authenticated user (LOGIN) audits.
 * Non authenticated tests are covered in {@link AuditLoggerTest}
 */

public class AuditLoggerAuthTest
{
    private static EmbeddedCassandraService embedded;

    private static final String TEST_USER = "testuser";
    private static final String TEST_ROLE = "testrole";
    private static final String TEST_PW = "testpassword";
    private static final String TEST_PW_HASH = "$2a$10$1fI9MDCe13ZmEYW4XXZibuASNKyqOY828ELGUtml/t.0Mk/6Kqnsq";
    private static final String CASS_USER = "cassandra";
    private static final String CASS_PW = "cassandra";

    @BeforeClass
    public static void setup() throws Exception
    {
        OverrideConfigurationLoader.override((config) -> {
            config.authenticator = new ParameterizedClass("PasswordAuthenticator");
            config.role_manager = "CassandraRoleManager";
            config.authorizer = "CassandraAuthorizer";
            config.audit_logging_options.enabled = true;
            config.audit_logging_options.logger = new ParameterizedClass("InMemoryAuditLogger", null);
        });

        SUPERUSER_SETUP_DELAY_MS.setLong(0);
        embedded = ServerTestUtils.startEmbeddedCassandraService();

        executeWithCredentials(
        Arrays.asList(getCreateRoleCql(TEST_USER, true, false, false),
                      getCreateRoleCql("testuser_nologin", false, false, false),
                      "CREATE KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
                      "CREATE TABLE testks.table1 (key text PRIMARY KEY, col1 int, col2 int)"),
        "cassandra", "cassandra", null);
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
    public void testCqlLoginAuditing() throws Throwable
    {
        executeWithCredentials(Collections.emptyList(), TEST_USER, "wrongpassword",
                               AuditLogEntryType.LOGIN_ERROR);
        assertEquals(0, getInMemAuditLogger().size());
        clearInMemoryLogger();

        executeWithCredentials(Collections.emptyList(), "wronguser", TEST_PW, AuditLogEntryType.LOGIN_ERROR);
        assertEquals(0, getInMemAuditLogger().size());
        clearInMemoryLogger();

        executeWithCredentials(Collections.emptyList(), "testuser_nologin",
                               TEST_PW, AuditLogEntryType.LOGIN_ERROR);
        assertEquals(0, getInMemAuditLogger().size());
        clearInMemoryLogger();

        executeWithCredentials(Collections.emptyList(), TEST_USER, TEST_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertEquals(0, getInMemAuditLogger().size());
        clearInMemoryLogger();
    }

    @Test
    public void testCqlCreateRoleAuditing()
    {
        createTestRole();
    }

    @Test
    public void testCqlCreateRoleSyntaxError()
    {
        String createTestRoleCQL = String.format("CREATE ROLE %s WITH LOGIN = %s ANDSUPERUSER = %s AND PASSWORD",
                                                 TEST_ROLE, true, false) + CASS_PW;
        String createTestRoleCQLExpected = String.format("CREATE ROLE %s WITH LOGIN = %s ANDSUPERUSER = %s AND PASSWORD ",
                                                         TEST_ROLE, true, false) + PasswordObfuscator.OBFUSCATION_TOKEN;

        executeWithCredentials(Arrays.asList(createTestRoleCQL), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.REQUEST_FAILURE, createTestRoleCQLExpected, CASS_USER,  TEST_PW);
        assertEquals(0, getInMemAuditLogger().size());
    }

    @Test
    public void testCqlCreateRoleSyntaxErrorWithHashedPwd()
    {
        String createTestRoleCQL = String.format("CREATE ROLE %s WITH LOGIN = %s ANDSUPERUSER = %s AND HASHED PASSWORD",
                                                 TEST_ROLE, true, false) + TEST_PW_HASH;
        String createTestRoleCQLExpected = String.format("CREATE ROLE %s WITH LOGIN = %s ANDSUPERUSER = %s AND HASHED PASSWORD ",
                                                         TEST_ROLE, true, false) + PasswordObfuscator.OBFUSCATION_TOKEN;

        executeWithCredentials(Collections.singletonList(createTestRoleCQL), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.REQUEST_FAILURE, createTestRoleCQLExpected, CASS_USER,  TEST_PW);
        assertEquals(0, getInMemAuditLogger().size());
    }

    @Test
    public void testCqlALTERRoleAuditing()
    {
        createTestRole();
        String cql = "ALTER ROLE " + TEST_ROLE + " WITH PASSWORD = 'foo_bar'";
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry,
                       AuditLogEntryType.ALTER_ROLE,
                       "ALTER ROLE " + TEST_ROLE + " WITH PASSWORD = '" + PasswordObfuscator.OBFUSCATION_TOKEN + "'",
                       CASS_USER,
                       "foo_bar");
        assertEquals(0, getInMemAuditLogger().size());
    }

    @Test
    public void testCqlALTERRoleAuditingWithHashedPwd()
    {
        createTestRole();
        String cql = "ALTER ROLE " + TEST_ROLE + " WITH HASHED PASSWORD = '" + TEST_PW_HASH + "'";
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry,
                       AuditLogEntryType.ALTER_ROLE,
                       "ALTER ROLE " + TEST_ROLE +
                       " WITH HASHED PASSWORD = '" + PasswordObfuscator.OBFUSCATION_TOKEN + "'",
                       CASS_USER,
                       TEST_PW_HASH);
        assertEquals(0, getInMemAuditLogger().size());
    }

    @Test
    public void testCqlDROPRoleAuditing()
    {
        createTestRole();
        String cql = "DROP ROLE " + TEST_ROLE;
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.DROP_ROLE, cql, CASS_USER, "");
        assertEquals(0, getInMemAuditLogger().size());
    }

    @Test
    public void testCqlLISTROLESAuditing()
    {
        String cql = "LIST ROLES";
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.LIST_ROLES, cql, CASS_USER, "");
    }

    @Test
    public void testCqlLISTPERMISSIONSAuditing()
    {
        String cql = "LIST ALL";
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.LIST_PERMISSIONS, cql, CASS_USER, "");
    }

    @Test
    public void testCqlGRANTAuditing()
    {
        createTestRole();
        String cql = "GRANT SELECT ON ALL KEYSPACES TO " + TEST_ROLE;
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.GRANT, cql, CASS_USER, "");
    }

    @Test
    public void testCqlREVOKEAuditing()
    {
        createTestRole();
        String cql = "REVOKE ALTER ON ALL KEYSPACES FROM " + TEST_ROLE;
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.REVOKE, cql, CASS_USER, "");
    }

    @Test
    public void testUNAUTHORIZED_ATTEMPTAuditing()
    {
        createTestRole();
        String cql = "ALTER ROLE " + TEST_ROLE + " WITH superuser = true";
        executeWithCredentials(Arrays.asList(cql), TEST_USER, TEST_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.UNAUTHORIZED_ATTEMPT, cql, TEST_USER, "");
        assertEquals(0, getInMemAuditLogger().size());
    }

    @Test
    public void testCqlUSERCommandsAuditing()
    {
        //CREATE USER and ALTER USER are supported only for backwards compatibility.

        String user = TEST_ROLE + "user";
        String cql = "CREATE USER " + user + " WITH PASSWORD '" + TEST_PW + "'";
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry,
                       AuditLogEntryType.CREATE_ROLE,
                       "CREATE USER " + user + " WITH PASSWORD '" + PasswordObfuscator.OBFUSCATION_TOKEN + "'",
                       CASS_USER,
                       TEST_PW);

        cql = "ALTER USER " + user + " WITH PASSWORD '" + TEST_PW + "'";
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry,
                       AuditLogEntryType.ALTER_ROLE,
                       "ALTER USER " + user + " WITH PASSWORD '" + PasswordObfuscator.OBFUSCATION_TOKEN + "'",
                       CASS_USER,
                       TEST_PW);

        cql = "ALTER USER " + user + " WITH PASSWORD " + TEST_PW;
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry,
                       AuditLogEntryType.REQUEST_FAILURE,
                       "ALTER USER " + user
                       + " WITH PASSWORD " + PasswordObfuscator.OBFUSCATION_TOKEN
                       + "; Syntax Exception. Obscured for security reasons.",
                       CASS_USER,
                       TEST_PW);
    }

    @Test
    public void testCqlUSERCommandsAuditingWithHashedPwd()
    {
        //CREATE USER and ALTER USER are supported only for backwards compatibility.

        String user = TEST_ROLE + "userHasedPwd";
        String cql = "CREATE USER " + user + " WITH HASHED PASSWORD '" + TEST_PW_HASH + "'";
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry,
                       AuditLogEntryType.CREATE_ROLE,
                       "CREATE USER " + user + " WITH HASHED PASSWORD '" + PasswordObfuscator.OBFUSCATION_TOKEN + "'",
                       CASS_USER,
                       TEST_PW_HASH);

        cql = "ALTER USER " + user + " WITH HASHED PASSWORD '" + TEST_PW_HASH + "'";
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry,
                       AuditLogEntryType.ALTER_ROLE,
                       "ALTER USER " + user + " WITH HASHED PASSWORD '" + PasswordObfuscator.OBFUSCATION_TOKEN + "'",
                       CASS_USER,
                       TEST_PW_HASH);

        cql = "ALTER USER " + user + " WITH HASHED PASSWORD " + TEST_PW_HASH;
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry,
                       AuditLogEntryType.REQUEST_FAILURE,
                       "ALTER USER " + user
                       + " WITH HASHED PASSWORD " + PasswordObfuscator.OBFUSCATION_TOKEN
                       + "; Syntax Exception. Obscured for security reasons.",
                       CASS_USER,
                       TEST_PW_HASH);
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
            catch (SyntaxError se)
            {
                // no-op, taken care of by caller
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

    private static void assertLogEntry(AuditLogEntry logEntry, AuditLogEntryType type, String cql, String username, String forbiddenPassword)
    {
        assertSource(logEntry, username);
        assertNotEquals(0, logEntry.getTimestamp());
        assertEquals(type, logEntry.getType());
        if (null != cql && !cql.isEmpty())
        {
            assertThat(logEntry.getOperation(), containsString(cql));
            if (!forbiddenPassword.isEmpty())
                assertThat(logEntry.getOperation(), CoreMatchers.not(containsString(forbiddenPassword)));
        }
    }

    private static void assertSource(AuditLogEntry logEntry, String username)
    {
        assertEquals(InetAddressAndPort.getLoopbackAddress().getAddress(), logEntry.getSource().getAddress());
        assertTrue(logEntry.getSource().getPort() > 0);
        if (logEntry.getType() != AuditLogEntryType.LOGIN_ERROR)
            assertEquals(username, logEntry.getUser());
    }

    private static String getCreateRoleCql(String role, boolean login, boolean superUser, boolean isPasswordObfuscated)
    {
        return String.format("CREATE ROLE IF NOT EXISTS %s WITH PASSWORD = '%s' AND LOGIN = %s AND SUPERUSER = %s",
                             role,
                             isPasswordObfuscated ? PasswordObfuscator.OBFUSCATION_TOKEN : TEST_PW,
                             login,
                             superUser);
    }

    private static void createTestRole()
    {
        String createTestRoleCQL = getCreateRoleCql(TEST_ROLE, true, false, false);
        executeWithCredentials(Arrays.asList(createTestRoleCQL), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.CREATE_ROLE, getCreateRoleCql(TEST_ROLE, true, false, true), CASS_USER, "");
        assertEquals(0, getInMemAuditLogger().size());
    }
}

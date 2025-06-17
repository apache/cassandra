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

import java.lang.NoSuchFieldError;
import java.lang.NoSuchFieldException;
import java.lang.reflect.Field;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;


import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageService;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

/** AuditUsersCacheTest class is responsible for covering test cases for AuditUsersCacheService */
public class AuditUsersCacheTest
{
    private static Field refreshTaskField;
    private static Field auditUserCacheField;
    private static Field auditLoggerField;
    private static Map<String, AuditUsersCacheService.UserProp> auditUserCache;
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
            config.audit_logging_options.role_filtering = true;
            config.audit_logging_options.logger = new ParameterizedClass("InMemoryAuditLogger", null);
        });
        CQLTester.prepareServer();

        System.setProperty("cassandra.superuser_setup_delay_ms", "0");
        embedded = new EmbeddedCassandraService();
        embedded.start();

        Thread.sleep(5000);
        AuditUsersCacheService.instance.setup();

        executeWithCredentials(
        Collections.singletonList(String.format("INSERT INTO system_distributed.audit_users " +
            "(role, account_type, filter_percent) VALUES ('%s', 'SERVICE', 100.0)", CASS_USER)),
            CASS_USER, CASS_PW, null);

        executeWithCredentials(
        Collections.singletonList(String.format("INSERT INTO system_distributed.audit_users " +
                                                "(role, account_type, filter_percent) VALUES ('%s', 'SERVICE', 100.0)", TEST_USER)),
        CASS_USER, CASS_PW, null);

        executeWithCredentials(
        Arrays.asList(getCreateRoleCql(TEST_USER, true, false, TEST_PW),
                      getCreateRoleCql(TEST_SERVICE, true, false, TEST_PW),
                      "CREATE KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
                      "CREATE TABLE testks.table1 (key text PRIMARY KEY, col1 int, col2 int)"),
        "cassandra", "cassandra", null);
        /**
         * Insert into system_distributed/audit_users table
         */
        executeWithCredentials(
        Arrays.asList(getInsertAuditRoleCql(TEST_USER, "Developer", 100.0),
                      getInsertAuditRoleCql(TEST_SERVICE, "Service", 0.0),
                      getGrantPermCql(TEST_USER, "testks"), getGrantPermCql(TEST_SERVICE, "testks")),
        "cassandra", "cassandra", null);

        AuditUsersCacheService.instance.refresh();

        try
        {
            refreshTaskField = AuditUsersCacheService.class.getDeclaredField("refreshTask");
            refreshTaskField.setAccessible(true);

            auditUserCacheField = AuditUsersCacheService.class.getDeclaredField("auditUserCache");
            auditUserCacheField.setAccessible(true);

            auditLoggerField = AuditLogManager.class.getDeclaredField("auditLogger");
            auditLoggerField.setAccessible(true);

            Field modifiersField3 = Field.class.getDeclaredField("modifiers");
            modifiersField3.setAccessible(true);
        } catch (NoSuchFieldException | NoSuchFieldError e) {
            fail("MUST change fild accessability: " + e.getMessage());
        }

        Object obj = auditUserCacheField.get(AuditUsersCacheService.instance);
        if (obj instanceof Map) {
            auditUserCache = (Map<String, AuditUsersCacheService.UserProp>)obj;
        } else {
            fail("should be of type Map<String, UserProp>");
        }
    }


    @AfterClass
    public static void shutdown()
    {
        StorageService.instance.doAuditUsersCacheServiceTeardown();
        embedded.stop();
    }

    @Before
    public void clearInMemoryLogger()
    {
        StorageService.instance.enableAuditLog(true, "InMemoryAuditLogger",
                                               Map.of(), "", "", "", "", "", "",
                                               10, true, "HOURLY",
                                               1024L, 1024, null);

        AuditUsersCacheService.instance.initialize();
        AuditUsersCacheService.instance.setup();
        AuditUsersCacheService.instance.refresh();
        getInMemAuditLogger().clear();
    }

    @After
    public void afterEachMethod()
    {
        AuditLogManager.instance.disableAuditLog();
    }

    @Test
    public void testAuditCaasUser()
    {
        auditUserCache.put(CASS_USER, new AuditUsersCacheService.UserProp("SERVICE", 100.0));
        String cql = "LIST ALL";
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS);
        assertTrue(getInMemAuditLogger().size() > 0);
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.PREPARE_STATEMENT, cql, CASS_USER);
        logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.LIST_PERMISSIONS, cql, CASS_USER);

        // test execute failure
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, AuditLogEntryType.LOGIN_SUCCESS, true);
        // remove all prepared statement related log
        getInMemAuditLogger().removeIf(auditLogEntry -> auditLogEntry.getType() == AuditLogEntryType.PREPARE_STATEMENT);
        assertEquals(2, getInMemAuditLogger().size());
        logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.REQUEST_FAILURE, null, CASS_USER);
        logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.LIST_PERMISSIONS, cql, CASS_USER);
    }

    @Test
    public void testAuditDeveloper()
    {
        String cql = "SELECT * FROM testks.table1";
        executeWithCredentials(Arrays.asList(cql), TEST_USER, TEST_PW, AuditLogEntryType.LOGIN_SUCCESS);
        System.out.println(getInMemAuditLogger().size());
        assertEquals(2, getInMemAuditLogger().size());
        AuditLogEntry logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.PREPARE_STATEMENT, cql, TEST_USER);
        logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.SELECT, cql, TEST_USER);

        // test execute failure
        executeWithCredentials(Arrays.asList(cql), TEST_USER, TEST_PW, AuditLogEntryType.LOGIN_SUCCESS, true);
        // remove all prepared statement related log
        getInMemAuditLogger().removeIf(auditLogEntry -> auditLogEntry.getType() == AuditLogEntryType.PREPARE_STATEMENT);
        assertEquals(2, getInMemAuditLogger().size());
        // first log is for PreparedQueryNotFoundException, the query should be null
        logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.REQUEST_FAILURE, null, TEST_USER);
        // Client will re-prepared automatically, the second non-prepare log would be normal select
        logEntry = getInMemAuditLogger().poll();
        assertLogEntry(logEntry, AuditLogEntryType.SELECT, cql, TEST_USER);
    }

    @Test
    public void testAuditService()
    {
        AuditUsersCacheService.instance.state = AuditUsersCacheService.State.READY;
        String cql = "SELECT * FROM testks.table1";
        executeWithCredentials(Arrays.asList(cql), TEST_SERVICE, TEST_PW, null);
        assertTrue(getInMemAuditLogger().size() == 0);
        // test execute failure
        executeWithCredentials(Arrays.asList(cql), TEST_SERVICE, TEST_PW, null, true);
        assertTrue(getInMemAuditLogger().size() == 0);
    }

    @Test
    public void cacheProbabilityIsHonouredWhenFilteringEnabled() throws Throwable {
        auditUserCache.put(TEST_USER, new AuditUsersCacheService.UserProp("EMPLOYEE", 0.0));

        // toggle the assigned logger to force audit log options reload
        StorageService.instance.enableAuditLog(false, "NoOpAuditLogger",
                                               Map.of(), "", "", "", "", "", "",
                                               10, true, "HOURLY",
                                               1024L, 1024, null);

        StorageService.instance.enableAuditLog(true, "InMemoryAuditLogger",
                                               Map.of(), "", "", "", "", "", "",
                                               10, true, "HOURLY",
                                               1024L, 1024, null);

        String cql = "SELECT * FROM testks.table1";
        getInMemAuditLogger().clear();
        AuditUsersCacheService.instance.state = AuditUsersCacheService.State.READY;
        executeWithCredentials(Arrays.asList(cql), TEST_USER, TEST_PW, null);
        assertEquals(0, getInMemAuditLogger().size());
    }

    @Test
    public void cacheStartedOnlyWhenRoleFilteringEnabled()
    {
        try
        {
            AuditUsersCacheService.instance.teardown();
            AuditUsersCacheService.instance.initCalled.set(false);

            // Enable without role filtering should not start refresh task
            StorageService.instance.enableAuditLog(false, "NoOpAuditLogger", Map.of(), "", "", "", "", "", "", 10, true, "HOURLY", 1024L, 1024, null);
            assertNull(refreshTaskField.get(AuditUsersCacheService.instance));
            assertFalse(AuditUsersCacheService.instance.initCalled.get());
            StorageService.instance.disableAuditLog();

            // Enable with role filtering should start refresh task once
            StorageService.instance.enableAuditLog(true, "BinAuditLogger", Map.of(), "", "", "", "", "", "", 10, true, "HOURLY", 1024L, 1024, null);
            assertTrue(AuditUsersCacheService.instance.initCalled.get());
            ScheduledFuture<?> first = (ScheduledFuture<?>) refreshTaskField.get(AuditUsersCacheService.instance);
            assertNotNull(first);
            assertFalse(first.isCancelled());

            // Additional calls should not create another task
            StorageService.instance.enableAuditLog(true, "InMemoryAuditLogger", Map.of(), "", "", "", "", "", "", 10, true, "HOURLY", 1024L, 1024, null);
            ScheduledFuture<?> second = (ScheduledFuture<?>) refreshTaskField.get(AuditUsersCacheService.instance);
            assertNotNull(second);
            assertSame(first, second);

            // Audit log disabled -> task is no longer running runs
            StorageService.instance.disableAuditLog();
            assertNull(refreshTaskField.get(AuditUsersCacheService.instance));
        }
        catch (IllegalAccessException e)
        {
            fail(e.getMessage());
        }
    }

    @Test
    public void testRefreshSkipsRowsMissingFilterPercent()
    {
        final String INVALID_ROLE = "invalid_role_missing_columns";

        executeWithCredentials(
        Arrays.asList(String.format(
        "INSERT INTO system_distributed.audit_users (role, account_type) " +
        "VALUES ('%s', 'SERVICE')",
        INVALID_ROLE)),
        CASS_USER, CASS_PW, null);

        // Re-load the cache
        AuditUsersCacheService.instance.refresh();

        // The incomplete row must NOT be present in the in-memory cache
        assertEquals("", AuditUsersCacheService.instance.getAccountType(INVALID_ROLE));
        assertFalse(AuditUsersCacheService.instance.shouldLog(INVALID_ROLE));

        executeWithCredentials(Arrays.asList(String.format(
                                             "DELETE FROM system_distributed.audit_users WHERE role = '%s'",
                                             INVALID_ROLE)
        ), CASS_USER, CASS_PW, null);
    }

    @Test
    public void testRefreshSkipsRowsMissingAccountType()
    {
        final String INVALID_ROLE = "invalid_role_missing_columns";

        executeWithCredentials(
        Arrays.asList(String.format(
        "INSERT INTO system_distributed.audit_users (role, filter_percent) " +
        "VALUES ('%s', 0.01)",
        INVALID_ROLE)),
        CASS_USER, CASS_PW, null);

        AuditUsersCacheService.instance.refresh();

        assertEquals("", AuditUsersCacheService.instance.getAccountType(INVALID_ROLE));
        assertFalse(AuditUsersCacheService.instance.shouldLog(INVALID_ROLE));

        executeWithCredentials(Arrays.asList(String.format(
                                             "DELETE FROM system_distributed.audit_users WHERE role = '%s'",
                                             INVALID_ROLE)
        ), CASS_USER, CASS_PW, null);;
    }

    @Test
    public void testRefreshRemovesOrphanRole() throws Exception
    {
        Field cacheField = AuditUsersCacheService.class.getDeclaredField("auditUserCache");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, AuditUsersCacheService.UserProp> originalCache =
        (ConcurrentHashMap<String, AuditUsersCacheService.UserProp>) cacheField.get(null);

        ConcurrentHashMap<String, AuditUsersCacheService.UserProp> testCache = new ConcurrentHashMap<>();
        cacheField.set(null, testCache);

        try
        {
            executeWithCredentials(List.of("INSERT INTO system_distributed.audit_users (role, account_type, filter_percent) VALUES ('shouldexist','SERVICE',0.01)",
                                           "INSERT INTO system_distributed.audit_users (role, account_type, filter_percent) VALUES ('ghost_role','SERVICE',1.0)"), CASS_USER, CASS_PW, null);

            AuditUsersCacheService.instance.insert("shouldexist", "SERVICE", 0.01);

            AuditUsersCacheService.instance.refresh();
            assertTrue(testCache.containsKey("shouldexist"));
            assertTrue(testCache.containsKey("ghost_role"));

            executeWithCredentials(List.of("DELETE FROM system_distributed.audit_users WHERE role = 'ghost_role'"), CASS_USER, CASS_PW, null);

            AuditUsersCacheService.instance.insert("ghost_role", "SERVICE", 1.0);
            AuditUsersCacheService.instance.insert("shouldexist", "SERVICE", 0.01);

            AuditUsersCacheService.instance.refresh();

            assertFalse(testCache.containsKey("ghost_role"));
            assertTrue(testCache.containsKey("shouldexist"));
            executeWithCredentials(List.of("DELETE FROM system_distributed.audit_users WHERE role = 'ghost_role'"), CASS_USER, CASS_PW, null);
            executeWithCredentials(List.of("DELETE FROM system_distributed.audit_users WHERE role = 'shouldexist'"), CASS_USER, CASS_PW, null);
        }
        finally
        {
            cacheField.set(null, originalCache);
        }
    }

    @Test
    public void testRefreshKeepsExistingRole() throws Exception
    {
        Field cacheField = AuditUsersCacheService.class.getDeclaredField("auditUserCache");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, AuditUsersCacheService.UserProp> originalCache =
        (ConcurrentHashMap<String, AuditUsersCacheService.UserProp>) cacheField.get(null);

        ConcurrentHashMap<String, AuditUsersCacheService.UserProp> testCache = new ConcurrentHashMap<>();
        cacheField.set(null, testCache);

        try
        {
            executeWithCredentials(List.of("INSERT INTO system_distributed.audit_users (role, account_type, filter_percent) VALUES ('shouldexist','SERVICE',0.01)"), CASS_USER, CASS_PW, null);

            AuditUsersCacheService.instance.refresh();

            assertTrue(testCache.containsKey("shouldexist"));
            assertTrue(testCache.containsKey("cassandra"));
        }
        finally
        {
            cacheField.set(null, originalCache);
            executeWithCredentials(List.of("DELETE FROM system_distributed.audit_users WHERE role = 'shouldexist'"), CASS_USER, CASS_PW, null);
            AuditUsersCacheService.instance.refresh();
        }
    }

    /**
     * Helper methods
     */

    private static void executeWithCredentials(List<String> queries, String username, String password,
                                               AuditLogEntryType expectedType)
    {
        executeWithCredentials(queries, username, password, expectedType, false);
    }

    // set clearPreparedStatementCache to true to get execute message failure because prepared statement is cleared on server end
    private static void executeWithCredentials(List<String> queries, String username, String password,
                                               AuditLogEntryType expectedType, boolean clearPreparedStatementCache)
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
                {
                    PreparedStatement preparedStatement = session.prepare(query);
                    if (clearPreparedStatementCache)
                    {
                        QueryProcessor.clearPreparedStatements(true);
                    }
                    session.execute(preparedStatement.bind());
                }

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
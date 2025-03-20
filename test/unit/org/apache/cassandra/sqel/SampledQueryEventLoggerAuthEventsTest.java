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

package org.apache.cassandra.sqel;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

// reuse the audit log entry - it is a well engineered class with all values we wish to capture
import org.apache.cassandra.audit.AuditLogEntry;
import org.apache.cassandra.audit.AuditLogEntryType;

import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.PasswordObfuscator;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.commons.lang3.StringUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SampledQueryEventLoggerAuthEventsTest extends SampledQueryEventLoggerTester {
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
        // Stop any running Cassandra instance before we start
        if (embedded != null) {
            embedded.stop();
        }

        requireAuthentication();

        embedded = ServerTestUtils.startEmbeddedCassandraService();

        // Execute setup queries
        List<String> setupQueries = Arrays.asList(
            genCreateRoleCql(TEST_USER, TEST_PW, true, false, false),
            genCreateRoleCql("testuser_nologin", TEST_PW, false, false, false),
            "CREATE KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
            "CREATE TABLE testks.table1 (key text PRIMARY KEY, col1 int, col2 int)"
        );

        executeWithCredentials(setupQueries, CASS_USER, CASS_PW, false);
        
        SampledQueryEventLoggerTester.setup();
    }

    @AfterClass
    public static void teardown() 
    {
        if (embedded != null) {
            embedded.stop();
        }
    }

    @Before
    public void beforeTest() 
    {
        SampledQueryEventLoggerOptions options = new SampledQueryEventLoggerOptions.Builder()
            .withEnabled(true)
            .withAuthSuccessSampleRate(1.0)
            .withAuthFailureSampleRate(1.0)
            .build();
        DatabaseDescriptor.setSampledQueryEventLoggingOptions(options);
        super.enableSampledQueryEventLoggerOptions(options);
        super.beforeTest();
    }

    @After
    public void afterTest() 
    {
        super.afterTest();
    }

    @Test
    public void testLoginEvents() throws Throwable 
    {
        // Login with incorrect user
        executeWithCredentials(Collections.emptyList(), "incorrectuser", TEST_PW, true);
        List<LogEntry> logEntries = assertNEntries(1);
        logEntries.get(logEntries.size() - 1).getOperation().contains("Provided user name incorrectuser and/or password");

        // Login with incorrect password
        executeWithCredentials(Collections.emptyList(), TEST_USER, "incorrectpassword", true);
        logEntries = assertNEntries(2);
        logEntries.get(logEntries.size() - 1).getOperation().contains("Provided username testuser and/or password");

        // Login with correct user who should not be able to login
        executeWithCredentials(Collections.emptyList(), "testuser_nologin", TEST_PW, true);
        logEntries = assertNEntries(3);
        logEntries.get(logEntries.size() - 1).getOperation().contains("testuser_nologin is not permitted to log in");

        // Login with correct credentials
        executeWithCredentials(Collections.emptyList(), TEST_USER, TEST_PW, false);
        logEntries = assertNEntries(5);
        logEntries.get(3).equals(logEntries.get(4));
    }

    @Test
    public void testCreateRoleCQL() throws Throwable
    {
        withQueryLogs();
        String cql = genCreateRoleCql(TEST_ROLE, TEST_PW, true, false, false);
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, false);
        List<LogEntry> logEntries = toLogEntries();
        assertLogEntry(logEntries.get(logEntries.size() - 1),
                AuditLogEntryType.CREATE_ROLE.toString(),
                genCreateRoleCql(TEST_ROLE, TEST_PW, true, false, true),
                CASS_USER,
                CASS_PW);
    }

    @Test
    public void testCreateRoleWithHashedPasswordCQL() throws Throwable
    {
        withQueryLogs();
        String cql = "CREATE ROLE IF NOT EXISTS %s WITH HASHED PASSWORD = '%s' AND LOGIN = %s AND SUPERUSER = %s";
        executeWithCredentials(
            Arrays.asList(String.format(cql, TEST_ROLE, TEST_PW_HASH, true, false)), 
            CASS_USER, CASS_PW, false);
        List<LogEntry> logEntries = toLogEntries();
        assertLogEntry(logEntries.get(logEntries.size() - 1),
                AuditLogEntryType.CREATE_ROLE.toString(),
                String.format(cql, TEST_ROLE, PasswordObfuscator.OBFUSCATION_TOKEN, true, false),
                CASS_USER,
                TEST_PW_HASH);
    }

    @Test
    public void testAlterRoleCQL() throws Throwable
    {
        createTestRole(CASS_USER, CASS_PW, TEST_ROLE, TEST_PW);
        withQueryLogs();
        String cql =  "ALTER ROLE " + TEST_ROLE + " WITH PASSWORD = 'foo_bar'";
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, false);
        
        List<LogEntry> logEntries = toLogEntries();
        assertLogEntry(logEntries.get(logEntries.size() - 1),
                AuditLogEntryType.ALTER_ROLE.toString(),
                "ALTER ROLE " + TEST_ROLE + " WITH PASSWORD = '" + PasswordObfuscator.OBFUSCATION_TOKEN + "'",
                CASS_USER,
                "foo_bar");
    }


    @Test
    public void testAlterRoleWithHashedPasswordCQL() throws Throwable 
    {
        createTestRole(CASS_USER, CASS_PW, TEST_ROLE, TEST_PW);
        withQueryLogs();
        String cql =  "ALTER ROLE " + TEST_ROLE + " WITH HASHED PASSWORD = '" + TEST_PW_HASH + "'";
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, false);
        
        List<LogEntry> logEntries = toLogEntries();
        assertLogEntry(logEntries.get(logEntries.size() - 1),
                AuditLogEntryType.ALTER_ROLE.toString(),
                "ALTER ROLE " + TEST_ROLE + " WITH HASHED PASSWORD = '" + PasswordObfuscator.OBFUSCATION_TOKEN + "'",
                CASS_USER,
                TEST_PW_HASH);
    }

    @Test
    public void testDropRoleCQL() throws Throwable
    {
        createTestRole(CASS_USER, CASS_PW, TEST_ROLE, TEST_PW);
        withQueryLogs();
        String cql =  "DROP ROLE " + TEST_ROLE;
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, false);
        
        List<LogEntry> logEntries = toLogEntries();
        assertLogEntry(logEntries.get(logEntries.size() - 1),
                AuditLogEntryType.DROP_ROLE.toString(),
                "DROP ROLE " + TEST_ROLE,
                CASS_USER,
                "");
    }

    @Test
    public void testListCQL() throws Throwable
    {
        createTestRole(CASS_USER, CASS_PW, TEST_ROLE, TEST_PW);
        withQueryLogs();
        String cql =  "LIST ALL";
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, false);
        
        List<LogEntry> logEntries = toLogEntries();
        assertLogEntry(logEntries.get(logEntries.size() - 1),
                AuditLogEntryType.LIST_PERMISSIONS.toString(),
                cql,
                CASS_USER,
                "");
    }

    @Test
    public void testGrantCQL() throws Throwable
    {
        createTestRole(CASS_USER, CASS_PW, TEST_ROLE, TEST_PW);
        withQueryLogs();
        String cql =  "GRANT SELECT ON ALL KEYSPACES TO " + TEST_ROLE;
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, false);
        
        List<LogEntry> logEntries = toLogEntries();
        assertLogEntry(logEntries.get(logEntries.size() - 1),
                AuditLogEntryType.GRANT.toString(),
                cql,
                CASS_USER,
                "");
    }

    @Test
    public void testRevokeCQL() throws Throwable
    {
        createTestRole(CASS_USER, CASS_PW, TEST_ROLE, TEST_PW);
        withQueryLogs();
        String cql =  "REVOKE ALTER ON ALL KEYSPACES FROM " + TEST_ROLE;
        executeWithCredentials(Arrays.asList(cql), CASS_USER, CASS_PW, false);
        List<LogEntry> logEntries = toLogEntries();
        assertLogEntry(logEntries.get(logEntries.size() - 1),
                AuditLogEntryType.REVOKE.toString(),
                cql,
                CASS_USER,
                "");
    }

    @Test
    public void testUnauthorizedAttemptCQL() throws Throwable
    {
        createTestRole(CASS_USER, CASS_PW, TEST_ROLE, TEST_PW);
        withQueryLogs();
        String cql =  "ALTER ROLE " + TEST_ROLE + " WITH superuser = true";
        executeWithCredentials(Arrays.asList(cql), TEST_USER, TEST_PW, false);
        List<LogEntry> logEntries = toLogEntries();
        assertLogEntry(logEntries.get(logEntries.size() - 1),
                AuditLogEntryType.UNAUTHORIZED_ATTEMPT.toString(),
                cql,
                TEST_USER,
                "");
    }

    @Test
    public void testBackwardsCompatibleCQL() throws Throwable
    {
        withQueryLogs();
        String user = TEST_ROLE + "user";
        String cql = "CREATE USER %s WITH PASSWORD '%s'";
        executeWithCredentials(Arrays.asList(String.format(cql, user, TEST_PW)), CASS_USER, CASS_PW, false);
        List<LogEntry> logEntries = toLogEntries();
        assertLogEntry(logEntries.get(logEntries.size() - 1),
                AuditLogEntryType.CREATE_ROLE.toString(),
                String.format(cql, user, PasswordObfuscator.OBFUSCATION_TOKEN),
                CASS_USER,
                TEST_PW);

        cql = "ALTER USER %s WITH PASSWORD '%s'";
        executeWithCredentials(Arrays.asList(String.format(cql, user, TEST_PW)), CASS_USER, CASS_PW, false);
        logEntries = toLogEntries();
        assertLogEntry(logEntries.get(logEntries.size() - 1),
                AuditLogEntryType.ALTER_ROLE.toString(),
                String.format(cql, user, PasswordObfuscator.OBFUSCATION_TOKEN),
                CASS_USER,
                TEST_PW);
    }

    // helper methods
    private void withQueryLogs(){
        SampledQueryEventLoggerOptions options = new SampledQueryEventLoggerOptions.Builder()
            .withEnabled(true)
            .withQuerySuccessSampleRate(1.0)
            .withQueryFailureSampleRate(1.0)
            .withBatchSuccessSampleRate(1.0)
            .withBatchFailureSampleRate(1.0)
            .withExecuteSuccessSampleRate(1.0)
            .withExecuteFailureSampleRate(1.0)
            .withPrepareSuccessSampleRate(1.0)
            .withPrepareFailureSampleRate(1.0)
            .withAuthSuccessSampleRate(1.0)
            .withAuthFailureSampleRate(1.0)
            .build();
        SampledQueryEventLogger.instance.update(options);

    }
}

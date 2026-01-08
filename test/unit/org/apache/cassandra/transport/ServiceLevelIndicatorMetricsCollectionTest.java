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

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PasswordObfuscator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.helpers.SubstituteLogger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ServiceLevelIndicatorMetricsCollectionTest
{
    private static Map<Level, Queue<Pair<String, Object[]>>> logged = new HashMap<>();

    public enum Level
    {
        INFO, WARN, ERROR
    }

    public static Logger mockLogger = new SubstituteLogger(null, null, true)
    {
        @Override
        public void info(String statement, Object... args)
        {
            logged.get(Level.INFO).offer(Pair.create(statement, args));
        }

        @Override
        public void warn(String statement, Object... args)
        {
            logged.get(Level.WARN).offer(Pair.create(statement, args));
        }

        @Override
        public void error(String statement, Object... args)
        {
            logged.get(Level.ERROR).offer(Pair.create(statement, args));
        }

        @Override
        public int hashCode()
        {
            return 42;
        }

        @Override
        public boolean equals(Object o)
        {
            return this == o;
        }
    };

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.clientInitialization();
        // Enable service level indicator error logging for testing
        DatabaseDescriptor.getRawConfig().service_level_indicator_error_log_enabled = true;
    }

    @Before
    public void setUp()
    {
        logged.put(Level.INFO, new ArrayDeque<>());
        logged.put(Level.WARN, new ArrayDeque<>());
        logged.put(Level.ERROR, new ArrayDeque<>());
        ServiceLevelIndicatorMetricsCollection.setLogger(mockLogger);
    }

    @After
    public void tearDown()
    {
        logged.clear();
    }

    @Test
    public void testPasswordObfuscationInCreateRole()
    {
        String query = "CREATE ROLE IF NOT EXISTS 'mySecretUser123' " +
                       "WITH PASSWORD='mySecretPassword123!' " +
                       "AND LOGIN=true";
        String expectedObfuscated = "CREATE ROLE IF NOT EXISTS 'mySecretUser123' " +
                                    "WITH PASSWORD *******";
        Exception ex = new InvalidRequestException("Test exception for testPasswordObfuscationInCreateRole");

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(ex, query);

        Pair<String, Object[]> loggedEntry = logged.get(Level.ERROR).poll();
        String loggedQuery = getLoggedQuery(loggedEntry);
        
        assertEquals("Logged query should be obfuscated", expectedObfuscated, loggedQuery);
    }

    @Test
    public void testPasswordObfuscationInCreateRoleWithPassword()
    {
        String query = "CREATE ROLE role1 WITH PASSWORD = 'mySecretPassword123!' AND LOGIN = true";
        String expectedObfuscated = "CREATE ROLE role1 WITH PASSWORD *******";
        Exception ex = new SyntaxException("Test syntax error for testPasswordObfuscationInCreateRoleWithPassword");

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(ex, query);

        Pair<String, Object[]> loggedEntry = logged.get(Level.ERROR).poll();
        String loggedQuery = getLoggedQuery(loggedEntry);
        
        assertEquals("Logged query should be obfuscated", expectedObfuscated, loggedQuery);
    }

    @Test
    public void testPasswordObfuscationInAlterRole()
    {
        String query = "ALTER ROLE role1 WITH PASSWORD = 'anotherSecretPass456$'";
        String expectedObfuscated = "ALTER ROLE role1 WITH PASSWORD *******";
        Exception ex = new ServerError("Test exception for testPasswordObfuscationInAlterRole");

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(ex, query);

        Pair<String, Object[]> loggedEntry = logged.get(Level.ERROR).poll();
        String loggedQuery = getLoggedQuery(loggedEntry);
        
        assertEquals("Logged query should be obfuscated", expectedObfuscated, loggedQuery);
    }

    @Test
    public void testPasswordObfuscationInCreateUser()
    {
        String query = "CREATE USER user1 WITH PASSWORD 'userPassword789#'";
        String expectedObfuscated = "CREATE USER user1 WITH PASSWORD *******";
        Exception ex = new ProtocolException("Test exception for testPasswordObfuscationInCreateUser");

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(ex, query);

        Pair<String, Object[]> loggedEntry = logged.get(Level.ERROR).poll();
        String loggedQuery = getLoggedQuery(loggedEntry);
        
        assertEquals("Logged query should be obfuscated", expectedObfuscated, loggedQuery);
    }

    @Test
    public void testNoObfuscationForQueriesWithoutPassword()
    {
        String query = "SELECT * FROM system.peers";
        Exception ex = new org.apache.cassandra.exceptions.ReadTimeoutException(
            org.apache.cassandra.db.ConsistencyLevel.ONE, 0, 1, false);

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(ex, query);

        Pair<String, Object[]> loggedEntry = logged.get(Level.ERROR).poll();
        String loggedQuery = getLoggedQuery(loggedEntry);
        
        assertEquals("Query without password should remain unchanged", query, loggedQuery);
    }

    @Test
    public void testNullQueryHandling()
    {
        Exception ex = new org.apache.cassandra.exceptions.WriteTimeoutException(
            org.apache.cassandra.db.WriteType.SIMPLE,
            org.apache.cassandra.db.ConsistencyLevel.ONE, 0, 1);

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(ex, null);

        Pair<String, Object[]> loggedEntry = logged.get(Level.ERROR).poll();
        String loggedQuery = getLoggedQuery(loggedEntry);
        
        assertEquals("Null query should be logged as 'null'", "null", loggedQuery);
    }

    @Test
    public void testPasswordObfuscationWithHashedPassword()
    {
        String query = "ALTER ROLE role1 WITH HASHED PASSWORD = '$2a$10$abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJK'";
        String expectedObfuscated = "ALTER ROLE role1 WITH HASHED PASSWORD *******";
        Exception ex = new org.apache.cassandra.exceptions.UnauthorizedException("Test exception for testPasswordObfuscationWithHashedPassword");

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(ex, query);

        Pair<String, Object[]> loggedEntry = logged.get(Level.ERROR).poll();
        String loggedQuery = getLoggedQuery(loggedEntry);
        
        assertEquals("Logged query should be obfuscated", expectedObfuscated, loggedQuery);
    }

    @Test
    public void testPasswordObfuscationWithMixedCase()
    {
        String query = "CREATE ROLE role1 WITH paSSwoRd = 'CaseSensitivePass!'";
        String expectedObfuscated = "CREATE ROLE role1 WITH paSSwoRd *******";
        Exception ex = new org.apache.cassandra.exceptions.AlreadyExistsException("keyspace", "table");

        ServiceLevelIndicatorMetricsCollection.collectMetricsAndLog(ex, query);

        Pair<String, Object[]> loggedEntry = logged.get(Level.ERROR).poll();
        String loggedQuery = getLoggedQuery(loggedEntry);
        
        assertEquals("Logged query should be obfuscated", expectedObfuscated, loggedQuery);
    }

    private String getLoggedQuery(Pair<String, Object[]> loggedEntry)
    {
        if (loggedEntry == null || loggedEntry.right == null || loggedEntry.right.length < 2)
        {
            return null;
        }
        // The query is the second argument (index 1) in the log statement
        Object queryArg = loggedEntry.right[1];
        return queryArg != null ? queryArg.toString() : "null";
    }
}


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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.ResultSet;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.audit.AuditLogEntry;
import org.apache.cassandra.audit.AuditLoggerTest;
import org.apache.cassandra.auth.AuthEvents;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryEvents;
import org.apache.cassandra.service.StorageService;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SampledQueryEventLoggerQueryEventsTest extends SampledQueryEventLoggerTester {
    @BeforeClass
    public static void setup() throws Exception {
        SampledQueryEventLoggerOptions options = new SampledQueryEventLoggerOptions();    
        DatabaseDescriptor.setSampledQueryEventLoggingOptions(options);
        requireNetwork();
        SampledQueryEventLoggerTester.setup();
    }

    @Before
    public void beforeTest() {
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
            .build();
        DatabaseDescriptor.setSampledQueryEventLoggingOptions(options);
        super.enableSampledQueryEventLoggerOptions(options);
        super.beforeTest();
    }

    @After
    public void afterTest() {
        super.afterTest();
    }
    
    @Test
    public void testInitialize() throws IOException
    {    
        StorageService.instance.disableSampledQueryEventLogger();
        assertEquals(0, QueryEvents.instance.listenerCount());
        assertEquals(0, AuthEvents.instance.listenerCount());
        SampledQueryEventLogger.instance.initialize();
        assertEquals(1, QueryEvents.instance.listenerCount());
        assertEquals(1, AuthEvents.instance.listenerCount());
    }

    @Test
    public void testEnableDisable() throws IOException
    {
        StorageService.instance.disableSampledQueryEventLogger();
        assertEquals(0, QueryEvents.instance.listenerCount());
        assertEquals(0, AuthEvents.instance.listenerCount());
        StorageService.instance.enableSampledQueryEventLogger(1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0);
        assertEquals(1, QueryEvents.instance.listenerCount());
        assertEquals(1, AuthEvents.instance.listenerCount());
        StorageService.instance.disableSampledQueryEventLogger();
        assertEquals(0, QueryEvents.instance.listenerCount());
        assertEquals(0, AuthEvents.instance.listenerCount());
    }

    @Test
    public void testShouldNotLogWhenEqualToZero() 
    {
        // Assemble
        SampledQueryEventLoggerOptions options = new SampledQueryEventLoggerOptions.Builder()
            .withQuerySuccessSampleRate(0.0)
            .withQueryFailureSampleRate(0.0)
            .withBatchSuccessSampleRate(0.0)
            .withBatchFailureSampleRate(0.0)
            .withExecuteSuccessSampleRate(0.0)
            .withExecuteFailureSampleRate(0.0)
            .withPrepareSuccessSampleRate(0.0)
            .withPrepareFailureSampleRate(0.0)
            .build();
        SampledQueryEventLogger.instance.update(options);
        Session session = sessionNet();

        // Act
        ResultSet rs = session.execute("select * from system.local");

        // Assert
        // should only contain "Updating sampled query event logger"
        assertEquals(1, listAppender.list.size());
        assertEquals("Updating sampled query event logger", listAppender.list.get(0).getMessage());
    }

    @Test
    public void testShouldLogQuerySuccesses() throws Throwable
    {
        // Assemble
        Session session = createSession();
        String cql = "select * from " + KEYSPACE + '.' + currentTable();

        // Act
        ResultSet rs = session.execute(cql);
        
        // Assert
        List<LogEntry> logEntries = assertNEntries(1);
        assertLogEntryAreEqual("anonymous", "", "SELECT", "QUERY", currentTable(), cql, logEntries.get(0));
    }

    @Test
    public void testShouldLogQueryFailures() throws Throwable
    {
        // Assemble
        Session session = createSession();
        String cql = "INSERT INTO " + KEYSPACE + '.' + currentTable() + "1 (id, v1, v2) VALUES (1, 'failures, 'test')";
       
        // Act
        try 
        {
            ResultSet rs = session.execute(cql);
            Assert.fail("Expected an exception");
        }
        catch(SyntaxError e)
        {
            // expected
        }
        
        // Assert
        List<LogEntry> logEntries = assertNEntries(1);
        assertLogEntryAreEqual("anonymous", "", "REQUEST_FAILURE", "ERROR", null, null, logEntries.get(0));
        assertTrue(listAppender.list.get(0).getMessage().contains(cql));
    }

    @Test
    public void testShouldLogBatchSuccesses() throws Throwable
    {
        // Assemble
        Session session = createSession();
        BatchStatement batchStatement = new BatchStatement();
        String cql = "INSERT INTO " + KEYSPACE + "." + currentTable() + " (id, v1, v2) VALUES (?, ?, ?)";
        Statement stmt = new SimpleStatement(cql, 1, "Apache", "Cassandra");
        batchStatement.add(stmt);

        // Act
        ResultSet rs = session.execute(batchStatement);

        // Assert
        List<LogEntry> logEntries = assertNEntries(2);
        assertLogEntryAreEqual("anonymous", "", "BATCH", "DML", null, null, logEntries.get(0));
        assertLogEntryAreEqual("anonymous", "", "UPDATE", "DML", currentTable(), cql, logEntries.get(1));
    }

    @Test
    public void testShouldLogBatchFailures() throws Throwable
    {
        // Assemble
        Session session = createSession();
        BatchStatement batchStatement = new BatchStatement();
        Statement stmt1 = new SimpleStatement("INSERT INTO " + KEYSPACE + "." + currentTable() + " (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        batchStatement.add(stmt1);
        Statement stmt2 = new SimpleStatement("INSERT INTO " + KEYSPACE + ".doesnotexist (id, v1, v2) VALUES (?, ?, ?)", 2, "Apache", "Cassandra");
        batchStatement.add(stmt2);

        // Act
        try
        {
            ResultSet rs = session.execute(batchStatement);
        }
        catch(InvalidQueryException e) 
        {
            // expected
        }

        // Assert
        List<LogEntry> logEntries = assertNEntries(1);
        assertLogEntryAreEqual("anonymous", "", "REQUEST_FAILURE", "ERROR", null, null, logEntries.get(0));
    }

    @Test
    public void testShouldLogExecuteSuccesses() throws Throwable
    {
        // Assemble
        Session session = createSession();
        String cql = "INSERT INTO " + KEYSPACE + "." + currentTable() + " (id, v1, v2) VALUES (?, ?, ?)";
        Statement stmt = new SimpleStatement(cql, 1, "Apache", "Cassandra");

        // Act
        ResultSet rs = session.execute(stmt);

        // Assert
        List<LogEntry> logEntries = assertNEntries(1);
        assertLogEntryAreEqual("anonymous", "", "UPDATE", "DML", currentTable(), cql, logEntries.get(0));
    }

    @Test
    public void testShouldLogExecuteFailures() throws Throwable
    {
        // Assemble
        Session session = createSession();
        String cql = "INSERT INOT " + KEYSPACE + "." + currentTable() + " (id, v1, v2) VALUES (?, ?, ?)";
        Statement stmt = new SimpleStatement(cql, 1, "Apache", "Cassandra");

        // Act
        try
        {
            ResultSet rs = session.execute(stmt);
        }
        catch (SyntaxError e)
        {
            // expected
        }

        // Assert
        List<LogEntry> logEntries = assertNEntries(1);
        assertLogEntryAreEqual("anonymous", "", "REQUEST_FAILURE", "ERROR", currentTable(), cql, logEntries.get(0));
        assertTrue(listAppender.list.get(0).getMessage().contains(cql));
    }

    @Test
    public void testShouldLogPrepareSuccesses() throws Throwable
    {
        // Assemble
        Session session = createSession();
        String cql = "INSERT INTO " + KEYSPACE + '.' + currentTable() + "(id, v1, v2) VALUES (?,?,?)";
        PreparedStatement prep = session.prepare(cql);

        // Act
        ResultSet rs = session.execute(prep.bind(1, "insert_audit", "test"));
            
        // Assert
        List<LogEntry> logEntries = assertNEntries(2);
        assertLogEntryAreEqual("anonymous", "", "PREPARE_STATEMENT", "PREPARE", currentTable(), cql, logEntries.get(0));
        assertLogEntryAreEqual("anonymous", "", "UPDATE", "DML", currentTable(), cql, logEntries.get(1));
    }

    @Test
    public void testShouldLogPrepareFailures() throws Throwable
    {
        // Assemble
        Session session = createSession();
        String cql = "INSERT INTO " + KEYSPACE + '.' + currentTable() + "(id, v1, v2) VALES (?,?,?)";

        // Act
        try
        {
            PreparedStatement prep = session.prepare(cql);
            ResultSet rs = session.execute(prep.bind(1, "insert_audit", "test"));
            Assert.fail("should not succeed");
        }
        catch (SyntaxError e)
        {
            // expected
        }

        // Assert
        List<LogEntry> logEntries = assertNEntries(1);
        assertLogEntryAreEqual("anonymous", "", "REQUEST_FAILURE", "ERROR", null, null, logEntries.get(0));
    }
}
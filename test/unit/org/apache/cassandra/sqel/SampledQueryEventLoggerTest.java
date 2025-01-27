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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

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
import java.util.Optional;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.regex.*;

import org.apache.cassandra.audit.AuditLogEntry;
import org.apache.cassandra.audit.AuditLoggerTest;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryEvents;
import org.apache.cassandra.service.StorageService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;
import org.junit.After;
import org.junit.Assert;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SampledQueryEventLoggerTest extends CQLTester
{
    private ListAppender<ILoggingEvent> listAppender;
    private final LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
    @BeforeClass
    public static void setup()
    {
        SampledQueryEventLoggerOptions options = new SampledQueryEventLoggerOptions();    
        DatabaseDescriptor.setSampledQueryEventLoggingOptions(options);
        requireNetwork();
    }

    @Before
    public void beforeTest()
    {
        // SampledQueryEventLoggerOptions options = new SampledQueryEventLoggerOptions();
        SampledQueryEventLoggerOptions options = new SampledQueryEventLoggerOptions.Builder()
            .withQuerySuccessSampleRate(1.0)
            .withQueryFailureSampleRate(1.0)
            .withBatchSuccessSampleRate(1.0)
            .withBatchFailureSampleRate(1.0)
            .withExecuteSuccessSampleRate(1.0)
            .withExecuteFailureSampleRate(1.0)
            .withPrepareSuccessSampleRate(1.0)
            .withPrepareFailureSampleRate(1.0)
            .build();
        enableSampledQueryEventLoggerOptions(options);
        lc.reset();
        Logger logger = (Logger) LoggerFactory.getLogger(SampledQueryEventLogger.class);
        listAppender = new ListAppender<>();
        logger.addAppender(listAppender);
        listAppender.start();
    }

    @After
    public void afterTest()
    {
        disableSampleQueryEventLoggerOptions();
        Logger logger = (Logger) LoggerFactory.getLogger(SampledQueryEventLogger.class);
        logger.detachAndStopAllAppenders();
        listAppender.stop();
        lc.reset();
    }

    private void enableSampledQueryEventLoggerOptions(SampledQueryEventLoggerOptions options)
    {
        SampledQueryEventLogger.instance.enable(options);
    }

    private void disableSampleQueryEventLoggerOptions()
    {
        SampledQueryEventLogger.instance.stop();
    }

    @Test
    public void testEnableDisable() throws IOException
    {
        StorageService.instance.disableSimpleQueryEventLogger();
        assertEquals(0, QueryEvents.instance.listenerCount());
        StorageService.instance.enableSimpleQueryEventLogger(1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0);
        assertEquals(1, QueryEvents.instance.listenerCount());
        StorageService.instance.disableSimpleQueryEventLogger();
        assertEquals(0, QueryEvents.instance.listenerCount());
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
        assertLogEntryAreEqual("SELECT", "QUERY", currentTable(), cql, logEntries.get(0));
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
        assertLogEntryAreEqual("REQUEST_FAILURE", "ERROR", null, null, logEntries.get(0));
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
        assertLogEntryAreEqual("BATCH", "DML", null, null, logEntries.get(0));
        assertLogEntryAreEqual("UPDATE", "DML", currentTable(), cql, logEntries.get(1));
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
        assertLogEntryAreEqual("REQUEST_FAILURE", "ERROR", null, null, logEntries.get(0));
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
        assertLogEntryAreEqual("UPDATE", "DML", currentTable(), cql, logEntries.get(0));
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
        assertLogEntryAreEqual("REQUEST_FAILURE", "ERROR", currentTable(), cql, logEntries.get(0));
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
        assertLogEntryAreEqual("PREPARE_STATEMENT", "PREPARE", currentTable(), cql, logEntries.get(0));
        assertLogEntryAreEqual("UPDATE", "DML", currentTable(), cql, logEntries.get(1));
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
        assertLogEntryAreEqual("REQUEST_FAILURE", "ERROR", null, null, logEntries.get(0));
    }


    // Helper functions:
    private void printLoggedEvents(List<ILoggingEvent> loggingEvents) {
        System.out.println("Captured Log Entries:");
        for (ILoggingEvent event : loggingEvents) {
            System.out.println("Log Level: " + event.getLevel() + ", Message: " + event.getMessage());
        }
    }
    
    private Session createSession() throws Throwable 
    {
        createKeyspace("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        return sessionNet();
    }

    private List<LogEntry> assertNEntries(int n) throws Throwable 
    {
        String logPattern = 
            "user:(?<user>\\S+)\\|userType:(?<userType>\\S*)\\|host:(?<host>[^|]+)\\|source:(?<source>[^|]+)\\|port:(?<port>\\d+)\\|timestamp:(?<timestamp>\\d+)\\|type:(?<type>\\S+)\\|category:(?<category>[^|]+)(\\|batch:(?<batch>[^|]+))?(\\|ks:(cql_test_keyspace))?(\\|scope:(?<scope>[^|]+))?\\|operation:(?<operation>.+)";

        List<LogEntry> matchingEntries = listAppender.list.stream()
            .map(event -> event.getMessage())
            .map(message -> parseLogMessage(message, logPattern))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

        assertEquals(n, matchingEntries.size());

        return matchingEntries;
    }

    private void assertLogEntryAreEqual(String type, String category, String scope, String cql, LogEntry logEntry) throws Throwable 
    {
        LogEntry expected = new LogEntry("anonymous", "", type, category, "cql_test_keyspace", scope, cql);
        assertTrue(logEntry.equals(expected));
    }

    private Optional<LogEntry> parseLogMessage(String message, String pattern) {
        Pattern regexPattern = Pattern.compile(pattern);
        Matcher matcher = regexPattern.matcher(message);
        
        if (matcher.find()) {
            LogEntry logEntry = new LogEntry(
                matcher.group("user"),
                matcher.group("userType"),
                matcher.group("host"),
                matcher.group("source"),
                Integer.parseInt(matcher.group("port")),
                Long.parseLong(matcher.group("timestamp")),
                matcher.group("type"),
                matcher.group("category"),
                "cql_test_keyspace",
                matcher.group("scope"),
                matcher.group("operation")
            );
            return Optional.of(logEntry);
        }
        return Optional.empty();
    }

    private static class LogEntry {
        private String user;
        private String userType;
        private String host;
        private String source;
        private int port;
        private long timestamp;
        private String type;
        private String category;
        private String ks;
        private String scope;
        private String operation;

        public LogEntry(String user, String userType, String type, String category, String ks, String scope, String operation) {
            this.user = user;
            this.userType = userType;
            this.type = type;
            this.category = category;
            this.ks = ks;
            this.scope = scope;
            this.operation = operation;
        }

        public LogEntry(String user, String userType, String host, String source, int port, long timestamp,
                        String type, String category, String ks, String scope, String operation) {
            this.user = user;
            this.userType = userType;
            this.host = host;
            this.source = source;
            this.port = port;
            this.timestamp = timestamp;
            this.type = type;
            this.category = category;
            this.ks = ks;
            this.scope = scope;
            this.operation = operation;
        }

        // Getters and Setters
        public String getUser() { return user; }
        public String getUserType() { return userType; }
        public String getHost() { return host; }
        public String getSource() { return source; }
        public int getPort() { return port; }
        public long getTimestamp() { return timestamp; }
        public String getType() { return type; }
        public String getCategory() { return category; }
        public String getKs() { return ks; }
        public String getScope() { return scope; }
        public String getOperation() { return operation; }

        // Override equals and hashCode for comparison in AssertJ
        @Override
        // ignore //port, timestamp, host, and source fields
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            LogEntry logEntry = (LogEntry) obj;
            return user.equals(logEntry.user) &&
                   userType.equals(logEntry.userType) &&
                   type.equals(logEntry.type) &&
                   category.equals(logEntry.category) &&
                   (ks == null || ks.equals(logEntry.ks)) &&
                   (scope == null || scope.equals(logEntry.scope)) &&
                   (operation.contains("BatchId") || 
                    operation.contains("; line ") || 
                    operation.contains("BATCH of ") || 
                    operation.contains(" does not exist") ||
                    operation.equals(logEntry.operation));
        }

        @Override
        public int hashCode() {
            return Objects.hash(user, userType, host, source, port, timestamp, type, category, ks, scope, operation);
        }

        @Override
        public String toString() {
            return "LogEntry{" +
                "user='" + user + '\'' +
                ", userType='" + userType + '\'' +
                ", host='" + host + '\'' +
                ", source='" + source + '\'' +
                ", port=" + port +
                ", timestamp=" + timestamp +
                ", type='" + type + '\'' +
                ", category='" + category + '\'' +
                ", ks='" + ks + '\'' +
                ", scope='" + scope + '\'' +
                ", operation='" + operation + '\'' +
                '}';
        }
    }
}
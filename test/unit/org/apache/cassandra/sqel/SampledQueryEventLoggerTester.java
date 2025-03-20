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

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import com.datastax.driver.core.Session;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.PasswordObfuscator;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.CoreMatchers;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNotEquals;

public class SampledQueryEventLoggerTester extends CQLTester {
    protected ListAppender<ILoggingEvent> listAppender;
    protected final LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
    private String logPattern = 
            "user:(?<user>\\S+)\\|userType:(?<userType>\\S*)\\|host:(?<host>[^|]+)\\|source:(?<source>[^|]+)\\|port:(?<port>\\d+)\\|timestamp:(?<timestamp>\\d+)\\|type:(?<type>\\S+)\\|category:(?<category>[^|]+)(\\|batch:(?<batch>[^|]+))?(\\|ks:(cql_test_keyspace|data))?(\\|scope:(?<scope>[^|]+))?\\|operation:(?<operation>.+)";

    public static void setup() throws Exception
    {
    }

    public void beforeTest()
    {
        lc.reset();
        Logger logger = (Logger) LoggerFactory.getLogger(SampledQueryEventLogger.class);
        listAppender = new ListAppender<>();
        logger.addAppender(listAppender);
        listAppender.start();
    }

    public void afterTest()
    {
        disableSampleQueryEventLoggerOptions();
        Logger logger = (Logger) LoggerFactory.getLogger(SampledQueryEventLogger.class);
        logger.detachAndStopAllAppenders();
        listAppender.stop();
        lc.reset();
    }

    protected void enableSampledQueryEventLoggerOptions(SampledQueryEventLoggerOptions options)
    {
        SampledQueryEventLogger.instance.enable(options);
    }

    protected void disableSampleQueryEventLoggerOptions()
    {
        SampledQueryEventLogger.instance.stop();
    }

    protected void printLoggedEvents(List<ILoggingEvent> loggingEvents) {
        System.out.println("Captured Log Entries:");
        for (ILoggingEvent event : loggingEvents) {
            System.out.println("Log Level: " + event.getLevel() + ", Message: " + event.getMessage());
        }
    }
    
    protected Session createSession() throws Throwable 
    {
        createKeyspace("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        return sessionNet();
    }

    protected List<LogEntry> toLogEntries() {
        List<LogEntry> matchingEntries = listAppender.list.stream()
            .map(event -> event.getMessage())
            .map(message -> parseLogMessage(message, logPattern))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

        return matchingEntries;
    }

    protected List<LogEntry> assertNEntries(int n) throws Throwable 
    {
        List<LogEntry> matchingEntries = listAppender.list.stream()
            .map(event -> event.getMessage())
            .map(message -> parseLogMessage(message, logPattern))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

        assertEquals(n, matchingEntries.size());

        return matchingEntries;
    }

    protected void assertLogEntryAreEqual(String user, String userType, String type, String category, String scope, String cql, LogEntry logEntry) throws Throwable 
    {
        LogEntry expected = new LogEntry(user, userType, type, category, "cql_test_keyspace", scope, cql);
        assertTrue(logEntry.equals(expected));
    }

    protected Optional<LogEntry> parseLogMessage(String message, String pattern) {
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

    protected static class LogEntry {
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
        // ignore port, timestamp, host, and source fields
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            LogEntry logEntry = (LogEntry) obj;
            return (StringUtils.isEmpty(user) ||
                    user.equals("null") ||
                    user.equals(logEntry.user)) &&
                   userType.equals(logEntry.userType) &&
                   type.equals(logEntry.type) &&
                   category.equals(logEntry.category) &&
                   (ks == null || ks.equals(logEntry.ks)) &&
                   (scope == null || scope.equals(logEntry.scope)) &&
                   (operation.contains("BatchId") || 
                    operation.contains("; line ") || 
                    operation.contains("BATCH of ") || 
                    operation.contains(" does not exist") ||
                    operation.contains("LOGIN FAILURE;") || 
                    operation.contains("LOGIN SUCCESS") ||
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

    protected static void executeWithCredentials(List<String> queries, String username, String password, Boolean shouldFail) 
    {
        Boolean authFailed = false;
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

        assertEquals(shouldFail, authFailed);
    }

    protected static String genCreateRoleCql(String role, String password, boolean login, boolean superUser, boolean isPasswordObfuscated)
    {
        return String.format("CREATE ROLE IF NOT EXISTS %s WITH PASSWORD = '%s' AND LOGIN = %s AND SUPERUSER = %s",
                             role,
                             isPasswordObfuscated ? PasswordObfuscator.OBFUSCATION_TOKEN : password,
                             login,
                             superUser);
    }

    protected void createTestRole(String cassUser, String cassPasswrod, String testRole, String testPassword) throws Throwable
    {
        String createTestRoleCQL = genCreateRoleCql(testRole, testPassword, true, false, false);
        executeWithCredentials(Arrays.asList(createTestRoleCQL), cassUser, cassPasswrod, false);
        List<LogEntry> logEntries = assertNEntries(2);
        assertLogEntryAreEqual("cassandra", "", "LOGIN_SUCCESS", "AUTH", null, null, logEntries.get(0));
        assertLogEntryAreEqual("cassandra", "", "LOGIN_SUCCESS", "AUTH", null, null, logEntries.get(1));
        assertTrue(logEntries.get(0).equals(logEntries.get(1)));
    }

    protected static void assertUser(LogEntry logEntry, String username)
    {
        if (logEntry.getType() != "LOGIN_ERROR")
        {
            assertEquals(username, logEntry.getUser());
        }
    }

    protected static void assertLogEntry(LogEntry logEntry, String type, String cql, String username, String password)
    {
        assertUser(logEntry, username);
        assertNotEquals(0, logEntry.getTimestamp());
        assertEquals(type, logEntry.getType());
        if (null != cql && !cql.isEmpty())
        {
            assertThat(logEntry.getOperation(), containsString(cql));
            if (!password.isEmpty())
                assertThat(logEntry.getOperation(), CoreMatchers.not(containsString(password)));
        }
    }
}
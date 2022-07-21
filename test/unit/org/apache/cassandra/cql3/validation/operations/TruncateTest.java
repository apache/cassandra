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
package org.apache.cassandra.cql3.validation.operations;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.TruncateException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.config.CassandraRelevantProperties.TRUNCATE_STATEMENT_PROVIDER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TruncateTest extends CQLTester
{
    public static final ByteBuffer[] VALUES = new ByteBuffer[0];
    public static boolean testTruncateProvider = false;

    @BeforeClass
    public static void setup()
    {
        TRUNCATE_STATEMENT_PROVIDER.setString(TestTruncateStatementProvider.class.getName());
    }

    @AfterClass
    public static void teardown()
    {
        System.clearProperty(TRUNCATE_STATEMENT_PROVIDER.getKey());        
    }
    
    @After
    public void afterTest()
    {
        testTruncateProvider = false;
        TestTruncateStatementProvider.testTruncateStatement = null;
    }

    @Test
    public void testTruncate() throws Throwable
    {
        for (String table : new String[] { "", "TABLE" })
        {
            createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a, b))");

            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);

            flush();

            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 0, 2);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 3);

            assertRows(execute("SELECT * FROM %s"), row(1, 0, 2), row(1, 1, 3), row(0, 0, 0), row(0, 1, 1));

            execute("TRUNCATE " + table + " %s");

            assertEmpty(execute("SELECT * FROM %s"));
        }
    }

    @Test
    public void testRemoteTruncateStmt() throws Throwable
    {
        testTruncateProvider = true;
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a, b))");
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);

        execute("TRUNCATE TABLE %s");
        assertTrue(TestTruncateStatementProvider.testTruncateStatement.executeLocallyInvoked);
    }

    @Test
    public void testTruncateUnknownTable() throws Throwable
    {
        testTruncateProvider = true;
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a, b))");
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);

        String query = "TRUNCATE TABLE doesnotexist";
        try
        {
            // Check TruncateStatement.exceuteLocally path
            execute(query);
            fail("Expected TruncationException");
        }
        catch (TruncateException e)
        {
            assertEquals("Error during truncate: Unknown keyspace/table system.doesnotexist", e.getMessage());
            assertTrue(TestTruncateStatementProvider.testTruncateStatement.executeLocallyInvoked);
        }

        try
        {
            // Check TruncateStatement.exceute path
            TestTruncateStatementProvider.testTruncateStatement.execute(QueryState.forInternalCalls(), QueryOptions.DEFAULT, 0L);
            fail("Expected TruncationException");
        }
        catch (TruncateException e)
        {
            assertEquals("Error during truncate: Unknown keyspace/table system.doesnotexist", e.getMessage());
            assertTrue(TestTruncateStatementProvider.testTruncateStatement.executeInvoked);
        }
    }

    @Test
    public void testTruncateView() throws Throwable
    {
        testTruncateProvider = true;
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY(a, b))");
        String qualifiedViewName = KEYSPACE + "." + createViewName();
        execute("CREATE MATERIALIZED VIEW " + qualifiedViewName + " AS SELECT * "
                + "FROM %s WHERE a IS NOT NULL and b IS NOT NULL "
                + "PRIMARY KEY (a, b)");
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);

        try
        {
            // Check TruncateStatement.exceuteLocally path
            execute("TRUNCATE TABLE " + qualifiedViewName);
            fail("Expected TruncationException");
        }
        catch (TruncateException e)
        {
            assertEquals("Error during truncate: Cannot TRUNCATE materialized view directly; must truncate base table instead", e.getMessage());
            assertTrue(TestTruncateStatementProvider.testTruncateStatement.executeLocallyInvoked);
        }

        try
        {
            // Check TruncateStatement.exceute path
            TestTruncateStatementProvider.testTruncateStatement.execute(QueryState.forInternalCalls(), QueryOptions.DEFAULT, 0L);
            fail("Expected TruncationException");
        }
        catch (TruncateException e)
        {
            assertEquals("Error during truncate: Cannot TRUNCATE materialized view directly; must truncate base table instead", e.getMessage());
            assertTrue(TestTruncateStatementProvider.testTruncateStatement.executeInvoked);
        }
    }

    public static class TestTruncateStatementProvider implements TruncateStatement.TruncateStatementProvider
    {
        public static TestTruncateStatement testTruncateStatement;

        @Override
        public TruncateStatement createTruncateStatement(String queryString, QualifiedName name)
        {
            if (TruncateTest.testTruncateProvider)
            {
                testTruncateStatement = new TestTruncateStatement(queryString, name);
                return testTruncateStatement;
            }
            else
                return new TruncateStatement(queryString, name);
        }
    }
    
    public static class TestTruncateStatement extends TruncateStatement
    {
        public boolean executeInvoked = false;
        public boolean executeLocallyInvoked = false;
        
        public TestTruncateStatement(String queryString, QualifiedName name)
        {
            super(queryString, name);
        }

        public void validate(QueryState state) throws InvalidRequestException
        {
            // accept anything
        }

        public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws InvalidRequestException, TruncateException
        {
            executeInvoked = true;
            return super.execute(state, options, queryStartNanoTime);
        }

        public ResultMessage executeLocally(QueryState state, QueryOptions options)
        {
            executeLocallyInvoked = true;
            return super.executeLocally(state, options);
        }
    }
}

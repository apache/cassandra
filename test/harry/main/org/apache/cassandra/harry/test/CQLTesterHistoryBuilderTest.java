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

package org.apache.cassandra.harry.test;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.execution.CQLTesterVisitExecutor;
import org.apache.cassandra.harry.execution.CQLVisitExecutor;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.op.Visit;

public class CQLTesterHistoryBuilderTest extends HistoryBuilderTest
{
    private final Tester tester;

    public CQLTesterHistoryBuilderTest()
    {
        this.tester = new Tester();
    }

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @AfterClass
    public static void tearDownClass()
    {
        CQLTester.tearDownClass();
    }

    @Before
    public void beforeTest() throws Throwable
    {
        tester.beforeTest();
    }

    @After
    public void afterTest() throws Throwable
    {
        tester.afterTest();
    }

    @Override
    protected String keyspace()
    {
        return CQLTester.KEYSPACE;
    }

    @Override
    protected void createTable(String schema)
    {
        tester.createTable(schema);
    }

    @Override
    protected void flush(String keyspace, String table)
    {
        tester.flush(keyspace, table);
    }

    @Override
    public void replay(SchemaSpec schema, HistoryBuilder historyBuilder)
    {
        CQLVisitExecutor executor = create(schema, historyBuilder);
        for (Visit visit : historyBuilder)
            executor.execute(visit);
    }

    @Override
    public CQLVisitExecutor create(SchemaSpec schema, HistoryBuilder historyBuilder)
    {
        DataTracker.SequentialDataTracker tracker = new DataTracker.SequentialDataTracker();
        return new CQLTesterVisitExecutor(schema, historyBuilder.valueGenerators(), tracker,
                                          new QuiescentChecker(historyBuilder.valueGenerators(), tracker),
                                          statement -> {
                                              if (logger.isTraceEnabled())
                                                  logger.trace(statement.toString());
                                              return tester.execute(statement.cql(), statement.bindings());
                                          });
    }

    private static class Tester extends CQLTester
    {
        @Override
        public String createTable(String query)
        {
            return super.createTable(query);
        }

        @Override
        public UntypedResultSet execute(String query, Object... values)
        {
            return super.execute(query, values);
        }
    }
}

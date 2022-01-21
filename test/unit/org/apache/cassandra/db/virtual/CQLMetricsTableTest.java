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

package org.apache.cassandra.db.virtual;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.BeforeClass;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.metrics.CQLMetrics;

public class CQLMetricsTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    private void queryAndValidateMetrics(CQLMetrics expectedMetrics) throws Throwable
    {
        String getMetricsQuery = "SELECT * FROM " + KS_NAME + "." + CQLMetricsTable.TABLE_NAME;
        ResultSet vtsRows = executeNet(getMetricsQuery);

        // validate - number of columns
        assertEquals(6, vtsRows.getColumnDefinitions().size());

        AtomicInteger rowCount = new AtomicInteger(0);
        vtsRows.forEach(r -> {
            assertEquals(expectedMetrics.preparedStatementsCount.getValue(), r.getDouble(CQLMetricsTable.PREPARED_STATEMENTS_COUNT_COL), 0);
            assertEquals(expectedMetrics.preparedStatementsEvicted.getCount(), r.getDouble(CQLMetricsTable.PREPARED_STATEMENTS_EVICTED_COL), 0);
            assertEquals(expectedMetrics.preparedStatementsExecuted.getCount(), r.getDouble(CQLMetricsTable.PREPARED_STATEMENTS_EXECUTED_COL), 0);
            assertEquals(expectedMetrics.preparedStatementsRatio.getValue(), r.getDouble(CQLMetricsTable.PREPARED_STATEMENTS_RATIO_COL), 0.01);
            assertEquals(expectedMetrics.regularStatementsExecuted.getCount(), r.getDouble(CQLMetricsTable.REGULAR_STATEMENTS_EXECUTED_COL), 0);
            rowCount.getAndIncrement();
        });

        // validate - number of rows
        assertEquals(1, rowCount.get());
    }

    @Test
    public void testUsingPrepareStmts() throws Throwable
    {
        CQLMetricsTable table = new CQLMetricsTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));

        String ks = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String tbl = createTable(ks, "CREATE TABLE %s (id int PRIMARY KEY, cid int, val text)");
        Session session = sessionNet();

        // prepare statements
        String insertCQL = "INSERT INTO " + ks + "." + tbl + " (id, cid, val) VALUES (?, ?, ?)";
        PreparedStatement preparedInsert = session.prepare(insertCQL);

        String selectCQL = "Select * from " + ks + "." + tbl + " where id = ?";
        PreparedStatement preparedSelect = session.prepare(selectCQL);

        for (int i = 0; i < 10; i++)
        {
            // execute prepared statements
            session.execute(preparedInsert.bind(i, i, "value" + i));
            session.execute(preparedSelect.bind(i));
        }

        queryAndValidateMetrics(QueryProcessor.metrics);
    }

    @Test
    public void testUsingInjectedValues() throws Throwable
    {
        CQLMetrics cqlMetrics = new CQLMetrics();
        CQLMetricsTable table = new CQLMetricsTable(KS_NAME, cqlMetrics);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));

        // With initial injected values
        cqlMetrics.preparedStatementsExecuted.inc(50);
        cqlMetrics.regularStatementsExecuted.inc(100);
        cqlMetrics.preparedStatementsEvicted.inc(25);
        queryAndValidateMetrics(cqlMetrics);

        // Test again with updated values
        cqlMetrics.preparedStatementsExecuted.inc(150);
        cqlMetrics.regularStatementsExecuted.inc(200);
        cqlMetrics.preparedStatementsEvicted.inc(50);
        queryAndValidateMetrics(cqlMetrics);
    }
}
